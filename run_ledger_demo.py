#!/usr/bin/env python3
"""Runnable end-to-end demo of the scan-attribution ledger.

Scenario (the README handover):  A/B/C swap stations through a completed
work order whose steps are

    0: offline B   (WS-2 falls vacant)
    1: update  A   (WS-1/CH-1 -> WS-2/CH-9)
    2: update  C   (WS-3/CH-3 -> WS-8/CH-2)
    3: online  B   (offline -> WS-3/CH-1)

so workstation WS-2 is first vacated and then changes owner, and steps 2
and 3 take effect in the same second.  Operations backfills the actual
effective time of every step, builds the ledger once from the completed
receiver, then corrects mis-recorded times in place -- including one
correction that crashes after its writes but before COMMIT.

Everything is generated at runtime in a temporary directory;  scans are
checked one by one against an independent replay of the order.
"""
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

from handover import core, ledger  # noqa: E402

OLD_ROWS = [
    {"id": "A", "ws": "WS-1", "ch": "CH-1"},
    {"id": "B", "ws": "WS-2", "ch": "CH-2"},
    {"id": "C", "ws": "WS-3", "ch": "CH-3"},
]
TARGET_ROWS = [
    {"id": "A", "ws": "WS-2", "ch": "CH-9"},
    {"id": "B", "ws": "WS-3", "ch": "CH-1"},
    {"id": "C", "ws": "WS-8", "ch": "CH-2"},
]
OLD = {r["id"]: (r["ws"], r["ch"]) for r in OLD_ROWS}

BASE = int(datetime(2026, 11, 22, 12, 0, 0, tzinfo=timezone.utc).timestamp())
T0, T1, T2, T3 = BASE + 100, BASE + 200, BASE + 300, BASE + 300

# scan log: (id, workstation, offset seconds from BASE)
SCAN_ROWS = [
    ("S01", "WS-2", 50), ("S02", "WS-2", 99), ("S03", "WS-2", 100),
    ("S04", "WS-2", 150), ("S05", "WS-2", 199), ("S06", "WS-2", 200),
    ("S07", "WS-2", 225), ("S08", "WS-2", 249), ("S09", "WS-2", 250),
    ("S10", "WS-1", 199), ("S11", "WS-1", 200), ("S12", "WS-1", 249),
    ("S13", "WS-1", 250), ("S14", "WS-3", 299), ("S15", "WS-3", 300),
    ("S16", "WS-8", 299), ("S17", "WS-8", 300), ("S18", "WS-9", 210),
    ("S19", "WS-2", 500),
    ("S20", "WS-2", 86450),          # next UTC day
    ("S21", "WS-9", 90000),          # next UTC day, never-bound station
]
SCANS = [{"id": i, "ws": w, "ts": BASE + t} for i, w, t in SCAN_ROWS]

# the read SQL documented in README.md, used verbatim below
SQL_SCAN = ("SELECT scan_id, ws, ts, device, ch FROM scans "
            "ORDER BY ts, scan_id")
SQL_COUNTS = ("SELECT day, COALESCE(ch, '(unattributed)') AS channel, n "
              "FROM counts ORDER BY day, channel")
SQL_TIMES = "SELECT seq, id, op, ts FROM steps ORDER BY seq"
SQL_CORRECTIONS = "SELECT hash, summary FROM corrections ORDER BY rowid"


def cli(*args, expect_rc=None):
    p = subprocess.run([sys.executable, "-m", "handover", *args],
                       capture_output=True, text=True,
                       env={**os.environ, "PYTHONPATH": ROOT})
    if expect_rc is not None:
        assert p.returncode == expect_rc, \
            f"handover {' '.join(args)}: rc={p.returncode}\n{p.stdout}\n{p.stderr}"
    return p


def show(title):
    print(f"\n=== {title} " + "=" * max(0, 62 - len(title)))


def run_sql(db, sql):
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    rows = list(con.execute(sql))
    con.close()
    return rows


def print_attributions(db):
    for sid, ws, ts, dev, ch in run_sql(db, SQL_SCAN):
        who = f"{dev}/{ch}" if dev else "(unattributed)"
        print(f"  {sid}  {ws:5s} @{ts - BASE:>6d}  -> {who}")


def print_counts(db):
    for day, channel, n in run_sql(db, SQL_COUNTS):
        print(f"  {day}  {channel:>15s}  {n}")


def ref_attributions(steps, times):
    """Independent replay: attribute every scan from the raw inputs."""
    out = {}
    for s in SCANS:
        state = {w: (i, c) for i, (w, c) in OLD.items()}
        for step, t in zip(steps, times):
            if t > s["ts"]:
                break
            b, a = step["before"], step["after"]
            if b["state"] == "online":
                del state[b["ws"]]
            if a["state"] == "online":
                state[a["ws"]] = (step["id"], a["ch"])
        out[s["id"]] = state.get(s["ws"], (None, None))
    return out


def check_against_reference(db, steps, times, label):
    want = ref_attributions(steps, times)
    got = {r[0]: (r[3], r[4]) for r in run_sql(db, SQL_SCAN)}
    assert got == want, f"{label}: ledger disagrees with the replay"
    # counts derived independently from the raw input, per (day, channel)
    import collections
    cnt = collections.Counter()
    for s in SCANS:
        cnt[(ledger.day_of(s["ts"]), want[s["id"]][1])] += 1
    stored = {(d, None if c == "(unattributed)" else c): n
              for d, c, n in run_sql(db, SQL_COUNTS)}
    assert stored == dict(cnt), f"{label}: stored counts disagree"
    print(f"per-scan attribution and stored counts match an independent "
          f"replay of the order ({label})")


def main():
    tmp = tempfile.mkdtemp(prefix="ledger-demo-")
    old_p = os.path.join(tmp, "old.jsonl")
    tgt_p = os.path.join(tmp, "target.jsonl")
    off_p = os.path.join(tmp, "offline.json")
    order_p = os.path.join(tmp, "order.json")
    recv_p = os.path.join(tmp, "receiver.db")
    times_p = os.path.join(tmp, "times.json")
    scans_p = os.path.join(tmp, "scans.jsonl")
    db_p = os.path.join(tmp, "ledger.db")

    with open(old_p, "w") as f:
        f.write("\n".join(json.dumps(r) for r in OLD_ROWS) + "\n")
    with open(tgt_p, "w") as f:
        f.write("\n".join(json.dumps(r) for r in TARGET_ROWS) + "\n")
    with open(off_p, "w") as f:
        json.dump(["A", "B"], f)

    show("Setup: plan the order and run the receiver to completion")
    cli("plan", "--old", old_p, "--target", tgt_p, "--offline", off_p,
        "--out", order_p, expect_rc=0)
    cli("init", "--db", recv_p, "--table", old_p, expect_rc=0)
    cli("run", "--db", recv_p, "--order", order_p, expect_rc=0)
    order = core.load_order(order_p)
    for s in order["steps"]:
        print(f"  step {s['seq']}: {s['op']:7s} {s['id']}  "
              f"{s['before']} -> {s['after']}")

    show("Operations backfills the actual effective time of each step")
    times = [T0, T1, T2, T3]
    with open(times_p, "w") as f:
        json.dump({"format": "handover-step-times/1",
                   "order_hash": order["order_hash"],
                   "times": [{"seq": q, "ts": t} for q, t in
                             enumerate(times)]}, f, indent=1)
    for q, t in enumerate(times):
        print(f"  step {q}: +{t - BASE}s  (steps 2 and 3 share a second)")
    with open(scans_p, "w") as f:
        for s in SCANS:
            f.write(json.dumps(s) + "\n")

    show("Build the ledger once (receiver is read-only proof)")
    p = cli("ledger-build", "--db", db_p, "--snapshot", old_p,
            "--order", order_p, "--receiver", recv_p,
            "--times", times_p, "--scans", scans_p, expect_rc=0)
    print(p.stdout.strip())
    print("\n-- SELECT scan_id, ws, ts, device, ch FROM scans "
          "ORDER BY ts, scan_id;")
    print_attributions(db_p)
    print("\n-- SELECT day, COALESCE(ch,'(unattributed)'), n FROM counts "
          "ORDER BY 1, 2;")
    print_counts(db_p)
    check_against_reference(db_p, order["steps"], times, "as built")

    show("Correction 1: step 1 actually took effect at +250, not +200")
    corr1_p = os.path.join(tmp, "corr1.json")
    with open(corr1_p, "w") as f:
        json.dump({"format": "handover-time-correction/1",
                   "order_hash": order["order_hash"],
                   "changes": [{"seq": 1, "ts": BASE + 250}]}, f)
    p = cli("ledger-correct", "--db", db_p, "--correction", corr1_p,
            expect_rc=0)
    out = json.loads(p.stdout)
    summ = out["summary"]
    print(f"result: {out['result']};  new times: {summ['times']}")
    print(f"windows checked: {summ['windows']}")
    print("moved scans:")
    for m in summ["moved"]:
        o = f"{m['old']['device']}/{m['old']['ch']}" if m["old"]["device"] \
            else "(unattributed)"
        n = f"{m['new']['device']}/{m['new']['ch']}" if m["new"]["device"] \
            else "(unattributed)"
        print(f"  {m['id']}  {m['ws']} @{m['ts'] - BASE:>4d}: {o} -> {n}")
    print("net count movement:")
    for n in summ["net"]:
        print(f"  {n['day']}  {n['ch'] or '(unattributed)':>15s}  {n['delta']:+d}")
    assert {m["id"] for m in summ["moved"]} == {"S06", "S07", "S08",
                                                "S11", "S12"}
    print("\n-- counts after the correction:")
    print_counts(db_p)
    check_against_reference(db_p, order["steps"],
                            [T0, BASE + 250, T2, T3], "after correction 1")

    show("Correction 2 crashes after its writes, before COMMIT")
    corr2_p = os.path.join(tmp, "corr2.json")
    with open(corr2_p, "w") as f:
        json.dump({"format": "handover-time-correction/1",
                   "order_hash": order["order_hash"],
                   "changes": [{"seq": 0, "ts": BASE + 50}]}, f)
    cli("ledger-correct", "--db", db_p, "--correction", corr2_p,
        "--crash-before-commit", expect_rc=12)
    print("process died mid-correction (exit 12);  reopened ledger:")
    rows = run_sql(db_p, SQL_TIMES)
    print(f"  step times still: {[t - BASE for _, _, _, t in rows]}")
    rows = run_sql(db_p, SQL_CORRECTIONS)
    print(f"  applied corrections on record: {len(rows)}")
    check_against_reference(db_p, order["steps"],
                            [T0, BASE + 250, T2, T3], "after the crash")

    show("Reopen, apply correction 2, then resend it")
    p = cli("ledger-correct", "--db", db_p, "--correction", corr2_p,
            expect_rc=0)
    out = json.loads(p.stdout)
    print(f"result: {out['result']};  moved: "
          f"{[m['id'] for m in out['summary']['moved']]}")
    p = cli("ledger-correct", "--db", db_p, "--correction", corr2_p,
            expect_rc=0)
    out = json.loads(p.stdout)
    print(f"resent identical correction -> {out['result']} "
          "(no double booking)")
    assert out["result"] == "already_applied"
    final_times = [BASE + 50, BASE + 250, T2, T3]
    check_against_reference(db_p, order["steps"], final_times, "final state")
    print("\n-- final per-scan attribution:")
    print_attributions(db_p)
    print("\n-- final counts:")
    print_counts(db_p)
    rows = run_sql(db_p, SQL_CORRECTIONS)
    print(f"\ncorrections on record: {len(rows)} "
          f"(each with new times, moved scans and net counts)")

    print("\nLEDGER DEMO OK")


if __name__ == "__main__":
    main()
