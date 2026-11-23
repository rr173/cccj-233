#!/usr/bin/env python3
"""Runnable end-to-end demo of the offline per-package missing-scan
verification (handover-verify/1).

Scenario:  the ledger demo's completed A/B/C handover (WS-2 first falls
vacant, then changes owner;  steps 2 and 3 share a second), plus extra
scans so the directed cases live on the real ledger:

  P-FULL    a 3-station route whose evidence moves to other scans when a
            correction shifts channels
  P-LONG    a 16-station route over one repeated channel;  the
            correction cuts its proven prefix from 16 to 14
  P-GAIN    gains its whole route only after the correction (0 -> 2)
  P-RESCAN  the earlier CH-3 scan cannot reach CH-9 in time, the later
            rescan can;  stable across the correction
  P-EMPTY   station 1's channel never appears:  empty prefix
  P-NOASSOC no scan associations at all:  still listed, empty prefix
  P-UNATTR  its only scan is unattributed in the ledger:  not evidence

Operations builds the ledger once, runs the offline verification, then
corrects one step time through the existing ledger flow and re-verifies
into a SECOND result database -- conclusions and evidence are compared
per package, never channel totals.  A final section generates dense
rescans at runtime and shows the work grows with the candidate count,
not with the number of matching scan pairs (VM-op counts, not wall
clock).  Everything is generated in a temporary directory.
"""
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import tracemalloc
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

from handover import ledger  # noqa: E402
from handover.verify import verify, verify_result  # noqa: E402

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

BASE = int(datetime(2026, 11, 22, 12, 0, 0, tzinfo=timezone.utc).timestamp())
TIMES = [BASE + 100, BASE + 200, BASE + 300, BASE + 300]

# scan log: (id, workstation, offset seconds from BASE)
SCAN_ROWS = [
    ("S01", "WS-2", 50), ("S02", "WS-2", 99), ("S03", "WS-2", 100),
    ("S04", "WS-2", 150), ("S05", "WS-2", 199), ("S06", "WS-2", 200),
    ("S07", "WS-2", 225), ("S08", "WS-2", 249), ("S09", "WS-2", 250),
    ("S10", "WS-1", 199), ("S11", "WS-1", 200), ("S12", "WS-1", 249),
    ("S13", "WS-1", 250), ("S14", "WS-3", 299), ("S15", "WS-3", 300),
    ("S16", "WS-8", 299), ("S17", "WS-8", 300), ("S18", "WS-9", 210),
    ("S19", "WS-2", 500),
    ("S20", "WS-2", 86450), ("S21", "WS-9", 90000),
    ("R01", "WS-3", 250), ("R02", "WS-3", 270), ("R09", "WS-3", 270),
    ("R03", "WS-2", 285), ("R04", "WS-2", 295), ("R05", "WS-2", 280),
    ("R06", "WS-2", 290), ("R07", "WS-2", 279), ("R08", "WS-2", 291),
    ("R10", "WS-2", 270), ("R11", "WS-2", 600), ("R12", "WS-2", 700),
    ("R13", "WS-2", 800), ("R14", "WS-2", 900), ("R15", "WS-2", 1000),
    ("Q1", "WS-2", 280), ("Q2", "WS-2", 285), ("Q3", "WS-2", 279),
    ("Q4", "WS-2", 291), ("Q5", "WS-2", 270),
]

ROUTES = {
    "P-FULL": [{"ch": "CH-2"}, {"ch": "CH-9", "min": 100, "max": 160},
               {"ch": "CH-1", "min": 1, "max": 110}],
    "P-LONG": [{"ch": "CH-9"}] + [{"ch": "CH-9", "min": 1, "max": 100000}
                                  for _ in range(15)],
    "P-GAIN": [{"ch": "CH-1"}, {"ch": "CH-2", "min": 45, "max": 55}],
    "P-RESCAN": [{"ch": "CH-3"}, {"ch": "CH-9", "min": 10, "max": 19}],
    "P-EMPTY": [{"ch": "CH-7"}],
    "P-NOASSOC": [{"ch": "CH-2"}],
    "P-UNATTR": [{"ch": "CH-9"}],
}
ASSOC = {
    "P-FULL": ["S01", "S02", "S06", "S09", "S10", "S15"],
    "P-LONG": ["S07", "S08", "R03", "R04", "R05", "R06", "R07", "R08",
               "R10", "R11", "R12", "R13", "R14", "R15", "S19", "S20"],
    "P-GAIN": ["S11", "S12", "S13", "S16", "S17"],
    "P-RESCAN": ["R01", "R02", "R09", "Q1", "Q2", "Q3", "Q4", "Q5"],
    "P-EMPTY": ["S18"],
    "P-NOASSOC": [],
    "P-UNATTR": ["S03"],
}


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


def ro(path):
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


def print_results(db):
    con = ro(db)
    rows = list(con.execute(
        "SELECT pkg, stations, passed, next_station, next_ch "
        "FROM results ORDER BY pkg"))
    ev = {}
    for pkg, st, sid, ts, ch in con.execute(
            "SELECT pkg, station, scan_id, ts, ch FROM evidence "
            "ORDER BY pkg, station"):
        ev.setdefault(pkg, []).append((st, sid, ts, ch))
    con.close()
    out = {}
    for pkg, stations, passed, next_st, next_ch in rows:
        nxt = "-" if next_st is None else f"station {next_st} ({next_ch})"
        print(f"  {pkg:9s} passed {passed:2d}/{stations:2d}   next: {nxt}")
        for st, sid, ts, ch in ev.get(pkg, []):
            print(f"      station {st:2d}: {sid:4s} @+{ts - BASE:<6d} {ch}")
        out[pkg] = (passed, ev.get(pkg, []))
    return out


def main():
    tmp = tempfile.mkdtemp(prefix="verify-demo-")
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

    show("Setup: completed handover, ledger built once (real pipeline)")
    cli("plan", "--old", old_p, "--target", tgt_p, "--offline", off_p,
        "--out", order_p, expect_rc=0)
    cli("init", "--db", recv_p, "--table", old_p, expect_rc=0)
    cli("run", "--db", recv_p, "--order", order_p, expect_rc=0)
    with open(order_p) as f:
        order = json.load(f)
    with open(times_p, "w") as f:
        json.dump({"format": "handover-step-times/1",
                   "order_hash": order["order_hash"],
                   "times": [{"seq": q, "ts": t} for q, t in
                             enumerate(TIMES)]}, f)
    with open(scans_p, "w") as f:
        for sid, ws, off in SCAN_ROWS:
            f.write(json.dumps({"id": sid, "ws": ws, "ts": BASE + off}) + "\n")
    p = cli("ledger-build", "--db", db_p, "--snapshot", old_p,
            "--order", order_p, "--receiver", recv_p,
            "--times", times_p, "--scans", scans_p, expect_rc=0)
    print(json.loads(p.stdout)["result"], f"({len(SCAN_ROWS)} scans)")

    show("Routes (JSONL) and scan associations (JSONL), unordered")
    routes_p = os.path.join(tmp, "routes.jsonl")
    assoc_p = os.path.join(tmp, "assoc.jsonl")
    with open(routes_p, "w") as f:
        for pkg in sorted(ROUTES, reverse=True):      # deliberately unordered
            f.write(json.dumps({"pkg": pkg, "route": ROUTES[pkg]}) + "\n")
    n_assoc = 0
    with open(assoc_p, "w") as f:
        for pkg in sorted(ASSOC, reverse=True):
            for sid in ASSOC[pkg]:
                f.write(json.dumps({"pkg": pkg, "scan": sid}) + "\n")
                n_assoc += 1
    print(f"  {len(ROUTES)} package routes, {n_assoc} associations")
    print("  P-RESCAN route: CH-3 then CH-9 within [10, 19] s;  R01@+250 "
          "is the earlier scan, R02/R09@+270 the later rescans")

    show("handover verify -> verify1.db")
    v1 = os.path.join(tmp, "verify1.db")
    p = cli("verify", "--ledger", db_p, "--routes", routes_p,
            "--assoc", assoc_p, "--out", v1, expect_rc=0)
    out1 = json.loads(p.stdout)
    print(f"  packages {out1['packages']}:  full {out1['full']}, "
          f"partial {out1['partial']}, empty {out1['empty']}")
    print("-- SELECT pkg, stations, passed, next_station, next_ch "
          "FROM results ORDER BY pkg;")
    before = print_results(v1)
    assert before["P-RESCAN"][0] == 2
    # the earlier R01@+250 cannot reach CH-9 within [10, 19] s;  the
    # later rescan R02@+270 reaches Q1@+280 (gap 10 = min exactly)
    assert [e[1] for e in before["P-RESCAN"][1]] == ["R02", "Q1"]
    print("  -> P-RESCAN proves the later rescan R02, not the earlier "
          "R01;  Q1@+280 sits exactly at the min gap")
    assert before["P-EMPTY"][0] == 0 and before["P-NOASSOC"][0] == 0
    assert before["P-UNATTR"][0] == 0
    assert before["P-LONG"][0] == 16 and before["P-GAIN"][0] == 0

    show("handover verify-check:  evidence re-checked against the ledger")
    p = cli("verify-check", "--result", v1, "--ledger", db_p, expect_rc=0)
    print(" ", json.loads(p.stdout)["result"])
    print("  sample cross-check (evidence joined with the ledger's scans):")
    con = ro(v1)
    con.execute(f"ATTACH DATABASE 'file:{db_p}?mode=ro' AS lg")
    for row in con.execute(
            "SELECT e.pkg, e.station, e.scan_id, e.ts, e.ch, s.ts, s.ch "
            "FROM evidence e JOIN lg.scans s ON s.scan_id = e.scan_id "
            "WHERE e.pkg = 'P-FULL' ORDER BY e.station"):
        pkg, st, sid, ets, ech, sts, sch = row
        mark = "OK" if (ets, ech) == (sts, sch) else "MISMATCH"
        print(f"    {pkg} station {st}: {sid} evidence @(+{ets - BASE}) "
              f"{ech}  ==  ledger @(+{sts - BASE}) {sch}  [{mark}]")
        assert mark == "OK"
    con.close()

    show("Correction through the existing ledger flow:  step 1 at +250")
    corr_p = os.path.join(tmp, "corr.json")
    with open(corr_p, "w") as f:
        json.dump({"format": "handover-time-correction/1",
                   "order_hash": order["order_hash"],
                   "changes": [{"seq": 1, "ts": BASE + 250}]}, f)
    p = cli("ledger-correct", "--db", db_p, "--correction", corr_p,
            expect_rc=0)
    moved = [m["id"] for m in json.loads(p.stdout)["summary"]["moved"]]
    print(f"  moved scans: {moved}")
    assert set(moved) == {"S06", "S07", "S08", "S11", "S12"}

    show("Re-verify into a SECOND result db:  verify2.db")
    v2 = os.path.join(tmp, "verify2.db")
    p = cli("verify", "--ledger", db_p, "--routes", routes_p,
            "--assoc", assoc_p, "--out", v2, expect_rc=0)
    print("-- per-package conclusions and evidence after the correction:")
    after = print_results(v2)
    print("\n  what the correction changed (per package, not totals):")
    for pkg in sorted(before):
        b, a = before[pkg], after[pkg]
        if b != a:
            print(f"    {pkg}: passed {b[0]} -> {a[0]};  "
                  f"evidence {[e[1] for e in b[1]]} -> {[e[1] for e in a[1]]}")
    assert before["P-FULL"][0] == after["P-FULL"][0] == 3
    assert [e[1] for e in before["P-FULL"][1]] == ["S01", "S06", "S15"]
    assert [e[1] for e in after["P-FULL"][1]] == ["S02", "S09", "S15"]
    assert before["P-LONG"][0] == 16 and after["P-LONG"][0] == 14
    assert before["P-GAIN"][0] == 0 and after["P-GAIN"][0] == 2
    assert before["P-RESCAN"] == after["P-RESCAN"]
    p = cli("verify-check", "--result", v2, "--ledger", db_p, expect_rc=0)
    print("  verify2.db vs current ledger: ",
          json.loads(p.stdout)["result"])
    p = cli("verify-check", "--result", v1, "--ledger", db_p, expect_rc=5)
    out = json.loads(p.stdout)
    print("  verify1.db vs current ledger: ", out["result"], "--",
          out["problems"][0][:80], "...")

    show("Dense rescans:  work follows the candidate count, not the pairs")
    T = 150

    def dense_case(d, tmpdir):
        """One package, 3 stations, d rescans per second per channel."""
        lp = os.path.join(tmpdir, "dense-ledger.db")
        ap = os.path.join(tmpdir, "dense-assoc.jsonl")
        con = sqlite3.connect(lp)
        con.executescript(ledger.SCHEMA)
        n = 0
        with open(ap, "w") as f:
            for t in range(T):
                for k in range(d):
                    for ch in "ABC":
                        sid = f"{ch}{t:04d}_{k:03d}"
                        con.execute("INSERT INTO scans (scan_id, ws, ts, "
                                    "device, ch) VALUES (?, 'W', ?, 'D', ?)",
                                    (sid, t, f"CH-{ch}"))
                        f.write(json.dumps({"pkg": "P-DENSE", "scan": sid})
                                + "\n")
                        n += 1
        con.executemany("INSERT INTO meta (k, v) VALUES (?, ?)", [
            ("format", ledger.LEDGER_FORMAT), ("order_hash", "0" * 64),
            ("initial_hash", "0" * 64), ("final_hash", "0" * 64),
            ("step_count", "0"), ("scan_count", str(n))])
        con.commit()
        con.close()
        route = [{"ch": "CH-A"}, {"ch": "CH-B", "min": 1, "max": 3},
                 {"ch": "CH-C", "min": 1, "max": 3}]
        return lp, ap, route

    def run_dense(d):
        with tempfile.TemporaryDirectory(dir=tmp) as td:
            lp, ap, route = dense_case(d, td)
            out = os.path.join(td, "v.db")
            with open(ap) as f:
                res = verify(lp, [json.dumps({"pkg": "P-DENSE",
                                              "route": route})],
                             f, out, progress_every=1000)
            ok, problems = verify_result(out, lp)
            assert ok, problems
            return res

    pairs = {d: 2 * (T * d) * (3 * d) for d in (1, 60)}
    r1 = run_dense(1)
    r60 = run_dense(60)
    print(f"  matching scan pairs:  {pairs[1]:>9,} -> {pairs[60]:>11,} "
          f"(x{pairs[60] // pairs[1]})")
    print(f"  measured VM-ops:      {r1['vm_ops']:>9,} -> "
          f"{r60['vm_ops']:>11,} "
          f"(x{r60['vm_ops'] / r1['vm_ops']:.1f})")
    assert r60["vm_ops"] < r1["vm_ops"] * 400
    with tempfile.TemporaryDirectory(dir=tmp) as td:
        lp, ap, route = dense_case(60, td)
        out = os.path.join(td, "v.db")
        with open(ap) as f:
            tracemalloc.start()
            verify(lp, [json.dumps({"pkg": "P-DENSE", "route": route})],
                   f, out)
            peak = tracemalloc.get_traced_memory()[1]
            tracemalloc.stop()
    print(f"  peak python memory of verify() at x60 density: "
          f"{peak:,} bytes (bounded by batches and tree depth, "
          f"not by candidates)")
    print("\nVERIFY DEMO OK")


if __name__ == "__main__":
    main()
