#!/usr/bin/env python3
"""Runnable end-to-end demo of the binding-table handover tool.

Scenario (three scanners):
    old:    A=(WS-1,CH-1)   B=(WS-2,CH-2)   C=(WS-3,CH-3)
    target: A=(WS-2,CH-9)   B=(WS-3,CH-1)   C=(WS-8,CH-2)

so A waits for B's workstation, B waits for C's workstation *and* A's
channel, and C waits for B's channel.

  Part 1: only A may go offline  -> the B<->C deadlock must be reported
          as a closed wait cycle (and re-verified against the
          orchestration-time state embedded in the report).
  Part 2: A and B may go offline -> a work order exists.  We apply it to a
          SQLite receiver, crashing once before a commit and once after a
          commit whose response is lost, resuming with fresh processes,
          resending the unacknowledged step, and finally proving for every
          committed prefix that both unique constraints hold and that the
          rows present in both versions were never deleted and reinserted.

Everything is generated at runtime in a temporary directory.
"""
import json
import os
import sqlite3
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

from handover import core  # noqa: E402

OLD_ROWS = [
    {"id": "A", "ws": "WS-1", "ch": "CH-1"},
    {"id": "B", "ws": "WS-2", "ch": "CH-2"},
    {"id": "C", "ws": "WS-3", "ch": "CH-3"},
]
TARGET_ROWS = [
    {"id": "A", "ws": "WS-2", "ch": "CH-9"},   # needs B's workstation
    {"id": "B", "ws": "WS-3", "ch": "CH-1"},   # needs C's workstation + A's channel
    {"id": "C", "ws": "WS-8", "ch": "CH-2"},   # needs B's channel
]
SHARED_IDS = {"A", "B", "C"}


def cli(*args, expect_rc=None):
    p = subprocess.run([sys.executable, "-m", "handover", *args],
                       capture_output=True, text=True,
                       env={**os.environ, "PYTHONPATH": ROOT})
    if expect_rc is not None:
        assert p.returncode == expect_rc, \
            f"handover {' '.join(args)}: rc={p.returncode}\n{p.stdout}\n{p.stderr}"
    return p


def out_json(p):
    return json.loads(p.stdout)


def show(title):
    print(f"\n=== {title} " + "=" * max(0, 60 - len(title)))


def main():
    tmp = tempfile.mkdtemp(prefix="handover-demo-")
    old_p = os.path.join(tmp, "old.jsonl")
    tgt_p = os.path.join(tmp, "target.jsonl")
    off_a = os.path.join(tmp, "offline_a.json")
    off_ab = os.path.join(tmp, "offline_ab.json")
    order_p = os.path.join(tmp, "order.json")
    db_p = os.path.join(tmp, "receiver.db")

    with open(old_p, "w", encoding="utf-8") as f:
        f.write("\n".join(json.dumps(r) for r in OLD_ROWS) + "\n")
    with open(tgt_p, "w", encoding="utf-8") as f:
        f.write("\n".join(json.dumps(r) for r in TARGET_ROWS) + "\n")
    with open(off_a, "w", encoding="utf-8") as f:
        json.dump(["A"], f)
    with open(off_ab, "w", encoding="utf-8") as f:
        json.dump(["A", "B"], f)

    old = {r["id"]: (r["ws"], r["ch"]) for r in OLD_ROWS}
    target = {r["id"]: (r["ws"], r["ch"]) for r in TARGET_ROWS}

    # ---------------------------------------------------------------- part 1
    show("Part 1: only A may go offline -> B and C are deadlocked")
    p = cli("plan", "--old", old_p, "--target", tgt_p,
            "--offline", off_a, "--out", order_p, expect_rc=4)
    report = out_json(p)
    assert not os.path.exists(order_p), "no order file may be delivered on failure"
    cycle_devices = [e["device"] for e in report["cycle"]]
    print(json.dumps(report, indent=2, ensure_ascii=False))
    assert sorted(cycle_devices) == ["B", "C"], cycle_devices
    edges = {e["device"]: e["needs"] for e in report["cycle"]}
    assert edges["B"] == {"column": "ws", "number": "WS-3", "held_by": "C"}
    assert edges["C"] == {"column": "ch", "number": "CH-2", "held_by": "B"}
    ok, problems = core.verify_cycle_report(report, {"A"})
    assert ok, problems
    print("cycle re-verified against the orchestration-time state: "
          "B needs C's workstation, C needs B's channel, neither may go "
          "offline -> neither can move first")

    # ---------------------------------------------------------------- part 2
    show("Part 2: A and B may go offline -> plan, crash, resume, prove")
    p = cli("plan", "--old", old_p, "--target", tgt_p,
            "--offline", off_ab, "--out", order_p, expect_rc=0)
    print(p.stdout.strip())
    order = core.load_order(order_p)
    for s in order["steps"]:
        print(f"  step {s['seq']}: {s['op']:7s} {s['id']}  "
              f"{s['before']} -> {s['after']}")
    assert [s["op"] for s in order["steps"]] == ["offline", "update", "update", "online"]
    assert all(not (s["op"] == "delete" and s["id"] in SHARED_IDS)
               for s in order["steps"]), "shared rows must never be deleted"

    cli("init", "--db", db_p, "--table", old_p, expect_rc=0)
    ro = sqlite3.connect(f"file:{db_p}?mode=ro", uri=True)
    rowids0 = dict(ro.execute("SELECT id, rowid FROM bindings"))

    # crash #1: stop right before committing step 2
    p = cli("run", "--db", db_p, "--order", order_p,
            "--crash-before-seq", "2", expect_rc=10)
    st = out_json(cli("status", "--db", db_p, expect_rc=0))
    assert st["committed"] == 2 and not st["complete"], st
    print(f"after crash-before-commit-2: committed={st['committed']}")

    # crash #2: commit step 3 but lose the response
    p = cli("run", "--db", db_p, "--order", order_p,
            "--crash-after-commit-seq", "3", expect_rc=11)
    st = out_json(cli("status", "--db", db_p, expect_rc=0))
    assert st["committed"] == 4 and st["complete"], st
    print(f"after crash-after-commit-3 (response lost): committed={st['committed']}")

    # the client never got the response for step 3 and resends it: no-op
    before = out_json(cli("status", "--db", db_p, "--dump", expect_rc=0))
    p = cli("submit", "--db", db_p, "--order", order_p, "--seq", "3", expect_rc=0)
    res = out_json(p)
    assert res["result"] == "already_committed", res
    after = out_json(cli("status", "--db", db_p, "--dump", expect_rc=0))
    assert before == after, "a resent step must not be applied twice"
    print("resent step 3 -> already_committed, database unchanged")

    # a fresh process finishes the order (it is already complete)
    p = cli("run", "--db", db_p, "--order", order_p, expect_rc=0)

    # ------------------------------------------------------------- the proof
    show("Proof over the persisted record")
    con = sqlite3.connect(f"file:{db_p}?mode=ro", uri=True)
    persisted = dict(con.execute("SELECT seq, hash FROM steps ORDER BY seq"))
    assert sorted(persisted) == list(range(len(order["steps"]))), persisted
    for k, s in enumerate(order["steps"]):
        assert persisted[k] == core.step_hash(s), f"step {k} hash mismatch"
    print(f"persisted step log: seqs 0..{len(persisted)-1}, hashes match the order")

    # replay every committed prefix from the initial table: both unique
    # constraints hold and the shared rows are never missing (never deleted
    # and reinserted)
    for k in range(1, len(order["steps"]) + 1):
        core.replay_check(old, order["steps"][:k], keep_ids=SHARED_IDS)
    print(f"all {len(order['steps'])} committed prefixes satisfy both unique "
          "constraints; A/B/C rows present in every prefix")

    final = core.replay_check(old, order["steps"], keep_ids=SHARED_IDS)
    assert final == target, f"{final} != {target}"
    rows = {r[0]: (r[1], r[2]) for r in con.execute("SELECT id, ws, ch FROM bindings")}
    assert rows == target, f"receiver {rows} != target {target}"
    # row identity: rows present in both versions kept their SQLite rowids,
    # i.e. they were updated in place, never deleted and reinserted
    rowids1 = dict(con.execute("SELECT id, rowid FROM bindings"))
    assert all(rowids0[i] == rowids1[i] for i in SHARED_IDS)
    print("shared rows kept their row identity (rowids unchanged)")
    print("receiver matches the target table row by row:")
    for rid in sorted(rows):
        print(f"  {rid}: ws={rows[rid][0]} ch={rows[rid][1]}")

    print("\nDEMO OK")


if __name__ == "__main__":
    main()
