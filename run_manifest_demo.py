#!/usr/bin/env python3
"""Runnable end-to-end demo of the offline shipment manifest
(handover-manifest/1).

Scenario:  the ledger demo's completed A/B/C handover, plus a packing log
on two channels (packing on CH-1, gating on CH-9).  Steps:

  1. Two packing scans build  R <- C1 <- C2  with two packages inside;
     the next scan reverses the two container levels in ONE operation,
     and the group's log lines are written in the reverse order -- the
     delivered manifest must be identical.
  2. One scan registers a genuinely cyclic move that also shifts a
     package;  the whole group is rejected, so the manifest still shows
     that package where it was.
  3. The outermost cage gates;  every package inside appears in the
     manifest with its direct parent and the packing scan that made that
     relation effective.  Afterwards the inner box cannot be packed
     elsewhere nor gated again.
  4. An empty cage gates too, and still gets a manifest.
  5. Operations corrects one step time through the existing ledger flow,
     revoking one scan's packing entitlement;  the same log is rebuilt
     into a SECOND manifest.  The two databases are compared package by
     package and layer by layer, and the first one keeps answering from
     its own frozen snapshot.
  6. A batch of unrelated containers is generated at runtime beside the
     ones being processed;  query plans and SQLite VM-step counts -- never
     wall-clock timing -- show they take no part in packing or gating.

Everything is generated at runtime in a temporary directory.
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

from handover import ledger  # noqa: E402
from handover.manifest import (  # noqa: E402
    SQL_CAND_PARENT, SQL_GATE_SUBTREE, SQL_LOOKUP,
    build_manifest,
)

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
PACK_CH = "CH-1"          # A's channel before the correction moves it
GATE_CH = "CH-9"

BASE = int(datetime(2026, 11, 22, 12, 0, 0, tzinfo=timezone.utc).timestamp())
TIMES = [BASE + 100, BASE + 200, BASE + 300, BASE + 300]
# packing scans on WS-1 before the A step, gate scan on WS-2 after it
SCAN_ROWS = [
    ("PK01", "WS-1", 50), ("PK02", "WS-1", 60), ("PK03", "WS-1", 70),
    ("PK04", "WS-1", 80), ("PK05", "WS-1", 90), ("PK08", "WS-1", 96),
    ("GA01", "WS-2", 400),
    ("PK07", "WS-2", 410),    # packing on the gate channel: skipped
    ("GA02", "WS-2", 420), ("GA03", "WS-2", 430),
    ("PK06", "WS-3", 500),    # CH-1 again: the post-gate packing attempt
]


def pack_line(scan, items):
    return json.dumps({"scan": scan, "pack": {"items": [
        {"kind": k, "id": i, "from": f, "to": t} for k, i, f, t in items]}},
        ensure_ascii=False)


def gate_line(scan, root):
    return json.dumps({"scan": scan, "gate": root}, ensure_ascii=False)


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


def read_manifest(path):
    con = ro(path)
    meta = dict(con.execute("SELECT k, v FROM meta"))
    groups = list(con.execute(
        "SELECT ord, scan_id, ts, use, adopted, reason FROM groups "
        "ORDER BY ord"))
    ships = list(con.execute(
        "SELECT gate_id, scan_id, ts, root_id FROM manifests ORDER BY gate_id"))
    rel = {}
    for gid, kind, oid, parent, via in con.execute(
            "SELECT gate_id, kind, id, parent, via FROM manifest "
            "ORDER BY gate_id, kind, id"):
        rel.setdefault(gid, []).append((kind, oid, parent, via))
    pkgs = {}
    for gid, p in con.execute(
            "SELECT gate_id, pkg FROM packages ORDER BY gate_id, pkg"):
        pkgs.setdefault(gid, []).append(p)
    con.close()
    return meta, groups, ships, rel, pkgs


def print_shipments(path):
    meta, groups, ships, rel, pkgs = read_manifest(path)
    for gid, sid, ts, root in ships:
        print(f"  gate {sid} @+{ts - BASE}:  root {root};  packages "
              f"{pkgs.get(gid, [])}")
        for kind, oid, parent, via in rel[gid]:
            print(f"      {kind:3s} {oid:6s} -> parent {str(parent):6s} "
                  f"(basis {via})")
    print("  group adoption:")
    for ord_, sid, ts, use, adopted, reason in groups:
        mark = "adopted" if adopted else "skipped"
        extra = ""
        if not adopted:
            extra = "  " + "; ".join(json.loads(reason))
        print(f"      {ord_:2d} {sid} @+{ts - BASE:<4d} {use:4s} {mark}{extra}")
    return meta, groups, ships, rel, pkgs


def ordered_log():
    return [
        pack_line("PK01", [("ctr", "R", None, None),
                           ("ctr", "C1", None, "R"),
                           ("ctr", "C2", None, "C1"),
                           ("pkg", "BOX-A", None, "C2"),
                           ("pkg", "BOX-B", None, "C2")]),
        # one operation reverses the two container levels
        pack_line("PK02", [("ctr", "C1", "R", "C2"),
                           ("ctr", "C2", "C1", "R")]),
        # a genuinely cyclic operation that also moves a package:
        # C2 would move into C1, which is inside C2
        pack_line("PK03", [("ctr", "C2", "R", "C1"),
                           ("pkg", "BOX-B", "C2", "R")]),
        # an empty top-level cage, to gate on its own
        pack_line("PK08", [("ctr", "EMPTY", None, None)]),
        # a packing scan on the gate channel: skipped, one group
        pack_line("PK07", [("pkg", "BOX-X", None, "C1")]),
        gate_line("GA01", "R"),
        # after the gate, try to pack the inner box elsewhere
        pack_line("PK06", [("ctr", "C1", "C2", "NEW")]),
        gate_line("GA02", "C2"),
        gate_line("GA03", "EMPTY"),
    ]


def reverse_log():
    """The same operations, every line reversed and the reversal group's
    rows reversed:  the delivered manifest must not differ."""
    lines = [json.loads(x) for x in reversed(ordered_log())]
    for obj in lines:
        if obj["scan"] == "PK02":
            obj["pack"]["items"].reverse()
    return [json.dumps(o, ensure_ascii=False) for o in lines]


def measure_ops(con, fn, every=100):
    ops = [0]

    def handler():
        ops[0] += 1
        return 0

    con.set_progress_handler(handler, every)
    try:
        fn()
    finally:
        con.set_progress_handler(None, 0)
    return ops[0] * every


def main():
    tmp = tempfile.mkdtemp(prefix="manifest-demo-")
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

    show("Setup: completed handover and a real ledger (read-only)")
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
    cli("ledger-build", "--db", db_p, "--snapshot", old_p,
        "--order", order_p, "--receiver", recv_p,
        "--times", times_p, "--scans", scans_p, expect_rc=0)
    con = ro(db_p)
    for sid, ws, ts, dev, ch in con.execute(
            "SELECT scan_id, ws, ts, device, ch FROM scans ORDER BY ts"):
        print(f"  {sid}  {ws} @+{ts - BASE:<4d} -> "
              f"{dev or '(unattributed)'}/{ch or '-'}")
    con.close()

    show("Build manifest 1: packing on CH-1, gating on CH-9")
    log1 = os.path.join(tmp, "log1.jsonl")
    with open(log1, "w") as f:
        f.write("\n".join(ordered_log()) + "\n")
    m1 = os.path.join(tmp, "manifest1.db")
    p = cli("manifest", "--ledger", db_p, "--pack-ch", PACK_CH,
            "--gate-ch", GATE_CH, "--log", log1, "--out", m1,
            expect_rc=0)
    out1 = json.loads(p.stdout)
    print(f"  groups {out1['log_groups']}: adopted {out1['adopted']}, "
          f"skipped {out1['skipped']};  manifests {out1['manifests']}, "
          f"packages {out1['packages']}")
    print_shipments(m1)
    meta1, groups1, ships1, rel1, pkgs1 = read_manifest(m1)
    gid = ships1[0][0]
    assert pkgs1[gid] == ["BOX-A", "BOX-B"]
    # the one-operation reversal: C2 rose to the top and C1 is inside it
    assert ("ctr", "C1", "C2", "PK02") in rel1[gid]
    # the cyclic group was rejected whole:  BOX-B never left C2
    assert ("pkg", "BOX-B", "C2", "PK01") in rel1[gid]
    # the packing scan on the gate channel and the post-gate moves are
    # refused, and the empty cage gets its own manifest
    assert ("pkg", "BOX-X", None, None) not in [
        r for rows in rel1.values() for r in rows]
    empty = [s for s in ships1 if s[3] == "EMPTY"]
    assert len(empty) == 1 and pkgs1.get(empty[0][0], []) == []
    for ord_, sid, ts, use, adopted, reason in groups1:
        if sid in ("PK03", "PK06", "PK07", "GA02"):
            assert not adopted, (sid, reason)
            print(f"  refused {sid}: {'; '.join(json.loads(reason))}")

    show("Reverse every log line and the reversal group -- byte for byte")
    log2 = os.path.join(tmp, "log2.jsonl")
    with open(log2, "w") as f:
        f.write("\n".join(reverse_log()) + "\n")
    m1b = os.path.join(tmp, "manifest1b.db")
    cli("manifest", "--ledger", db_p, "--pack-ch", PACK_CH,
        "--gate-ch", GATE_CH, "--log", log2, "--out", m1b, expect_rc=0)
    assert read_manifest(m1b) == read_manifest(m1)
    print("  identical manifest: same groups, same relations, same basis "
          "scans")

    show("Correction: A's move landed at +20, before the packing scans")
    corr = os.path.join(tmp, "corr.json")
    with open(corr, "w") as f:
        json.dump({"format": "handover-time-correction/1",
                   "order_hash": order["order_hash"],
                   "changes": [{"seq": 0, "ts": BASE + 10},
                               {"seq": 1, "ts": BASE + 20}]}, f)
    p = cli("ledger-correct", "--db", db_p, "--correction", corr,
            expect_rc=0)
    moved = [m["id"] for m in json.loads(p.stdout)["summary"]["moved"]]
    print(f"  re-attributed scans: {moved}")
    print("  build manifest 2 from the SAME log against the corrected ledger")
    m2 = os.path.join(tmp, "manifest2.db")
    cli("manifest", "--ledger", db_p, "--pack-ch", PACK_CH,
        "--gate-ch", GATE_CH, "--log", log1, "--out", m2, expect_rc=0)
    meta2, groups2, ships2, rel2, pkgs2 = read_manifest(m2)
    print_shipments(m2)
    # the packing scans on WS-1 lost their entitlement;  the gate still
    # carries the cage, now empty
    assert all(not g[4] for g in groups2 if g[3] == "pack")
    assert ships2[0][1] == "GA01"
    assert pkgs2.get(ships2[0][0], []) == []
    assert rel2[ships2[0][0]] == [("ctr", "R", None, None)]
    print("  manifest 2: the same cage, no package inside -- the revoked "
          "packing evidence is gone")
    # the delivered manifest 1 is self-contained, unaffected by the
    # later correction
    assert read_manifest(m1) == (meta1, groups1, ships1, rel1, pkgs1)
    assert meta1["ledger_scans_sha256"] != meta2["ledger_scans_sha256"]
    print("  manifest 1 still answers from its own snapshot "
          f"({meta1['ledger_scans_sha256'][:12]}... vs "
          f"{meta2['ledger_scans_sha256'][:12]}...)")
    con = ro(m1)
    print("  carrier query on manifest 1 -- "
          "SELECT * FROM shipment_packages WHERE scan_id = 'GA01';")
    for row in con.execute(
            "SELECT scan_id, ts, root_id, pkg FROM shipment_packages "
            "WHERE scan_id = 'GA01' ORDER BY pkg"):
        print(f"      {row}")
    for row in con.execute(
            "SELECT kind, id, parent, via FROM manifest m "
            "JOIN manifests s USING (gate_id) WHERE s.scan_id = 'GA01' "
            "ORDER BY kind, id"):
        print(f"      layer: {row}")
    con.close()

    show("Unrelated containers: only the group and the subtree are touched")
    con = sqlite3.connect(":memory:")
    con.executescript(open(os.path.join(
        ROOT, "handover", "manifest.py")).read().split('STAGING_SCHEMA = """')[1]
        .split('"""')[0])
    for sql, args, want in (
            (SQL_LOOKUP, ("pkg", "X"), "SEARCH own USING PRIMARY KEY"),
            (SQL_CAND_PARENT, (1, "pkg", "X"), "SEARCH owned_list USING"),
            (SQL_GATE_SUBTREE, ("R", 1), "SEARCH o USING INDEX own_parent")):
        plan = [r[3] for r in con.execute("EXPLAIN QUERY PLAN " + sql, args)]
        assert any(p.startswith(want) for p in plan), (sql, plan)
        assert not any(p.startswith("SCAN own") for p in plan), plan
        print(f"  plan: {want}  (no table scan of own)")
    con.close()

    noise = [("pkg", f"N{i}", "BIG", "PK00") for i in range(30000)]
    noise += [("ctr", f"B{i}", "BIG", "PK00") for i in range(3000)]

    def run(n_noise):
        con = sqlite3.connect(":memory:")
        con.executescript(open(os.path.join(
            ROOT, "handover", "manifest.py")).read()
            .split('STAGING_SCHEMA = """')[1].split('"""')[0])
        con.isolation_level = None
        con.executemany("INSERT INTO own (kind, id, parent, via) "
                        "VALUES (?, ?, ?, ?)", noise[:n_noise])
        con.executemany("INSERT INTO own (kind, id, parent, via) "
                        "VALUES (?, ?, ?, ?)",
                        [("ctr", "R", None, None),
                         ("ctr", "C1", "R", "PK01"),
                         ("ctr", "C2", "C1", "PK02"),
                         ("pkg", "BOX-B", "C2", "PK04")])
        from handover.manifest import apply_pack, apply_gate

        def work():
            apply_pack(con, "PK05", [("pkg", "BOX-B", "C2", "C2")], 900)
            apply_pack(con, "PK06", [("pkg", "BOX-B", "C2", "R")], 901)
            apply_gate(con, "R", 902)
        ops = measure_ops(con, work)
        con.close()
        return ops

    small, big = run(300), run(len(noise))
    print(f"  {len(noise):,} unrelated rows beside the working set:  "
          f"VM-ops {small} -> {big} (same access, not a timing comparison)")
    assert big <= small * 1.2 + 200, (small, big)
    print("\nMANIFEST DEMO OK")


if __name__ == "__main__":
    main()
