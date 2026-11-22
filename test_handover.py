#!/usr/bin/env python3
"""Test suite for the handover tool.

  * validation errors vs. valid-but-unmigratable distinction
  * wait-cycle report re-verification (the A/B/C scenario)
  * directed cases: additions, removals, both-resource swaps, no-op,
    untouched unchanged devices, case sensitivity
  * input reordering invariance (same content, shuffled lines -> same order)
  * independent small-scale reachability enumeration (BFS over all legal
    device states, not calling the tool's planner) cross-checked against
    the planner's verdict on hundreds of random instances
  * workload: runtime-generated thousand-long dependency chain plus many
    independent swap groups; the dependency handling must stay linear
  * receiver protocol over real processes: crash before a commit, crash
    after a commit with a lost response, resend, gap, foreign order,
    resume -- all asserted against the real rows and persisted step numbers
"""
import json
import os
import random
import sqlite3
import subprocess
import sys
import tempfile
import time
from collections import deque

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

from handover import core  # noqa: E402
from handover.core import (  # noqa: E402
    SourceInvalid, MigrationImpossible,
    plan, validate_table, validate_offline_list, verify_cycle_report,
    replay_check, build_order, canon,
)
from handover.receiver import Receiver, Reject  # noqa: E402


# --------------------------------------------------------------------- helpers

def cli(*args, expect_rc=None):
    p = subprocess.run([sys.executable, "-m", "handover", *args],
                       capture_output=True, text=True,
                       env={**os.environ, "PYTHONPATH": ROOT})
    if expect_rc is not None:
        assert p.returncode == expect_rc, \
            f"handover {' '.join(args)}: rc={p.returncode}\n{p.stdout}\n{p.stderr}"
    return p


def simulate(old, steps):
    """Independent in-memory application of an order.

    Returns (final_state, history); states map id -> ('a',) absent |
    ('f',) offline | ('o', ws, ch).  Every prefix is checked for both
    unique constraints."""
    st = {i: ("o", w, c) for i, (w, c) in old.items()}
    history = []
    for s in steps:
        rid, b, a = s["id"], s["before"], s["after"]
        cur = st.get(rid, ("a",))
        exp = (("a",) if b["state"] == "absent"
               else ("f",) if b["state"] == "offline"
               else ("o", b["ws"], b["ch"]))
        assert cur == exp, f"step {s['seq']}: before-state {exp}, actual {cur}"
        nxt = dict(st)
        if a["state"] == "absent":
            nxt.pop(rid, None)
        elif a["state"] == "offline":
            nxt[rid] = ("f",)
        else:
            nxt[rid] = ("o", a["ws"], a["ch"])
        ws = [v[1] for v in nxt.values() if v[0] == "o"]
        ch = [v[2] for v in nxt.values() if v[0] == "o"]
        assert len(set(ws)) == len(ws), f"step {s['seq']}: duplicate workstation"
        assert len(set(ch)) == len(ch), f"step {s['seq']}: duplicate channel"
        st = nxt
        history.append(st)
    return st, history


def bfs_reachable(old, target, offline):
    """Independent reachability check: BFS over every legal combination of
    per-device states.  Does NOT use the tool's planner."""
    ids = sorted(set(old) | set(target))
    init = tuple(("o",) + old[i] if i in old else ("a",) for i in ids)
    goal = tuple(("o",) + target[i] if i in target else ("a",) for i in ids)

    def neighbors(st):
        ws_occ, ch_occ = {}, {}
        for k, i in enumerate(ids):
            s = st[k]
            if s[0] == "o":
                ws_occ[s[1]] = i
                ch_occ[s[2]] = i
        for k, i in enumerate(ids):
            s = st[k]
            in_old, in_new = i in old, i in target
            if in_old and not in_new:                      # old-only: delete
                if s[0] == "o":
                    yield st[:k] + (("a",),) + st[k + 1:]
            elif in_new and not in_old:                    # new-only: insert
                if s[0] == "a":
                    w, c = target[i]
                    if w not in ws_occ and c not in ch_occ:
                        yield st[:k] + (("o", w, c),) + st[k + 1:]
            elif old[i] != target[i]:                      # changed binding
                w, c = target[i]
                if s[0] == "o":
                    if ws_occ.get(w, i) == i and ch_occ.get(c, i) == i:
                        yield st[:k] + (("o", w, c),) + st[k + 1:]
                    if i in offline:
                        yield st[:k] + (("f",),) + st[k + 1:]
                elif s[0] == "f":
                    if w not in ws_occ and c not in ch_occ:
                        yield st[:k] + (("o", w, c),) + st[k + 1:]

    seen = {init}
    dq = deque([init])
    while dq:
        st = dq.popleft()
        if st == goal:
            return True
        for nb in neighbors(st):
            if nb not in seen:
                seen.add(nb)
                dq.append(nb)
    return False


def check_instance(old, target, offline):
    """Planner verdict must equal independent BFS reachability; a produced
    order must simulate to the target with all prefixes valid."""
    try:
        order, _ = plan(old, target, offline)
        verdict = True
    except MigrationImpossible:
        verdict = False
    reach = bfs_reachable(old, target, offline)
    assert verdict == reach, (
        f"verdict mismatch: planner={verdict} bfs={reach}\n"
        f"old={old}\ntarget={target}\noffline={sorted(offline)}")
    if not verdict:
        return None
    final, history = simulate(old, order["steps"])
    assert set(final) == set(target), f"{set(final)} != {set(target)}"
    for i, v in final.items():
        assert v == ("o",) + target[i], f"{i}: {v}"
    shared = set(old) & set(target)
    for st in history:  # shared rows are never deleted+reinserted
        assert all(i in st for i in shared)
    return order


def random_instance(rng, overlap=False):
    ids_pool = ["d0", "d1", "d2", "d3"]
    if overlap:  # same strings usable in both columns
        ws_pool = ch_pool = ["N0", "N1", "N2", "N3"]
    else:
        ws_pool = ["W0", "W1", "W2", "W3"]
        ch_pool = ["C0", "C1", "C2", "C3"]

    def rand_table():
        ids = sorted(rng.sample(ids_pool, rng.randint(1, 4)))
        ws = rng.sample(ws_pool, len(ids))
        ch = rng.sample(ch_pool, len(ids))
        return {i: (w, c) for i, w, c in zip(ids, ws, ch)}

    old, target = rand_table(), rand_table()
    changed = [i for i in old if i in target and old[i] != target[i]]
    rng.shuffle(changed)
    offline = frozenset(changed[:rng.randint(0, len(changed))])
    return old, target, offline


def write_jsonl(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps({"id": r[0], "ws": r[1][0], "ch": r[1][1]}) + "\n")


# ----------------------------------------------------------------------- tests

def test_validation():
    bad_tables = [
        (['{"id": "X", "ws": "W1", "ch": "C1"}',
          '{"id": "X", "ws": "W2", "ch": "C2"}'], "duplicate id"),
        (['{"id": "X", "ws": "W1", "ch": "C1"}',
          '{"id": "Y", "ws": "W1", "ch": "C2"}'], "duplicate workstation"),
        (['{"id": "X", "ws": "W1", "ch": "C1"}',
          '{"id": "Y", "ws": "W2", "ch": "C1"}'], "duplicate channel"),
        (['{"id": "X", "ws": "", "ch": "C1"}'], "empty workstation"),
        (['{"id": "X", "ws": "W1"}'], "missing key"),
        (['{"id": "X", "ws": "W1", "ch": "C1", "n": 1}'], "extra key"),
        (['{"id": "X", "ws": 7, "ch": "C1"}'], "non-string"),
        (['not json'], "bad JSON"),
    ]
    for lines, why in bad_tables:
        try:
            validate_table(lines, "t")
            raise AssertionError(f"accepted invalid table: {why}")
        except SourceInvalid:
            pass
    # case sensitivity: W1 and w1 are different numbers
    t = validate_table(['{"id": "X", "ws": "W1", "ch": "C1"}',
                        '{"id": "Y", "ws": "w1", "ch": "c1"}'], "t")
    assert t == {"X": ("W1", "C1"), "Y": ("w1", "c1")}
    # offline list rules
    old = {"A": ("W1", "C1"), "B": ("W2", "C2")}
    target = {"A": ("W1", "C1"), "B": ("W3", "C2"), "C": ("W4", "C4")}
    for bad in (["A"], ["C"], ["ZZ"], ["B", "B"], ["B", 7], "B"):
        try:
            validate_offline_list(bad, old, target)
            raise AssertionError(f"accepted invalid offline list {bad!r}")
        except SourceInvalid:
            pass
    assert validate_offline_list(["B"], old, target) == frozenset({"B"})


def test_impossible_vs_invalid():
    # valid source, but a plain 2-swap without any offline permission
    old = {"P": ("W1", "C1"), "Q": ("W2", "C2")}
    target = {"P": ("W2", "C1"), "Q": ("W1", "C2")}
    try:
        plan(old, target, frozenset())
        raise AssertionError("should be impossible")
    except MigrationImpossible as e:
        assert e.report["kind"] == "migration_impossible"
    except SourceInvalid:
        raise AssertionError("must not be reported as invalid source")
    # invalid source must not be reported as unmigratable
    try:
        validate_table(['{"id": "P", "ws": "W1", "ch": "C1"}',
                        '{"id": "Q", "ws": "W1", "ch": "C2"}'], "old")
        raise AssertionError("should be invalid")
    except SourceInvalid:
        pass
    except MigrationImpossible:
        raise AssertionError("must not be reported as unmigratable")


def test_cycle_report():
    # A waits for B's workstation; B waits for C's workstation and A's
    # channel; C waits for B's channel.  Only A may go offline.
    old = {"A": ("WS-1", "CH-1"), "B": ("WS-2", "CH-2"), "C": ("WS-3", "CH-3")}
    target = {"A": ("WS-2", "CH-9"), "B": ("WS-3", "CH-1"), "C": ("WS-8", "CH-2")}
    try:
        plan(old, target, frozenset({"A"}))
        raise AssertionError("should be impossible")
    except MigrationImpossible as e:
        report = e.report
    devs = sorted(e["device"] for e in report["cycle"])
    assert devs == ["B", "C"], devs
    edges = {e["device"]: e["needs"] for e in report["cycle"]}
    assert edges["B"] == {"column": "ws", "number": "WS-3", "held_by": "C"}
    assert edges["C"] == {"column": "ch", "number": "CH-2", "held_by": "B"}
    ok, problems = verify_cycle_report(report, {"A"})
    assert ok, problems
    # a tampered report must fail verification
    bad = json.loads(json.dumps(report))
    bad["cycle"][0]["needs"]["held_by"] = "A"
    ok, _ = verify_cycle_report(bad, {"A"})
    assert not ok
    # with B also permitted the instance becomes solvable
    order, _ = plan(old, target, frozenset({"A", "B"}))
    assert [s["op"] for s in order["steps"]] == ["offline", "update", "update", "online"]
    final, _ = simulate(old, order["steps"])
    assert all(final[i] == ("o",) + target[i] for i in target)


def test_directed():
    # additions only
    order = check_instance({"X": ("W1", "C1")},
                           {"X": ("W1", "C1"), "Y": ("W2", "C2"), "Z": ("W3", "C3")},
                           frozenset())
    assert [s["op"] for s in order["steps"]] == ["insert", "insert"]
    # removals only
    order = check_instance({"X": ("W1", "C1"), "Y": ("W2", "C2"), "Z": ("W3", "C3")},
                           {"X": ("W1", "C1")}, frozenset())
    assert [s["op"] for s in order["steps"]] == ["delete", "delete"]
    # conflict on both resource types at once: full swap of both numbers
    old = {"P": ("W1", "C1"), "Q": ("W2", "C2")}
    target = {"P": ("W2", "C2"), "Q": ("W1", "C1")}
    assert check_instance(old, target, frozenset()) is None
    order = check_instance(old, target, frozenset({"Q"}))
    assert [s["op"] for s in order["steps"]] == ["offline", "update", "online"]
    # no-op: empty order
    order = check_instance({"X": ("W1", "C1")}, {"X": ("W1", "C1")}, frozenset())
    assert order["steps"] == []
    # unchanged devices are never touched
    order = check_instance({"U": ("W0", "C0"), "X": ("W1", "C1")},
                           {"U": ("W0", "C0"), "X": ("W2", "C2")}, frozenset())
    assert [s["id"] for s in order["steps"]] == ["X"]
    # a device swapping its own two numbers across columns
    order = check_instance({"X": ("W1", "C1"), "Y": ("W2", "C2")},
                           {"X": ("C1", "W1"), "Y": ("W2", "C2")}, frozenset())
    assert [s["op"] for s in order["steps"]] == ["update"]
    # an inserted device reuses exactly the numbers of a deleted one
    order = check_instance({"X": ("W1", "C1")}, {"Y": ("W1", "C1")}, frozenset())
    assert [s["op"] for s in order["steps"]] == ["delete", "insert"]
    # empty old table: insertions only
    order = check_instance({}, {"X": ("W1", "C1")}, frozenset())
    assert [s["op"] for s in order["steps"]] == ["insert"]
    # case sensitivity: W1 -> w1 is a real change, and W1/w1 coexist
    order = check_instance({"X": ("W1", "C1"), "Y": ("w1", "C2")},
                           {"X": ("w2", "C1"), "Y": ("W1", "C2")}, frozenset())
    assert order is not None


def test_reorder_invariance():
    rng = random.Random(7)
    checked = 0
    while checked < 30:
        old, target, offline = random_instance(rng)
        try:
            order0, _ = plan(old, target, offline)
        except MigrationImpossible:
            continue
        base = canon(order0)
        for _ in range(4):
            old_lines = [json.dumps({"id": i, "ws": w, "ch": c})
                         for i, (w, c) in old.items()]
            tgt_lines = [json.dumps({"id": i, "ws": w, "ch": c})
                         for i, (w, c) in target.items()]
            rng.shuffle(old_lines)
            rng.shuffle(tgt_lines)
            old2 = validate_table(old_lines, "old")
            target2 = validate_table(tgt_lines, "target")
            order2, _ = plan(old2, target2, offline)
            assert canon(order2) == base, "line order changed the work order"
        checked += 1
    # end-to-end through the CLI with shuffled files
    old = {"A": ("WS-1", "CH-1"), "B": ("WS-2", "CH-2"), "C": ("WS-3", "CH-3")}
    target = {"A": ("WS-2", "CH-9"), "B": ("WS-3", "CH-1"), "C": ("WS-8", "CH-2")}
    with tempfile.TemporaryDirectory() as d:
        outs = []
        for seed in range(5):
            r = random.Random(seed)
            ol, tl = sorted(old.items(), key=lambda x: r.random()), \
                     sorted(target.items(), key=lambda x: r.random())
            op, tp = os.path.join(d, "o.jsonl"), os.path.join(d, "t.jsonl")
            write_jsonl(op, ol)
            write_jsonl(tp, tl)
            off = os.path.join(d, "off.json")
            with open(off, "w") as f:
                json.dump(["A", "B"], f)
            out = os.path.join(d, f"order{seed}.json")
            cli("plan", "--old", op, "--target", tp, "--offline", off,
                "--out", out, expect_rc=0)
            with open(out) as f:
                outs.append(f.read())
        assert len(set(outs)) == 1, "CLI plan output depends on input line order"


def test_bfs_cross_check():
    rng = random.Random(20261122)
    solvable = unsolvable = 0
    for i in range(600):
        old, target, offline = random_instance(rng, overlap=(i % 3 == 2))
        if check_instance(old, target, offline) is None:
            unsolvable += 1
        else:
            solvable += 1
    assert solvable > 100 and unsolvable > 20, (solvable, unsolvable)
    print(f"    ({solvable} solvable, {unsolvable} unsolvable; verdicts all match BFS)")


def test_workload():
    n_chain, n_pairs = 1200, 600
    old, target, offline = {}, {}, set()
    for i in range(n_chain):  # d_i waits for d_{i+1}'s workstation
        old[f"d{i}"] = (f"W{i}", f"C{i}")
    for i in range(n_chain):
        target[f"d{i}"] = (f"W{i+1}" if i + 1 < n_chain else "WNEW", f"C{i}")
    for p in range(n_pairs):  # independent workstation-swap pairs
        a, b = f"pa{p}", f"pb{p}"
        old[a] = (f"SWA{p}", f"SCA{p}")
        old[b] = (f"SWB{p}", f"SCB{p}")
        target[a] = (f"SWB{p}", f"SCA{p}")
        target[b] = (f"SWA{p}", f"SCB{p}")
        offline.add(a)
    n = len(old)
    t0 = time.perf_counter()
    order, stats = plan(old, target, offline)
    plan_dt = time.perf_counter() - t0
    assert len(order["steps"]) == n_chain + 3 * n_pairs
    assert stats["offlines"] == n_pairs
    assert stats["cycle_scans"] <= n_pairs + 1, stats
    assert stats["release_events"] <= 3 * n, stats  # linear, no per-step rescans
    assert plan_dt < 10, plan_dt
    print(f"    (n={n} devices, {len(order['steps'])} steps, planned in "
          f"{plan_dt:.3f}s, stats={stats})")

    # apply the whole order to a real receiver, checking both unique
    # constraints in SQL after every committed step
    with tempfile.TemporaryDirectory() as d:
        r = Receiver(os.path.join(d, "r.db"))
        r.load_initial(old)
        dup_sql = """
          SELECT (SELECT COUNT(*) FROM (SELECT ws FROM bindings
                    WHERE ws IS NOT NULL GROUP BY ws HAVING COUNT(*) > 1)),
                 (SELECT COUNT(*) FROM (SELECT ch FROM bindings
                    WHERE ch IS NOT NULL GROUP BY ch HAVING COUNT(*) > 1))
        """
        t0 = time.perf_counter()
        for k in range(len(order["steps"])):
            res = r.submit(order, k)
            assert res["result"] == "committed"
            assert r.conn.execute(dup_sql).fetchone() == (0, 0), f"step {k}"
        apply_dt = time.perf_counter() - t0
        assert r.status()["complete"]
        assert r.bindings() == target
        print(f"    (applied {len(order['steps'])} steps in {apply_dt:.2f}s, "
              "uniqueness checked after every step)")


def test_receiver_crash_resume():
    # 8-long dependency chain plus one swap pair -> 11 steps
    old, target = {}, {}
    for i in range(8):
        old[f"d{i}"] = (f"W{i}", f"C{i}")
        target[f"d{i}"] = (f"W{i+1}" if i < 7 else "WNEW", f"C{i}")
    old["sa"], old["sb"] = ("SWA", "SCA"), ("SWB", "SCB")
    target["sa"], target["sb"] = ("SWB", "SCA"), ("SWA", "SCB")

    with tempfile.TemporaryDirectory() as d:
        op, tp = os.path.join(d, "o.jsonl"), os.path.join(d, "t.jsonl")
        write_jsonl(op, list(old.items()))
        write_jsonl(tp, list(target.items()))
        off = os.path.join(d, "off.json")
        with open(off, "w") as f:
            json.dump(["sa"], f)
        order_p = os.path.join(d, "order.json")
        db = os.path.join(d, "r.db")
        cli("plan", "--old", op, "--target", tp, "--offline", off,
            "--out", order_p, expect_rc=0)
        order = core.load_order(order_p)
        assert len(order["steps"]) == 11
        cli("init", "--db", db, "--table", op, expect_rc=0)
        con_ro = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        rowids0 = dict(con_ro.execute("SELECT id, rowid FROM bindings"))

        def status():
            return json.loads(cli("status", "--db", db, expect_rc=0).stdout)

        def rows():
            return json.loads(cli("status", "--db", db, "--dump",
                                  expect_rc=0).stdout)["bindings"]

        # crash #1: stop before committing step 4
        cli("run", "--db", db, "--order", order_p,
            "--crash-before-seq", "4", expect_rc=10)
        st = status()
        assert st["committed"] == 4 and not st["complete"], st
        state = replay_check(old, order["steps"][:4], keep_ids=set(old) & set(target))
        assert {r["id"]: (r["ws"], r["ch"]) for r in rows()} == state

        # crash #2: commit step 7, response lost
        cli("run", "--db", db, "--order", order_p,
            "--crash-after-commit-seq", "7", expect_rc=11)
        st = status()
        assert st["committed"] == 8, st
        state = replay_check(old, order["steps"][:8], keep_ids=set(old) & set(target))
        assert {r["id"]: (r["ws"], r["ch"]) for r in rows()} == state

        # the uncertain client resends step 7: verified no-op
        before = rows()
        res = json.loads(cli("submit", "--db", db, "--order", order_p,
                             "--seq", "7", expect_rc=0).stdout)
        assert res["result"] == "already_committed", res
        assert rows() == before and status()["committed"] == 8

        # gap: step 10 cannot be submitted while 8 is next
        res = json.loads(cli("submit", "--db", db, "--order", order_p,
                             "--seq", "10", expect_rc=5).stdout)
        assert res["code"] == "gap", res
        assert status()["committed"] == 8

        # a foreign order's step with the same seq is not a resend
        target2 = dict(target)
        target2["d0"] = ("W1", "C999")
        tp2 = os.path.join(d, "t2.jsonl")
        write_jsonl(tp2, list(target2.items()))
        order2_p = os.path.join(d, "order2.json")
        cli("plan", "--old", op, "--target", tp2, "--offline", off,
            "--out", order2_p, expect_rc=0)
        res = json.loads(cli("submit", "--db", db, "--order", order2_p,
                             "--seq", "0", expect_rc=5).stdout)
        assert res["code"] == "foreign_order", res
        assert status()["committed"] == 8

        # a fresh process resumes from the persisted position and finishes
        cli("run", "--db", db, "--order", order_p, expect_rc=0)
        st = status()
        assert st["committed"] == 11 and st["complete"], st

        # real rows match the target table row by row
        got = {r["id"]: (r["ws"], r["ch"]) for r in rows()}
        assert got == target, got

        # persisted step numbers are exactly 0..10 and match the order
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        persisted = dict(con.execute("SELECT seq, hash FROM steps ORDER BY seq"))
        assert sorted(persisted) == list(range(11)), persisted
        for k, s in enumerate(order["steps"]):
            assert persisted[k] == core.step_hash(s)

        # every committed prefix provably satisfies both unique constraints
        # and never drops a shared row
        for k in range(1, 12):
            replay_check(old, order["steps"][:k], keep_ids=set(old) & set(target))

        # rows present in both versions kept their row identity throughout:
        # their SQLite rowids are unchanged (a delete+reinsert would change them)
        shared = set(old) & set(target)
        rowids = dict(con.execute("SELECT id, rowid FROM bindings"))
        assert {i: rowids[i] for i in shared} == rowids0


def test_receiver_rejects():
    with tempfile.TemporaryDirectory() as d:
        r = Receiver(os.path.join(d, "r.db"))
        r.load_initial({"X": ("W1", "C1")})

        # a hand-crafted order violating the real unique key must be rolled back
        bad_steps = [{"seq": 0, "id": "Y", "op": "insert",
                      "before": {"state": "absent"},
                      "after": {"state": "online", "ws": "W1", "ch": "C9"}}]
        order = build_order({"X": ("W1", "C1")},
                            {"X": ("W1", "C1"), "Y": ("W1", "C9")}, bad_steps)
        try:
            r.submit(order, 0)
            raise AssertionError("unique violation not caught")
        except Reject as e:
            assert e.code == "constraint_violation", e.code
        assert r.status()["committed"] == 0
        assert r.bindings() == {"X": ("W1", "C1")}

        # a step whose before-state lies must be rejected without effect
        lie = [{"seq": 0, "id": "X", "op": "update",
                "before": {"state": "online", "ws": "W9", "ch": "C1"},
                "after": {"state": "online", "ws": "W2", "ch": "C2"}}]
        order2 = build_order({"X": ("W1", "C1")}, {"X": ("W2", "C2")}, lie)
        try:
            r.submit(order2, 0)
            raise AssertionError("state mismatch not caught")
        except Reject as e:
            assert e.code == "state_mismatch", e.code
        assert r.bindings() == {"X": ("W1", "C1")}
        assert r.status()["order_hash"] is None  # nothing was accepted

        # receiver with data cannot be re-initialised
        try:
            r.load_initial({"Z": ("W3", "C3")})
            raise AssertionError("re-init not caught")
        except Reject as e:
            assert e.code == "not_fresh", e.code

        # bad seq
        try:
            r.submit(order2, 5)
            raise AssertionError("bad seq not caught")
        except Reject as e:
            assert e.code == "bad_seq", e.code

        # initial-table mismatch on first acceptance
        order3, _ = plan({"X": ("W2", "C2")}, {"X": ("W3", "C3")}, frozenset())
        try:
            r.submit(order3, 0)
            raise AssertionError("initial mismatch not caught")
        except Reject as e:
            assert e.code == "initial_mismatch", e.code

        # empty order on a matching receiver completes immediately
        r2 = Receiver(os.path.join(d, "r2.db"))
        r2.load_initial({"X": ("W1", "C1")})
        empty, _ = plan({"X": ("W1", "C1")}, {"X": ("W1", "C1")}, frozenset())
        assert empty["steps"] == []
        assert r2.begin(empty)["complete"]


TESTS = [
    ("validation of source material", test_validation),
    ("invalid source vs. unmigratable distinction", test_impossible_vs_invalid),
    ("wait-cycle report and re-verification", test_cycle_report),
    ("directed cases", test_directed),
    ("input reorder invariance", test_reorder_invariance),
    ("BFS cross-check on random small instances", test_bfs_cross_check),
    ("workload: long chain + many swap groups", test_workload),
    ("receiver crash/resend/resume across processes", test_receiver_crash_resume),
    ("receiver rejects bad writes", test_receiver_rejects),
]


def main():
    for name, fn in TESTS:
        fn()
        print(f"PASS: {name}")
    print("\nALL TESTS PASSED")


if __name__ == "__main__":
    main()
