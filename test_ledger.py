#!/usr/bin/env python3
"""Test suite for the scan-attribution ledger.

  * build-time proof chain: snapshot digest, order identity and the
    receiver's persisted step numbers/hashes must all match;  an
    incomplete receiver is never treated as history
  * attribution semantics checked per scan against an independent
    replay implementation:  initial bindings before the first step, a
    workstation falling vacant and changing owner, same-second
    consecutive steps seen as all applied, never-bound workstations
  * counts stored per (UTC day, channel) with the unattributed group
    separate and no zero-count groups, cross-checked against counts
    derived independently from the raw input (not just totals, so
    mutually cancelling mistakes cannot hide)
  * corrections:  whole-group validation (unknown/duplicate steps,
    illegal times, out-of-order results leave no change), windowed
    re-attribution ([earlier, later) per changed step, same-workstation
    windows merged, previously unattributed scans included), net count
    movement, idempotent resend, multiple corrections on one ledger
  * crash after the correction's writes but before COMMIT:  reopening
    shows the old state, then the correction is applied and resent
  * scaling:  with runtime-generated masses of unrelated scans,
    correction access grows with the records inside the correction
    window, not with the base ledger size -- shown via the query plan
    and SQLite VM-step access counts, never via wall-clock timing
"""
import collections
import json
import os
import random
import sqlite3
import subprocess
import sys
import tempfile
import urllib.parse

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

from handover import core  # noqa: E402
from handover import ledger  # noqa: E402
from handover.core import SourceInvalid  # noqa: E402
from handover.ledger import Ledger, LedgerReject  # noqa: E402
from handover.receiver import Receiver  # noqa: E402

OLD = {"A": ("WS-1", "CH-1"), "B": ("WS-2", "CH-2"), "C": ("WS-3", "CH-3")}
TARGET = {"A": ("WS-2", "CH-9"), "B": ("WS-3", "CH-1"), "C": ("WS-8", "CH-2")}
TIMES0 = [100, 200, 300, 300]          # steps 2 and 3 share a second
# order steps: 0=offline B, 1=update A, 2=update C, 3=online B

SCAN_ROWS = [  # (id, ws, ts)
    ("S01", "WS-2", 50), ("S02", "WS-2", 99), ("S03", "WS-2", 100),
    ("S04", "WS-2", 150), ("S05", "WS-2", 199), ("S06", "WS-2", 200),
    ("S07", "WS-2", 225), ("S08", "WS-2", 249), ("S09", "WS-2", 250),
    ("S10", "WS-1", 199), ("S11", "WS-1", 200), ("S12", "WS-1", 249),
    ("S13", "WS-1", 250), ("S14", "WS-3", 299), ("S15", "WS-3", 300),
    ("S16", "WS-8", 299), ("S17", "WS-8", 300), ("S18", "WS-9", 210),
    ("S19", "WS-2", 500),
    ("S20", "WS-2", 86450),            # next UTC day
    ("S21", "WS-9", 90000),            # next UTC day, never-bound ws
]
SCANS = [{"id": i, "ws": w, "ts": t} for i, w, t in SCAN_ROWS]


# --------------------------------------------------------------------- helpers

def cli(*args, expect_rc=None):
    p = subprocess.run([sys.executable, "-m", "handover", *args],
                       capture_output=True, text=True,
                       env={**os.environ, "PYTHONPATH": ROOT})
    if expect_rc is not None:
        assert p.returncode == expect_rc, \
            f"handover {' '.join(args)}: rc={p.returncode}\n{p.stdout}\n{p.stderr}"
    return p


def make_order():
    order, _ = core.plan(OLD, TARGET, frozenset({"A", "B"}))
    assert [s["op"] for s in order["steps"]] == ["offline", "update",
                                                 "update", "online"]
    assert [s["id"] for s in order["steps"]] == ["B", "A", "C", "B"]
    return order


def make_receiver(d, order, commit=None, name="recv.db"):
    """A receiver with the order committed up to `commit` steps (all by
    default).  Returns the db path."""
    path = os.path.join(d, name)
    r = Receiver(path)
    r.load_initial(OLD)
    for k, s in enumerate(order["steps"]):
        if commit is not None and k >= commit:
            break
        r.submit(order, k)
    r.close()
    return path


def times_doc(order, times):
    return {"format": ledger.TIMES_FORMAT,
            "order_hash": order["order_hash"],
            "times": [{"seq": q, "ts": t} for q, t in enumerate(times)]}


def corr_doc(order, changes):
    return ledger.validate_correction(
        {"format": ledger.CORRECTION_FORMAT,
         "order_hash": order["order_hash"],
         "changes": [{"seq": q, "ts": t} for q, t in changes]})


def fresh_ledger(d, order, times=TIMES0, scans=SCANS, name="ledger.db"):
    path = os.path.join(d, name)
    recv = make_receiver(d, order, name=name + ".recv.db")
    Ledger.build(path, OLD, order, recv,
                 ledger.validate_step_times(times_doc(order, times), order),
                 list(scans))
    return path


def ref_attributions(initial, steps, times, scans):
    """Independent attribution: replay the committed prefix (steps with
    time <= scan ts, in seq order) per scan and look the workstation up.
    Shares no code with the ledger's per-workstation timelines."""
    out = {}
    for s in scans:
        state = {w: (i, c) for i, (w, c) in initial.items()}
        for step, t in zip(steps, times):
            if t > s["ts"]:
                break                      # times are non-decreasing
            b, a = step["before"], step["after"]
            if b["state"] == "online":
                assert state.get(b["ws"]) == (step["id"], b["ch"])
                del state[b["ws"]]
            if a["state"] == "online":
                state[a["ws"]] = (step["id"], a["ch"])
        out[s["id"]] = state.get(s["ws"], (None, None))
    return out


def ref_counts(scans, attrs):
    c = collections.Counter()
    for s in scans:
        c[(ledger.day_of(s["ts"]), attrs[s["id"]][1])] += 1
    return dict(c)


def ledger_scans(path):
    con = sqlite3.connect(f"file:{urllib.parse.quote(path)}?mode=ro", uri=True)
    rows = {r[0]: (r[1], r[2]) for r in
            con.execute("SELECT scan_id, device, ch FROM scans")}
    con.close()
    return rows


def ledger_counts(path):
    con = sqlite3.connect(f"file:{urllib.parse.quote(path)}?mode=ro", uri=True)
    rows = {(d, c): n for d, c, n in con.execute("SELECT day, ch, n FROM counts")}
    zeros = con.execute("SELECT COUNT(*) FROM counts WHERE n <= 0").fetchone()[0]
    con.close()
    assert zeros == 0, "zero-count groups must never be stored"
    return rows


def ledger_times(path):
    con = sqlite3.connect(f"file:{urllib.parse.quote(path)}?mode=ro", uri=True)
    rows = [t for (t,) in con.execute("SELECT ts FROM steps ORDER BY seq")]
    con.close()
    return rows


def dump_db(path):
    con = sqlite3.connect(f"file:{urllib.parse.quote(path)}?mode=ro", uri=True)
    out = {}
    for t in ("meta", "initial", "steps", "scans", "counts", "corrections"):
        out[t] = sorted(con.execute(f"SELECT * FROM {t}"), key=repr)
    con.close()
    return out


def check_against_reference(path, order, times, scans):
    """Per-scan attribution and full count-table comparison against the
    independent replay -- per group, never just totals."""
    attrs = ref_attributions(OLD, order["steps"], times, scans)
    got = ledger_scans(path)
    assert set(got) == {s["id"] for s in scans}
    for s in scans:
        assert got[s["id"]] == attrs[s["id"]], \
            f"scan {s['id']}: ledger {got[s['id']]} != reference {attrs[s['id']]}"
    assert ledger_counts(path) == ref_counts(scans, attrs)
    assert ledger_times(path) == list(times)
    return attrs


def measure_ops(conn, fn):
    """Count SQLite VM instructions executed while running fn()."""
    ops = [0]

    def handler():
        ops[0] += 1
        return 0

    conn.set_progress_handler(handler, 1)
    try:
        fn()
    finally:
        conn.set_progress_handler(None, 0)
    return ops[0]


# ----------------------------------------------------------------------- tests

def test_build_validation():
    order = make_order()
    times = ledger.validate_step_times(times_doc(order, TIMES0), order)
    with tempfile.TemporaryDirectory() as d:
        recv = make_receiver(d, order)

        def attempt(name, **kw):
            args = dict(snapshot_rows=OLD, order=order, receiver_path=recv,
                        times=times, scans=list(SCANS))
            args.update(kw)
            path = os.path.join(d, name)
            try:
                Ledger.build(path, **args)
            except (LedgerReject, SourceInvalid):
                assert not os.path.exists(path), f"{name}: partial ledger left"
                return
            raise AssertionError(f"{name}: build unexpectedly succeeded")

        # the receiver has not committed the whole order: not history
        part = make_receiver(d, order, commit=2, name="part.db")
        attempt("l1.db", receiver_path=part)
        # a receiver that never accepted any order
        empty_recv = os.path.join(d, "empty.db")
        Receiver(empty_recv).load_initial(OLD)
        attempt("l2.db", receiver_path=empty_recv)
        # a receiver that completed a *different* order
        order2, _ = core.plan(OLD, {**TARGET, "C": ("WS-8", "CH-5")},
                              frozenset({"A", "B"}))
        recv2 = make_receiver(d, order2, name="recv2.db")
        attempt("l3.db", receiver_path=recv2)
        # snapshot digest must match the order's initial table
        attempt("l4.db", snapshot_rows=TARGET)
        # times: missing / duplicate / out-of-range / decreasing / bool
        bad_times = [
            {"format": ledger.TIMES_FORMAT, "order_hash": order["order_hash"],
             "times": [{"seq": 0, "ts": 100}]},
            {"format": ledger.TIMES_FORMAT, "order_hash": order["order_hash"],
             "times": [{"seq": 0, "ts": 100}, {"seq": 0, "ts": 100},
                       {"seq": 1, "ts": 200}, {"seq": 2, "ts": 300},
                       {"seq": 3, "ts": 300}]},
            {"format": ledger.TIMES_FORMAT, "order_hash": order["order_hash"],
             "times": [{"seq": q, "ts": t} for q, t in
                       enumerate([100, 200, 300, ledger.MAX_TS + 1])]},
            {"format": ledger.TIMES_FORMAT, "order_hash": order["order_hash"],
             "times": [{"seq": q, "ts": t} for q, t in
                       enumerate([100, 200, 300, 250])]},
            {"format": ledger.TIMES_FORMAT, "order_hash": order["order_hash"],
             "times": [{"seq": q, "ts": t} for q, t in
                       enumerate([100, True, 300, 300])]},
            {"format": ledger.TIMES_FORMAT, "order_hash": order["order_hash"],
             "times": [{"seq": q, "ts": t} for q, t in
                       enumerate([100, 200, 300, -1])]},
        ]
        for k, bt in enumerate(bad_times):
            try:
                ledger.validate_step_times(bt, order)
                raise AssertionError(f"bad times {k} accepted")
            except SourceInvalid:
                pass
        # times naming a different order
        try:
            ledger.validate_step_times(times_doc(order2, TIMES0), order)
            raise AssertionError("foreign times accepted")
        except LedgerReject as e:
            assert e.code == "foreign_order"
        # scans: duplicate id / bad shapes / bad ts
        bad_scans = [
            ['{"id":"X","ws":"WS-1","ts":1}', '{"id":"X","ws":"WS-2","ts":2}'],
            ['{"id":"X","ws":"WS-1","ts":-3}'],
            ['{"id":"X","ws":"WS-1","ts":253402300800}'],
            ['{"id":"X","ws":"WS-1","ts":true}'],
            ['{"id":"X","ws":"WS-1"}'],
            ['{"id":"X","ws":"","ts":1}'],
            ['not json'],
        ]
        for k, lines in enumerate(bad_scans):
            try:
                ledger.validate_scans(lines)
                raise AssertionError(f"bad scans {k} accepted")
            except SourceInvalid:
                pass
        # a tampered receiver step log must not match
        con = sqlite3.connect(recv)
        con.execute("UPDATE steps SET hash = 'tampered' WHERE seq = 1")
        con.commit()
        con.close()
        attempt("l5.db")
        # the max legal timestamp is accepted (9999-12-31)
        ok_scans = [{"id": "Z", "ws": "WS-2", "ts": ledger.MAX_TS}]
        path = os.path.join(d, "ok.db")
        Ledger.build(path, OLD, order, make_receiver(d, order, name="recv3.db"), times, ok_scans)
        assert ledger.day_of(ledger.MAX_TS) == "9999-12-31"
        assert ledger_scans(path)["Z"] == ("A", "CH-9")
        # building twice over the same path is refused
        try:
            Ledger.build(path, OLD, order, recv, times, ok_scans)
            raise AssertionError("second build accepted")
        except LedgerReject as e:
            assert e.code == "ledger_exists"


def test_attribution_and_counts():
    order = make_order()
    with tempfile.TemporaryDirectory() as d:
        path = fresh_ledger(d, order)
        attrs = check_against_reference(path, order, TIMES0, SCANS)
        # the directed expectations behind the reference run
        expect = {
            "S01": ("B", "CH-2"),   # initial binding, before step 0
            "S02": ("B", "CH-2"),   # last second before the vacancy
            "S03": (None, None),    # step 0's second: B is already offline
            "S04": (None, None),    # vacant between owners -- not B's leak
            "S05": (None, None),    # last vacant second
            "S06": ("A", "CH-9"),   # step 1's second: A holds WS-2 now
            "S09": ("A", "CH-9"),
            "S10": ("A", "CH-1"),   # A still on WS-1 with its old channel
            "S11": (None, None),    # A left WS-1; nobody ever takes it
            "S13": (None, None),
            "S14": ("C", "CH-3"),
            "S15": ("B", "CH-1"),   # steps 2+3 share a second: the scan sees
                                    # both applied in order, not the vacant
                                    # intermediate state
            "S16": (None, None),
            "S17": ("C", "CH-2"),
            "S18": (None, None),    # WS-9 is never bound
            "S20": ("A", "CH-9"),   # next UTC day
            "S21": (None, None),
        }
        for sid, want in expect.items():
            assert attrs[sid] == want, f"{sid}: {attrs[sid]} != {want}"
        # the unattributed group is stored as its own (NULL channel) group,
        # on both days, and every stored group is non-zero
        counts = ledger_counts(path)
        assert counts[("1970-01-01", None)] == 8
        assert counts[("1970-01-02", None)] == 1
        assert counts[("1970-01-01", "CH-9")] == 5
        assert counts[("1970-01-02", "CH-9")] == 1
        assert sum(counts.values()) == len(SCANS)


def test_correction_window():
    order = make_order()
    with tempfile.TemporaryDirectory() as d:
        path = fresh_ledger(d, order)
        lg = Ledger(path)
        res = lg.correct(corr_doc(order, [(1, 250)]))  # step 1: 200 -> 250
        assert res["result"] == "applied"
        summ = res["summary"]
        # windows: exactly the involved workstations, [earlier, later)
        assert summ["windows"] == [{"ws": "WS-1", "lo": 200, "hi": 250},
                                   {"ws": "WS-2", "lo": 200, "hi": 250}]
        assert summ["times"] == [{"seq": 1, "ts": 250}]
        moved = {m["id"]: m for m in summ["moved"]}
        # WS-2 scans inside the window lose A; WS-1 scans inside the window
        # (previously unattributed) gain A with its *old* channel
        assert set(moved) == {"S06", "S07", "S08", "S11", "S12"}
        assert moved["S06"]["old"] == {"device": "A", "ch": "CH-9"}
        assert moved["S06"]["new"] == {"device": None, "ch": None}
        assert moved["S11"]["old"] == {"device": None, "ch": None}
        assert moved["S11"]["new"] == {"device": "A", "ch": "CH-1"}
        # net count movement, unattributed group included
        net = {(n["day"], n["ch"]): n["delta"] for n in summ["net"]}
        assert net == {("1970-01-01", "CH-9"): -3,
                       ("1970-01-01", "CH-1"): 2,
                       ("1970-01-01", None): 1}, net
        lg.close()
        # boundary scans: 199 untouched (before the window), 250 untouched
        # (window is exclusive at the later end), other workstations and
        # other days untouched -- full per-scan + per-group comparison
        attrs = check_against_reference(path, order, [100, 250, 300, 300], SCANS)
        assert attrs["S05"] == (None, None)
        assert attrs["S06"] == (None, None)      # was A/CH-9
        assert attrs["S09"] == ("A", "CH-9")     # new boundary, still A
        assert attrs["S10"] == ("A", "CH-1")     # before the window
        assert attrs["S11"] == ("A", "CH-1")     # was unattributed
        assert attrs["S13"] == (None, None)      # window's exclusive end
        assert attrs["S15"] == ("B", "CH-1")     # untouched same-second case
        assert attrs["S18"] == (None, None)      # WS-9: inside the window's
                                                 # time range, wrong ws
        assert attrs["S20"] == ("A", "CH-9")     # another day entirely


def test_correction_merge():
    order = make_order()
    with tempfile.TemporaryDirectory() as d:
        path = fresh_ledger(d, order)
        lg = Ledger(path)
        # step 0: 100 -> 40 and step 1: 200 -> 90;  their WS-2 windows
        # [40,100) and [90,200) intersect and must merge into [40,200)
        res = lg.correct(corr_doc(order, [(0, 40), (1, 90)]))
        summ = res["summary"]
        assert summ["windows"] == [{"ws": "WS-1", "lo": 90, "hi": 200},
                                   {"ws": "WS-2", "lo": 40, "hi": 200}]
        moved = {m["id"]: m for m in summ["moved"]}
        assert set(moved) == {"S01", "S02", "S03", "S04", "S05", "S10"}
        assert moved["S01"]["new"] == {"device": None, "ch": None}   # B left at 40
        assert moved["S02"]["old"] == {"device": "B", "ch": "CH-2"}  # B -> A hop
        assert moved["S02"]["new"] == {"device": "A", "ch": "CH-9"}  # at ts 99
        assert moved["S03"]["new"] == {"device": "A", "ch": "CH-9"}  # was vacant
        assert moved["S10"]["new"] == {"device": None, "ch": None}   # A left WS-1
        lg.close()
        check_against_reference(path, order, [40, 90, 300, 300], SCANS)


def test_correction_rejections():
    order = make_order()
    with tempfile.TemporaryDirectory() as d:
        path = fresh_ledger(d, order)
        before = dump_db(path)

        def rejected(doc, code):
            lg = Ledger(path)
            try:
                lg.correct(doc)
                raise AssertionError(f"{code} accepted")
            except LedgerReject as e:
                assert e.code == code, (e.code, code)
            finally:
                lg.close()
            assert dump_db(path) == before, f"{code} left changes"

        def invalid(obj):
            try:
                ledger.validate_correction(obj)
                raise AssertionError("invalid correction accepted")
            except SourceInvalid:
                pass
            assert dump_db(path) == before

        # unknown step: a correction cannot add steps
        rejected(corr_doc(order, [(9, 150)]), "bad_correction")
        # out-of-order results, both directions
        rejected(corr_doc(order, [(1, 350)]), "order_violation")   # past step 2
        rejected(corr_doc(order, [(3, 250)]), "order_violation")   # before step 2
        rejected(corr_doc(order, [(0, 250), (1, 200)]), "order_violation")
        # a correction naming another order
        order2, _ = core.plan(OLD, {**TARGET, "C": ("WS-8", "CH-5")},
                              frozenset({"A", "B"}))
        rejected(corr_doc(order2, [(1, 250)]), "foreign_order")
        # structural problems: duplicate/negative/bool/huge/empty/missing keys
        invalid({"format": ledger.CORRECTION_FORMAT,
                 "order_hash": order["order_hash"],
                 "changes": [{"seq": 1, "ts": 150}, {"seq": 1, "ts": 160}]})
        invalid({"format": ledger.CORRECTION_FORMAT,
                 "order_hash": order["order_hash"],
                 "changes": [{"seq": 1, "ts": -1}]})
        invalid({"format": ledger.CORRECTION_FORMAT,
                 "order_hash": order["order_hash"],
                 "changes": [{"seq": 1, "ts": True}]})
        invalid({"format": ledger.CORRECTION_FORMAT,
                 "order_hash": order["order_hash"],
                 "changes": [{"seq": 1, "ts": ledger.MAX_TS + 1}]})
        invalid({"format": ledger.CORRECTION_FORMAT,
                 "order_hash": order["order_hash"], "changes": []})
        invalid({"format": ledger.CORRECTION_FORMAT,
                 "order_hash": order["order_hash"],
                 "changes": [{"seq": 1}]})
        invalid({"format": "handover-order/1",
                 "order_hash": order["order_hash"],
                 "changes": [{"seq": 1, "ts": 150}]})
        # correcting a db that is not a ledger (the receiver itself)
        recv = make_receiver(d, order, name="recv-not-ledger.db")
        lg = None
        try:
            lg = Ledger(recv)
            raise AssertionError("receiver accepted as ledger")
        except LedgerReject as e:
            assert e.code == "not_a_ledger"
        # correcting a missing db
        try:
            Ledger(os.path.join(d, "nope.db"))
            raise AssertionError("missing ledger accepted")
        except LedgerReject as e:
            assert e.code == "ledger_missing"
        assert dump_db(path) == before


def test_multiple_corrections_and_resend():
    order = make_order()
    with tempfile.TemporaryDirectory() as d:
        path = fresh_ledger(d, order)
        lg = Ledger(path)
        c1 = corr_doc(order, [(1, 250)])
        c2 = corr_doc(order, [(0, 50)])
        r1 = lg.correct(c1)
        assert r1["result"] == "applied"
        r2 = lg.correct(c2)
        assert r2["result"] == "applied"
        assert r2["summary"]["windows"] == [{"ws": "WS-2", "lo": 50, "hi": 100}]
        lg.close()
        # the two corrections compose: final state equals the reference
        # under times [50, 250, 300, 300]
        attrs = check_against_reference(path, order, [50, 250, 300, 300], SCANS)
        assert attrs["S01"] == (None, None)      # B left WS-2 at 50
        assert attrs["S02"] == (None, None)
        assert attrs["S06"] == (None, None)      # A arrives at 250 now
        # resending an already-applied identical correction is a no-op
        snap = dump_db(path)
        lg = Ledger(path)
        again = lg.correct(c1)
        assert again["result"] == "already_applied"
        assert again["correction"] == r1["correction"]
        again2 = lg.correct(c2)
        assert again2["result"] == "already_applied"
        lg.close()
        assert dump_db(path) == snap, "resent corrections must not double-book"
        # both corrections are on record, in application order
        con = sqlite3.connect(f"file:{urllib.parse.quote(path)}?mode=ro",
                              uri=True)
        hashes = [h for (h,) in con.execute("SELECT hash FROM corrections "
                                            "ORDER BY rowid")]
        con.close()
        assert hashes == [r1["correction"], r2["correction"]]


def test_empty_order():
    old = {"X": ("WS-1", "CH-1")}
    order, _ = core.plan(old, old, frozenset())
    assert order["steps"] == []
    with tempfile.TemporaryDirectory() as d:
        recv = os.path.join(d, "recv.db")
        r = Receiver(recv)
        r.load_initial(old)
        r.begin(order)
        assert r.status()["complete"]
        r.close()
        path = os.path.join(d, "ledger.db")
        times = ledger.validate_step_times(times_doc(order, []), order)
        scans = [{"id": "E1", "ws": "WS-1", "ts": 10},
                 {"id": "E2", "ws": "WS-2", "ts": 20}]
        Ledger.build(path, old, order, recv, times, scans)
        assert ledger_scans(path) == {"E1": ("X", "CH-1"), "E2": (None, None)}
        lg = Ledger(path)
        try:
            lg.correct(corr_doc(order, [(0, 5)]))
            raise AssertionError("correction on a stepless order accepted")
        except LedgerReject as e:
            assert e.code == "bad_correction"
        lg.close()


def test_crash_before_commit():
    order = make_order()
    with tempfile.TemporaryDirectory() as d:
        # build the whole fixture through the CLI, as operations would
        old_p = os.path.join(d, "old.jsonl")
        tgt_p = os.path.join(d, "target.jsonl")
        off_p = os.path.join(d, "offline.json")
        order_p = os.path.join(d, "order.json")
        recv_p = os.path.join(d, "recv.db")
        with open(old_p, "w") as f:
            for i, (w, c) in OLD.items():
                f.write(json.dumps({"id": i, "ws": w, "ch": c}) + "\n")
        with open(tgt_p, "w") as f:
            for i, (w, c) in TARGET.items():
                f.write(json.dumps({"id": i, "ws": w, "ch": c}) + "\n")
        with open(off_p, "w") as f:
            json.dump(["A", "B"], f)
        cli("plan", "--old", old_p, "--target", tgt_p, "--offline", off_p,
            "--out", order_p, expect_rc=0)
        cli("init", "--db", recv_p, "--table", old_p, expect_rc=0)
        cli("run", "--db", recv_p, "--order", order_p, expect_rc=0)
        order = core.load_order(order_p)
        times_p = os.path.join(d, "times.json")
        with open(times_p, "w") as f:
            json.dump(times_doc(order, TIMES0), f)
        scans_p = os.path.join(d, "scans.jsonl")
        with open(scans_p, "w") as f:
            for s in SCANS:
                f.write(json.dumps(s) + "\n")
        db_p = os.path.join(d, "ledger.db")
        out = json.loads(cli("ledger-build", "--db", db_p, "--snapshot", old_p,
                             "--order", order_p, "--receiver", recv_p,
                             "--times", times_p, "--scans", scans_p,
                             expect_rc=0).stdout)
        assert out["result"] == "built" and out["scans"] == len(SCANS)

        corr_p = os.path.join(d, "corr.json")
        with open(corr_p, "w") as f:
            json.dump({"format": ledger.CORRECTION_FORMAT,
                       "order_hash": order["order_hash"],
                       "changes": [{"seq": 0, "ts": 50}]}, f)
        snap0 = dump_db(db_p)
        # crash after the correction's writes, before COMMIT
        cli("ledger-correct", "--db", db_p, "--correction", corr_p,
            "--crash-before-commit", expect_rc=12)
        assert dump_db(db_p) == snap0, "uncommitted correction must roll back"
        st = json.loads(cli("ledger-status", "--db", db_p, expect_rc=0).stdout)
        assert [s["ts"] for s in st["steps"]] == TIMES0
        assert st["corrections"] == []
        # now apply it for real
        out = json.loads(cli("ledger-correct", "--db", db_p,
                             "--correction", corr_p, expect_rc=0).stdout)
        assert out["result"] == "applied"
        st = json.loads(cli("ledger-status", "--db", db_p, expect_rc=0).stdout)
        assert [s["ts"] for s in st["steps"]] == [50, 200, 300, 300]
        assert len(st["corrections"]) == 1
        # and resend it: verified no-op
        out = json.loads(cli("ledger-correct", "--db", db_p,
                             "--correction", corr_p, expect_rc=0).stdout)
        assert out["result"] == "already_applied"
        st2 = json.loads(cli("ledger-status", "--db", db_p, "--scans",
                             expect_rc=0).stdout)
        assert len(st2["corrections"]) == 1
        check_against_reference(db_p, order, [50, 200, 300, 300], SCANS)


def test_windowed_access_scaling():
    order = make_order()
    rng = random.Random(20261122)

    def gen_scans(n_unrelated, n_window):
        scans = []
        for i in range(n_unrelated):  # never inside the correction window
            if i % 2:
                scans.append({"id": f"N{i}", "ws": f"WS-Q{i}",
                              "ts": rng.randint(0, 300000)})
            else:
                scans.append({"id": f"N{i}", "ws": "WS-2" if i % 4 else "WS-1",
                              "ts": rng.choice([rng.randint(0, 39),
                                                rng.randint(300, 300000)])})
        for i in range(n_window):     # inside [200, 250) on WS-1/WS-2
            scans.append({"id": f"W{i}", "ws": "WS-1" if i % 2 else "WS-2",
                          "ts": 200 + i % 50})
        return scans

    with tempfile.TemporaryDirectory() as d:
        small = gen_scans(20000, 50)
        big = gen_scans(200000, 50)
        wide = gen_scans(20000, 500)
        # the window scans of `small` and `big` differ (rng state); rebuild
        # `big` with the *same* window scans as `small` for a fair compare
        big[-50:] = small[-50:]
        la = fresh_ledger(d, order, scans=small, name="a.db")
        lb = fresh_ledger(d, order, scans=big, name="b.db")
        lc = fresh_ledger(d, order, scans=wide, name="c.db")

        # the candidate query is an index range search, never a table scan
        con = sqlite3.connect(f"file:{urllib.parse.quote(lb)}?mode=ro",
                              uri=True)
        plan = [r[3] for r in con.execute("EXPLAIN QUERY PLAN " +
                                          ledger.CANDIDATE_SQL,
                                          ("WS-2", 200, 250))]
        con.close()
        assert any(p.startswith("SEARCH scans USING INDEX scans_ws_ts")
                   for p in plan), plan
        assert not any(p.startswith("SCAN") for p in plan), plan

        def candidates(db):
            con = sqlite3.connect(f"file:{urllib.parse.quote(db)}?mode=ro",
                                  uri=True)
            rows = []

            def run():
                for ws in ("WS-1", "WS-2"):
                    rows.extend(con.execute(ledger.CANDIDATE_SQL,
                                            (ws, 200, 250)).fetchall())
            ops = measure_ops(con, run)
            con.close()
            return ops, rows

        ops_a, rows_a = candidates(la)
        ops_b, rows_b = candidates(lb)
        ops_c, rows_c = candidates(lc)
        assert len(rows_a) == len(rows_b) == 50 and len(rows_c) == 500
        assert rows_a == rows_b
        # 10x the base ledger, same window: access stays essentially flat
        assert ops_b < ops_a * 1.5 + 100, (ops_a, ops_b)
        # 10x the records inside the window: access grows with the window
        assert ops_c > ops_a * 3, (ops_a, ops_c)

        # whole corrections show the same window-bounded behaviour
        doc = corr_doc(order, [(1, 250)])
        lg_a, lg_b = Ledger(la), Ledger(lb)
        ops_ca = measure_ops(lg_a.conn, lambda: lg_a.correct(doc))
        ops_cb = measure_ops(lg_b.conn, lambda: lg_b.correct(doc))
        assert ops_cb < ops_ca * 2, (ops_ca, ops_cb)
        lg_a.close()
        lg_b.close()
        # and both ledgers end up equal to the reference, detail and counts
        check_against_reference(la, order, [100, 250, 300, 300], small)
        check_against_reference(lb, order, [100, 250, 300, 300], big)
        print(f"    (candidate VM-ops: base 20k+50w -> {ops_a}, "
              f"base 200k+50w -> {ops_b}, base 20k+500w -> {ops_c}; "
              f"whole-correction ops {ops_ca} vs {ops_cb})")


def test_ledger_cli():
    order = make_order()
    with tempfile.TemporaryDirectory() as d:
        recv = make_receiver(d, order)
        old_p = os.path.join(d, "old.jsonl")
        with open(old_p, "w") as f:
            for i, (w, c) in OLD.items():
                f.write(json.dumps({"id": i, "ws": w, "ch": c}) + "\n")
        order_p = os.path.join(d, "order.json")
        with open(order_p, "w") as f:
            f.write(core.dump_order(order))
        times_p = os.path.join(d, "times.json")
        with open(times_p, "w") as f:
            json.dump(times_doc(order, TIMES0), f)
        scans_p = os.path.join(d, "scans.jsonl")
        with open(scans_p, "w") as f:
            for s in SCANS:
                f.write(json.dumps(s) + "\n")
        db_p = os.path.join(d, "ledger.db")
        out = json.loads(cli("ledger-build", "--db", db_p, "--snapshot", old_p,
                             "--order", order_p, "--receiver", recv,
                             "--times", times_p, "--scans", scans_p,
                             expect_rc=0).stdout)
        assert out["result"] == "built"
        # a second build over the same path is rejected
        out = json.loads(cli("ledger-build", "--db", db_p, "--snapshot", old_p,
                             "--order", order_p, "--receiver", recv,
                             "--times", times_p, "--scans", scans_p,
                             expect_rc=5).stdout)
        assert out["code"] == "ledger_exists"
        # status on the receiver db (not a ledger) is rejected
        out = json.loads(cli("ledger-status", "--db", recv,
                             expect_rc=5).stdout)
        assert out["code"] == "not_a_ledger"
        # an invalid correction file exits 3 and changes nothing
        snap = dump_db(db_p)
        bad_p = os.path.join(d, "bad.json")
        with open(bad_p, "w") as f:
            json.dump({"format": ledger.CORRECTION_FORMAT,
                       "order_hash": order["order_hash"],
                       "changes": [{"seq": 1, "ts": -5}]}, f)
        out = json.loads(cli("ledger-correct", "--db", db_p,
                             "--correction", bad_p, expect_rc=3).stdout)
        assert out["result"] == "invalid_source"
        assert dump_db(db_p) == snap
        # a valid correction via CLI
        corr_p = os.path.join(d, "corr.json")
        with open(corr_p, "w") as f:
            json.dump({"format": ledger.CORRECTION_FORMAT,
                       "order_hash": order["order_hash"],
                       "changes": [{"seq": 1, "ts": 250}]}, f)
        out = json.loads(cli("ledger-correct", "--db", db_p,
                             "--correction", corr_p, expect_rc=0).stdout)
        assert out["result"] == "applied"
        st = json.loads(cli("ledger-status", "--db", db_p, "--scans",
                            expect_rc=0).stdout)
        assert [s["ts"] for s in st["steps"]] == [100, 250, 300, 300]
        assert len(st["scan_details"]) == len(SCANS)
        by_id = {s["id"]: s for s in st["scan_details"]}
        assert by_id["S06"]["device"] is None and by_id["S06"]["ch"] is None
        assert by_id["S11"]["device"] == "A" and by_id["S11"]["ch"] == "CH-1"
        check_against_reference(db_p, order, [100, 250, 300, 300], SCANS)


TESTS = [
    ("build-time proof chain and input validation", test_build_validation),
    ("attribution and stored counts vs independent replay",
     test_attribution_and_counts),
    ("correction: windowed re-attribution with net counts",
     test_correction_window),
    ("correction: intersecting same-workstation windows merge",
     test_correction_merge),
    ("correction: rejections leave no change", test_correction_rejections),
    ("multiple corrections and idempotent resend",
     test_multiple_corrections_and_resend),
    ("empty order edge case", test_empty_order),
    ("crash after writes, before commit; reopen, apply, resend",
     test_crash_before_commit),
    ("correction access scales with the window, not the base ledger",
     test_windowed_access_scaling),
    ("ledger CLI end to end", test_ledger_cli),
]


def main():
    for name, fn in TESTS:
        fn()
        print(f"PASS: {name}")
    print("\nALL LEDGER TESTS PASSED")


if __name__ == "__main__":
    main()
