#!/usr/bin/env python3
"""Test suite for the offline per-package missing-scan verification.

  * input validation:  malformed rows, duplicate package routes (even
    identical ones), duplicate scan associations, dangling references --
    all rejected and none of them leaves a result database (or a .tmp)
    behind
  * directed semantics:  an earlier scan that cannot reach the next
    station in time while a later rescan can;  closed-interval
    boundaries;  same-second scans barred from adjacent stations;
    deterministic (second, scan id) tie-breaking;  one channel reused at
    different stations;  empty prefix vs. full pass;  associated but
    unattributed scans and unassociated scans excluded;  1-station and
    16-station routes
  * several hundred randomized instances cross-checked against an
    independent pairwise (O(pairs)) reference implementation, plus input
    shuffling invariance
  * a real ledger corrected once, then re-verified:  package conclusions
    and evidence (not channel totals) are compared before/after
  * snapshot guarantee:  a correction attempted mid-verification cannot
    commit, and the run still reflects exactly one committed state
  * dense rescans:  VM-op counts and peak Python memory grow with the
    number of candidate scans, not with the number of matching scan
    pairs (never wall-clock timing)
"""
import collections
import json
import os
import random
import sqlite3
import subprocess
import sys
import tempfile
import tracemalloc
import urllib.parse

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

from handover import core, ledger  # noqa: E402
from handover.core import SourceInvalid  # noqa: E402
from handover.ledger import Ledger  # noqa: E402
from handover.receiver import Receiver  # noqa: E402
from handover.verify import (  # noqa: E402
    STAGING_SCHEMA, SQL_PROBE_LO, SQL_PROBE_HI,
    VerifyReject, _decode_key, _pair_key, verify, verify_result,
)

OLD = {"A": ("WS-1", "CH-1"), "B": ("WS-2", "CH-2"), "C": ("WS-3", "CH-3")}
TARGET = {"A": ("WS-2", "CH-9"), "B": ("WS-3", "CH-1"), "C": ("WS-8", "CH-2")}
TIMES0 = [100, 200, 300, 300]          # steps 2 and 3 share a second


# --------------------------------------------------------------------- helpers

def cli(*args, expect_rc=None):
    p = subprocess.run([sys.executable, "-m", "handover", *args],
                       capture_output=True, text=True,
                       env={**os.environ, "PYTHONPATH": ROOT})
    if expect_rc is not None:
        assert p.returncode == expect_rc, \
            f"handover {' '.join(args)}: rc={p.returncode}\n{p.stdout}\n{p.stderr}"
    return p


def make_ledger(path, scans, order_hash="0" * 64):
    """A minimal but faithful handover-scan-ledger/1 db.

    scans: iterable of (scan_id, ws, ts, device, ch);  ch None marks an
    unattributed scan."""
    scans = list(scans)
    con = sqlite3.connect(path)
    con.executescript(ledger.SCHEMA)
    counts = collections.Counter()
    for sid, w, ts, dev, ch in scans:
        counts[(ledger.day_of(ts), ch)] += 1
    con.executemany(
        "INSERT INTO scans (scan_id, ws, ts, device, ch) VALUES (?,?,?,?,?)",
        scans)
    con.executemany(
        "INSERT INTO counts (day, ch, n) VALUES (?,?,?)",
        [(d, c, n) for (d, c), n in counts.items()])
    con.executemany("INSERT INTO meta (k, v) VALUES (?, ?)", [
        ("format", ledger.LEDGER_FORMAT), ("order_hash", order_hash),
        ("initial_hash", "0" * 64), ("final_hash", "0" * 64),
        ("step_count", "0"), ("scan_count", str(len(scans)))])
    con.commit()
    con.close()


def route_line(pkg, route):
    return json.dumps({"pkg": pkg, "route": route}, ensure_ascii=False)


def assoc_line(pkg, scan):
    return json.dumps({"pkg": pkg, "scan": scan}, ensure_ascii=False)


def ro(path):
    return sqlite3.connect(f"file:{urllib.parse.quote(path)}?mode=ro",
                           uri=True)


def read_result(path):
    con = ro(path)
    res = {r[0]: tuple(r[1:]) for r in con.execute(
        "SELECT pkg, stations, passed, next_station, next_ch FROM results")}
    ev = {}
    for pkg, st, sid, ts, ch in con.execute(
            "SELECT pkg, station, scan_id, ts, ch FROM evidence "
            "ORDER BY pkg, station"):
        ev.setdefault(pkg, []).append((st, sid, ts, ch))
    routes = {p: json.loads(r) for p, r in
              con.execute("SELECT pkg, route FROM routes")}
    meta = dict(con.execute("SELECT k, v FROM meta"))
    con.close()
    return res, ev, routes, meta


def ref_verify(route, cands):
    """Independent pairwise reference:  longest provable prefix and its
    lexicographically smallest chain, computed by checking every pair.

    cands: list of (ts, scan_id, ch) -- the package's associated scans
    with a ledger attribution.  Returns (passed, chain) with chain a
    tuple of (ts, scan_id)."""
    by_ch = {}
    for ts, sid, ch in cands:
        by_ch.setdefault(ch, []).append((ts, sid))
    prev = [((ts, sid), ((ts, sid),)) for ts, sid in
            by_ch.get(route[0]["ch"], [])]
    last = 0 if prev else -1
    for i in range(1, len(route)):
        if not prev:
            break
        lo, hi = route[i]["min"], route[i]["max"]
        cur = []
        for ts, sid in by_ch.get(route[i]["ch"], []):
            best = None
            for (pts, psid), chain in prev:
                if lo <= ts - pts <= hi and (best is None or chain < best):
                    best = chain
            if best is not None:
                cur.append(((ts, sid), best + ((ts, sid),)))
        if not cur:
            break
        prev = cur
        last = i
    passed = last + 1
    chain = min((c for _, c in prev), default=()) if passed else ()
    return passed, tuple(chain)


def check_against_ref(result_path, routes, cand_of):
    """Compare a result database against the independent reference,
    package by package (conclusions and full evidence chains)."""
    res, ev, db_routes, meta = read_result(result_path)
    assert set(res) == set(routes), (set(res) ^ set(routes))
    assert db_routes == {p: r for p, r in routes.items()}
    for pkg, route in routes.items():
        passed, chain = ref_verify(route, cand_of(pkg))
        stations, db_passed, next_st, next_ch = res[pkg]
        assert stations == len(route)
        assert db_passed == passed, \
            f"{pkg}: passed {db_passed} != reference {passed}"
        want_next = (passed + 1, route[passed]["ch"]) \
            if passed < len(route) else (None, None)
        assert (next_st, next_ch) == want_next, \
            f"{pkg}: next {(next_st, next_ch)} != {want_next}"
        want_ev = [(i + 1, sid, ts, route[i]["ch"])
                   for i, (ts, sid) in enumerate(chain)]
        assert ev.get(pkg, []) == want_ev, \
            f"{pkg}: evidence {ev.get(pkg)} != reference {want_ev}"
    assert int(meta["packages"]) == len(routes)
    assert int(meta["evidence_rows"]) == sum(len(v) for v in ev.values())
    return res, ev, meta


# ----------------------------------------------------------------------- tests

def test_validation():
    scans = [("S1", "W", 100, "D", "CH-A"), ("S2", "W", 200, "D", "CH-B")]
    good_routes = [route_line("P1", [{"ch": "CH-A"},
                                     {"ch": "CH-B", "min": 1, "max": 5}])]
    good_assoc = [assoc_line("P1", "S1"), assoc_line("P1", "S2")]
    with tempfile.TemporaryDirectory() as d:
        lp = os.path.join(d, "ledger.db")
        make_ledger(lp, scans)

        def attempt(name, routes=None, assoc=None, ledger_path=None):
            out = os.path.join(d, name + ".db")
            try:
                verify(ledger_path or lp,
                       good_routes if routes is None else routes,
                       good_assoc if assoc is None else assoc, out)
            except SourceInvalid:
                pass
            else:
                raise AssertionError(f"{name}: invalid input accepted")
            assert not os.path.exists(out), f"{name}: result db left behind"
            assert not os.path.exists(out + ".tmp"), \
                f"{name}: tmp result left behind"
            assert not [f for f in os.listdir(d) if "staging" in f], \
                f"{name}: staging db left behind"

        bad_routes = [
            ["not json"],
            [json.dumps({"pkg": "P1"})],                          # missing route
            [json.dumps({"pkg": "", "route": [{"ch": "A"}]})],    # empty pkg
            [json.dumps({"pkg": "P1", "route": "CH-A"})],         # route not a list
            [json.dumps({"pkg": "P1", "route": []})],             # 0 stations
            [json.dumps({"pkg": "P1", "route": [{"ch": "A"}] * 17})],  # 17 stations
            [json.dumps({"pkg": "P1", "route": [{"ch": "A", "min": 1, "max": 2}]})],
            # station 1 must not carry an interval
            [json.dumps({"pkg": "P1", "route": [{"ch": "A"}, {"ch": "B"}]})],
            # station 2 lacks its interval
            [json.dumps({"pkg": "P1", "route": [{"ch": "A"},
                                                {"ch": "B", "min": 1, "max": 2,
                                                 "x": 1}]})],           # extra key
            [json.dumps({"pkg": "P1", "route": [{"ch": ""}]})],   # empty channel
            [json.dumps({"pkg": "P1", "route": [{"ch": "A"},
                                                {"ch": "B", "min": 0, "max": 2}]})],
            [json.dumps({"pkg": "P1", "route": [{"ch": "A"},
                                                {"ch": "B", "min": -1, "max": 2}]})],
            [json.dumps({"pkg": "P1", "route": [{"ch": "A"},
                                                {"ch": "B", "min": True, "max": 2}]})],
            [json.dumps({"pkg": "P1", "route": [{"ch": "A"},
                                                {"ch": "B", "min": 1.5, "max": 2}]})],
            [json.dumps({"pkg": "P1", "route": [{"ch": "A"},
                                                {"ch": "B", "min": 3, "max": 2}]})],
            # duplicate package routes -- even identical ones
            good_routes + good_routes,
            [route_line("P1", [{"ch": "A"}]), route_line("P1", [{"ch": "B"}])],
        ]
        for k, routes in enumerate(bad_routes):
            attempt(f"bad-route-{k}", routes=routes)
        bad_assoc = [
            ["not json"],
            [json.dumps({"pkg": "P1"})],                          # missing scan
            [json.dumps({"pkg": "P1", "scan": ""})],              # empty scan
            [json.dumps({"pkg": "", "scan": "S1"})],              # empty pkg
            [json.dumps({"pkg": "P1", "scan": "S1", "x": 1})],    # extra key
            # duplicate scan associations -- even identical ones
            [assoc_line("P1", "S1"), assoc_line("P1", "S1")],
            # one scan linked to two packages
            [assoc_line("P1", "S1"), assoc_line("P2", "S1")],
            # dangling package reference
            [assoc_line("P2", "S1")],
            # dangling scan reference
            [assoc_line("P1", "S9")],
        ]
        for k, assoc in enumerate(bad_assoc):
            attempt(f"bad-assoc-{k}", assoc=assoc)
        # not a ledger at all
        not_ledger = os.path.join(d, "recv.db")
        Receiver(not_ledger).load_initial({"A": ("WS-1", "CH-1")})
        out = os.path.join(d, "nl.db")
        try:
            verify(not_ledger, good_routes, good_assoc, out)
            raise AssertionError("receiver db accepted as ledger")
        except VerifyReject as e:
            assert e.code == "not_a_ledger", e.code
        assert not os.path.exists(out)
        # missing ledger
        try:
            verify(os.path.join(d, "nope.db"), good_routes, good_assoc, out)
            raise AssertionError("missing ledger accepted")
        except VerifyReject as e:
            assert e.code == "ledger_missing", e.code
        assert not os.path.exists(out)
        # empty inputs are legal: zero packages, an empty (but valid) result
        out = os.path.join(d, "empty.db")
        res = verify(lp, [], [], out)
        assert res["packages"] == 0 and res["evidence_rows"] == 0
        ok, problems = verify_result(out, lp)
        assert ok, problems
        # a result is written once: the same path is refused afterwards
        try:
            verify(lp, good_routes, good_assoc, out)
            raise AssertionError("second verification over one path accepted")
        except VerifyReject as e:
            assert e.code == "result_exists", e.code


def test_directed():
    """The discriminating cases, each pinned to exact evidence rows."""
    scans = [
        # story A: the earlier scan cannot reach the next station in
        # time;  the later rescan can
        ("A1", "W", 100, "D", "CH-A"), ("A2", "W", 130, "D", "CH-A"),
        ("B0", "W", 105, "D", "CH-B"),   # 5 s after A1: below min 10
        ("B2", "W", 145, "D", "CH-B"),   # 15 s after A2: inside [10, 20]
        ("B3", "W", 151, "D", "CH-B"),   # 21 s after A2: above max 20
        # story B: closed-interval boundaries and a same-second decoy
        ("C1", "W", 200, "D", "CH-C"),
        ("D5", "W", 200, "D", "CH-D"),   # same second: gap 0 < min 10
        ("D1", "W", 209, "D", "CH-D"),   # gap 9 < min
        ("D2", "W", 210, "D", "CH-D"),   # gap 10 = min
        ("D3", "W", 220, "D", "CH-D"),   # gap 20 = max
        ("D4", "W", 221, "D", "CH-D"),   # gap 21 > max
        # story C: deterministic (second, scan id) tie-breaking
        ("SEC", "W", 299, "D", "CH-E"),
        ("SEA", "W", 300, "D", "CH-E"),
        ("SEB", "W", 300, "D", "CH-E"),
        ("F1", "W", 310, "D", "CH-F"),
        ("SEA2", "W", 300, "D", "CH-E"),
        ("SEB2", "W", 300, "D", "CH-E"),
        ("F2", "W", 310, "D", "CH-F"),
        # story D: one channel reused at different stations
        ("G1", "W", 400, "D", "CH-G"), ("H1", "W", 410, "D", "CH-H"),
        ("G2", "W", 430, "D", "CH-G"),
        # story E: a partial route and its own scans
        ("PA1", "W", 130, "D", "CH-A"), ("PB1", "W", 145, "D", "CH-B"),
        ("PC1", "W", 200, "D", "CH-C"),
        # story F: an associated but unattributed scan is not evidence
        ("U1", "W", 500, None, None),
        ("U2", "W", 510, "D", "CH-U"),
        ("U3", "W", 505, None, None),
        # more one-package scans
        ("B9", "W", 700, "D", "CH-B"),
        ("Z1", "W", 100, "D", "CH-A"),
        # story G: sixteen stations
        *[(f"J{k:02d}", "W", 1000 + 10 * k, "D", "CH-J")
          for k in range(1, 17)],
        # an unassociated scan whose channel a package wants
        ("X1", "W", 600, "D", "CH-X"),
    ]
    routes = {
        "P-RESCAN": [{"ch": "CH-A"}, {"ch": "CH-B", "min": 10, "max": 20}],
        "P-LIMIT": [{"ch": "CH-C"}, {"ch": "CH-D", "min": 10, "max": 20}],
        "P-TIE": [{"ch": "CH-E"}, {"ch": "CH-F", "min": 10, "max": 20}],
        "P-TIE2": [{"ch": "CH-E"}, {"ch": "CH-F", "min": 10, "max": 20}],
        "P-REUSE": [{"ch": "CH-G"}, {"ch": "CH-H", "min": 5, "max": 15},
                    {"ch": "CH-G", "min": 10, "max": 30}],
        "P-PARTIAL": [{"ch": "CH-A"}, {"ch": "CH-B", "min": 10, "max": 20},
                      {"ch": "CH-C", "min": 1, "max": 5}],
        "P-EMPTY": [{"ch": "CH-Z"}, {"ch": "CH-A", "min": 1, "max": 99}],
        "P-NOASSOC": [{"ch": "CH-X"}],
        "P-UNATTR": [{"ch": "CH-U"}],
        "P-UNATTR2": [{"ch": "CH-U"}],
        "P-ONE": [{"ch": "CH-B"}],
        "P-16": [{"ch": "CH-J"}] + [{"ch": "CH-J", "min": 10, "max": 10}
                                    for _ in range(15)],
    }
    assoc = {
        "P-RESCAN": ["A1", "A2", "B0", "B2", "B3"],
        "P-LIMIT": ["C1", "D5", "D1", "D2", "D3", "D4"],
        "P-TIE": ["SEC", "SEA", "SEB", "F1"],
        "P-TIE2": ["SEA2", "SEB2", "F2"],
        "P-REUSE": ["G1", "H1", "G2"],
        "P-PARTIAL": ["PA1", "PB1", "PC1"],
        "P-EMPTY": ["Z1"],
        "P-NOASSOC": [],
        "P-UNATTR": ["U1"],
        "P-UNATTR2": ["U3", "U2"],
        "P-ONE": ["B9"],
        "P-16": [f"J{k:02d}" for k in range(1, 17)],
    }
    with tempfile.TemporaryDirectory() as d:
        lp = os.path.join(d, "ledger.db")
        make_ledger(lp, scans)
        route_lines = [route_line(p, r) for p, r in routes.items()]
        assoc_lines = [assoc_line(p, s) for p, ss in assoc.items()
                       for s in ss]
        out = os.path.join(d, "v.db")
        verify(lp, route_lines, assoc_lines, out)
        res, ev, _, meta = read_result(out)

        def expect(pkg, passed, chain, next_st, next_ch):
            stations, db_passed, db_next_st, db_next_ch = res[pkg]
            assert (db_passed, db_next_st, db_next_ch) == \
                (passed, next_st, next_ch), \
                f"{pkg}: {(db_passed, db_next_st, db_next_ch)}"
            want = [(i + 1, sid, ts, routes[pkg][i]["ch"])
                    for i, (ts, sid) in enumerate(chain)]
            assert ev.get(pkg, []) == want, f"{pkg}: {ev.get(pkg)} != {want}"

        # the earlier scan A1@100 cannot reach CH-B in time (B0@105 is
        # 5 s away, below min 10);  the later rescan A2@130 reaches B2@145
        expect("P-RESCAN", 2, [(130, "A2"), (145, "B2")], None, None)
        # closed interval: gap 10 (=min) and gap 20 (=max) both count;
        # the lexicographically smaller witness D2@210 is selected;
        # the same-second D5@200 is never a successor
        expect("P-LIMIT", 2, [(200, "C1"), (210, "D2")], None, None)
        # deterministic selection: smallest (second, scan id) wins --
        # first by second, then by scan id for equal seconds
        expect("P-TIE", 2, [(299, "SEC"), (310, "F1")], None, None)
        expect("P-TIE2", 2, [(300, "SEA2"), (310, "F2")], None, None)
        # CH-G serves stations 1 and 3 with two different scans
        expect("P-REUSE", 3, [(400, "G1"), (410, "H1"), (430, "G2")],
               None, None)
        # the prefix stops where no scan can continue it
        expect("P-PARTIAL", 2, [(130, "PA1"), (145, "PB1")], 3, "CH-C")
        # empty prefix:  no scan of station 1's channel exists
        expect("P-EMPTY", 0, [], 1, "CH-Z")
        # X1@600 has the wanted channel but is not associated:  excluded
        expect("P-NOASSOC", 0, [], 1, "CH-X")
        # U1 is associated but unattributed in the ledger:  not evidence
        expect("P-UNATTR", 0, [], 1, "CH-U")
        expect("P-UNATTR2", 1, [(510, "U2")], None, None)
        # one-station route, fully passed
        expect("P-ONE", 1, [(700, "B9")], None, None)
        # sixteen stations, the maximum
        expect("P-16", 16, [(1000 + 10 * k, f"J{k:02d}")
                            for k in range(1, 17)], None, None)
        # tallies:  8 full, 1 partial, 3 empty
        assert (meta["packages"], meta["full"], meta["partial"],
                meta["empty"]) == ("12", "8", "1", "3"), meta
        ok, problems = verify_result(out, lp)
        assert ok, problems


def test_vs_bruteforce():
    rng = random.Random(20261122)
    channels = ["A", "B", "C", "a"]      # case-sensitive on purpose
    with tempfile.TemporaryDirectory() as d:
        for case in range(300):
            lp = os.path.join(d, "ledger.db")
            out = os.path.join(d, "v.db")
            scans = []
            assoc = []
            routes = {}
            n = 0

            def add_scan(ts, ch):
                nonlocal n
                sid = f"S{n:03d}"
                n += 1
                scans.append((sid, "W", ts, "D" if ch else None, ch))
                return sid

            for k in range(rng.randint(1, 6)):
                pkg = f"P{k}"
                n_st = rng.randint(1, 6)
                route = [{"ch": rng.choice(channels)}]
                for _ in range(n_st - 1):
                    lo = rng.randint(1, 15)
                    route.append({"ch": rng.choice(channels),
                                  "min": lo, "max": lo + rng.randint(0, 15)})
                routes[pkg] = route
                for _ in range(rng.randint(0, 12)):
                    ch = rng.choice(channels + [None])
                    assoc.append((pkg, add_scan(rng.randint(0, 80), ch)))
            for _ in range(rng.randint(0, 5)):      # unassociated scans
                add_scan(rng.randint(0, 80), rng.choice(channels))
            make_ledger(lp, scans)
            by_id = {s[0]: s for s in scans}

            def cand_of(pkg, assoc=assoc, by_id=by_id):
                out = []
                for p, sid in assoc:
                    if p == pkg:
                        _, _, ts, _, ch = by_id[sid]
                        if ch is not None:
                            out.append((ts, sid, ch))
                return out

            route_lines = [route_line(p, r) for p, r in routes.items()]
            assoc_lines = [assoc_line(p, s) for p, s in assoc]
            rng.shuffle(route_lines)
            rng.shuffle(assoc_lines)
            verify(lp, route_lines, assoc_lines, out)
            res1, ev1, meta1 = check_against_ref(out, routes, cand_of)
            ok, problems = verify_result(out, lp)
            assert ok, (case, problems)
            if case < 8:
                # shuffled input lines -> identical result content
                out2 = os.path.join(d, "v2.db")
                rng.shuffle(route_lines)
                rng.shuffle(assoc_lines)
                verify(lp, route_lines, assoc_lines, out2)
                res2, ev2, routes2, meta2 = read_result(out2)
                assert res2 == res1 and ev2 == ev1
                assert meta2 == meta1
                os.unlink(out2)
            os.unlink(lp)
            os.unlink(out)


def test_chain_key_codec():
    """The order-preserving chain-key encoding compares exactly like the
    (second, scan id) sequence -- including nasty ids."""
    rng = random.Random(20261123)
    alphabet = ["A", "a", "Z", "0", "/", "\x00", "\x01", "é", "中", "🚚",
                " ", "FF", "ff"]
    ids = ["A", "AB", "A\x00", "A\x00B", "é", "中", "🚚🚚", "/", "//",
           "\x00", "\x00\x00"]
    for _ in range(400):
        ids.append("".join(rng.choice(alphabet)
                           for _ in range(rng.randint(1, 6))))
    pairs = sorted({(rng.randint(0, ledger.MAX_TS), rng.choice(ids))
                    for _ in range(600)})
    keys = [_pair_key(ts, sid) for ts, sid in pairs]
    # pairwise: encoded order == tuple order
    for i in range(len(pairs)):
        for j in range(i + 1, len(pairs)):
            assert keys[i] < keys[j], (pairs[i], pairs[j])
    # concatenated chains compare like the concatenated tuples, and the
    # codec round-trips
    for _ in range(300):
        ca = [rng.choice(pairs) for _ in range(rng.randint(1, 16))]
        cb = [rng.choice(pairs) for _ in range(rng.randint(1, 16))]
        ka = "".join(_pair_key(*p) for p in ca)
        kb = "".join(_pair_key(*p) for p in cb)
        assert (ka < kb) == (ca < cb), (ca, cb)
        assert (ka == kb) == (ca == cb)
        assert _decode_key(ka) == [[ts, sid] for ts, sid in ca]


def test_medium_random():
    """Medium candidate counts (multi-level segment trees) vs the
    independent pairwise reference."""
    rng = random.Random(20261124)
    channels = ["A", "B"]
    with tempfile.TemporaryDirectory() as d:
        for case in range(15):
            lp = os.path.join(d, "ledger.db")
            out = os.path.join(d, "v.db")
            scans = []
            assoc = []
            routes = {}
            n = 0
            for k in range(rng.randint(1, 2)):
                pkg = f"P{k}"
                n_st = rng.randint(2, 6)
                route = [{"ch": rng.choice(channels)}]
                for _ in range(n_st - 1):
                    lo = rng.randint(1, 8)
                    route.append({"ch": rng.choice(channels),
                                  "min": lo, "max": lo + rng.randint(0, 8)})
                routes[pkg] = route
                for _ in range(rng.randint(80, 150)):
                    sid = f"S{n:04d}"
                    n += 1
                    ch = rng.choice(channels + [None])
                    scans.append((sid, "W", rng.randint(0, 100),
                                  "D" if ch else None, ch))
                    assoc.append((pkg, sid))
            make_ledger(lp, scans)
            by_id = {s[0]: s for s in scans}

            def cand_of(pkg, assoc=assoc, by_id=by_id):
                return [(s[2], s[0], s[4]) for p, s in
                        ((p, by_id[sid]) for p, sid in assoc)
                        if p == pkg and s[4] is not None]

            verify(lp, [route_line(p, r) for p, r in routes.items()],
                   [assoc_line(p, s) for p, s in assoc], out)
            check_against_ref(out, routes, cand_of)
            ok, problems = verify_result(out, lp)
            assert ok, (case, problems)
            os.unlink(lp)
            os.unlink(out)


def make_real_ledger(d, scans, name="ledger.db"):
    """A real ledger through the full pipeline:  plan, receiver, build."""
    order, _ = core.plan(OLD, TARGET, frozenset({"A", "B"}))
    recv = os.path.join(d, name + ".recv.db")
    r = Receiver(recv)
    r.load_initial(OLD)
    for k in range(len(order["steps"])):
        r.submit(order, k)
    r.close()
    path = os.path.join(d, name)
    times = ledger.validate_step_times(
        {"format": ledger.TIMES_FORMAT, "order_hash": order["order_hash"],
         "times": [{"seq": q, "ts": t} for q, t in enumerate(TIMES0)]},
        order)
    Ledger.build(path, OLD, order, recv, times,
                 [{"id": i, "ws": w, "ts": t} for i, w, t in scans])
    return path, order


# the ledger demo's scan set plus extra scans for the directed packages
REAL_SCANS = [
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

REAL_ROUTES = {
    # evidence moves to other scans when a correction shifts channels
    "P-FULL": [{"ch": "CH-2"}, {"ch": "CH-9", "min": 100, "max": 160},
               {"ch": "CH-1", "min": 1, "max": 110}],
    # a 16-station route over one repeated channel
    "P-LONG": [{"ch": "CH-9"}] + [{"ch": "CH-9", "min": 1, "max": 100000}
                                  for _ in range(15)],
    # gains its whole route only after the correction
    "P-GAIN": [{"ch": "CH-1"}, {"ch": "CH-2", "min": 45, "max": 55}],
    # the earlier CH-3 scan cannot reach CH-9 in time;  the later rescan
    # can -- stable across the correction
    "P-RESCAN": [{"ch": "CH-3"}, {"ch": "CH-9", "min": 10, "max": 19}],
    "P-EMPTY": [{"ch": "CH-7"}],
    "P-NOASSOC": [{"ch": "CH-2"}],
    "P-UNATTR": [{"ch": "CH-9"}],
}
REAL_ASSOC = {
    "P-FULL": ["S01", "S02", "S06", "S09", "S10", "S15"],
    "P-LONG": ["S07", "S08", "R03", "R04", "R05", "R06", "R07", "R08",
               "R10", "R11", "R12", "R13", "R14", "R15", "S19", "S20"],
    "P-GAIN": ["S11", "S12", "S13", "S16", "S17"],
    "P-RESCAN": ["R01", "R02", "R09", "Q1", "Q2", "Q3", "Q4", "Q5"],
    "P-EMPTY": ["S18"],
    "P-NOASSOC": [],
    "P-UNATTR": ["S03"],
}


def real_inputs():
    route_lines = [route_line(p, r) for p, r in REAL_ROUTES.items()]
    assoc_lines = [assoc_line(p, s) for p, ss in REAL_ASSOC.items()
                   for s in ss]
    return route_lines, assoc_lines


def test_correction_reverify():
    """Correct one attribution in the existing ledger, re-verify, and
    compare package conclusions and evidence -- not channel totals."""
    with tempfile.TemporaryDirectory() as d:
        lp, order = make_real_ledger(d, REAL_SCANS)
        route_lines, assoc_lines = real_inputs()
        v1 = os.path.join(d, "verify1.db")
        verify(lp, route_lines, assoc_lines, v1)
        res1, ev1, _, meta1 = read_result(v1)
        ok, problems = verify_result(v1, lp)
        assert ok, problems

        def expect(res, ev, pkg, passed, chain, next_st, next_ch):
            stations, db_passed, db_next_st, db_next_ch = res[pkg]
            assert (db_passed, db_next_st, db_next_ch) == \
                (passed, next_st, next_ch), \
                f"{pkg}: {(db_passed, db_next_st, db_next_ch)}"
            want = [(i + 1, sid, ts, REAL_ROUTES[pkg][i]["ch"])
                    for i, (ts, sid) in enumerate(chain)]
            assert ev.get(pkg, []) == want, \
                f"{pkg}: {ev.get(pkg)} != {want}"

        # conclusions and evidence on the ledger as built
        expect(res1, ev1, "P-FULL", 3,
               [(50, "S01"), (200, "S06"), (300, "S15")], None, None)
        expect(res1, ev1, "P-LONG", 16,
               [(t, s) for t, s in
                [(225, "S07"), (249, "S08"), (270, "R10"), (279, "R07"),
                 (280, "R05"), (285, "R03"), (290, "R06"), (291, "R08"),
                 (295, "R04"), (500, "S19"), (600, "R11"), (700, "R12"),
                 (800, "R13"), (900, "R14"), (1000, "R15"),
                 (86450, "S20")]], None, None)
        expect(res1, ev1, "P-GAIN", 0, [], 1, "CH-1")
        expect(res1, ev1, "P-RESCAN", 2,
               [(270, "R02"), (280, "Q1")], None, None)
        expect(res1, ev1, "P-EMPTY", 0, [], 1, "CH-7")
        expect(res1, ev1, "P-NOASSOC", 0, [], 1, "CH-2")
        expect(res1, ev1, "P-UNATTR", 0, [], 1, "CH-9")

        # one correction through the existing ledger flow:  step 1 (A's
        # move to WS-2/CH-9) actually took effect at 250, not 200 --
        # S06/S07/S08 lose CH-9, S11/S12 gain CH-1
        lg = Ledger(lp)
        doc = ledger.validate_correction(
            {"format": ledger.CORRECTION_FORMAT,
             "order_hash": order["order_hash"],
             "changes": [{"seq": 1, "ts": 250}]})
        assert lg.correct(doc)["result"] == "applied"
        lg.close()

        v2 = os.path.join(d, "verify2.db")
        verify(lp, route_lines, assoc_lines, v2)
        res2, ev2, _, meta2 = read_result(v2)
        # the snapshot identity moved with the correction
        assert meta1["ledger_scans_sha256"] != meta2["ledger_scans_sha256"]
        assert meta2["ledger_correction_count"] == "1"
        # package conclusions and evidence after the correction
        expect(res2, ev2, "P-FULL", 3,
               [(99, "S02"), (250, "S09"), (300, "S15")], None, None)
        expect(res2, ev2, "P-LONG", 14,
               [(t, s) for t, s in
                [(270, "R10"), (279, "R07"), (280, "R05"), (285, "R03"),
                 (290, "R06"), (291, "R08"), (295, "R04"), (500, "S19"),
                 (600, "R11"), (700, "R12"), (800, "R13"), (900, "R14"),
                 (1000, "R15"), (86450, "S20")]], 15, "CH-9")
        expect(res2, ev2, "P-GAIN", 2,
               [(249, "S12"), (300, "S17")], None, None)
        expect(res2, ev2, "P-RESCAN", 2,
               [(270, "R02"), (280, "Q1")], None, None)
        expect(res2, ev2, "P-EMPTY", 0, [], 1, "CH-7")
        expect(res2, ev2, "P-NOASSOC", 0, [], 1, "CH-2")
        expect(res2, ev2, "P-UNATTR", 0, [], 1, "CH-9")
        # the new result re-checks against the corrected ledger...
        ok, problems = verify_result(v2, lp)
        assert ok, problems
        # ...while the old result is reported as a different snapshot,
        # not silently accepted
        ok, problems = verify_result(v1, lp)
        assert not ok
        assert any("snapshot" in p for p in problems), problems


def test_snapshot_lock():
    """A correction attempted mid-verification cannot commit;  the run
    still reflects exactly one committed state."""
    with tempfile.TemporaryDirectory() as d:
        lp, order = make_real_ledger(d, REAL_SCANS)
        route_lines, assoc_lines = real_inputs()
        attempted = []

        def hook():
            lg = Ledger(lp)
            lg.conn.execute("PRAGMA busy_timeout=0")
            doc = ledger.validate_correction(
                {"format": ledger.CORRECTION_FORMAT,
                 "order_hash": order["order_hash"],
                 "changes": [{"seq": 1, "ts": 250}]})
            try:
                lg.correct(doc)
                attempted.append("committed")
            except sqlite3.OperationalError as e:
                attempted.append(f"blocked: {e}")
            finally:
                lg.close()

        v1 = os.path.join(d, "verify1.db")
        verify(lp, route_lines, assoc_lines, v1, snapshot_hook=hook)
        assert attempted and attempted[0].startswith("blocked"), attempted
        # the ledger was not changed by the blocked attempt
        assert ledger_times(lp) == TIMES0
        # and the result shows the pre-correction conclusions
        res1, _, _, _ = read_result(v1)
        assert res1["P-GAIN"][1] == 0, res1["P-GAIN"]
        # after the verification the correction goes through
        lg = Ledger(lp)
        doc = ledger.validate_correction(
            {"format": ledger.CORRECTION_FORMAT,
             "order_hash": order["order_hash"],
             "changes": [{"seq": 1, "ts": 250}]})
        assert lg.correct(doc)["result"] == "applied"
        lg.close()
        v2 = os.path.join(d, "verify2.db")
        verify(lp, route_lines, assoc_lines, v2)
        res2, _, _, _ = read_result(v2)
        assert res2["P-GAIN"][1] == 2, res2["P-GAIN"]


def ledger_times(path):
    con = ro(path)
    rows = [t for (t,) in con.execute("SELECT ts FROM steps ORDER BY seq")]
    con.close()
    return rows


def gen_dense(d, T):
    """One package, three stations, d rescans per second per channel."""
    scans = []
    assoc = []
    for t in range(T):
        for k in range(d):
            for ch in "ABC":
                sid = f"{ch}{t:04d}_{k:03d}"
                scans.append((sid, "W", t, "D", f"CH-{ch}"))
                assoc.append(("P-DENSE", sid))
    route = [{"ch": "CH-A"}, {"ch": "CH-B", "min": 1, "max": 3},
             {"ch": "CH-C", "min": 1, "max": 3}]
    return scans, assoc, route


def gen_dense_streaming(d, T, lp, assoc_p):
    """The same dense case, but the ledger and the association file are
    written row by row -- the inputs never live in memory as a whole."""
    con = sqlite3.connect(lp)
    con.executescript(ledger.SCHEMA)
    n = 0
    with open(assoc_p, "w", encoding="utf-8") as f:
        for t in range(T):
            for k in range(d):
                for ch in "ABC":
                    sid = f"{ch}{t:04d}_{k:03d}"
                    con.execute("INSERT INTO scans (scan_id, ws, ts, device, "
                                "ch) VALUES (?, 'W', ?, 'D', ?)",
                                (sid, t, f"CH-{ch}"))
                    f.write(json.dumps({"pkg": "P-DENSE", "scan": sid}) + "\n")
                    n += 1
    con.executemany("INSERT INTO meta (k, v) VALUES (?, ?)", [
        ("format", ledger.LEDGER_FORMAT), ("order_hash", "0" * 64),
        ("initial_hash", "0" * 64), ("final_hash", "0" * 64),
        ("step_count", "0"), ("scan_count", str(n))])
    con.commit()
    con.close()
    return [{"ch": "CH-A"}, {"ch": "CH-B", "min": 1, "max": 3},
            {"ch": "CH-C", "min": 1, "max": 3}]


def test_dense_scaling():
    """Work grows with the candidate count, not with the number of
    matching scan pairs -- shown by VM-op counts and peak memory."""
    T = 200

    def pairs(d):
        # per transition: every successor sees 3*d predecessors in its
        # window;  two transitions
        return 2 * (T * d) * (3 * d)

    def run(d, progress=False):
        with tempfile.TemporaryDirectory() as tmp:
            scans, assoc, route = gen_dense(d, T)
            lp = os.path.join(tmp, "ledger.db")
            make_ledger(lp, scans)
            out = os.path.join(tmp, "v.db")
            kw = {"progress_every": 1000} if progress else {}
            res = verify(lp, [route_line("P-DENSE", route)],
                         [assoc_line(p, s) for p, s in assoc], out, **kw)
            res_, ev, _, _ = read_result(out)
            assert res_["P-DENSE"][1] == 3            # full pass
            want = [(1, "A0000_000", 0, "CH-A"), (2, "B0001_000", 1, "CH-B"),
                    (3, "C0002_000", 2, "CH-C")]
            assert ev["P-DENSE"] == want, ev["P-DENSE"][:3]
            ok, problems = verify_result(out, lp)
            assert ok, problems
            return res

    # query plans: the per-candidate probes are index range searches
    con = sqlite3.connect(":memory:")
    con.executescript(STAGING_SCHEMA)
    for sql in (SQL_PROBE_LO, SQL_PROBE_HI):
        plan = [r[3] for r in con.execute("EXPLAIN QUERY PLAN " + sql,
                                          ("P", 0, 5))]
        assert any(p.startswith("SEARCH reach USING") and "reach_probe" in p
                   for p in plan), plan
        assert not any(p.startswith("SCAN") for p in plan), plan
    con.close()

    r1 = run(1, progress=True)
    r100 = run(100, progress=True)
    p1, p100 = pairs(1), pairs(100)
    assert p100 >= p1 * 5000, (p1, p100)
    # VM-ops grow ~linearly with the candidate count (x100, plus a small
    # log factor), nowhere near the x10000 growth of the pair count
    assert r100["vm_ops"] < r1["vm_ops"] * 400, \
        (r1["vm_ops"], r100["vm_ops"])
    print(f"    (dense: pairs {p1} -> {p100} (x{p100 // p1}); "
          f"VM-ops {r1['vm_ops']} -> {r100['vm_ops']} "
          f"(x{r100['vm_ops'] / max(r1['vm_ops'], 1):.1f}))")

    # peak Python memory of verify() itself stays flat as the windows get
    # 100x denser:  inputs are streamed from disk, candidates staged in
    # the temporary SQLite db
    def peak_mem(d):
        with tempfile.TemporaryDirectory() as tmp:
            lp = os.path.join(tmp, "ledger.db")
            assoc_p = os.path.join(tmp, "assoc.jsonl")
            route = gen_dense_streaming(d, T, lp, assoc_p)
            out = os.path.join(tmp, "v.db")
            with open(assoc_p, encoding="utf-8") as f:
                tracemalloc.start()
                verify(lp, [route_line("P-DENSE", route)], f, out)
                peak = tracemalloc.get_traced_memory()[1]
                tracemalloc.stop()
            return peak

    peak_mem(1)                                # warm up interpreter caches
    peak1 = peak_mem(1)
    peak100 = peak_mem(100)
    assert peak100 < peak1 * 3 + 1024 * 1024, (peak1, peak100)
    print(f"    (dense: peak python memory of verify() "
          f"{peak1} -> {peak100} bytes)")


def test_verify_cli():
    with tempfile.TemporaryDirectory() as d:
        lp = os.path.join(d, "ledger.db")
        make_ledger(lp, [("S1", "W", 100, "D", "CH-A"),
                         ("S2", "W", 200, "D", "CH-B")])
        routes_p = os.path.join(d, "routes.jsonl")
        assoc_p = os.path.join(d, "assoc.jsonl")
        with open(routes_p, "w") as f:
            f.write(route_line("P1", [{"ch": "CH-A"},
                                      {"ch": "CH-B", "min": 1, "max": 500}])
                    + "\n")
        with open(assoc_p, "w") as f:
            f.write(assoc_line("P1", "S1") + "\n")
            f.write(assoc_line("P1", "S2") + "\n")
        out_p = os.path.join(d, "v.db")
        out = json.loads(cli("verify", "--ledger", lp, "--routes", routes_p,
                             "--assoc", assoc_p, "--out", out_p,
                             expect_rc=0).stdout)
        assert out["result"] == "verified" and out["packages"] == 1
        assert out["full"] == 1 and out["evidence_rows"] == 2
        con = ro(out_p)
        assert list(con.execute("SELECT pkg, passed, next_station FROM "
                                "results")) == [("P1", 2, None)]
        assert list(con.execute(
            "SELECT station, scan_id, ts, ch FROM evidence "
            "ORDER BY station")) == [(1, "S1", 100, "CH-A"),
                                     (2, "S2", 200, "CH-B")]
        con.close()
        out = json.loads(cli("verify-check", "--result", out_p,
                             "--ledger", lp, expect_rc=0).stdout)
        assert out["result"] == "consistent"
        # the same output path is refused
        out = json.loads(cli("verify", "--ledger", lp, "--routes", routes_p,
                             "--assoc", assoc_p, "--out", out_p,
                             expect_rc=5).stdout)
        assert out["code"] == "result_exists"
        # invalid input: exit 3 and no result database appears
        bad_p = os.path.join(d, "bad.jsonl")
        with open(bad_p, "w") as f:
            f.write(assoc_line("P1", "S1") + "\n")
            f.write(assoc_line("P1", "S1") + "\n")   # duplicate association
        out2_p = os.path.join(d, "v2.db")
        out = json.loads(cli("verify", "--ledger", lp, "--routes", routes_p,
                             "--assoc", bad_p, "--out", out2_p,
                             expect_rc=3).stdout)
        assert out["result"] == "invalid_source"
        assert any("duplicate" in p for p in out["problems"])
        assert not os.path.exists(out2_p)
        assert not os.path.exists(out2_p + ".tmp")
        # a dangling scan reference
        with open(bad_p, "w") as f:
            f.write(assoc_line("P1", "S9") + "\n")
        out = json.loads(cli("verify", "--ledger", lp, "--routes", routes_p,
                             "--assoc", bad_p, "--out", out2_p,
                             expect_rc=3).stdout)
        assert any("dangling" in p for p in out["problems"])
        assert not os.path.exists(out2_p)
        # the receiver db is not a ledger
        recv_p = os.path.join(d, "recv.db")
        r = Receiver(recv_p)
        r.load_initial({"A": ("WS-1", "CH-1")})
        r.close()
        out = json.loads(cli("verify", "--ledger", recv_p,
                             "--routes", routes_p, "--assoc", assoc_p,
                             "--out", out2_p, expect_rc=5).stdout)
        assert out["code"] == "not_a_ledger"
        assert not os.path.exists(out2_p)
        # verify-check reports a changed ledger instead of accepting it
        other_lp = os.path.join(d, "other.db")
        make_ledger(other_lp, [("S1", "W", 100, "D", "CH-A"),
                               ("S2", "W", 201, "D", "CH-B")])
        out = json.loads(cli("verify-check", "--result", out_p,
                             "--ledger", other_lp, expect_rc=5).stdout)
        assert out["result"] == "inconsistent"
        assert any("snapshot" in p for p in out["problems"])
        # verify-check on something that is not a result db
        out = json.loads(cli("verify-check", "--result", lp,
                             "--ledger", lp, expect_rc=5).stdout)
        assert out["code"] == "not_a_result"


TESTS = [
    ("input validation leaves no result behind", test_validation),
    ("chain-key codec matches tuple order", test_chain_key_codec),
    ("directed discriminating cases", test_directed),
    ("randomized cross-check vs independent pairwise reference",
     test_vs_bruteforce),
    ("medium candidate counts vs pairwise reference", test_medium_random),
    ("ledger correction, then re-verification of conclusions and evidence",
     test_correction_reverify),
    ("a correction cannot interleave with a running verification",
     test_snapshot_lock),
    ("dense rescans: work scales with candidates, not matching pairs",
     test_dense_scaling),
    ("verify CLI end to end", test_verify_cli),
]


def main():
    for name, fn in TESTS:
        fn()
        print(f"PASS: {name}")
    print("\nALL VERIFY TESTS PASSED")


if __name__ == "__main__":
    main()
