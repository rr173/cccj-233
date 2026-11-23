#!/usr/bin/env python3
"""Test suite for the offline shipment manifest (handover-manifest/1).

  * the engine compared item by item against an independent, naive
    reference implementation on randomized packing logs (packing log
    shuffled, replayed per second and scan-id bytes)
  * directed cases: the two-container ownership reversal is order
    independent, a truly cyclic operation is rejected with its package
    move, same-number package/container are distinct objects, a no-change
    move keeps the old evidence scan, gated objects cannot be packed or
    re-gated, gate failures never change packing state, empty containers
    still get a manifest
  * channel and attribution gates skip exactly one group; a malformed
    log, a broken group or a dangling scan rejects the whole request and
    leaves neither a result nor a staging file
  * the ledger correction flow runs between two builds: the two result
    databases are compared per package and per direct relation, and the
    first one still answers from its own frozen snapshot
  * scaling: with runtime-generated unrelated containers beside the ones
    being processed, packing and gating access is shown by query plans and
    SQLite VM-step counts to touch only the group/subtree at hand
    (never wall-clock timing)
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

from handover import ledger  # noqa: E402
from handover.core import SourceInvalid  # noqa: E402
from handover.manifest import (  # noqa: E402
    KIND_CTR, KIND_PKG, STAGING_SCHEMA,
    SQL_CAND_PARENT, SQL_GATE_SUBTREE, SQL_LOOKUP,
    SQL_MARK_GONE, ManifestReject, build_manifest,
)

PACK_CH = "CH-PACK"
GATE_CH = "CH-GATE"


# --------------------------------------------------------------------- helpers

def cli(*args, expect_rc=None):
    p = subprocess.run([sys.executable, "-m", "handover", *args],
                       capture_output=True, text=True,
                       env={**os.environ, "PYTHONPATH": ROOT})
    if expect_rc is not None:
        assert p.returncode == expect_rc, \
            f"handover {' '.join(args)}: rc={p.returncode}\n{p.stdout}\n{p.stderr}"
    return p


def make_ledger(path, scans, order_hash="0" * 64, corrections=0):
    """A minimal but faithful handover-scan-ledger/1 db.

    scans: iterable of (scan_id, ws, ts, device, ch);  ch None marks an
    unattributed scan.  Packing scans belong to CH-PACK, gate scans to
    CH-GATE unless a test says otherwise."""
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
    for k in range(corrections):
        con.execute("INSERT INTO corrections (hash, summary) VALUES (?, ?)",
                    (f"h{k}", "{}"))
    con.commit()
    con.close()


def pack_line(scan, items):
    return json.dumps({"scan": scan, "pack": {"items": [
        {"kind": k, "id": i, "from": f, "to": t} for k, i, f, t in items]}},
        ensure_ascii=False)


def gate_line(scan, root):
    return json.dumps({"scan": scan, "gate": root}, ensure_ascii=False)


def ro(path):
    return sqlite3.connect(f"file:{urllib.parse.quote(path)}?mode=ro",
                           uri=True)


def read_db(path):
    con = ro(path)
    meta = dict(con.execute("SELECT k, v FROM meta"))
    groups = {r[1]: r for r in con.execute(
        "SELECT ord, scan_id, ts, use, adopted, reason FROM groups")}
    manifests = {r[1]: (r[0], r[2], r[3]) for r in con.execute(
        "SELECT gate_id, scan_id, ts, root_id FROM manifests")}
    rel = {}
    for gid, kind, oid, parent, via in con.execute(
            "SELECT gate_id, kind, id, parent, via FROM manifest "
            "ORDER BY gate_id, kind, id"):
        rel.setdefault(gid, []).append((kind, oid, parent, via))
    pkgs = {}
    for gid, p in con.execute(
            "SELECT gate_id, pkg FROM packages ORDER BY gate_id, pkg"):
        pkgs.setdefault(gid, []).append(p)
    scans = {r[0]: tuple(r[1:]) for r in con.execute(
        "SELECT scan_id, ws, ts, device, ch FROM scans")}
    con.close()
    return {"meta": meta, "groups": groups, "manifests": manifests,
            "rel": rel, "pkgs": pkgs, "scans": scans}


def measure_ops(conn, fn, every=100):
    """Count SQLite VM instructions executed while running fn()."""
    ops = [0]

    def handler():
        ops[0] += 1
        return 0

    conn.set_progress_handler(handler, every)
    try:
        fn()
    finally:
        conn.set_progress_handler(None, 0)
    return ops[0] * every


# ------------------------------------------------- independent reference model

def ref_replay(scans, log, pack_ch, gate_ch):
    """Naive in-Python reference: full ownership dict, cycle check by
    walking paths, no indexes, no clever structure."""
    ledger_by_id = {s[0]: s for s in scans}
    groups = collections.OrderedDict()
    for raw in log:
        obj = json.loads(raw)
        if "pack" in obj:
            groups.setdefault(obj["scan"], []).extend(
                (i["kind"], i["id"], i["from"], i["to"])
                for i in obj["pack"]["items"])
        else:
            groups[obj["scan"]] = ("GATE", obj["gate"])

    order = sorted(
        groups,
        key=lambda s: (ledger_by_id[s][2], s.encode("utf-8")))
    own = {}          # (kind, id) -> (parent, via)
    gated = set()     # (kind, id)
    out = []          # (scan, ts, root, [(kind,id,parent,via)], [pkgs])
    verdict = {}
    for scan in order:
        sid, ws, ts, dev, ch = ledger_by_id[scan]
        want = gate_ch if groups[scan][0] == "GATE" else pack_ch
        if dev is None:
            verdict[scan] = (False, ["unattributed"])
            continue
        if ch != want:
            verdict[scan] = (False, ["wrong channel"])
            continue
        spec = groups[scan]
        if spec[0] == "GATE":
            root = spec[1]
            cur = own.get((KIND_CTR, root))
            if cur is not None and cur[0] is not None:
                verdict[scan] = (False, ["nested"])
                continue
            if (KIND_CTR, root) in gated:
                verdict[scan] = (False, ["gone"])
                continue
            sub = []
            stack = [(KIND_CTR, root)]
            seen_nodes = set()
            while stack:
                node = stack.pop()
                if node in seen_nodes:
                    continue
                seen_nodes.add(node)
                parent, via = own.get(node, (None, None))
                sub.append((node[0], node[1], parent, via))
                if node[0] == KIND_CTR:
                    stack.extend(k for k in own if own[k][0] == node[1])
            for kind, oid, _, _ in sub:
                gated.add((kind, oid))
            pkgs = sorted(row[1] for row in sub if row[0] == KIND_PKG)
            out.append((scan, ts, root, sorted(sub), pkgs))
            verdict[scan] = (True, [])
            continue
        reasons = []
        pending = []
        for kind, oid, src, dst in spec:
            parent, _ = own.get((kind, oid), (None, None))
            if parent != src:
                reasons.append("source")
                continue
            if src == dst:
                continue
            if (kind, oid) in gated:
                reasons.append("gated")
                continue
            if (KIND_CTR, dst) in gated:
                reasons.append("dest-gated")
                continue
            pending.append((kind, oid, dst))
        if reasons:
            verdict[scan] = (False, reasons)
            continue
        cand = dict(own)
        for kind, oid, dst in pending:
            cand[(kind, oid)] = (dst, scan)
        cyc = False
        for kind, oid, _ in pending:
            seen = set()
            node = (kind, oid)
            while node is not None:
                if node in seen:
                    cyc = True
                    break
                seen.add(node)
                parent = cand.get(node, (None, None))[0]
                node = None if parent is None else (KIND_CTR, parent)
            if cyc:
                break
        if cyc:
            verdict[scan] = (False, ["cycle"])
            continue
        own = cand
        verdict[scan] = (True, [])
    return out, verdict


# ----------------------------------------------------------------------- tests

SCANS = [  # (id, ws, ts, device, ch)
    ("PK01", "WS-P", 100, "D", PACK_CH),
    ("PK02", "WS-P", 110, "D", PACK_CH),
    ("PK03", "WS-P", 120, "D", PACK_CH),
    ("PK04", "WS-P", 130, "D", PACK_CH),
    ("PK05", "WS-P", 140, "D", PACK_CH),
    ("PK06", "WS-P", 150, "D", PACK_CH),
    ("PK07", "WS-P", 400, "D", PACK_CH),
    ("PK08", "WS-P", 410, "D", PACK_CH),
    ("PK09", "WS-P", 420, "D", PACK_CH),
    ("GA01", "WS-G", 300, "E", GATE_CH),
    ("GA02", "WS-G", 430, "E", GATE_CH),
    ("GA03", "WS-G", 440, "E", GATE_CH),
    ("GA04", "WS-G", 450, "E", GATE_CH),

    # attribution / channel decoys
    ("BADCH", "WS-P", 105, "D", GATE_CH),
    ("UNAT", "WS-P", 106, None, None),
]


def base_scans():
    return list(SCANS)


def directed_log():
    """The story the task asks for, as an unordered log."""
    return [
        # 1: R <- C1 <- C2, two packages in C2
        pack_line("PK01", [(KIND_CTR, "R", None, None),
                           (KIND_CTR, "C1", None, "R"),
                           (KIND_CTR, "C2", None, "C1"),
                           (KIND_PKG, "PA", None, "C2"),
                           (KIND_PKG, "PB", None, "C2")]),
        # 2: swap the two levels of R and C1 in one operation
        pack_line("PK02", [(KIND_CTR, "C1", "R", "C2"),
                           (KIND_CTR, "C2", "C1", "R")]),
        # 3: move PB out, and register a no-op move of PA (keeps PK01)
        pack_line("PK03", [(KIND_PKG, "PB", "C2", "R"),
                           (KIND_PKG, "PA", "C2", "C2")]),
        # 4: a genuinely cyclic operation that also moves a package
        pack_line("PK04", [(KIND_CTR, "R", None, "C1"),
                           (KIND_PKG, "PB", "R", "C1")]),
        # 5: same number, different objects: an empty container PA in R
        #    receives the package PA
        pack_line("PK05", [(KIND_CTR, "PA", None, "R"),
                           (KIND_PKG, "PA", "C2", "PA")]),
        # 6: a channel decoy and an unattributed scan: skipped, one group each
        pack_line("BADCH", [(KIND_PKG, "PX", None, "C1")]),
        pack_line("UNAT", [(KIND_PKG, "PY", None, "C1")]),
        # 7: put a package into the container PA, then gate the cage
        pack_line("PK06", [(KIND_PKG, "PC", None, "PA")]),
        gate_line("GA01", "R"),
        # 8: after the gate, try to pack the inner box elsewhere -- refused
        pack_line("PK07", [(KIND_CTR, "C1", "C2", "NEW")]),
        pack_line("PK08", [(KIND_CTR, "C2", "R", None)]),
        # 9: the inner box cannot be gated on its own either, and neither
        #    can the cage that already left
        gate_line("GA02", "C2"),
        gate_line("GA04", "R"),
        # 10: an empty container gates and still yields a manifest
        pack_line("PK09", [(KIND_CTR, "EMPTY", None, None)]),
        gate_line("GA03", "EMPTY"),
    ]


def scenario_expected():
    """The directed run, pinned item by item."""
    return {
        "GA01": (300, "R", ["PA", "PB", "PC"], [
            (KIND_CTR, "C1", "C2", "PK02"),
            (KIND_CTR, "C2", "R", "PK02"),
            (KIND_CTR, "PA", "R", "PK05"),
            (KIND_CTR, "R", None, None),
            (KIND_PKG, "PA", "PA", "PK05"),
            (KIND_PKG, "PB", "R", "PK03"),
            (KIND_PKG, "PC", "PA", "PK06"),
        ]),
        "GA03": (440, "EMPTY", [], [
            (KIND_CTR, "EMPTY", None, None),
        ]),
    }

def test_directed():
    with tempfile.TemporaryDirectory() as d:
        lp = os.path.join(d, "ledger.db")
        make_ledger(lp, base_scans())
        out = os.path.join(d, "m.db")
        log = directed_log()
        res = build_manifest(lp, PACK_CH, GATE_CH, log, out)
        assert res["manifests"] == 2, res
        assert res["packages"] == 3, res
        db = read_db(out)
        got = {}
        for sid, (gid, ts, root) in db["manifests"].items():
            got[sid] = (ts, root, db["pkgs"].get(gid, []), db["rel"][gid])
        assert got == scenario_expected(), \
            json.dumps(got, indent=1, default=str)

        g = db["groups"]
        assert g["PK04"][4] == 0 and "cycle" in g["PK04"][5]
        # PK04 rejected as a whole: its package move did not land either
        gid = db["manifests"]["GA01"][0]
        assert (KIND_PKG, "PB", "C1", "PK04") not in db["rel"][gid]
        assert (KIND_PKG, "PB", "R", "PK03") in db["rel"][gid]
        assert g["BADCH"][4] == 0 and "channel" in g["BADCH"][5]
        assert g["UNAT"][4] == 0 and "unattributed" in g["UNAT"][5]
        assert g["PK07"][4] == 0 and "left the yard" in g["PK07"][5]
        assert g["PK08"][4] == 0 and "left the yard" in g["PK08"][5]
        assert g["GA02"][4] == 0 and "left the yard" in g["GA02"][5]
        assert g["GA04"][4] == 0 and "left the yard" in g["GA04"][5]
        # a no-change move keeps the original evidence scan
        assert (KIND_PKG, "PA", "PA", "PK05") in db["rel"][gid]
        # the frozen ledger rows carried into the result
        assert set(db["scans"]) == {s[0] for s in base_scans()}


def test_order_and_row_invariance():
    """Reversing the log line order and the row order inside the
    ownership-reversal group must not change a single delivered byte."""
    with tempfile.TemporaryDirectory() as d:
        lp = os.path.join(d, "ledger.db")
        make_ledger(lp, base_scans())
        log = directed_log()
        a = os.path.join(d, "a.db")
        build_manifest(lp, PACK_CH, GATE_CH, log, a)
        # rebuild with every line reversed and the PK02 rows reversed too
        log2 = [json.loads(x) for x in reversed(log)]
        for obj in log2:
            if obj["scan"] == "PK02":
                obj["pack"]["items"].reverse()
        b = os.path.join(d, "b.db")
        build_manifest(lp, PACK_CH, GATE_CH,
                       [json.dumps(o, ensure_ascii=False) for o in log2], b)
        da, dbn = read_db(a), read_db(b)
        assert da["manifests"] == dbn["manifests"]
        assert da["rel"] == dbn["rel"] and da["pkgs"] == dbn["pkgs"]
        assert {k: (v[4], v[5]) for k, v in da["groups"].items()} == \
            {k: (v[4], v[5]) for k, v in dbn["groups"].items()}


def test_validation_rejects_whole_request():
    with tempfile.TemporaryDirectory() as d:
        lp = os.path.join(d, "ledger.db")
        make_ledger(lp, base_scans())
        good = [pack_line("PK01", [(KIND_CTR, "R", None, None)])]

        def attempt(name, log, ledger_path=None, pack_ch=PACK_CH,
                    gate_ch=GATE_CH):
            out = os.path.join(d, name + ".db")
            try:
                build_manifest(ledger_path or lp, pack_ch, gate_ch, log, out)
            except SourceInvalid:
                pass
            else:
                raise AssertionError(f"{name}: invalid log accepted")
            assert not os.path.exists(out), f"{name}: result left behind"
            assert not os.path.exists(out + ".tmp"), f"{name}: tmp left"
            assert not [f for f in os.listdir(d) if "staging" in f], \
                f"{name}: staging left behind"

        bad = [
            ["not json"],
            [json.dumps({"scan": "PK01"})],
            [json.dumps({"scan": "PK01", "pack": {"items": []}})],
            [json.dumps({"scan": "", "gate": "R"})],
            [json.dumps({"scan": "PK01", "pack": {"items": [
                {"kind": "box", "id": "X", "from": None, "to": None}]}})],
            [pack_line("PK01", [("pkg", "", None, None)])],
            [pack_line("PK01", [(KIND_PKG, "X", "", None)])],
            [json.dumps({"scan": "PK01", "gate": ""})],
            [json.dumps({"scan": "PK01", "gate": 7})],
            # the same object twice in one group (even across lines)
            [pack_line("PK01", [(KIND_PKG, "X", None, "R")]),
             pack_line("PK01", [(KIND_PKG, "X", "R", None)])],
            # a gate split over two lines / mixed with packing lines
            [gate_line("GA01", "R"), gate_line("GA01", "C1")],
            [gate_line("GA01", "R"),
             pack_line("GA01", [(KIND_PKG, "X", None, "R")])],
            # a dangling ledger reference
            [pack_line("NOPE", [(KIND_PKG, "X", None, "R")])],
        ]
        for k, log in enumerate(bad):
            attempt(f"bad-{k}", log)
        # two channel numbers must be non-empty and different
        out = os.path.join(d, "same.db")
        for a, b in (("", GATE_CH), (PACK_CH, ""), (PACK_CH, PACK_CH)):
            try:
                build_manifest(lp, a, b, good, out)
            except SourceInvalid:
                pass
            else:
                raise AssertionError("bad channels accepted")
        assert not os.path.exists(out)
        # a receiver db is not a ledger; a missing ledger is refused
        from handover.receiver import Receiver
        recv = os.path.join(d, "recv.db")
        Receiver(recv).load_initial({"A": ("WS-1", "CH-1")})
        for path, code in ((recv, "not_a_ledger"),
                           (os.path.join(d, "nope.db"), "ledger_missing")):
            out = os.path.join(d, "x.db")
            try:
                build_manifest(path, PACK_CH, GATE_CH, good, out)
            except ManifestReject as e:
                assert e.code == code, e.code
            else:
                raise AssertionError(f"{path}: accepted")
            assert not os.path.exists(out)
        # a manifest is written once
        out = os.path.join(d, "once.db")
        build_manifest(lp, PACK_CH, GATE_CH, good, out)
        try:
            build_manifest(lp, PACK_CH, GATE_CH, good, out)
        except ManifestReject as e:
            assert e.code == "manifest_exists", e.code
        else:
            raise AssertionError("second manifest over one path accepted")


def run_ref(scans, log):
    """Build through the engine and compare with the reference model."""
    with tempfile.TemporaryDirectory() as d:
        lp = os.path.join(d, "ledger.db")
        make_ledger(lp, scans)
        out = os.path.join(d, "m.db")
        build_manifest(lp, PACK_CH, GATE_CH, log, out)
        return read_db(out)


def test_vs_reference():
    rng = random.Random(20261123)
    names = ["A", "B", "C", "D", "E", "a"]      # case-sensitive on purpose
    for case in range(120):
        parents = [None] + names
        seen = set()
        scans = []
        log = []
        # packing scans first, then gates;  each scan is its own group
        n_pack = rng.randint(3, 14)
        for k in range(n_pack):
            sid = f"P{k:02d}"
            scans.append((sid, "W", 100 + k, "D", PACK_CH))
            items = []
            used = set()
            for _ in range(rng.randint(1, 4)):
                kind = rng.choice([KIND_PKG, KIND_CTR])
                oid = rng.choice(names)
                if (kind, oid) in used:
                    continue
                used.add((kind, oid))
                src = rng.choice(parents)
                dst = rng.choice(parents)
                items.append((kind, oid, src, dst))
            if items:
                log.append(pack_line(sid, items))
        n_gate = rng.randint(1, 5)
        for k in range(n_gate):
            sid = f"G{k:02d}"
            scans.append((sid, "W", 300 + k, "E", GATE_CH))
            log.append(gate_line(sid, rng.choice(names)))
        rng.shuffle(log)
        want, verdict = ref_replay(scans, log, PACK_CH, GATE_CH)
        db = run_ref(scans, log)
        # compare every group's adoption, then every delivered relation
        for sid, (adopted, _) in verdict.items():
            assert db["groups"][sid][4] == (1 if adopted else 0), \
                (case, sid, db["groups"][sid], adopted)
        got = []
        for sid, (gid, ts, root) in db["manifests"].items():
            got.append((sid, ts, root, db["rel"][gid],
                        db["pkgs"].get(gid, [])))
        got.sort(key=lambda x: x[1])
        want.sort(key=lambda x: x[0])       # want is already in replay order
        want2 = [(s, ts, root, sub, pkgs) for s, ts, root, sub, pkgs in want]
        got_sorted = sorted(got, key=lambda x: x[0])
        want_sorted = sorted(want2, key=lambda x: x[0])
        assert got_sorted == want_sorted, (case, got_sorted, want_sorted)


def test_ledger_correction_rebuild():
    """Use the existing ledger correction flow to revoke one scan's
    packing entitlement, rebuild from the same log, and compare the two
    manifests package by package and relation by relation."""
    with tempfile.TemporaryDirectory() as d:
        # a real ledger through the full pipeline, with a real correction
        from handover import core
        from handover.ledger import Ledger
        from handover.receiver import Receiver
        OLD = {"A": ("WS-1", "CH-1"), "B": ("WS-2", "CH-2")}
        TARGET = {"A": ("WS-2", "CH-9"), "B": ("WS-3", "CH-1")}
        order, _ = core.plan(OLD, TARGET, frozenset({"A", "B"}))
        recv = os.path.join(d, "recv.db")
        r = Receiver(recv)
        r.load_initial(OLD)
        for k in range(len(order["steps"])):
            r.submit(order, k)
        r.close()
        # scans: packing on WS-1 while A still holds it;  the gate on WS-2
        # after A moved there -- so packing is CH-1 and gating is CH-9.
        # P3 lands after B took WS-3/CH-1 and stays valid either way, so it
        # gives a surviving relation to compare layer by layer.
        scans = [{"id": "P1", "ws": "WS-1", "ts": 100},
                 {"id": "P2", "ws": "WS-1", "ts": 110},
                 {"id": "P3", "ws": "WS-3", "ts": 200},
                 {"id": "G1", "ws": "WS-2", "ts": 400},
                 {"id": "G2", "ws": "WS-2", "ts": 420}]
        times = ledger.validate_step_times(
            {"format": ledger.TIMES_FORMAT,
             "order_hash": order["order_hash"],
             "times": [{"seq": q, "ts": t} for q, t in
                       enumerate([150, 160, 170])]}, order)
        lp = os.path.join(d, "ledger.db")
        Ledger.build(lp, OLD, order, recv, times, scans)
        log = [pack_line("P1", [(KIND_CTR, "R", None, None),
                                (KIND_PKG, "X", None, "R")]),
               pack_line("P2", [(KIND_PKG, "Y", None, "R")]),
               pack_line("P3", [(KIND_CTR, "S", None, None),
                                (KIND_PKG, "Z", None, "S")]),
               gate_line("G1", "R"),
               gate_line("G2", "S")]
        m1 = os.path.join(d, "m1.db")
        build_manifest(lp, "CH-1", "CH-9", log, m1)
        db1 = read_db(m1)
        assert db1["groups"]["P2"][4] == 1, db1["groups"]["P2"]
        assert db1["groups"]["P1"][4] == 1
        assert db1["groups"]["G1"][4] == 1
        assert db1["groups"]["G2"][4] == 1
        assert db1["pkgs"][1] == ["X", "Y"] and db1["pkgs"][2] == ["Z"]
        # Operations corrects step 0 to land before the packing scans:
        # WS-1 is vacant by then, so P1/P2 lose their packing entitlement
        corr = os.path.join(d, "corr.json")
        with open(corr, "w") as f:
            json.dump({"format": ledger.CORRECTION_FORMAT,
                       "order_hash": order["order_hash"],
                       "changes": [{"seq": 0, "ts": 50}]}, f)
        p = cli("ledger-correct", "--db", lp, "--correction", corr,
                expect_rc=0)
        assert json.loads(p.stdout)["result"] == "applied"
        m2 = os.path.join(d, "m2.db")
        build_manifest(lp, "CH-1", "CH-9", log, m2)
        db2 = read_db(m2)
        # both WS-1 packing scans lost their entitlement -> the first cage
        # gates empty, while the S subtree keeps its layer and evidence
        assert db2["groups"]["P1"][4] == 0 and db2["groups"]["P2"][4] == 0
        assert db2["groups"]["P3"][4] == 1
        assert db2["groups"]["G1"][4] == 1
        assert db2["groups"]["G2"][4] == 1
        assert db2["pkgs"].get(1, []) == [] and db2["pkgs"][2] == ["Z"]
        assert db2["rel"][1] == [(KIND_CTR, "R", None, None)]
        assert db2["rel"][2] == [(KIND_CTR, "S", None, None),
                                 (KIND_PKG, "Z", "S", "P3")]
        # the delivered first manifest is self-contained and unchanged
        assert read_db(m1) == db1
        assert db1["meta"]["ledger_scans_sha256"] != \
            db2["meta"]["ledger_scans_sha256"]
        assert db2["meta"]["ledger_correction_count"] == "1"
        # per-relation comparison: P1's evidence rows changed nowhere
        assert (KIND_PKG, "X", "R", "P1") not in db2["rel"][1]


def test_isolated_access():
    """Unrelated containers beside the ones being processed must not be
    touched by packing or gating -- shown by plans and VM-step counts."""
    with tempfile.TemporaryDirectory() as d:
        lp = os.path.join(d, "ledger.db")
        make_ledger(lp, base_scans())

        # query plans: every per-group probe is a key search, and the
        # subtree walk is an index search;  nothing scans own
        con = sqlite3.connect(":memory:")
        con.executescript(STAGING_SCHEMA)
        for sql, args, prefix in (
                (SQL_LOOKUP, ("pkg", "X"), "SEARCH own USING PRIMARY KEY"),
                (SQL_CAND_PARENT, (1, "pkg", "X"),
                 "SEARCH owned_list USING"),
                (SQL_GATE_SUBTREE, ("R", 1),
                 "SEARCH o USING INDEX own_parent")):
            plan = [r[3] for r in con.execute("EXPLAIN QUERY PLAN " + sql,
                                              args)]
            assert any(p.startswith(prefix) for p in plan), (sql, plan)
            assert not any(p.startswith("SCAN own") for p in plan), plan
        for sql in (SQL_MARK_GONE,):
            plan = [r[3] for r in con.execute("EXPLAIN QUERY PLAN " + sql,
                                              (1, 1))]
            assert not any(p.startswith("SCAN own") for p in plan), plan
            assert any(p.startswith("SEARCH owned_list USING")
                       for p in plan), plan
        con.close()

        # runtime-generated noise: a huge unrelated cage, never mentioned
        noise = [(f"pkg", f"N{i}", "BIG", "P0") for i in range(20000)]
        noise += [(KIND_CTR, f"B{i}", "BIG", "P0") for i in range(2000)]

        def run(n_noise, progress):
            with tempfile.TemporaryDirectory(dir=d) as td:
                con2 = sqlite3.connect(os.path.join(td, "s.db"))
                con2.executescript(STAGING_SCHEMA)
                con2.executemany(
                    "INSERT INTO own (kind, id, parent, via) "
                    "VALUES (?, ?, ?, ?)", noise[:n_noise])
                con2.executemany(
                    "INSERT INTO own (kind, id, parent, via) "
                    "VALUES (?, ?, ?, ?)",
                    [(KIND_CTR, "R", None, None),
                     (KIND_CTR, "C1", "R", "PK01"),
                     (KIND_CTR, "C2", "C1", "PK01"),
                     (KIND_PKG, "PA", "C2", "PK01"),
                     (KIND_PKG, "PB", "C2", "PK01")])
                # one no-change pack, one real pack, one gate
                def work():
                    from handover.manifest import apply_pack, apply_gate
                    apply_pack(con2, "PK03", [
                        (KIND_PKG, "PA", "C2", "C2")], 100)
                    apply_pack(con2, "PK04", [
                        (KIND_PKG, "PB", "C2", "R")], 101)
                    apply_gate(con2, "R", 102)
                ops = measure_ops(con2, work, every=100)
                con2.close()
                return ops

        small = run(200, False)
        big = run(len(noise), False)
        assert big <= small * 1.2 + 200, (small, big)
        print(f"    (unrelated containers: {len(noise)} more rows -> "
              f"VM-ops {small} -> {big})")

        # and the whole manifest run's peak Python memory stays flat
        # while the log itself is large: the log lives on disk
        def peak(log_size):
            with tempfile.TemporaryDirectory(dir=d) as td:
                logp = os.path.join(td, "log.jsonl")
                with open(logp, "w") as f:
                    f.write(pack_line("PK01", [
                        (KIND_CTR, "R", None, None)]) + "\n")
                    for i in range(log_size):
                        f.write(gate_line(f"GA{i:05d}", "R") + "\n")
                scans = base_scans() + [
                    (f"GA{i:05d}", "WS-G", 1000 + i, "E", GATE_CH)
                    for i in range(log_size)]
                lp2 = os.path.join(td, "l.db")
                make_ledger(lp2, scans)
                out = os.path.join(td, "m.db")
                with open(logp) as f:
                    tracemalloc.start()
                    build_manifest(lp2, PACK_CH, GATE_CH, f, out)
                    pk = tracemalloc.get_traced_memory()[1]
                    tracemalloc.stop()
                return pk

        peak(50)
        p1, p2 = peak(200), peak(2000)
        assert p2 < p1 * 3 + 1024 * 1024, (p1, p2)
        print(f"    (log size 200 -> 2000 groups: peak python memory "
              f"{p1} -> {p2} bytes)")


def test_cli():
    with tempfile.TemporaryDirectory() as d:
        lp = os.path.join(d, "ledger.db")
        make_ledger(lp, base_scans())
        logp = os.path.join(d, "log.jsonl")
        with open(logp, "w") as f:
            f.write("\n".join(directed_log()) + "\n")
        out = os.path.join(d, "m.db")
        res = json.loads(cli("manifest", "--ledger", lp,
                             "--pack-ch", PACK_CH, "--gate-ch", GATE_CH,
                             "--log", logp, "--out", out,
                             expect_rc=0).stdout)
        assert res["result"] == "rebuilt" and res["manifests"] == 2
        st = json.loads(cli("manifest-status", "--db", out,
                            "--scan", "GA01", expect_rc=0).stdout)
        ship = st["shipment"]
        assert ship["root"] == "R" and ship["packages"] == ["PA", "PB", "PC"]
        assert {(o["kind"], o["id"]) for o in ship["objects"]} == {
            (KIND_CTR, "R"), (KIND_CTR, "C1"), (KIND_CTR, "C2"),
            (KIND_CTR, "PA"), (KIND_PKG, "PA"), (KIND_PKG, "PB"),
            (KIND_PKG, "PC")}
        bad = [o for o in ship["objects"] if o["kind"] == KIND_PKG
               and o["id"] == "PA"]
        assert [o["parent"] for o in bad] == ["PA"]
        # an unknown gate scan is simply absent
        st = json.loads(cli("manifest-status", "--db", out,
                            "--scan", "GA02", expect_rc=0).stdout)
        assert st["shipment"] is None
        # the same output path is refused
        r = json.loads(cli("manifest", "--ledger", lp, "--pack-ch", PACK_CH,
                           "--gate-ch", GATE_CH, "--log", logp,
                           "--out", out, expect_rc=5).stdout)
        assert r["code"] == "manifest_exists"
        # invalid source: exit 3 and no result
        badp = os.path.join(d, "bad.jsonl")
        with open(badp, "w") as f:
            f.write("not json\n")
        out2 = os.path.join(d, "m2.db")
        r = json.loads(cli("manifest", "--ledger", lp, "--pack-ch", PACK_CH,
                           "--gate-ch", GATE_CH, "--log", badp,
                           "--out", out2, expect_rc=3).stdout)
        assert r["result"] == "invalid_source"
        assert not os.path.exists(out2) and not os.path.exists(out2 + ".tmp")
        # manifest-status on something that is not a manifest
        r = json.loads(cli("manifest-status", "--db", lp, expect_rc=5).stdout)
        assert r["code"] == "not_a_manifest"


TESTS = [
    ("directed packing/gating story", test_directed),
    ("log line and group row order invariance", test_order_and_row_invariance),
    ("invalid logs and requests leave nothing behind",
     test_validation_rejects_whole_request),
    ("randomized cross-check vs independent reference model",
     test_vs_reference),
    ("ledger correction between two builds: compare both manifests",
     test_ledger_correction_rebuild),
    ("unrelated containers never take part in packing or gating",
     test_isolated_access),
    ("manifest CLI end to end", test_cli),
]


def main():
    for name, fn in TESTS:
        fn()
        print(f"PASS: {name}")
    print("\nALL MANIFEST TESTS PASSED")


if __name__ == "__main__":
    main()
