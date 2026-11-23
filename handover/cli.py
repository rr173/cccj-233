"""Command line interface for the handover tool.

  handover plan   --old O.jsonl --target T.jsonl --offline P.json --out ORDER.json
  handover init   --db R.db --table O.jsonl
  handover submit --db R.db --order ORDER.json --seq K
  handover run    --db R.db --order ORDER.json [--crash-before-seq K]
                  [--crash-after-commit-seq K]
  handover status --db R.db [--dump]

  handover ledger-build   --db L.db --snapshot O.jsonl --order ORDER.json
                          --receiver R.db --times TIMES.json --scans SCANS.jsonl
  handover ledger-correct --db L.db --correction C.json [--crash-before-commit]
  handover ledger-status  --db L.db [--scans]

  handover verify       --ledger L.db --routes R.jsonl --assoc A.jsonl --out V.db
  handover verify-check --result V.db --ledger L.db

  handover manifest        --ledger L.db --pack-ch CH-A --gate-ch CH-B
                           --log LOG.jsonl --out M.db
  handover manifest-status --db M.db [--scan GATE_SCAN]

Exit codes: 0 ok; 2 usage/IO error; 3 invalid source material; 4 valid
source but no legal migration exists (a wait-cycle report is printed);
5 a submission was rejected by the receiver / a ledger request was
rejected / a verification or manifest request was rejected or a result
failed its re-check; 12 the ledger-correct crash hook fired (test hook
only).
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import urllib.parse

from . import core
from . import ledger
from . import manifest as manifest_mod
from . import verify
from .ledger import Ledger, LedgerReject
from .manifest import ManifestReject
from .receiver import Receiver, Reject
from .verify import VerifyReject


def _read_lines(path):
    with open(path, encoding="utf-8") as f:
        return f.read().splitlines()


def _iter_lines(path):
    """Stream the lines of a JSONL file (never the whole file in memory)."""
    with open(path, encoding="utf-8") as f:
        for line in f:
            yield line


def _print(obj):
    print(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True))


def cmd_plan(a):
    try:
        old = core.validate_table(_read_lines(a.old), "old")
        target = core.validate_table(_read_lines(a.target), "target")
        with open(a.offline, encoding="utf-8") as f:
            offline = core.validate_offline_list(json.load(f), old, target)
    except core.SourceInvalid as e:
        _print({"result": "invalid_source", "problems": e.problems})
        return 3
    except (OSError, json.JSONDecodeError) as e:
        _print({"result": "invalid_source", "problems": [str(e)]})
        return 3
    try:
        order, stats = core.plan(old, target, offline)
    except core.MigrationImpossible as e:
        _print(e.report)  # no order file is written on failure
        return 4
    tmp = a.out + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(core.dump_order(order))
    os.replace(tmp, a.out)
    _print({"result": "planned",
            "steps": len(order["steps"]),
            "order_hash": order["order_hash"],
            "stats": stats})
    return 0


def cmd_init(a):
    try:
        rows = core.validate_table(_read_lines(a.table), "table")
    except core.SourceInvalid as e:
        _print({"result": "invalid_source", "problems": e.problems})
        return 3
    except OSError as e:
        _print({"result": "invalid_source", "problems": [str(e)]})
        return 3
    r = Receiver(a.db)
    try:
        r.load_initial(rows)
    except Reject as e:
        _print({"result": "rejected", "code": e.code, "detail": e.detail})
        return 5
    _print({"result": "loaded", "rows": len(rows),
            "bindings_hash": r.bindings_hash()})
    return 0


def cmd_submit(a):
    try:
        order = core.load_order(a.order)
    except core.SourceInvalid as e:
        _print({"result": "invalid_order", "problems": e.problems})
        return 3
    r = Receiver(a.db)
    try:
        res = r.submit(order, a.seq)
    except Reject as e:
        _print({"result": "rejected", "seq": a.seq,
                "code": e.code, "detail": e.detail})
        return 5
    _print({"seq": a.seq, **res})
    return 0


def cmd_run(a):
    try:
        order = core.load_order(a.order)
    except core.SourceInvalid as e:
        _print({"result": "invalid_order", "problems": e.problems})
        return 3
    r = Receiver(a.db)
    if not order["steps"]:
        try:
            r.begin(order)
        except Reject as e:
            _print({"result": "rejected", "code": e.code, "detail": e.detail})
            return 5
        st = r.status()
        _print({"result": "complete" if st["complete"] else "incomplete", **st})
        return 0 if st["complete"] else 1
    while True:
        st = r.status()
        if st["order_hash"] is not None:
            if st["order_hash"] != order["order_hash"]:
                _print({"result": "rejected", "code": "foreign_order",
                        "detail": "receiver holds a different work order"})
                return 5
            if st["complete"]:
                _print({"result": "complete", **st})
                return 0
        seq = st["committed"]
        if a.crash_before_seq is not None and seq == a.crash_before_seq:
            os._exit(10)  # simulated stop before the commit of step `seq`
        try:
            res = r.submit(order, seq)
        except Reject as e:
            _print({"result": "rejected", "seq": seq,
                    "code": e.code, "detail": e.detail})
            return 5
        if a.crash_after_commit_seq is not None and seq == a.crash_after_commit_seq:
            os._exit(11)  # commit landed; the response is lost
        _print({"seq": seq, **res})
        if res.get("complete"):
            return 0


def cmd_status(a):
    r = Receiver(a.db)
    st = r.status()
    if a.dump:
        st["bindings"] = [
            {"id": i, "ws": w, "ch": c}
            for i, (w, c) in sorted(r.bindings().items())
        ]
    _print(st)
    return 0


def cmd_ledger_build(a):
    try:
        snapshot = core.validate_table(_read_lines(a.snapshot), "snapshot")
        order = core.load_order(a.order)
        with open(a.times, encoding="utf-8") as f:
            times_obj = json.load(f)
        times = ledger.validate_step_times(times_obj, order)
        scans = ledger.validate_scans(_read_lines(a.scans))
    except core.SourceInvalid as e:
        _print({"result": "invalid_source", "problems": e.problems})
        return 3
    except LedgerReject as e:
        _print({"result": "rejected", "code": e.code, "detail": e.detail})
        return 5
    except (OSError, json.JSONDecodeError) as e:
        _print({"result": "invalid_source", "problems": [str(e)]})
        return 3
    try:
        res = Ledger.build(a.db, snapshot, order, a.receiver, times, scans)
    except LedgerReject as e:
        _print({"result": "rejected", "code": e.code, "detail": e.detail})
        return 5
    _print({"result": "built", **res})
    return 0


def cmd_ledger_correct(a):
    try:
        with open(a.correction, encoding="utf-8") as f:
            doc = ledger.validate_correction(json.load(f))
    except core.SourceInvalid as e:
        _print({"result": "invalid_source", "problems": e.problems})
        return 3
    except (OSError, json.JSONDecodeError) as e:
        _print({"result": "invalid_source", "problems": [str(e)]})
        return 3
    try:
        lg = Ledger(a.db)
        res = lg.correct(doc, crash_before_commit=a.crash_before_commit)
    except LedgerReject as e:
        _print({"result": "rejected", "code": e.code, "detail": e.detail})
        return 5
    _print(res)
    return 0


def cmd_ledger_status(a):
    try:
        lg = Ledger(a.db, write=False)
    except LedgerReject as e:
        _print({"result": "rejected", "code": e.code, "detail": e.detail})
        return 5
    _print(lg.status(with_scans=a.scans))
    return 0


def cmd_verify(a):
    try:
        res = verify.verify(a.ledger, _iter_lines(a.routes),
                            _iter_lines(a.assoc), a.out)
    except core.SourceInvalid as e:
        _print({"result": "invalid_source", "problems": e.problems})
        return 3
    except VerifyReject as e:
        _print({"result": "rejected", "code": e.code, "detail": e.detail})
        return 5
    except OSError as e:
        _print({"result": "invalid_source", "problems": [str(e)]})
        return 3
    _print(res)
    return 0


def cmd_verify_check(a):
    try:
        ok, problems = verify.verify_result(a.result, a.ledger)
    except VerifyReject as e:
        _print({"result": "rejected", "code": e.code, "detail": e.detail})
        return 5
    except OSError as e:
        _print({"result": "rejected", "code": "io_error", "detail": str(e)})
        return 5
    if ok:
        _print({"result": "consistent"})
        return 0
    _print({"result": "inconsistent", "problems": problems})
    return 5


def cmd_manifest(a):
    try:
        res = manifest_mod.build_manifest(a.ledger, a.pack_ch, a.gate_ch,
                                          _iter_lines(a.log), a.out)
    except core.SourceInvalid as e:
        _print({"result": "invalid_source", "problems": e.problems})
        return 3
    except ManifestReject as e:
        _print({"result": "rejected", "code": e.code, "detail": e.detail})
        return 5
    except OSError as e:
        _print({"result": "invalid_source", "problems": [str(e)]})
        return 3
    _print(res)
    return 0


def cmd_manifest_status(a):
    try:
        con = sqlite3.connect(
            f"file:{urllib.parse.quote(a.db)}?mode=ro", uri=True)
    except sqlite3.Error as e:
        _print({"result": "rejected", "code": "manifest_unreadable",
                "detail": str(e)})
        return 5
    try:
        try:
            meta = dict(con.execute("SELECT k, v FROM meta"))
        except sqlite3.Error as e:
            _print({"result": "rejected", "code": "not_a_manifest",
                    "detail": str(e)})
            return 5
        if meta.get("format") != manifest_mod.MANIFEST_FORMAT:
            _print({"result": "rejected", "code": "not_a_manifest",
                    "detail": f"{a.db} is not a "
                              f"{manifest_mod.MANIFEST_FORMAT} database"})
            return 5
        out = {"format": meta["format"], "meta": meta,
               "groups": [
                   {"ord": o, "scan": s, "ts": t, "use": u,
                    "adopted": bool(ad),
                    "reasons": json.loads(r) if r is not None else []}
                   for o, s, t, u, ad, r in con.execute(
                       "SELECT ord, scan_id, ts, use, adopted, reason "
                       "FROM groups ORDER BY ord")]}
        if a.scan is not None:
            row = con.execute(
                "SELECT gate_id, scan_id, ts, root_id FROM manifests "
                "WHERE scan_id = ?", (a.scan,)).fetchone()
            if row is None:
                out["shipment"] = None
            else:
                gid, sid, ts, root = row
                objs = [
                    {"kind": k, "id": i, "parent": p, "via": v}
                    for k, i, p, v in con.execute(
                        "SELECT kind, id, parent, via FROM manifest "
                        "WHERE gate_id = ? ORDER BY kind, id", (gid,))]
                out["shipment"] = {
                    "scan_id": sid, "ts": ts, "root": root,
                    "packages": [o["id"] for o in objs
                                 if o["kind"] == manifest_mod.KIND_PKG],
                    "objects": objs}
        _print(out)
        return 0
    finally:
        con.close()


def build_parser():
    p = argparse.ArgumentParser(
        prog="handover",
        description="Offline handover of scanner-station binding tables to a "
                    "legacy per-device receiver.")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("plan", help="build a work order from old/target tables")
    sp.add_argument("--old", required=True, help="old binding table (JSONL)")
    sp.add_argument("--target", required=True, help="target binding table (JSONL)")
    sp.add_argument("--offline", required=True,
                    help="JSON array of ids allowed to go temporarily offline")
    sp.add_argument("--out", required=True,
                    help="work order output path (written only on success)")
    sp.set_defaults(func=cmd_plan)

    sp = sub.add_parser("init", help="load the initial table into a fresh receiver db")
    sp.add_argument("--db", required=True)
    sp.add_argument("--table", required=True)
    sp.set_defaults(func=cmd_init)

    sp = sub.add_parser("submit", help="submit exactly one step of an order")
    sp.add_argument("--db", required=True)
    sp.add_argument("--order", required=True)
    sp.add_argument("--seq", required=True, type=int)
    sp.set_defaults(func=cmd_submit)

    sp = sub.add_parser("run", help="submit steps until the order is complete")
    sp.add_argument("--db", required=True)
    sp.add_argument("--order", required=True)
    sp.add_argument("--crash-before-seq", type=int, default=None,
                    help="test hook: die right before committing this step")
    sp.add_argument("--crash-after-commit-seq", type=int, default=None,
                    help="test hook: die right after committing this step, "
                         "before reporting it")
    sp.set_defaults(func=cmd_run)

    sp = sub.add_parser("status", help="show receiver progress and bindings hash")
    sp.add_argument("--db", required=True)
    sp.add_argument("--dump", action="store_true", help="also dump all rows")
    sp.set_defaults(func=cmd_status)

    sp = sub.add_parser(
        "ledger-build",
        help="build a scan-attribution ledger from a completed handover")
    sp.add_argument("--db", required=True, help="ledger path (must not exist)")
    sp.add_argument("--snapshot", required=True,
                    help="original binding snapshot (JSONL)")
    sp.add_argument("--order", required=True, help="work order (JSON)")
    sp.add_argument("--receiver", required=True,
                    help="completed receiver db (opened read-only)")
    sp.add_argument("--times", required=True,
                    help="step effective times (handover-step-times/1 JSON)")
    sp.add_argument("--scans", required=True, help="scan records (JSONL)")
    sp.set_defaults(func=cmd_ledger_build)

    sp = sub.add_parser("ledger-correct",
                        help="apply a group of step-time corrections in place")
    sp.add_argument("--db", required=True)
    sp.add_argument("--correction", required=True,
                    help="handover-time-correction/1 JSON")
    sp.add_argument("--crash-before-commit", action="store_true",
                    help="test hook: die after the correction's writes, "
                         "before COMMIT (exit code 12)")
    sp.set_defaults(func=cmd_ledger_correct)

    sp = sub.add_parser("ledger-status",
                        help="show step times, counts and applied corrections")
    sp.add_argument("--db", required=True)
    sp.add_argument("--scans", action="store_true",
                    help="also dump every scan's attribution")
    sp.set_defaults(func=cmd_ledger_status)

    sp = sub.add_parser(
        "verify",
        help="offline per-package missing-scan verification against the ledger")
    sp.add_argument("--ledger", required=True,
                    help="scan-attribution ledger db (opened read-only)")
    sp.add_argument("--routes", required=True, help="package routes (JSONL)")
    sp.add_argument("--assoc", required=True,
                    help="package/scan associations (JSONL)")
    sp.add_argument("--out", required=True,
                    help="verification result db path (must not exist)")
    sp.set_defaults(func=cmd_verify)

    sp = sub.add_parser(
        "verify-check",
        help="re-check a verification result against the current ledger")
    sp.add_argument("--result", required=True, help="verification result db")
    sp.add_argument("--ledger", required=True,
                    help="scan-attribution ledger db (opened read-only)")
    sp.set_defaults(func=cmd_verify_check)

    sp = sub.add_parser(
        "manifest",
        help="rebuild a shipment manifest offline from the ledger and a "
             "packing log")
    sp.add_argument("--ledger", required=True,
                    help="scan-attribution ledger db (opened read-only)")
    sp.add_argument("--pack-ch", required=True,
                    help="channel number registered for packing scans")
    sp.add_argument("--gate-ch", required=True,
                    help="channel number registered for gate scans")
    sp.add_argument("--log", required=True, help="packing log (JSONL)")
    sp.add_argument("--out", required=True,
                    help="manifest result db path (must not exist)")
    sp.set_defaults(func=cmd_manifest)

    sp = sub.add_parser(
        "manifest-status",
        help="read a manifest back: groups, adoption and one shipment")
    sp.add_argument("--db", required=True, help="manifest result db")
    sp.add_argument("--scan", default=None,
                    help="also print the shipment of this gate scan id")
    sp.set_defaults(func=cmd_manifest_status)
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    return args.func(args)
