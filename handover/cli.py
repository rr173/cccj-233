"""Command line interface for the handover tool.

  handover plan   --old O.jsonl --target T.jsonl --offline P.json --out ORDER.json
  handover init   --db R.db --table O.jsonl
  handover submit --db R.db --order ORDER.json --seq K
  handover run    --db R.db --order ORDER.json [--crash-before-seq K]
                  [--crash-after-commit-seq K]
  handover status --db R.db [--dump]

Exit codes: 0 ok; 2 usage/IO error; 3 invalid source material; 4 valid
source but no legal migration exists (a wait-cycle report is printed);
5 a submission was rejected by the receiver.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

from . import core
from .receiver import Receiver, Reject


def _read_lines(path):
    with open(path, encoding="utf-8") as f:
        return f.read().splitlines()


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
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    return args.func(args)
