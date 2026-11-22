"""SQLite scan-attribution ledger for one completed handover order.

The receiver records which workstation a device ends up at, but cannot
answer which channel a scan on handover day belongs to: scan logs carry
only a timestamp and a workstation number, and a workstation may fall
vacant mid-handover and later be taken over by another scanner.  This
ledger closes that gap.

Build (once):
  inputs  = the original binding snapshot (JSONL), the work order
            (handover-order/1), the *completed* receiver db (opened
            read-only) and a table of actual effective times, one per
            order step, non-decreasing by step number;
  checks  = order identity (order_hash), the snapshot digest
            (initial_hash) and the receiver's persisted step numbers and
            step hashes all match;  an incomplete receiver is not
            history and is never treated as such;
  effect  = every scan (JSONL: unique id, workstation, Unix seconds) is
            attributed to the (device, channel) holding its workstation
            at its second, or to the unattributed group;  per-(UTC day,
            channel) counts are stored so operations never re-scan the
            scan detail.  Steps sharing one second are seen by a scan of
            that second as all applied, in original step order;  before
            the first step the initial bindings hold;  a workstation
            that is offline or vacant attributes to nobody -- the
            previous occupant is never reused.

Correct (any number of times, in place):
  a correction submits a group of {seq, ts};  the group is validated as
  a whole (existing steps only, no duplicates, legal times, still
  non-decreasing by step number) -- any violation leaves the ledger
  untouched;  then only scans on the workstations the changed steps
  touch, inside [min(old, new), max(old, new)) per changed step
  (same-workstation windows merged), are re-attributed in place;  counts
  are moved net;  the whole correction is one transaction, so a crash
  before commit leaves the old state;  resending an already-applied
  identical correction is a verified no-op.
"""
from __future__ import annotations

import bisect
import collections
import json
import os
import sqlite3
import time
import urllib.parse

from . import core

LEDGER_FORMAT = "handover-scan-ledger/1"
TIMES_FORMAT = "handover-step-times/1"
CORRECTION_FORMAT = "handover-time-correction/1"

MAX_TS = 253402300799  # 9999-12-31T23:59:59Z

SCHEMA = """
CREATE TABLE meta(
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);
CREATE TABLE initial(          -- the original binding snapshot (all online)
  id TEXT PRIMARY KEY,
  ws TEXT NOT NULL,
  ch TEXT NOT NULL
);
CREATE TABLE steps(            -- the order's steps plus current effective time
  seq INTEGER PRIMARY KEY,
  id TEXT NOT NULL,
  op TEXT NOT NULL,
  before TEXT NOT NULL,        -- JSON before-state (content is immutable)
  after TEXT NOT NULL,         -- JSON after-state
  hash TEXT NOT NULL,          -- step hash, bound to the order identity
  ts INTEGER NOT NULL          -- current actual effective time
);
CREATE TABLE scans(
  scan_id TEXT PRIMARY KEY,
  ws TEXT NOT NULL,
  ts INTEGER NOT NULL,
  device TEXT,                 -- NULL = unattributed
  ch TEXT                      -- NULL = unattributed
);
CREATE INDEX scans_ws_ts ON scans(ws, ts);
CREATE TABLE counts(           -- per (UTC day, channel); NULL ch = unattributed
  day TEXT NOT NULL,
  ch TEXT,
  n INTEGER NOT NULL CHECK(n > 0)   -- zero-count groups are never stored
);
-- real channels are non-empty strings, so '' is a safe NULL sentinel here
CREATE UNIQUE INDEX counts_key ON counts(day, IFNULL(ch, ''));
CREATE TABLE corrections(
  hash TEXT PRIMARY KEY,       -- canonical hash of the correction document
  summary TEXT NOT NULL        -- JSON: changes, new times, windows, moved, net
);
"""

# The exact candidate query used by Ledger.correct();  tests measure this
# statement to show window-bounded access.
CANDIDATE_SQL = ("SELECT scan_id, ts, device, ch FROM scans "
                 "WHERE ws = ? AND ts >= ? AND ts < ? ORDER BY ts, scan_id")


class LedgerReject(Exception):
    """A build/correct request was rejected; the ledger was left unchanged."""

    def __init__(self, code, detail):
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}")


# ------------------------------------------------------------------ helpers

def _is_ts(v):
    return isinstance(v, int) and not isinstance(v, bool) and 0 <= v <= MAX_TS


def day_of(ts):
    """UTC calendar date (YYYY-MM-DD) of a Unix-seconds timestamp."""
    t = time.gmtime(ts)
    return f"{t.tm_year:04d}-{t.tm_mon:02d}-{t.tm_mday:02d}"


def _uri(path, mode):
    return f"file:{urllib.parse.quote(path)}?mode={mode}"


# ------------------------------------------------------------------ validation

def validate_scans(lines, name="scans"):
    """Parse scan JSONL.  Returns [{'id':..,'ws':..,'ts':..}, ...] in order.

    Raises SourceInvalid listing every problem found."""
    problems = []
    scans = []
    seen = set()
    for ln, raw in enumerate(lines, 1):
        if not raw.strip():
            continue
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError as e:
            problems.append(f"{name}:{ln}: invalid JSON: {e}")
            continue
        if not isinstance(obj, dict) or set(obj) != {"id", "ws", "ts"}:
            problems.append(f"{name}:{ln}: row must be an object with exactly "
                            "the keys id/ws/ts")
            continue
        sid, w, ts = obj["id"], obj["ws"], obj["ts"]
        ok = True
        if not isinstance(sid, str) or sid == "":
            problems.append(f"{name}:{ln}: id must be a non-empty string")
            ok = False
        if not isinstance(w, str) or w == "":
            problems.append(f"{name}:{ln}: ws must be a non-empty string")
            ok = False
        if not _is_ts(ts):
            problems.append(f"{name}:{ln}: ts must be an integer Unix second "
                            f"in [0, {MAX_TS}]")
            ok = False
        if not ok:
            continue
        if sid in seen:
            problems.append(f"{name}:{ln}: duplicate scan id {sid!r}")
            continue
        seen.add(sid)
        scans.append({"id": sid, "ws": w, "ts": ts})
    if problems:
        raise core.SourceInvalid(problems)
    return scans


def validate_step_times(obj, order):
    """Validate a step-times document against an order.

    Returns the list of effective times indexed by step seq.  Every step
    must be covered exactly once and times must be non-decreasing by seq."""
    if not isinstance(obj, dict) or obj.get("format") != TIMES_FORMAT:
        raise core.SourceInvalid([f"not a {TIMES_FORMAT} document"])
    if obj.get("order_hash") != order["order_hash"]:
        raise LedgerReject("foreign_order",
                           "step-times document names a different work order")
    n = len(order["steps"])
    raw = obj.get("times")
    if not isinstance(raw, list):
        raise core.SourceInvalid(["times must be a list"])
    problems = []
    times = [None] * n
    for k, e in enumerate(raw):
        if not isinstance(e, dict) or set(e) != {"seq", "ts"}:
            problems.append(f"times[{k}]: must be an object with exactly seq/ts")
            continue
        q, ts = e["seq"], e["ts"]
        if not isinstance(q, int) or isinstance(q, bool) or not 0 <= q < n:
            problems.append(f"times[{k}]: seq must be an integer in [0, {n})")
            continue
        if not _is_ts(ts):
            problems.append(f"times[{k}]: ts must be an integer Unix second "
                            f"in [0, {MAX_TS}]")
            continue
        if times[q] is not None:
            problems.append(f"times[{k}]: duplicate seq {q}")
            continue
        times[q] = ts
    missing = [q for q, t in enumerate(times) if t is None]
    if missing:
        problems.append(f"missing effective times for steps {missing}")
    if problems:
        raise core.SourceInvalid(problems)
    _check_non_decreasing(times)
    return times


def _check_non_decreasing(times):
    for q in range(1, len(times)):
        if times[q] < times[q - 1]:
            raise core.SourceInvalid(
                [f"effective times must be non-decreasing by step: "
                 f"step {q - 1} at {times[q - 1]} > step {q} at {times[q]}"])


def validate_correction(obj):
    """Structural validation of a time-correction document.

    Returns a normalized document (changes sorted by seq) so that the
    correction hash is independent of file key/element order."""
    if not isinstance(obj, dict) or obj.get("format") != CORRECTION_FORMAT:
        raise core.SourceInvalid([f"not a {CORRECTION_FORMAT} document"])
    problems = []
    if not isinstance(obj.get("order_hash"), str) or not obj["order_hash"]:
        problems.append("order_hash must be a non-empty string")
    changes = obj.get("changes")
    if not isinstance(changes, list) or not changes:
        problems.append("changes must be a non-empty list")
        changes = []
    out = []
    seen = set()
    for k, e in enumerate(changes):
        if not isinstance(e, dict) or set(e) != {"seq", "ts"}:
            problems.append(f"changes[{k}]: must be an object with exactly seq/ts")
            continue
        q, ts = e["seq"], e["ts"]
        if not isinstance(q, int) or isinstance(q, bool) or q < 0:
            problems.append(f"changes[{k}]: seq must be a non-negative integer")
            continue
        if not _is_ts(ts):
            problems.append(f"changes[{k}]: ts must be an integer Unix second "
                            f"in [0, {MAX_TS}]")
            continue
        if q in seen:
            problems.append(f"changes[{k}]: duplicate seq {q}")
            continue
        seen.add(q)
        out.append({"seq": q, "ts": ts})
    if problems:
        raise core.SourceInvalid(problems)
    return {"format": CORRECTION_FORMAT,
            "order_hash": obj["order_hash"],
            "changes": sorted(out, key=lambda c: c["seq"])}


def correction_hash(doc):
    """Identity of a normalized correction document (for idempotent resend)."""
    return core.sha(core.canon(doc))


# ------------------------------------------------------------------ timelines

def _step_ws(before, after):
    """Workstations a step touches: where the device leaves and/or arrives."""
    out = []
    if before["state"] == core.ONLINE:
        out.append(before["ws"])
    if after["state"] == core.ONLINE:
        out.append(after["ws"])
    return list(dict.fromkeys(out))


def build_timelines(initial_rows, steps, times):
    """Per-workstation occupation timelines.

    initial_rows: iterable of (id, ws, ch).  steps: order steps;  times:
    effective time per step, non-decreasing by seq.  Returns
    {ws: (initial_occupant, [event_ts, ...], [event_occupant, ...])} with
    strictly increasing event times;  an occupant is (id, ch) or None
    (vacant).  Steps sharing one second collapse to that second's final
    state in original step order, so a scan of that second sees all of
    them applied -- never an intermediate state.
    """
    init = {}
    events = {}
    for rid, w, c in initial_rows:
        init[w] = (rid, c)
    for step, ts in zip(steps, times):
        before, after = step["before"], step["after"]
        for ws in _step_ws(before, after):
            occ = ((step["id"], after["ch"])
                   if ws == after.get("ws") and after["state"] == core.ONLINE
                   else None)
            lst = events.setdefault(ws, [])
            if lst and lst[-1][0] == ts:
                lst[-1] = (ts, occ)   # same second: the later step wins
            else:
                lst.append((ts, occ))
    out = {}
    for ws in set(init) | set(events):
        ev = events.get(ws, [])
        out[ws] = (init.get(ws), [t for t, _ in ev], [o for _, o in ev])
    return out


def attribute(timeline, ts):
    """Occupant of a workstation at time ts: (id, ch) or None (unattributed)."""
    if timeline is None:
        return None
    occ, ets, occs = timeline
    i = bisect.bisect_right(ets, ts)
    if i:
        occ = occs[i - 1]
    return occ


# ------------------------------------------------------------------ the ledger

def _read_receiver_proof(path):
    """Read (read-only) the receiver's order proof: meta, step log, rows."""
    try:
        con = sqlite3.connect(_uri(path, "ro"), uri=True)
    except sqlite3.OperationalError as e:
        raise LedgerReject("receiver_unreadable", str(e)) from e
    try:
        meta = dict(con.execute("SELECT k, v FROM meta"))
        steps = list(con.execute("SELECT seq, hash FROM steps ORDER BY seq"))
        rows = list(con.execute("SELECT id, ws, ch FROM bindings"))
    except sqlite3.Error as e:
        raise LedgerReject("receiver_unreadable", str(e)) from e
    finally:
        con.close()
    return meta, steps, rows


class Ledger:
    """A scan-attribution ledger bound to one completed handover order."""

    def __init__(self, path, write=True):
        self.path = path
        try:
            self.conn = sqlite3.connect(_uri(path, "rw" if write else "ro"),
                                        uri=True)
        except sqlite3.OperationalError as e:
            raise LedgerReject("ledger_missing", str(e)) from e
        self.conn.isolation_level = None  # explicit BEGIN/COMMIT/ROLLBACK
        try:
            meta = dict(self.conn.execute("SELECT k, v FROM meta"))
        except sqlite3.Error as e:
            self.conn.close()
            raise LedgerReject("not_a_ledger", str(e)) from e
        if meta.get("format") != LEDGER_FORMAT:
            self.conn.close()
            raise LedgerReject("not_a_ledger",
                               f"{path} is not a {LEDGER_FORMAT} database")
        self.meta = meta

    def close(self):
        self.conn.close()

    # -------------------------------------------------------------- build

    @classmethod
    def build(cls, db_path, snapshot_rows, order, receiver_path, times, scans):
        """Create a ledger at db_path (which must not exist yet).

        snapshot_rows: {id: (ws, ch)} validated snapshot;  order: a checked
        handover-order/1 document;  times: per-step effective times from
        validate_step_times;  scans: from validate_scans.  Everything is
        validated before the ledger file is written;  the file itself is
        written to a temporary name and moved into place, so a failed or
        interrupted build never leaves a partial ledger.
        """
        if os.path.exists(db_path) or os.path.exists(db_path + ".tmp"):
            raise LedgerReject("ledger_exists",
                               f"{db_path} already exists; a ledger is built once")

        # Prove the receiver completed exactly this order (read-only).
        meta, rsteps, rrows = _read_receiver_proof(receiver_path)
        n = len(order["steps"])
        if meta.get("order_hash") != order["order_hash"]:
            raise LedgerReject("foreign_order",
                               "receiver holds a different work order")
        if (meta.get("initial_hash") != order["initial_hash"]
                or meta.get("final_hash") != order["final_hash"]):
            raise LedgerReject("order_mismatch",
                               "receiver's stored hashes differ from the order")
        if (meta.get("complete") != "1" or meta.get("committed") != str(n)
                or meta.get("total") != str(n)):
            raise LedgerReject(
                "receiver_incomplete",
                "receiver has not committed the whole order; an unsubmitted "
                "order is not history")
        if ([q for q, _ in rsteps] != list(range(n))
                or any(h != core.step_hash(s)
                       for (_, h), s in zip(rsteps, order["steps"]))):
            raise LedgerReject("steps_mismatch",
                               "receiver's persisted step numbers/hashes "
                               "differ from the order")
        if core.table_hash(rrows) != order["final_hash"]:
            raise LedgerReject("final_mismatch",
                               "receiver bindings differ from the order's "
                               "final table")
        snap_hash = core.table_hash(
            [(i, w, c) for i, (w, c) in snapshot_rows.items()])
        if snap_hash != order["initial_hash"]:
            raise LedgerReject("initial_mismatch",
                               "snapshot differs from the order's initial table")

        # Attribute every scan against the per-workstation timelines.
        timelines = build_timelines(
            [(i, w, c) for i, (w, c) in snapshot_rows.items()],
            order["steps"], times)
        attributed = []
        counts = collections.Counter()
        for s in scans:
            occ = attribute(timelines.get(s["ws"]), s["ts"])
            dev, ch = occ if occ else (None, None)
            attributed.append((s["id"], s["ws"], s["ts"], dev, ch))
            counts[(day_of(s["ts"]), ch)] += 1

        tmp = db_path + ".tmp"
        con = sqlite3.connect(tmp)
        try:
            con.isolation_level = None
            con.executescript(SCHEMA)
            con.execute("BEGIN")
            con.executemany(
                "INSERT INTO meta (k, v) VALUES (?, ?)",
                [("format", LEDGER_FORMAT),
                 ("order_hash", order["order_hash"]),
                 ("initial_hash", order["initial_hash"]),
                 ("final_hash", order["final_hash"]),
                 ("step_count", str(n)),
                 ("scan_count", str(len(attributed)))])
            con.executemany(
                "INSERT INTO initial (id, ws, ch) VALUES (?, ?, ?)",
                [(i, w, c) for i, (w, c) in sorted(snapshot_rows.items())])
            con.executemany(
                "INSERT INTO steps (seq, id, op, before, after, hash, ts) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                [(q, s["id"], s["op"], core.canon(s["before"]),
                  core.canon(s["after"]), core.step_hash(s), times[q])
                 for q, s in enumerate(order["steps"])])
            con.executemany(
                "INSERT INTO scans (scan_id, ws, ts, device, ch) "
                "VALUES (?, ?, ?, ?, ?)", attributed)
            con.executemany(
                "INSERT INTO counts (day, ch, n) VALUES (?, ?, ?)",
                [(d, c, k) for (d, c), k in sorted(
                    counts.items(), key=lambda kv: (kv[0][0], kv[0][1] or ""))])
            con.execute("COMMIT")
            con.close()
            os.replace(tmp, db_path)
        except BaseException:
            con.close()
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
        return {"order_hash": order["order_hash"], "steps": n,
                "scans": len(attributed), "count_groups": len(counts)}

    # --------------------------------------------------------------- reads

    def times(self):
        return [{"seq": q, "id": i, "op": o, "ts": t} for q, i, o, t in
                self.conn.execute(
                    "SELECT seq, id, op, ts FROM steps ORDER BY seq")]

    def scan(self, scan_id):
        row = self.conn.execute(
            "SELECT scan_id, ws, ts, device, ch FROM scans WHERE scan_id = ?",
            (scan_id,)).fetchone()
        if row is None:
            return None
        return {"id": row[0], "ws": row[1], "ts": row[2],
                "device": row[3], "ch": row[4]}

    def counts(self):
        return [{"day": d, "ch": c, "n": k} for d, c, k in self.conn.execute(
            "SELECT day, ch, n FROM counts ORDER BY day, ch")]

    def corrections(self):
        return [{"hash": h, "summary": json.loads(s)} for h, s in
                self.conn.execute("SELECT hash, summary FROM corrections "
                                  "ORDER BY rowid")]

    def status(self, with_scans=False):
        st = {
            "format": self.meta["format"],
            "order_hash": self.meta["order_hash"],
            "steps": self.times(),
            "scans": int(self.meta["scan_count"]),
            "counts": self.counts(),
            "corrections": self.corrections(),
        }
        if with_scans:
            st["scan_details"] = [
                {"id": i, "ws": w, "ts": t, "device": d, "ch": c}
                for i, w, t, d, c in self.conn.execute(
                    "SELECT scan_id, ws, ts, device, ch FROM scans "
                    "ORDER BY ts, scan_id")]
        return st

    # ------------------------------------------------------------- correct

    def correct(self, doc, crash_before_commit=False):
        """Apply a normalized time-correction document in one transaction.

        Returns a result dict (result='applied' or 'already_applied').
        Raises LedgerReject leaving the ledger unchanged for foreign
        orders, unknown step numbers and order violations."""
        ch_hash = correction_hash(doc)
        if doc["order_hash"] != self.meta["order_hash"]:
            raise LedgerReject("foreign_order",
                               "correction names a different work order")
        row = self.conn.execute(
            "SELECT summary FROM corrections WHERE hash = ?",
            (ch_hash,)).fetchone()
        if row is not None:
            return {"result": "already_applied", "correction": ch_hash,
                    "summary": json.loads(row[0])}

        con = self.conn
        con.execute("BEGIN IMMEDIATE")
        try:
            steps = [{"seq": q, "id": i, "op": o,
                      "before": json.loads(b), "after": json.loads(a),
                      "hash": h, "ts": t}
                     for q, i, o, b, a, h, t in con.execute(
                         "SELECT seq, id, op, before, after, hash, ts "
                         "FROM steps ORDER BY seq")]
            by_seq = {s["seq"]: s for s in steps}
            for c in doc["changes"]:
                if c["seq"] not in by_seq:
                    raise LedgerReject(
                        "bad_correction",
                        f"no step {c['seq']} in this order; a correction "
                        "cannot add, remove or alter steps")
            times = [s["ts"] for s in steps]
            for c in doc["changes"]:
                times[c["seq"]] = c["ts"]
            for q in range(1, len(times)):
                if times[q] < times[q - 1]:
                    raise LedgerReject(
                        "order_violation",
                        f"corrected times would be out of order: step {q - 1} "
                        f"at {times[q - 1]} > step {q} at {times[q]}")

            # Per-workstation windows [min(old,new), max(old,new)) of the
            # changed steps; intersecting windows on one workstation merge.
            intervals = {}
            for c in doc["changes"]:
                s = by_seq[c["seq"]]
                old, new = s["ts"], c["ts"]
                if old == new:
                    continue
                lo, hi = (old, new) if old < new else (new, old)
                for ws in _step_ws(s["before"], s["after"]):
                    intervals.setdefault(ws, []).append((lo, hi))
            merged = {ws: _merge(iv) for ws, iv in intervals.items()}

            for c in doc["changes"]:
                con.execute("UPDATE steps SET ts = ? WHERE seq = ?",
                            (c["ts"], c["seq"]))

            initial_rows = list(con.execute("SELECT id, ws, ch FROM initial"))
            timelines = build_timelines(initial_rows, steps, times)

            moved = []
            net = collections.Counter()
            for ws in sorted(merged):
                tl = timelines.get(ws)
                for lo, hi in merged[ws]:
                    for sid, ts, odev, och in con.execute(
                            CANDIDATE_SQL, (ws, lo, hi)):
                        occ = attribute(tl, ts)
                        ndev, nch = occ if occ else (None, None)
                        if (ndev, nch) == (odev, och):
                            continue  # attribution unchanged: leave the row
                        con.execute(
                            "UPDATE scans SET device = ?, ch = ? "
                            "WHERE scan_id = ?", (ndev, nch, sid))
                        day = day_of(ts)
                        _bump(con, day, och, -1)
                        _bump(con, day, nch, +1)
                        net[(day, och)] -= 1
                        net[(day, nch)] += 1
                        moved.append({"id": sid, "ws": ws, "ts": ts,
                                      "old": {"device": odev, "ch": och},
                                      "new": {"device": ndev, "ch": nch}})
            summary = {
                "changes": doc["changes"],
                "times": [{"seq": c["seq"], "ts": times[c["seq"]]}
                          for c in doc["changes"]],
                "windows": [{"ws": ws, "lo": lo, "hi": hi}
                            for ws in sorted(merged)
                            for lo, hi in merged[ws]],
                "moved": moved,
                "net": [{"day": d, "ch": c, "delta": k}
                        for (d, c), k in sorted(
                            net.items(), key=lambda kv: (kv[0][0],
                                                         kv[0][1] or ""))
                        if k],
            }
            con.execute("INSERT INTO corrections (hash, summary) "
                        "VALUES (?, ?)",
                        (ch_hash, json.dumps(summary, ensure_ascii=False,
                                             sort_keys=True)))
        except BaseException:
            try:
                con.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        if crash_before_commit:
            os._exit(12)  # test hook: writes done, COMMIT never issued
        con.execute("COMMIT")
        return {"result": "applied", "correction": ch_hash, "summary": summary}


def _merge(iv):
    """Merge intersecting or touching [lo, hi) windows."""
    out = []
    for lo, hi in sorted(iv):
        if out and lo <= out[-1][1]:
            out[-1][1] = max(out[-1][1], hi)
        else:
            out.append([lo, hi])
    return [tuple(w) for w in out]


def _bump(con, day, ch, delta):
    """Add delta to one (day, channel) count; zero-count groups vanish."""
    row = con.execute(
        "SELECT rowid, n FROM counts WHERE day = ? AND IFNULL(ch, '') = "
        "IFNULL(?, '')", (day, ch)).fetchone()
    if row is None:
        if delta <= 0:
            raise AssertionError("count would go negative")
        con.execute("INSERT INTO counts (day, ch, n) VALUES (?, ?, ?)",
                    (day, ch, delta))
    else:
        n = row[1] + delta
        if n < 0:
            raise AssertionError("count would go negative")
        if n == 0:
            con.execute("DELETE FROM counts WHERE rowid = ?", (row[0],))
        else:
            con.execute("UPDATE counts SET n = ? WHERE rowid = ?", (n, row[0]))
