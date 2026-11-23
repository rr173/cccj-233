"""Offline shipment manifest rebuilt from the scan ledger and a packing log.

The carrier scans only the outermost roll cage at the gate, yet the
shipping list has to name every package inside it -- including boxes that
were shifted into another cage before the gate.  This module derives that
list offline from two bodies of evidence:

  * the scan-attribution ledger (handover-scan-ledger/1, opened read-only):
    it supplies each scan's occurrence second, the channel the scan really
    counted towards and whether it is attributed at all;
  * a packing log (JSON Lines, streamed -- the whole log is never held in
    Python memory, only in a disk staging database): packing lines say
    which package or container moved from which direct container to which
    direct container (either parent may be null, "no parent"); gate lines
    name only the container that was scanned.  Every line carries the scan
    id under which the operator registered it:

        {"scan": "P07", "pack": {"items": [
            {"kind": "pkg", "id": "PKG-1", "from": null, "to": "CAGE-9"}]}}
        {"scan": "G02", "gate": "CAGE-9"}

    A package and a container may carry the same number; they are still
    different objects.  Identifiers and channel numbers keep their case.

A batch names two different channel numbers, one for packing and one for
gating.  Log order is never trusted: scanner groups are replayed in the
ledger's occurrence-second order, ties broken by scan id in UTF-8 byte
order.  A group whose scan is unattributed, or whose ledger channel is not
the registered channel for its purpose, is skipped -- only that group.  A
batch starts with no ownership and no object that has already left the
yard; an object that has left in this batch may not be reused.

Packing semantics
  Every source of one packing operation is compared against the state
  before the group, and all transfers of a group take effect together: a
  single mismatch, a move of an object that has left the yard, a move into
  a container that has left, or a cycle in the resulting ownership rejects
  the whole group, so none of its items lands.  A group registers each
  object at most once; a package id and a container id are different
  objects even when the numbers are equal.  A move whose parent would not
  change is not a transfer and keeps the scan id that made the old
  relation effective.  Judging a group reads only its own objects and the
  parent chain they need -- never the rest of the ownership table.  Cycle
  detection walks from each moved object up its (candidate) parent chain,
  so the row order inside one group can never change the verdict.

Gating semantics
  Only a container that has not left the yard and has no parent may gate.
  A successful gate takes that container's entire present subtree of
  containers and packages with it; afterwards none of them can be packed
  or gated again.  An empty container still yields a manifest.  A failed
  gate changes no packing state.  Only the subtree of the container being
  gated is expanded -- never the whole ownership table or the whole
  pending set of gates.

The result database (handover-manifest/1) is self-contained: the carrier
looks up an outbound scan id and gets its second, the outermost container,
every package and each package/container's direct parent together with the
packing scan id that made that relation effective.  Whole paths are not
repeated per package, and a later ledger correction does not rewrite a
manifest that was already delivered.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile

from . import core
from .ledger import LEDGER_FORMAT, _uri

MANIFEST_FORMAT = "handover-manifest/1"

KIND_PKG = "pkg"
KIND_CTR = "ctr"
USE_PACK = "pack"
USE_GATE = "gate"

STAGING_SCHEMA = """
CREATE TABLE log_scan(              -- one row per registered scanner group
  scan_id TEXT PRIMARY KEY,
  use TEXT NOT NULL
);
CREATE TABLE log_items(             -- registration order kept for determinism
  scan_id TEXT NOT NULL,
  ln INTEGER NOT NULL,
  idx INTEGER NOT NULL,
  kind TEXT NOT NULL,
  id TEXT NOT NULL,
  from_id TEXT,                     -- NULL = no parent
  to_id TEXT,                       -- NULL = no parent
  PRIMARY KEY (scan_id, ln, idx)
);
-- the group rule: one object per packing group, whatever its line
CREATE UNIQUE INDEX log_items_obj ON log_items(scan_id, kind, id);
CREATE TABLE own(                   -- current direct ownership, on disk
  kind TEXT NOT NULL,
  id TEXT NOT NULL,
  parent TEXT,                      -- NULL = no parent
  via TEXT,                         -- packing scan that made it effective
  PRIMARY KEY (kind, id)
) WITHOUT ROWID;
CREATE INDEX own_parent ON own(parent, kind, id);
CREATE TABLE owned_list(            -- one gate's subtree;  persists after the gate
  gate_id INTEGER NOT NULL,
  kind TEXT NOT NULL,
  id TEXT NOT NULL,
  parent TEXT,
  via TEXT,
  PRIMARY KEY (gate_id, kind, id)
);
CREATE TABLE gone(                  -- every object that has left the yard
  kind TEXT NOT NULL,
  id TEXT NOT NULL,
  gate_id INTEGER NOT NULL,
  PRIMARY KEY (kind, id)
) WITHOUT ROWID;
"""

RESULT_SCHEMA = """
CREATE TABLE meta(
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);
CREATE TABLE manifests(             -- one row per effective outbound scan
  gate_id INTEGER PRIMARY KEY,
  scan_id TEXT NOT NULL UNIQUE,     -- the outbound (gate) scan
  ts INTEGER NOT NULL,              -- its occurrence second
  root_id TEXT NOT NULL             -- the outermost container scanned
);
CREATE TABLE manifest(              -- direct relations as they stood at the gate
  gate_id INTEGER NOT NULL,
  kind TEXT NOT NULL,               -- 'ctr' or 'pkg'
  id TEXT NOT NULL,
  parent TEXT,                      -- direct parent; NULL for the root
  via TEXT,                         -- packing scan id that made it effective
  PRIMARY KEY (gate_id, kind, id)
);
CREATE INDEX manifest_parent ON manifest(gate_id, parent, kind, id);
CREATE TABLE packages(              -- every package the gate carried away
  gate_id INTEGER NOT NULL,
  pkg TEXT NOT NULL,
  PRIMARY KEY (gate_id, pkg)
);
CREATE TABLE groups(                -- every registered group: adopted or not
  ord INTEGER PRIMARY KEY,          -- replay position (second, scan bytes)
  scan_id TEXT NOT NULL UNIQUE,
  ts INTEGER NOT NULL,
  use TEXT NOT NULL,                -- 'pack' or 'gate'
  adopted INTEGER NOT NULL,
  reason TEXT                       -- JSON list of business reasons; NULL if adopted
);
CREATE TABLE scans(                 -- the ledger rows this batch used, frozen
  scan_id TEXT PRIMARY KEY,
  ws TEXT NOT NULL,
  ts INTEGER NOT NULL,
  device TEXT,
  ch TEXT
);
"""

RESULT_VIEW = """
CREATE VIEW shipment_packages AS
  SELECT m.gate_id, m.scan_id, m.ts, m.root_id, p.pkg
  FROM manifests m JOIN packages p ON p.gate_id = m.gate_id;
"""

# Per-group access statements.  Each one is a primary-key or index search;
# tests assert that none of them ever scans the ownership table.
SQL_LOOKUP = "SELECT parent, via FROM own WHERE kind = ? AND id = ?"
SQL_CAND_PARENT = ("SELECT parent FROM owned_list "
                   "WHERE gate_id = ? AND kind = ? AND id = ?")
SQL_APPLY = ("INSERT OR REPLACE INTO own (kind, id, parent, via) "
             "SELECT kind, id, parent, via FROM owned_list "
             "WHERE gate_id = ?")
SQL_GATE_SUBTREE = """
INSERT INTO owned_list (gate_id, kind, id, parent, via)
WITH RECURSIVE sub(kind, id, parent, via) AS (
    SELECT 'ctr', ?1, NULL, (SELECT via FROM own WHERE kind = 'ctr' AND id = ?1)
    UNION
    SELECT o.kind, o.id, o.parent, o.via
    FROM own o INDEXED BY own_parent
    JOIN sub s ON s.kind = 'ctr' AND o.parent = s.id
)
SELECT ?2, kind, id, parent, via FROM sub
"""
SQL_MARK_GONE = ("INSERT OR REPLACE INTO gone (kind, id, gate_id) "
                 "SELECT kind, id, ? FROM owned_list WHERE gate_id = ?")
SQL_IS_GONE = "SELECT 1 FROM gone WHERE kind = ? AND id = ?"
SQL_REPLAY = ("SELECT l.scan_id, s.ts, s.device, s.ch, l.use "
              "FROM log_scan l JOIN lg.scans s ON s.scan_id = l.scan_id "
              "ORDER BY s.ts, CAST(l.scan_id AS BLOB), l.scan_id")
# The scans the third-party navigator may read: occurrences and attribution
# are pinned at build time, so a later ledger correction cannot rewrite a
# delivered manifest.
SQL_SNAPSHOT_SCANS = ("INSERT INTO out.scans (scan_id, ws, ts, device, ch) "
                      "SELECT s.scan_id, s.ws, s.ts, s.device, s.ch "
                      "FROM lg.scans s JOIN log_scan l "
                      "ON l.scan_id = s.scan_id")


class ManifestReject(Exception):
    """A manifest request was rejected; no manifest was left behind."""

    def __init__(self, code, detail):
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}")


# ------------------------------------------------------------------ helpers

def _nonempty(v):
    return isinstance(v, str) and v != ""


def _qualified(schema, alias):
    return (schema.replace("CREATE TABLE ", f"CREATE TABLE {alias}.")
            .replace("CREATE INDEX ", f"CREATE INDEX {alias}."))


def _sha_rows(cur):
    h = hashlib.sha256()
    for row in cur:
        h.update(json.dumps(row, ensure_ascii=False,
                            separators=(",", ":")).encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def _validate_channels(pack_ch, gate_ch):
    problems = []
    if not _nonempty(pack_ch):
        problems.append("packing channel must be a non-empty string")
    if not _nonempty(gate_ch):
        problems.append("gating channel must be a non-empty string")
    if _nonempty(pack_ch) and pack_ch == gate_ch:
        problems.append("packing and gating channels must differ")
    if problems:
        raise core.SourceInvalid(problems)


# -------------------------------------------------------------- log parsing

_PACK_KEYS = {"scan", "pack"}
_GATE_KEYS = {"scan", "gate"}
_ITEM_KEYS = {"kind", "id", "from", "to"}


def _check_item(obj, ln, k, problems):
    """Validate one packing item;  returns (kind, id, from, to) or None."""
    if not isinstance(obj, dict) or set(obj) != _ITEM_KEYS:
        problems.append(f"log:{ln}: pack[{k}] must be an object with exactly "
                        "the keys kind/id/from/to")
        return None
    kind, oid = obj["kind"], obj["id"]
    src, dst = obj["from"], obj["to"]
    ok = True
    if kind not in (KIND_PKG, KIND_CTR):
        problems.append(f"log:{ln}: pack[{k}]: kind must be 'pkg' or 'ctr'")
        ok = False
    if not _nonempty(oid):
        problems.append(f"log:{ln}: pack[{k}]: id must be a non-empty string")
        ok = False
    for label, v in (("from", src), ("to", dst)):
        if v is not None and not _nonempty(v):
            problems.append(f"log:{ln}: pack[{k}]: {label} must be null or "
                            "a non-empty string")
            ok = False
    return (kind, oid, src, dst) if ok else None


def _check_gate_obj(obj, ln, problems):
    """A gate line records only the container that was scanned."""
    if not _nonempty(obj):
        problems.append(f"log:{ln}: gate must be a non-empty container id")
        return None
    return obj


def parse_log(con, lines):
    """Stream the packing log into the staging tables.

    Collects every format and grouping violation;  raises SourceInvalid
    listing all of them (the request is then rejected as a whole).  Never
    reads the ledger and never keeps the file in Python memory."""
    problems = []
    for ln, raw in enumerate(lines, 1):
        if not raw.strip():
            continue
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError as e:
            problems.append(f"log:{ln}: invalid JSON: {e}")
            continue
        if not isinstance(obj, dict):
            problems.append(f"log:{ln}: row must be a JSON object")
            continue
        keys = set(obj)
        if keys == _PACK_KEYS:
            use, payload = USE_PACK, obj["pack"]
        elif keys == _GATE_KEYS:
            use, payload = USE_GATE, obj["gate"]
        else:
            problems.append(f"log:{ln}: row must have exactly the keys "
                            "scan/pack (a packing line) or scan/gate "
                            "(a gate line)")
            continue
        scan = obj["scan"]
        if not _nonempty(scan):
            problems.append(f"log:{ln}: scan must be a non-empty string")
            continue
        if use == USE_PACK:
            if (not isinstance(payload, dict) or set(payload) != {"items"}
                    or not isinstance(payload["items"], list)
                    or not payload["items"]):
                problems.append(
                    f"log:{ln}: pack must be an object with exactly the key "
                    "items, holding a non-empty list of moves")
                continue
            items = []
            for k, obj_item in enumerate(payload["items"]):
                item = _check_item(obj_item, ln, k, problems)
                if item is not None:
                    items.append(item)
            if len(items) != len(payload["items"]):
                continue
        else:
            root = _check_gate_obj(payload, ln, problems)
            if root is None:
                continue
            items = [(KIND_CTR, root, None, None)]
        # Group the lines of one scan id; a gate is exactly one group of
        # one line and never mixes with packing lines.
        cur = con.execute("INSERT OR IGNORE INTO log_scan (scan_id, use) "
                          "VALUES (?, ?)", (scan, use))
        if cur.rowcount == 0:
            (old_use,) = con.execute(
                "SELECT use FROM log_scan WHERE scan_id = ?",
                (scan,)).fetchone()
            if old_use != use or use == USE_GATE:
                problems.append(
                    f"log:{ln}: scan {scan!r} already registered as a "
                    f"{old_use} group; a gate is one line and one group and "
                    "never mixes with packing lines")
                continue
        for k, (kind, oid, src, dst) in enumerate(items):
            try:
                con.execute(
                    "INSERT INTO log_items (scan_id, ln, idx, kind, id, "
                    "from_id, to_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (scan, ln, k, kind, oid, src, dst))
            except sqlite3.IntegrityError:
                problems.append(
                    f"log:{ln}: scan {scan!r} registers {kind} {oid!r} twice; "
                    "each object appears at most once per packing operation")
    if problems:
        raise core.SourceInvalid(problems)
    return con.execute("SELECT COUNT(*) FROM log_scan").fetchone()[0]


def _check_references(con):
    problems = []
    for (s,) in con.execute(
            "SELECT scan_id FROM log_scan l WHERE NOT EXISTS ("
            "SELECT 1 FROM lg.scans s WHERE s.scan_id = l.scan_id) "
            "ORDER BY scan_id"):
        problems.append(f"log: scan {s!r} is not in the ledger "
                        "(dangling reference)")
    if problems:
        raise core.SourceInvalid(problems)


# ---------------------------------------------------------------- the engine

def _lookup(con, kind, oid):
    return con.execute(SQL_LOOKUP, (kind, oid)).fetchone()


def _candidate_parent(con, gate_id, kind, oid):
    """Parent under the group's candidate transfers, falling back to the
    committed ownership row.  One primary-key probe per level."""
    row = con.execute(SQL_CAND_PARENT, (gate_id, kind, oid)).fetchone()
    if row is not None:
        return row[0]
    row = _lookup(con, kind, oid)
    return row[0] if row else None


def _cycle_start(con, gate_id, kind, oid):
    """True when ownership starting at (kind, oid) closes a cycle."""
    seen = set()
    node = (kind, oid)
    while True:
        if node in seen:
            return True
        seen.add(node)
        parent = _candidate_parent(con, gate_id, node[0], node[1])
        if parent is None:
            return False
        node = (KIND_CTR, parent)


def apply_pack(con, scan, items, gate_id):
    """Judge and, only if wholly legal, apply one packing operation.

    items: iterable of (kind, id, from, to) in registration order.  Every
    source is compared against the state before the group, so the order of
    lines is irrelevant.  Returns (adopted, reasons); on rejection nothing
    is changed.  Only the group's objects and the parent chain they need
    are read.  A package and a container may carry the same number;  the
    package's own parent chain stops at its container parent, so object
    identity -- never the bare number -- decides a cycle."""
    reasons = []
    pending = []
    for kind, oid, src, dst in items:
        row = _lookup(con, kind, oid)
        parent = row[0] if row else None
        if gate_is_gone(con, kind, oid):
            reasons.append(f"{kind} {oid!r} has already left the yard")
            continue
        if parent != src:
            reasons.append(f"{kind} {oid!r} is in {parent!r}, not in {src!r}")
            continue
        if dst == src:
            continue                 # not a transfer: keep the old basis
        if gate_is_gone(con, KIND_CTR, dst):
            reasons.append(f"container {dst!r} has already left the yard")
            continue
        pending.append((kind, oid, dst))
    if reasons:
        return False, sorted(set(reasons))

    con.executemany(
        "INSERT INTO owned_list (gate_id, kind, id, parent, via) "
        "VALUES (?, ?, ?, ?, ?)",
        [(gate_id, kind, oid, dst, scan) for kind, oid, dst in pending])
    for kind, oid, _ in pending:
        if _cycle_start(con, gate_id, kind, oid):
            con.execute("DELETE FROM owned_list WHERE gate_id = ?",
                        (gate_id,))
            return False, [f"{kind} {oid!r}: the operation would close an "
                           "ownership cycle"]
    con.execute(SQL_APPLY, (gate_id,))
    con.execute("DELETE FROM owned_list WHERE gate_id = ?", (gate_id,))
    return True, []


def apply_gate(con, root, gate_id):
    """Gate one container: check it is not gone and is top-level, then
    expand (and mark) only its own present subtree."""
    if gate_is_gone(con, KIND_CTR, root):
        return False, [f"container {root!r} has already left the yard"]
    row = _lookup(con, KIND_CTR, root)
    if row is not None and row[0] is not None:
        return False, [f"container {root!r} is inside {row[0]!r}, not at the "
                       "top level"]
    con.execute(SQL_GATE_SUBTREE, (root, gate_id))
    try:
        con.execute(SQL_MARK_GONE, (gate_id, gate_id))
    except BaseException:
        con.execute("DELETE FROM owned_list WHERE gate_id = ?", (gate_id,))
        raise
    return True, []


def gate_is_gone(con, kind, oid):
    """True when this object has left the yard.

    A gate carries the whole subtree away and records every object it took
    in `gone`, so the check is one primary-key probe -- it never walks
    through unrelated containers."""
    return con.execute(SQL_IS_GONE, (kind, oid)).fetchone() is not None


# ------------------------------------------------------------------ delivery

def _copy_subtree(con, run_ord, gate_ord, scan_id, ts, root):
    """Copy one gate's present subtree into the result tables, then clear
    it from the staging table."""
    for kind, oid, parent, via in con.execute(
            "SELECT kind, id, parent, via FROM owned_list "
            "WHERE gate_id = ? ORDER BY kind, id", (run_ord,)):
        con.execute(
            "INSERT INTO out.manifest (gate_id, kind, id, parent, via) "
            "VALUES (?, ?, ?, ?, ?)", (gate_ord, kind, oid, parent, via))
        if kind == KIND_PKG:
            con.execute(
                "INSERT INTO out.packages (gate_id, pkg) VALUES (?, ?)",
                (gate_ord, oid))
    con.execute(
        "INSERT INTO out.manifests (gate_id, scan_id, ts, root_id) "
        "VALUES (?, ?, ?, ?)", (gate_ord, scan_id, ts, root))
    con.execute("DELETE FROM owned_list WHERE gate_id = ?", (run_ord,))


def build_manifest(ledger_path, pack_ch, gate_ch, log_lines, out_path,
                   snapshot_hook=None, progress_every=None):
    """Rebuild and write one shipment manifest database.

    ledger_path: a handover-scan-ledger/1 db (opened read-only;  the whole
    run reads one committed state).  pack_ch / gate_ch: the two distinct
    channel numbers the batch registered.  log_lines: iterable of JSONL
    lines (streamed).  out_path: result path, which must not exist yet.

    Raises SourceInvalid (malformed log, illegal group, dangling scan --
    nothing is left behind) and ManifestReject (output exists / not a
    ledger).  Returns a summary dict.
    """
    _validate_channels(pack_ch, gate_ch)
    tmp_out = out_path + ".tmp"
    if os.path.exists(out_path) or os.path.exists(tmp_out):
        raise ManifestReject(
            "manifest_exists",
            f"{out_path} already exists; a manifest is written once -- "
            "remove it (and any leftover .tmp) to rebuild")
    out_dir = os.path.dirname(os.path.abspath(out_path))
    fd, staging_path = tempfile.mkstemp(prefix=".manifest-staging-",
                                        dir=out_dir)
    os.close(fd)
    con = sqlite3.connect(staging_path)
    con.isolation_level = None
    try:
        con.execute("PRAGMA journal_mode=MEMORY")
        con.execute("PRAGMA synchronous=OFF")
        con.executescript(STAGING_SCHEMA)
        try:
            con.execute("ATTACH DATABASE ? AS lg", (_uri(ledger_path, "ro"),))
        except sqlite3.Error as e:
            raise ManifestReject("ledger_missing", str(e)) from e
        con.execute("ATTACH DATABASE ? AS out", (tmp_out,))
        con.executescript(_qualified(RESULT_SCHEMA, "out"))
        con.execute("PRAGMA out.journal_mode=MEMORY")
        con.execute("PRAGMA out.synchronous=OFF")
        ops = [0]
        if progress_every:
            def _progress():
                ops[0] += 1
                return 0
            con.set_progress_handler(_progress, progress_every)
        con.execute("BEGIN")

        try:
            lmeta = dict(con.execute("SELECT k, v FROM lg.meta"))
        except sqlite3.Error as e:
            raise ManifestReject(
                "not_a_ledger",
                f"{ledger_path} is not a {LEDGER_FORMAT} database: {e}") from e
        if lmeta.get("format") != LEDGER_FORMAT:
            raise ManifestReject(
                "not_a_ledger",
                f"{ledger_path} is not a {LEDGER_FORMAT} database")
        scans_sha = _sha_rows(con.execute(
            "SELECT scan_id, ws, ts, device, ch FROM lg.scans "
            "ORDER BY scan_id"))
        if snapshot_hook is not None:
            snapshot_hook()          # test hook: inside the snapshot

        # ---- stream and validate the log (all or nothing) ----
        n_groups = parse_log(con, log_lines)
        _check_references(con)
        # ---- content digest of the validated log (order independent) ----
        # Each object appears at most once per packing group, so sorting by
        # (scan, kind, id) is a canonical form:  shuffled lines and rows
        # produce the same digest.
        log_sha = _sha_rows(con.execute(
            "SELECT scan_id, kind, id, from_id, to_id FROM log_items "
            "ORDER BY scan_id, kind, id"))

        # ---- replay in ledger second order, ties by scan id bytes ----
        n_adopted = n_skipped = 0
        # Every group gets its ledger replay position;  a skipped group
        # keeps that position too, so adoption is always legible per scan.
        run_ord = 0
        gate_ord = 0
        for scan_id, ts, dev, ch, use in con.execute(SQL_REPLAY):
            run_ord += 1
            want = pack_ch if use == USE_PACK else gate_ch
            if dev is None:
                adopted, reasons = False, ["scan is unattributed in the ledger"]
            elif ch != want:
                adopted, reasons = False, [
                    f"ledger channel {ch!r} != registered {use} channel "
                    f"{want!r}"]
            elif use == USE_PACK:
                items = list(con.execute(
                    "SELECT kind, id, from_id, to_id FROM log_items "
                    "WHERE scan_id = ? ORDER BY ln, idx", (scan_id,)))
                adopted, reasons = apply_pack(con, scan_id, items, run_ord)
            else:
                (root,) = con.execute(
                    "SELECT id FROM log_items WHERE scan_id = ?",
                    (scan_id,)).fetchone()
                adopted, reasons = apply_gate(con, root, run_ord)
            con.execute(
                "INSERT INTO out.groups (ord, scan_id, ts, use, adopted, "
                "reason) VALUES (?, ?, ?, ?, ?, ?)",
                (run_ord, scan_id, ts, use, 1 if adopted else 0,
                 None if adopted else json.dumps(reasons,
                                                 ensure_ascii=False)))
            if not adopted:
                n_skipped += 1
            elif use == USE_PACK:
                n_adopted += 1
            else:
                n_adopted += 1
                gate_ord += 1
                _copy_subtree(con, run_ord, gate_ord, scan_id, ts, root)
        con.execute("DELETE FROM owned_list")
        con.execute(SQL_SNAPSHOT_SCANS)

        n_corr = con.execute(
            "SELECT COUNT(*) FROM lg.corrections").fetchone()[0]
        con.executemany(
            "INSERT INTO out.meta (k, v) VALUES (?, ?)",
            [("format", MANIFEST_FORMAT),
             ("ledger_format", LEDGER_FORMAT),
             ("ledger_order_hash", lmeta.get("order_hash", "")),
             ("ledger_scan_count", lmeta.get("scan_count", "")),
             ("ledger_correction_count", str(n_corr)),
             ("ledger_scans_sha256", scans_sha),
             ("log_sha256", log_sha),
             ("packing_channel", pack_ch),
             ("gating_channel", gate_ch),
             ("log_groups", str(n_groups)),
             ("adopted_groups", str(n_adopted)),
             ("skipped_groups", str(n_skipped)),
             ("manifests", str(gate_ord)),
             ("packages", str(con.execute(
                 "SELECT COUNT(*) FROM out.packages").fetchone()[0]))])
        con.execute("COMMIT")
        if progress_every:
            con.set_progress_handler(None, 0)
        n_packages = con.execute(
            "SELECT COUNT(*) FROM out.packages").fetchone()[0]
        con.execute("DETACH DATABASE out")
        vcon = sqlite3.connect(tmp_out)
        try:
            vcon.executescript(RESULT_VIEW)
            vcon.close()
        except BaseException:
            vcon.close()
            raise
        con.close()
        os.replace(tmp_out, out_path)
    except BaseException:
        try:
            con.close()
        except Exception:
            pass
        for p in (staging_path, tmp_out):
            try:
                os.unlink(p)
            except OSError:
                pass
        raise
    try:
        os.unlink(staging_path)
    except OSError:
        pass
    summary = {
        "result": "rebuilt",
        "log_groups": n_groups,
        "adopted": n_adopted,
        "skipped": n_skipped,
        "manifests": gate_ord,
        "packages": n_packages,
        "ledger_scans_sha256": scans_sha,
        "log_sha256": log_sha,
        "out": out_path,
    }
    if progress_every:
        summary["vm_ops"] = ops[0] * progress_every
    return summary
