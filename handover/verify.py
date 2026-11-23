"""Offline per-package missing-scan verification against the scan ledger.

The scan-attribution ledger can say which channel a scan counted towards,
but "this box scanned A and B" does not prove it travelled A -> B as
prescribed.  This module is the offline verification entry point:  given
the ledger (opened read-only), a route list and a package/scan association
table (both unordered JSON Lines), it proves for every package the longest
continuous prefix of its route and writes ONE SQLite result database that
locates each package's break point and carries the selected evidence, so
every conclusion can be re-checked against the original scan records.  The
ledger's data and its correction flow are not touched.

Inputs (unordered JSON Lines, streamed -- never held in memory as a whole)
  routes: {"pkg": "P1", "route": [{"ch": "CH-1"},
             {"ch": "CH-2", "min": 5, "max": 60}, ...]}
      1..16 stations;  from the second station on, min/max are the allowed
      seconds from the previous station (positive integers, min <= max).
      Channels may repeat on a route and may never appear in this batch of
      scans.  One route per package.
  assoc:  {"pkg": "P1", "scan": "S07"}
      links a package to a scan id from the ledger;  a scan links to at
      most one package.

Duplicate package routes, duplicate scan associations (even identical
ones), dangling references (association naming a package without a route
or a scan that is not in the ledger) and malformed rows are all rejected,
and no result database is left behind in that case.  The result path must
not exist yet:  a verification result is written once, to a temporary
file that is moved into place only after everything succeeded.

Semantics
  For each package the verifier proves the longest continuous prefix of
  its route, starting at station 1, never skipping a station.  Every
  station of the prefix selects one scan associated with the package whose
  ledger-attributed channel matches the station exactly (associated but
  unattributed scans cannot serve as evidence;  unassociated scans do not
  participate at all).  Adjacent selected scans must differ by a number of
  seconds inside the station's closed [min, max] interval -- since min >=
  1, two scans of the same second can never serve adjacent stations.  A
  package with no associations still appears in the result (empty prefix).
  When several evidence chains prove the same prefix length, the chosen
  chain is the lexicographically smallest by (second, scan id) compared
  station by station from station 1 on.

Snapshot
  All ledger rows used by one verification come from a single committed
  state:  the ledger is attached read-only and read inside one
  transaction, so a correction landing mid-run either is entirely before
  the snapshot or cannot commit until the run ends -- pre- and
  post-correction attributions are never mixed into one evidence chain.
  The result's meta table pins the exact state used
  (ledger_scans_sha256), and handover.verify.verify_result() re-checks a
  result database against the ledger's current scan records.

Result database (handover-verify/1)
  meta(k, v)            format, ledger identity + snapshot hash, input
                        digests, package tallies
  routes(pkg, stations, route)   the validated routes (canonical JSON)
  results(pkg, stations, passed, next_station, next_ch)
                        passed = proven prefix length (0..stations);
                        next_station/next_ch = the 1-based next station to
                        check and its channel, NULL when fully passed --
                        an empty prefix (passed=0, next_station=1) and a
                        full pass (next_station NULL) are told apart
  evidence(pkg, station, scan_id, ts, ch)
                        the selected witness scans in station order
                        (1-based), with the occurrence second and the
                        channel the ledger attributes the scan to

Scaling
  Busy periods put many rescans of one package into the same
  adjacent-station time window.  The verifier never checks or
  materializes window scan pairs:  per station transition, the reachable
  predecessor candidates (kept in a staging SQLite database on disk, not
  in process memory) are covered by a persisted segment tree, so each
  successor candidate costs O(log n) index probes -- two position probes
  plus one node lookup -- and the total work grows with the number of
  candidate scans, not with the number of matching pairs.  The staging
  database is deleted after the run;  only the result database remains.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile

from . import core
from .ledger import LEDGER_FORMAT, _uri

RESULT_FORMAT = "handover-verify/1"
MAX_STATIONS = 16

STAGING_SCHEMA = """
CREATE TABLE routes_in(
  pkg TEXT PRIMARY KEY,
  route TEXT NOT NULL             -- canonical JSON of the validated route
);
CREATE TABLE assoc_in(
  pkg TEXT NOT NULL,
  scan TEXT NOT NULL
);
CREATE UNIQUE INDEX assoc_in_scan ON assoc_in(scan);
CREATE INDEX assoc_in_pkg ON assoc_in(pkg);
CREATE TABLE cand(                -- one package's usable scans (associated + attributed)
  pkg TEXT NOT NULL,
  ch TEXT NOT NULL,
  ts INTEGER NOT NULL,
  scan TEXT NOT NULL
);
CREATE INDEX cand_idx ON cand(pkg, ch, ts, scan);
CREATE TABLE reach(               -- reachable candidates of one station, in ts order
  pkg TEXT NOT NULL,
  st INTEGER NOT NULL,
  pos INTEGER NOT NULL,           -- rank in (ts, scan) order
  ts INTEGER NOT NULL,
  ckey TEXT NOT NULL              -- order-preserving key of the best chain ending here
);
CREATE INDEX reach_probe ON reach(pkg, st, ts, pos);
CREATE TABLE seg(                 -- persisted segment tree over one station's reach rows
  pkg TEXT NOT NULL,
  st INTEGER NOT NULL,
  node INTEGER NOT NULL,
  ckey TEXT NOT NULL,             -- minimum chain key in the node's interval
  PRIMARY KEY (pkg, st, node)
) WITHOUT ROWID;
"""

RESULT_SCHEMA = """
CREATE TABLE meta(
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);
CREATE TABLE routes(
  pkg TEXT PRIMARY KEY,
  stations INTEGER NOT NULL,
  route TEXT NOT NULL             -- canonical JSON
);
CREATE TABLE results(
  pkg TEXT PRIMARY KEY,
  stations INTEGER NOT NULL,
  passed INTEGER NOT NULL,        -- proven prefix length (0..stations)
  next_station INTEGER,           -- 1-based next station to check; NULL when fully passed
  next_ch TEXT                    -- its channel; NULL when fully passed
);
CREATE TABLE evidence(
  pkg TEXT NOT NULL,
  station INTEGER NOT NULL,       -- 1-based
  scan_id TEXT NOT NULL,
  ts INTEGER NOT NULL,
  ch TEXT NOT NULL,               -- the channel the ledger attributes the scan to
  PRIMARY KEY (pkg, station)
);
"""

# The exact probe statements used per successor candidate;  tests measure
# these to show index-bounded (not pair-bounded) access.
SQL_PROBE_LO = ("SELECT pos FROM reach WHERE pkg = ? AND st = ? AND ts >= ? "
                "ORDER BY ts, pos LIMIT 1")
SQL_PROBE_HI = ("SELECT pos FROM reach WHERE pkg = ? AND st = ? AND ts <= ? "
                "ORDER BY ts DESC, pos DESC LIMIT 1")

_SQL_CAND_FILL = ("INSERT INTO cand SELECT ?, s.ch, s.ts, s.scan_id "
                  "FROM assoc_in a JOIN lg.scans s ON s.scan_id = a.scan "
                  "WHERE a.pkg = ? AND s.ch IS NOT NULL")
_SQL_CAND_ANY = "SELECT 1 FROM cand WHERE pkg = ? AND ch = ? LIMIT 1"
_SQL_CAND_STREAM = ("SELECT ts, scan FROM cand WHERE pkg = ? AND ch = ? "
                    "ORDER BY ts, scan")
_SQL_REACH_INS = ("INSERT INTO reach (pkg, st, pos, ts, ckey) "
                  "VALUES (?, ?, ?, ?, ?)")


class VerifyReject(Exception):
    """A verification request was rejected; no result database was left."""

    def __init__(self, code, detail):
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}")


# ------------------------------------------------------------------ chain keys

def _pair_key(ts, sid):
    """Order-preserving encoding of one (second, scan id) chain element.

    The second as zero-padded decimal (ledger timestamps are validated to
    be at most 253402300799, so 12 digits always suffice), then the scan
    id as hex-encoded UTF-8 (byte order = code-point order), terminated
    by '/', which sorts before every hex digit -- so the concatenation of
    pair keys compares exactly like the sequence of (second, scan id)
    pairs, and plain string comparison (Python or SQLite BINARY)
    implements the required lexicographic chain order."""
    return f"{ts:012d}{sid.encode('utf-8').hex()}/"


def _decode_key(ckey):
    return [[int(p[:12]), bytes.fromhex(p[12:]).decode("utf-8")]
            for p in ckey.split("/")[:-1]]


# ------------------------------------------------------------------ validation

def _nonempty_str(v):
    return isinstance(v, str) and v != ""


def _pos_int(v):
    return isinstance(v, int) and not isinstance(v, bool) and v >= 1


def _check_route(obj, ln, name, problems):
    """Validate one route line.  Returns (pkg, canonical route JSON) or None."""
    if not isinstance(obj, dict) or set(obj) != {"pkg", "route"}:
        problems.append(f"{name}:{ln}: row must be an object with exactly "
                        "the keys pkg/route")
        return None
    pkg, route = obj["pkg"], obj["route"]
    ok = True
    if not _nonempty_str(pkg):
        problems.append(f"{name}:{ln}: pkg must be a non-empty string")
        ok = False
    if not isinstance(route, list) or not 1 <= len(route) <= MAX_STATIONS:
        problems.append(f"{name}:{ln}: route must be a list of "
                        f"1..{MAX_STATIONS} stations")
        ok = False
    if not ok:
        return None
    for k, st in enumerate(route):
        want = {"ch"} if k == 0 else {"ch", "min", "max"}
        if not isinstance(st, dict) or set(st) != want:
            keys = "/".join(sorted(want))
            problems.append(f"{name}:{ln}: station {k + 1} must be an object "
                            f"with exactly the keys {keys}")
            ok = False
            continue
        if not _nonempty_str(st["ch"]):
            problems.append(f"{name}:{ln}: station {k + 1}: ch must be a "
                            "non-empty string")
            ok = False
        if k > 0:
            lo, hi = st["min"], st["max"]
            if not _pos_int(lo) or not _pos_int(hi):
                problems.append(f"{name}:{ln}: station {k + 1}: min/max must "
                                "be positive integers")
                ok = False
            elif lo > hi:
                problems.append(f"{name}:{ln}: station {k + 1}: "
                                f"min {lo} > max {hi}")
                ok = False
    if not ok:
        return None
    canon = json.dumps(route, ensure_ascii=False, sort_keys=True,
                       separators=(",", ":"))
    return pkg, canon


def _check_assoc(obj, ln, name, problems):
    """Validate one association line.  Returns (pkg, scan) or None."""
    if not isinstance(obj, dict) or set(obj) != {"pkg", "scan"}:
        problems.append(f"{name}:{ln}: row must be an object with exactly "
                        "the keys pkg/scan")
        return None
    pkg, scan = obj["pkg"], obj["scan"]
    ok = True
    if not _nonempty_str(pkg):
        problems.append(f"{name}:{ln}: pkg must be a non-empty string")
        ok = False
    if not _nonempty_str(scan):
        problems.append(f"{name}:{ln}: scan must be a non-empty string")
        ok = False
    return (pkg, scan) if ok else None


def _parse_lines(lines, name, check, problems):
    """Yield (line_number, checked) for each non-blank line;  malformed
    lines append to `problems` and yield a None checked value."""
    for ln, raw in enumerate(lines, 1):
        if not raw.strip():
            continue
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError as e:
            problems.append(f"{name}:{ln}: invalid JSON: {e}")
            continue
        yield ln, check(obj, ln, name, problems)


# ------------------------------------------------------------------ seg tree

def _seg_nodes(l, r, M):
    """Node ids of the canonical cover of position range [l, r] in a
    segment tree with M leaves (leaf j is node M + j)."""
    out = []
    l += M
    r += M + 1
    while l < r:
        if l & 1:
            out.append(l)
            l += 1
        if r & 1:
            r -= 1
            out.append(r)
        l //= 2
        r //= 2
    return out


def _build_seg(con, pkg, st, m):
    """Persist a segment tree over station `st`'s m reach rows (positions
    0..m-1) and return its leaf count M (a power of two).

    Pure SQL:  leaves copied from reach, then each internal level is one
    INSERT..SELECT grouping child pairs with MIN(ckey) -- no level is ever
    materialized in process memory."""
    M = 1 << (m - 1).bit_length()
    con.execute("INSERT INTO seg SELECT ?, ?, ? + pos, ckey FROM reach "
                "WHERE pkg = ? AND st = ?", (pkg, st, M, pkg, st))
    lo, hi = M, M + m - 1
    while lo > 1:
        con.execute("INSERT INTO seg SELECT pkg, st, node / 2, MIN(ckey) "
                    "FROM seg WHERE pkg = ? AND st = ? AND node BETWEEN ? AND ? "
                    "GROUP BY node / 2", (pkg, st, lo, hi))
        lo, hi = lo // 2, hi // 2
    return M


def _seg_min(con, pkg, st, l, r, M):
    """Minimum chain key among station `st`'s reach rows at positions
    [l, r] -- O(log M) node lookups, never a scan of the range."""
    nodes = _seg_nodes(l, r, M)
    q = ",".join("?" * len(nodes))
    best = None
    for (ckey,) in con.execute(
            f"SELECT ckey FROM seg WHERE pkg = ? AND st = ? AND node IN ({q})",
            [pkg, st, *nodes]):
        if best is None or ckey < best:
            best = ckey
    return best


def _probe(con, pkg, st, ts, sql):
    row = con.execute(sql, (pkg, st, ts)).fetchone()
    return row[0] if row else None


# -------------------------------------------------------------- per-package DP

def _compute_package(con, pkg, route):
    """Longest provable prefix for one package plus its lexicographically
    smallest evidence chain.

    Returns (passed, chain) with chain = [[ts, scan_id], ...].  Only a
    handful of rows is held in memory at any time;  all candidates live in
    the staging tables, which are emptied before returning."""
    con.execute(_SQL_CAND_FILL, (pkg, pkg))
    n_st = len(route)
    last = -1                       # highest station index with evidence
    prev_m = 0
    for st in range(n_st):
        ch = route[st]["ch"]
        if st > 0:
            if prev_m == 0:
                break
            if con.execute(_SQL_CAND_ANY, (pkg, ch)).fetchone() is None:
                break
            M = _build_seg(con, pkg, st - 1, prev_m)
            lo, hi = route[st]["min"], route[st]["max"]
        pos = 0
        batch = []
        for ts, sid in con.execute(_SQL_CAND_STREAM, (pkg, ch)):
            if st == 0:
                ckey = _pair_key(ts, sid)
            else:
                a = ts - hi
                b = ts - lo
                if b < 0:
                    continue
                l = _probe(con, pkg, st - 1, a if a > 0 else 0, SQL_PROBE_LO)
                if l is None:
                    continue
                r = _probe(con, pkg, st - 1, b, SQL_PROBE_HI)
                if r is None or l > r:
                    continue
                best = _seg_min(con, pkg, st - 1, l, r, M)
                if best is None:
                    continue
                ckey = best + _pair_key(ts, sid)
            batch.append((pkg, st, pos, ts, ckey))
            pos += 1
            if len(batch) >= 500:
                con.executemany(_SQL_REACH_INS, batch)
                batch.clear()
        if batch:
            con.executemany(_SQL_REACH_INS, batch)
        if pos == 0:
            break
        last = st
        prev_m = pos
    chain = []
    if last >= 0:
        (best,) = con.execute(
            "SELECT MIN(ckey) FROM reach WHERE pkg = ? AND st = ?",
            (pkg, last)).fetchone()
        chain = _decode_key(best)
    for t in ("cand", "reach", "seg"):
        con.execute(f"DELETE FROM {t} WHERE pkg = ?", (pkg,))
    return last + 1, chain


# -------------------------------------------------------------------- helpers

def _hash_rows(cur):
    h = hashlib.sha256()
    for row in cur:
        h.update(json.dumps(row, ensure_ascii=False,
                            separators=(",", ":")).encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def _qualified(schema, alias):
    return schema.replace("CREATE TABLE ", f"CREATE TABLE {alias}.")


# ---------------------------------------------------------------------- verify

def verify(ledger_path, routes_lines, assoc_lines, out_path,
           snapshot_hook=None, progress_every=None):
    """Verify all packages and write the result database.

    ledger_path: a handover-scan-ledger/1 database (opened read-only).
    routes_lines / assoc_lines: iterables of JSONL lines (streamed).
    out_path: result path;  must not exist yet.

    snapshot_hook: test hook called once, inside the ledger snapshot
    transaction, after the snapshot hash is taken.  progress_every: test
    hook installing a SQLite progress handler (every N opcodes) and adding
    an approximate "vm_ops" count to the summary.

    Raises SourceInvalid (input problems, nothing left behind) and
    VerifyReject (result exists / not a ledger).  Returns a summary dict.
    """
    tmp_out = out_path + ".tmp"
    if os.path.exists(out_path) or os.path.exists(tmp_out):
        raise VerifyReject(
            "result_exists",
            f"{out_path} already exists; a verification result is written "
            "once -- remove it (and any leftover .tmp) to re-verify")
    out_dir = os.path.dirname(os.path.abspath(out_path))
    fd, staging_path = tempfile.mkstemp(prefix=".verify-staging-",
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
            raise VerifyReject("ledger_missing", str(e)) from e
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

        # ---- the ledger snapshot: one committed state for the whole run
        try:
            lmeta = dict(con.execute("SELECT k, v FROM lg.meta"))
        except sqlite3.Error as e:
            raise VerifyReject(
                "not_a_ledger",
                f"{ledger_path} is not a {LEDGER_FORMAT} database: {e}") from e
        if lmeta.get("format") != LEDGER_FORMAT:
            raise VerifyReject(
                "not_a_ledger",
                f"{ledger_path} is not a {LEDGER_FORMAT} database")
        n_scans = 0
        scans_cur = con.execute(
            "SELECT scan_id, ws, ts, device, ch FROM lg.scans "
            "ORDER BY scan_id")
        # hash the snapshot while streaming it (constant memory)
        scans_sha = hashlib.sha256()
        for row in scans_cur:
            scans_sha.update(json.dumps(row, ensure_ascii=False,
                                        separators=(",", ":")).encode("utf-8"))
            scans_sha.update(b"\n")
            n_scans += 1
        scans_sha = scans_sha.hexdigest()
        n_corr = con.execute(
            "SELECT COUNT(*) FROM lg.corrections").fetchone()[0]
        if snapshot_hook is not None:
            snapshot_hook()          # test hook: runs inside the snapshot

        # ---- ingest and validate both JSONL inputs (streaming) ----
        problems = []
        for ln, checked in _parse_lines(routes_lines, "routes",
                                        _check_route, problems):
            if checked is None:
                continue
            pkg, canon = checked
            cur = con.execute(
                "INSERT OR IGNORE INTO routes_in (pkg, route) VALUES (?, ?)",
                (pkg, canon))
            if cur.rowcount == 0:
                problems.append(
                    f"routes:{ln}: duplicate route for package {pkg!r}")
                continue
            con.execute(
                "INSERT INTO out.routes (pkg, stations, route) "
                "VALUES (?, ?, ?)",
                (pkg, len(json.loads(canon)), canon))
        for ln, checked in _parse_lines(assoc_lines, "assoc",
                                        _check_assoc, problems):
            if checked is None:
                continue
            pkg, scan = checked
            cur = con.execute(
                "INSERT OR IGNORE INTO assoc_in (pkg, scan) VALUES (?, ?)",
                (pkg, scan))
            if cur.rowcount == 0:
                problems.append(
                    f"assoc:{ln}: duplicate association for scan {scan!r}; "
                    "a scan links to at most one package")
        if not problems:
            for (p,) in con.execute(
                    "SELECT DISTINCT pkg FROM assoc_in a "
                    "WHERE NOT EXISTS (SELECT 1 FROM routes_in r "
                    "WHERE r.pkg = a.pkg) ORDER BY pkg"):
                problems.append(
                    f"assoc: package {p!r} has no route (dangling reference)")
            for (s,) in con.execute(
                    "SELECT DISTINCT scan FROM assoc_in a "
                    "WHERE NOT EXISTS (SELECT 1 FROM lg.scans s "
                    "WHERE s.scan_id = a.scan) ORDER BY scan"):
                problems.append(
                    f"assoc: scan {s!r} is not in the ledger "
                    "(dangling reference)")
        if problems:
            raise core.SourceInvalid(problems)

        # ---- content digests of the validated inputs (order-independent)
        routes_sha = _hash_rows(con.execute(
            "SELECT pkg, route FROM routes_in ORDER BY pkg"))
        assoc_sha = _hash_rows(con.execute(
            "SELECT pkg, scan FROM assoc_in ORDER BY pkg, scan"))

        # ---- per-package longest provable prefix ----
        tallies = {"full": 0, "partial": 0, "empty": 0}
        n_evidence = 0
        for pkg, canon in con.execute(
                "SELECT pkg, route FROM routes_in ORDER BY pkg"):
            route = json.loads(canon)
            passed, chain = _compute_package(con, pkg, route)
            n_st = len(route)
            key = ("full" if passed == n_st
                   else "empty" if passed == 0 else "partial")
            tallies[key] += 1
            con.execute(
                "INSERT INTO out.results "
                "(pkg, stations, passed, next_station, next_ch) "
                "VALUES (?, ?, ?, ?, ?)",
                (pkg, n_st, passed,
                 passed + 1 if passed < n_st else None,
                 route[passed]["ch"] if passed < n_st else None))
            for i, (ts, sid) in enumerate(chain, 1):
                con.execute(
                    "INSERT INTO out.evidence "
                    "(pkg, station, scan_id, ts, ch) VALUES (?, ?, ?, ?, ?)",
                    (pkg, i, sid, ts, route[i - 1]["ch"]))
            n_evidence += len(chain)
        n_pkg = sum(tallies.values())
        con.executemany(
            "INSERT INTO out.meta (k, v) VALUES (?, ?)",
            [("format", RESULT_FORMAT),
             ("ledger_format", LEDGER_FORMAT),
             ("ledger_order_hash", lmeta.get("order_hash", "")),
             ("ledger_scan_count", str(n_scans)),
             ("ledger_correction_count", str(n_corr)),
             ("ledger_scans_sha256", scans_sha),
             ("routes_sha256", routes_sha),
             ("assoc_sha256", assoc_sha),
             ("packages", str(n_pkg)),
             ("full", str(tallies["full"])),
             ("partial", str(tallies["partial"])),
             ("empty", str(tallies["empty"])),
             ("evidence_rows", str(n_evidence))])
        con.execute("COMMIT")
        if progress_every:
            con.set_progress_handler(None, 0)
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
        "result": "verified",
        "packages": n_pkg,
        "full": tallies["full"],
        "partial": tallies["partial"],
        "empty": tallies["empty"],
        "evidence_rows": n_evidence,
        "ledger_scans": n_scans,
        "ledger_corrections": n_corr,
        "ledger_scans_sha256": scans_sha,
        "out": out_path,
    }
    if progress_every:
        summary["vm_ops"] = ops[0] * progress_every
    return summary


# ------------------------------------------------------------------ re-checker

def verify_result(result_path, ledger_path):
    """Re-check a verification result against the ledger's current scan
    records.  Returns (ok, problems).

    Checks:  the result is a handover-verify/1 database;  the ledger state
    is the very state the verification ran on (snapshot hash, order hash,
    correction count -- a ledger corrected since is reported, not silently
    re-judged);  every package's evidence rows match the ledger's scans in
    second and channel, match the route's channels station by station, and
    respect every station interval;  passed/next_station/next_ch and the
    meta tallies are consistent with the stored evidence."""
    try:
        con = sqlite3.connect(_uri(result_path, "ro"), uri=True)
    except sqlite3.Error as e:
        raise VerifyReject("result_unreadable", str(e)) from e
    try:
        try:
            meta = dict(con.execute("SELECT k, v FROM meta"))
        except sqlite3.Error as e:
            raise VerifyReject(
                "not_a_result",
                f"{result_path} is not a {RESULT_FORMAT} database: {e}") from e
        if meta.get("format") != RESULT_FORMAT:
            raise VerifyReject(
                "not_a_result",
                f"{result_path} is not a {RESULT_FORMAT} database")
        try:
            con.execute("ATTACH DATABASE ? AS lg", (_uri(ledger_path, "ro"),))
        except sqlite3.Error as e:
            raise VerifyReject("ledger_missing", str(e)) from e
        try:
            lmeta = dict(con.execute("SELECT k, v FROM lg.meta"))
        except sqlite3.Error as e:
            raise VerifyReject(
                "not_a_ledger",
                f"{ledger_path} is not a {LEDGER_FORMAT} database: {e}") from e
        if lmeta.get("format") != LEDGER_FORMAT:
            raise VerifyReject(
                "not_a_ledger",
                f"{ledger_path} is not a {LEDGER_FORMAT} database")

        problems = []
        snap_sha = _hash_rows(con.execute(
            "SELECT scan_id, ws, ts, device, ch FROM lg.scans "
            "ORDER BY scan_id"))
        if snap_sha != meta.get("ledger_scans_sha256"):
            problems.append(
                "ledger scan records differ from the verified snapshot "
                "(ledger_scans_sha256 mismatch); the ledger was probably "
                "corrected after this verification ran")
        if lmeta.get("order_hash", "") != meta.get("ledger_order_hash", ""):
            problems.append("ledger order_hash differs from the verified "
                            "snapshot")
        n_corr = con.execute(
            "SELECT COUNT(*) FROM lg.corrections").fetchone()[0]
        if str(n_corr) != meta.get("ledger_correction_count"):
            problems.append("ledger correction count differs from the "
                            "verified snapshot")
        if problems:
            return False, problems

        tallies = {"full": 0, "partial": 0, "empty": 0}
        n_results = 0
        n_evidence = 0
        for pkg, stations, passed, next_st, next_ch in con.execute(
                "SELECT pkg, stations, passed, next_station, next_ch "
                "FROM results ORDER BY pkg"):
            n_results += 1
            row = con.execute(
                "SELECT stations, route FROM routes WHERE pkg = ?",
                (pkg,)).fetchone()
            if row is None:
                problems.append(f"{pkg}: result without a route")
                continue
            r_stations, route = row[0], json.loads(row[1])
            if r_stations != stations or stations != len(route):
                problems.append(f"{pkg}: station count mismatch between "
                                "results and routes")
                continue
            if not 0 <= passed <= stations:
                problems.append(f"{pkg}: passed {passed} outside "
                                f"[0, {stations}]")
            ev = list(con.execute(
                "SELECT station, scan_id, ts, ch FROM evidence "
                "WHERE pkg = ? ORDER BY station", (pkg,)))
            if len(ev) != passed:
                problems.append(f"{pkg}: passed {passed} but "
                                f"{len(ev)} evidence rows")
            if [e[0] for e in ev] != list(range(1, len(ev) + 1)):
                problems.append(f"{pkg}: evidence stations are not "
                                "1..passed in order")
            prev_ts = None
            for station, sid, ts, ch in ev:
                n_evidence += 1
                if station < 1 or station > stations:
                    problems.append(f"{pkg} station {station}: outside the "
                                    "route")
                    continue
                want_ch = route[station - 1]["ch"]
                if ch != want_ch:
                    problems.append(f"{pkg} station {station}: evidence "
                                    f"channel {ch!r} != route channel "
                                    f"{want_ch!r}")
                srow = con.execute(
                    "SELECT ts, ch FROM lg.scans WHERE scan_id = ?",
                    (sid,)).fetchone()
                if srow is None:
                    problems.append(f"{pkg} station {station}: scan {sid!r} "
                                    "is not in the ledger")
                else:
                    if srow[0] != ts:
                        problems.append(f"{pkg} station {station}: evidence "
                                        f"second {ts} != ledger second "
                                        f"{srow[0]} of scan {sid!r}")
                    if srow[1] != ch:
                        problems.append(f"{pkg} station {station}: evidence "
                                        f"channel {ch!r} != ledger channel "
                                        f"{srow[1]!r} of scan {sid!r}")
                if station > 1 and prev_ts is not None:
                    lo = route[station - 1]["min"]
                    hi = route[station - 1]["max"]
                    if not lo <= ts - prev_ts <= hi:
                        problems.append(
                            f"{pkg} station {station}: gap {ts - prev_ts} s "
                            f"outside [{lo}, {hi}]")
                prev_ts = ts
            if passed == stations:
                if next_st is not None or next_ch is not None:
                    problems.append(f"{pkg}: fully passed but a next "
                                    "station is recorded")
                tallies["full"] += 1
            else:
                if next_st != passed + 1:
                    problems.append(f"{pkg}: next station {next_st} != "
                                    f"{passed + 1}")
                if next_ch != route[passed]["ch"]:
                    problems.append(f"{pkg}: next channel {next_ch!r} != "
                                    f"route channel {route[passed]['ch']!r}")
                tallies["empty" if passed == 0 else "partial"] += 1
        n_routes = con.execute("SELECT COUNT(*) FROM routes").fetchone()[0]
        if n_routes != n_results:
            problems.append(f"{n_routes} routes but {n_results} results")
        for key, got in (("packages", n_results),
                         ("full", tallies["full"]),
                         ("partial", tallies["partial"]),
                         ("empty", tallies["empty"]),
                         ("evidence_rows", n_evidence)):
            if meta.get(key) != str(got):
                problems.append(f"meta {key}={meta.get(key)} != {got}")
        return (not problems), problems
    finally:
        con.close()
