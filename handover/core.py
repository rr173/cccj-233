"""Core validation, planning, work-order and verification logic for the
scanner-station binding-table handover tool.

A binding table maps a stable scanner id to a (workstation, channel) pair;
both number columns are individually unique among online rows and compared
case-sensitively.  The legacy receiver only accepts per-device steps:

  insert  : absent            -> online(ws,ch)   (target-only ids)
  delete  : online(ws,ch)     -> absent          (old-only ids)
  update  : online(ws,ch)     -> online(ws',ch') (ids in both, one step)
  offline : online(ws,ch)     -> offline         (permitted ids only)
  online  : offline           -> online(ws',ch') (permitted ids only)

Rules honoured by the planner: no half-written bindings, no delete+reinsert
masquerading as an in-place move, no borrowed numbers (every online write
uses exactly the device's own target pair), unchanged devices are never
touched, and rows present in both versions keep their identity.
"""
from __future__ import annotations

import hashlib
import heapq
import json

FORMAT = "handover-order/1"

ABSENT = "absent"
OFFLINE = "offline"
ONLINE = "online"

REQUIRED_KEYS = {"id", "ws", "ch"}


# --------------------------------------------------------------------- errors

class SourceInvalid(Exception):
    """The source material (tables / offline list / order file) is invalid."""

    def __init__(self, problems):
        self.problems = [str(p) for p in problems]
        super().__init__("; ".join(self.problems))


class MigrationImpossible(Exception):
    """Source is valid, but no legal per-device order exists.

    Carries a report describing a closed wait cycle of devices that are not
    permitted to go offline."""

    def __init__(self, report):
        self.report = report
        super().__init__(report["reason"])


# ---------------------------------------------------------- canonical hashing

def canon(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def table_hash(rows) -> str:
    """rows: iterable of (id, ws, ch); ws/ch may be None for offline rows."""
    return sha(canon(sorted(([r[0], r[1], r[2]] for r in rows), key=lambda r: r[0])))


def step_hash(step) -> str:
    return sha(canon(step))


def order_hash(initial_hash, final_hash, steps) -> str:
    return sha(canon({"initial_hash": initial_hash,
                      "final_hash": final_hash,
                      "steps": steps}))


# ------------------------------------------------------------------ validation

def validate_table(lines, name):
    """Parse and validate a JSONL binding table.  Returns {id: (ws, ch)}.

    Raises SourceInvalid listing every problem found."""
    problems = []
    rows = {}
    ws_seen = {}
    ch_seen = {}
    for ln, raw in enumerate(lines, 1):
        if not raw.strip():
            continue
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError as e:
            problems.append(f"{name}:{ln}: invalid JSON: {e}")
            continue
        if not isinstance(obj, dict):
            problems.append(f"{name}:{ln}: row is not a JSON object")
            continue
        keys = set(obj.keys())
        if keys != REQUIRED_KEYS:
            problems.append(f"{name}:{ln}: keys must be exactly "
                            f"{sorted(REQUIRED_KEYS)}, got {sorted(keys)}")
            continue
        rid, w, c = obj["id"], obj["ws"], obj["ch"]
        bad = False
        for label, v in (("id", rid), ("ws", w), ("ch", c)):
            if not isinstance(v, str) or v == "":
                problems.append(f"{name}:{ln}: {label} must be a non-empty string")
                bad = True
        if bad:
            continue
        if rid in rows:
            problems.append(f"{name}:{ln}: duplicate id {rid!r}")
            continue
        if w in ws_seen:
            problems.append(f"{name}:{ln}: workstation {w!r} already used by {ws_seen[w]!r}")
            continue
        if c in ch_seen:
            problems.append(f"{name}:{ln}: channel {c!r} already used by {ch_seen[c]!r}")
            continue
        rows[rid] = (w, c)
        ws_seen[w] = rid
        ch_seen[c] = rid
    if problems:
        raise SourceInvalid(problems)
    return rows


def validate_offline_list(obj, old, target):
    """Validate the temporary-offline id list against both tables.

    Only ids present in both versions whose binding changed may be listed.
    Returns a frozenset."""
    problems = []
    if not isinstance(obj, list):
        raise SourceInvalid(["offline list must be a JSON array of id strings"])
    seen = set()
    out = []
    for k, v in enumerate(obj):
        if not isinstance(v, str) or v == "":
            problems.append(f"offline[{k}]: must be a non-empty string")
            continue
        if v in seen:
            problems.append(f"offline[{k}]: duplicate id {v!r}")
            continue
        seen.add(v)
        if v not in old or v not in target:
            problems.append(f"offline id {v!r}: must exist in both old and target tables")
            continue
        if old[v] == target[v]:
            problems.append(f"offline id {v!r}: binding unchanged; temporary offline not allowed")
            continue
        out.append(v)
    if problems:
        raise SourceInvalid(problems)
    return frozenset(out)


# -------------------------------------------------------------------- planner

def plan(old, target, offline):
    """Build a work order migrating `old` to `target`.

    old/target: {id: (ws, ch)} (validated).  offline: frozenset of ids
    permitted to go temporarily offline.  Returns (order, stats).

    Raises SourceInvalid if the inputs are inconsistent, MigrationImpossible
    with a closed wait cycle if no legal order exists.

    Scheduling is incremental: a ready-heap plus per-number waiter indexes,
    so each committed step costs O(log n) -- the device set is never
    rescanned per step.  A cycle scan only happens when no device can move.
    """
    _check_unique(old, "old")
    _check_unique(target, "target")
    bad_off = [i for i in offline
               if i not in old or i not in target or old[i] == target[i]]
    if bad_off:
        raise SourceInvalid(
            [f"offline id {i!r}: must exist in both tables with a changed binding"
             for i in sorted(bad_off)])

    stats = {"release_events": 0, "cycle_scans": 0, "offlines": 0}
    steps = []
    cur_ws = {}   # workstation number -> id currently holding it (online rows)
    cur_ch = {}   # channel number -> id currently holding it
    state = {}    # id -> [kind, ws, ch], kind in {online, offline, absent}
    for rid, (w, c) in old.items():
        state[rid] = [ONLINE, w, c]
        cur_ws[w] = rid
        cur_ch[c] = rid
    for rid in target:
        state.setdefault(rid, [ABSENT, None, None])

    def emit(rid, op, before, after):
        steps.append({"seq": len(steps), "id": rid, "op": op,
                      "before": before, "after": after})

    # 1) deletions of old-only ids first: always legal, only frees numbers.
    for rid in sorted(set(old) - set(target)):
        w, c = old[rid]
        emit(rid, "delete",
             {"state": ONLINE, "ws": w, "ch": c}, {"state": ABSENT})
        del cur_ws[w]
        del cur_ch[c]
        state[rid] = [ABSENT, None, None]

    # 2) movers: ids in both with a changed binding, plus target-only ids.
    movers = {}
    for rid, (w, c) in target.items():
        if rid not in old or old[rid] != (w, c):
            movers[rid] = (w, c)

    waiter_ws = {}  # number -> the (unique) mover waiting for that workstation
    waiter_ch = {}  # number -> the (unique) mover waiting for that channel
    blocked = {}    # mover id -> number of its target columns still occupied
    ready = []      # heap of mover ids with blocked == 0
    done = set()

    for rid, (w, c) in movers.items():
        cnt = 0
        _, ow, oc = state[rid]
        if w != ow and w in cur_ws:
            waiter_ws[w] = rid
            cnt += 1
        if c != oc and c in cur_ch:
            waiter_ch[c] = rid
            cnt += 1
        blocked[rid] = cnt
        if cnt == 0:
            heapq.heappush(ready, rid)

    def release(num, col):
        stats["release_events"] += 1
        m = (waiter_ws if col == "ws" else waiter_ch).pop(num, None)
        if m is not None:
            blocked[m] -= 1
            if blocked[m] == 0:
                heapq.heappush(ready, m)

    def blocker_of(rid):
        """The online mover currently blocking `rid`, as (holder, col, number)."""
        w, c = movers[rid]
        _, ow, oc = state[rid]
        if w != ow:
            holder = cur_ws.get(w)
            if holder is not None and holder != rid:
                return (holder, "ws", w)
        if c != oc:
            holder = cur_ch.get(c)
            if holder is not None and holder != rid:
                return (holder, "ch", c)
        return None

    remaining = len(movers)
    while remaining:
        if ready:
            rid = heapq.heappop(ready)
            w, c = movers[rid]
            kind, ow, oc = state[rid]
            if kind == ABSENT:
                emit(rid, "insert",
                     {"state": ABSENT}, {"state": ONLINE, "ws": w, "ch": c})
            elif kind == ONLINE:
                emit(rid, "update",
                     {"state": ONLINE, "ws": ow, "ch": oc},
                     {"state": ONLINE, "ws": w, "ch": c})
                if ow != w:
                    del cur_ws[ow]
                    release(ow, "ws")
                if oc != c:
                    del cur_ch[oc]
                    release(oc, "ch")
            else:  # offline -> online with the target binding
                emit(rid, "online",
                     {"state": OFFLINE}, {"state": ONLINE, "ws": w, "ch": c})
            cur_ws[w] = rid
            cur_ch[c] = rid
            state[rid] = [ONLINE, w, c]
            done.add(rid)
            remaining -= 1
            continue

        # Nobody can move: every remaining online mover waits on another one,
        # so the wait graph among them contains a directed cycle.
        stats["cycle_scans"] += 1
        online_remaining = [r for r in movers
                            if r not in done and state[r][0] == ONLINE]
        if not online_remaining:  # unreachable: blocked movers imply online holders
            raise AssertionError("planner bug: blocked movers but no online mover")
        seen = {}
        path = []
        x = min(online_remaining)
        while x not in seen:
            seen[x] = len(path)
            path.append(x)
            x = blocker_of(x)[0]
        cycle = path[seen[x]:]
        permitted = [r for r in cycle if r in offline]
        if not permitted:
            raise MigrationImpossible(
                _build_cycle_report(cycle, blocker_of, state, movers, offline,
                                    at_step=len(steps)))
        d = min(permitted)
        _, w0, c0 = state[d]
        emit(d, "offline",
             {"state": ONLINE, "ws": w0, "ch": c0}, {"state": OFFLINE})
        del cur_ws[w0]
        del cur_ch[c0]
        release(w0, "ws")
        release(c0, "ch")
        state[d] = [OFFLINE, None, None]
        stats["offlines"] += 1

    return build_order(old, target, steps), stats


def _check_unique(table, name):
    ws = [w for w, _ in table.values()]
    ch = [c for _, c in table.values()]
    problems = []
    if len(set(ws)) != len(ws):
        problems.append(f"{name} table has duplicate workstation numbers")
    if len(set(ch)) != len(ch):
        problems.append(f"{name} table has duplicate channel numbers")
    if problems:
        raise SourceInvalid(problems)


def _build_cycle_report(cycle, blocker_of, state, movers, offline, at_step):
    edges = []
    for rid in cycle:
        holder, col, num = blocker_of(rid)
        tw, tc = movers[rid]
        _, cw, cc = state[rid]
        edges.append({
            "device": rid,
            "needs": {"column": col, "number": num, "held_by": holder},
            "current": {"ws": cw, "ch": cc},
            "target": {"ws": tw, "ch": tc},
        })
    return {
        "kind": "migration_impossible",
        "reason": "closed wait cycle among devices not permitted to go offline",
        "at_step": at_step,
        "cycle": edges,
        "offline_permitted": sorted(offline),
    }


def verify_cycle_report(report, offline):
    """Re-check a cycle report against the orchestration-time state embedded
    in it.  Returns (ok, problems).  When ok is True, every device on the
    cycle needs a number currently held by the next device on the cycle and
    none of them may go offline, so none of them can move first."""
    problems = []
    if report.get("kind") != "migration_impossible":
        return False, ["not a migration_impossible report"]
    edges = report.get("cycle") or []
    if len(edges) < 2:
        problems.append("cycle must contain at least two devices")
        return False, problems
    devices = [e.get("device") for e in edges]
    if len(set(devices)) != len(devices):
        problems.append("cycle contains a device twice")
    n = len(edges)
    for k, e in enumerate(edges):
        dev = e["device"]
        nxt = edges[(k + 1) % n]
        need = e.get("needs") or {}
        col, num = need.get("column"), need.get("number")
        if col not in ("ws", "ch"):
            problems.append(f"{dev}: bad column {col!r}")
            continue
        if need.get("held_by") != nxt["device"]:
            problems.append(f"{dev}: held_by {need.get('held_by')!r} != next device "
                            f"{nxt['device']!r}; cycle does not close")
        if (e.get("target") or {}).get(col) != num:
            problems.append(f"{dev}: needed number {num!r} is not its own target {col}")
        if (nxt.get("current") or {}).get(col) != num:
            problems.append(f"{dev}: holder {nxt['device']} does not currently hold "
                            f"{num!r} in column {col}")
        if (e.get("current") or {}).get(col) == num:
            problems.append(f"{dev}: already holds {num!r} in column {col}; not blocked")
        if dev in offline:
            problems.append(f"{dev}: is offline-permitted and must not be on this cycle")
    return (not problems), problems


# ----------------------------------------------------------------- work order

def build_order(old, target, steps):
    ih = table_hash([(i, w, c) for i, (w, c) in old.items()])
    fh = table_hash([(i, w, c) for i, (w, c) in target.items()])
    return {
        "format": FORMAT,
        "initial_hash": ih,
        "final_hash": fh,
        "order_hash": order_hash(ih, fh, steps),
        "steps": steps,
    }


def dump_order(order) -> str:
    return json.dumps(order, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def load_order(path):
    with open(path, encoding="utf-8") as f:
        try:
            obj = json.load(f)
        except json.JSONDecodeError as e:
            raise SourceInvalid([f"order file is not valid JSON: {e}"])
    check_order(obj)
    return obj


_OP_SHAPES = {
    "insert": (ABSENT, ONLINE),
    "delete": (ONLINE, ABSENT),
    "update": (ONLINE, ONLINE),
    "offline": (ONLINE, OFFLINE),
    "online": (OFFLINE, ONLINE),
}


def _state_ok(s):
    if not isinstance(s, dict):
        return False
    st = s.get("state")
    if st in (ABSENT, OFFLINE):
        return set(s) == {"state"}
    if st == ONLINE:
        return (set(s) == {"state", "ws", "ch"}
                and isinstance(s["ws"], str) and s["ws"] != ""
                and isinstance(s["ch"], str) and s["ch"] != "")
    return False


def check_order(order):
    """Structural + integrity validation of a work order document."""
    problems = []
    if not isinstance(order, dict) or order.get("format") != FORMAT:
        raise SourceInvalid([f"not a {FORMAT} document"])
    steps = order.get("steps")
    if not isinstance(steps, list):
        problems.append("steps must be a list")
        steps = []
    for k, st in enumerate(steps):
        if not isinstance(st, dict):
            problems.append(f"step {k}: not an object")
            continue
        if st.get("seq") != k:
            problems.append(f"step {k}: seq mismatch")
        if not isinstance(st.get("id"), str) or st.get("id") == "":
            problems.append(f"step {k}: id must be a non-empty string")
        op = st.get("op")
        if op not in _OP_SHAPES:
            problems.append(f"step {k}: unknown op {op!r}")
            continue
        before, after = st.get("before"), st.get("after")
        if not _state_ok(before) or not _state_ok(after):
            problems.append(f"step {k}: malformed before/after state")
            continue
        want_b, want_a = _OP_SHAPES[op]
        if before["state"] != want_b or after["state"] != want_a:
            problems.append(f"step {k}: op {op} does not match before/after states")
    ih, fh = order.get("initial_hash"), order.get("final_hash")
    if not isinstance(ih, str) or not isinstance(fh, str):
        problems.append("missing initial_hash/final_hash")
    if problems:
        raise SourceInvalid(problems)
    if order.get("order_hash") != order_hash(ih, fh, steps):
        raise SourceInvalid(["order_hash does not match order content"])


# ------------------------------------------------------------- replay checker

def replay_check(initial, steps, keep_ids=()):
    """Independently replay committed steps on top of the initial table and
    prove that every committed prefix satisfies both unique constraints and
    that `keep_ids` rows (present in both versions) are never deleted.

    initial: {id: (ws, ch)}.  steps: order steps (a committed prefix).
    Returns the final state {id: (ws, ch)} with (None, None) for offline rows.
    Raises AssertionError on any violation."""
    state = {i: (w, c) for i, (w, c) in initial.items()}
    for st in steps:
        rid = st["id"]
        before, after = st["before"], st["after"]
        cur = state.get(rid)
        bs = before["state"]
        if bs == ABSENT:
            assert cur is None, f"step {st['seq']}: {rid} expected absent"
        elif bs == OFFLINE:
            assert cur == (None, None), f"step {st['seq']}: {rid} expected offline"
        else:
            assert cur == (before["ws"], before["ch"]), \
                f"step {st['seq']}: {rid} expected online {(before['ws'], before['ch'])}, got {cur}"
        as_ = after["state"]
        if as_ == ABSENT:
            del state[rid]
        elif as_ == OFFLINE:
            state[rid] = (None, None)
        else:
            state[rid] = (after["ws"], after["ch"])
        ws_seen, ch_seen = {}, {}
        for i, (w, c) in state.items():
            if w is not None:
                assert w not in ws_seen, \
                    f"prefix {st['seq']}: workstation {w!r} held by both {ws_seen.get(w)!r} and {i!r}"
                ws_seen[w] = i
            if c is not None:
                assert c not in ch_seen, \
                    f"prefix {st['seq']}: channel {c!r} held by both {ch_seen.get(c)!r} and {i!r}"
                ch_seen[c] = i
        for i in keep_ids:
            assert i in state, \
                f"prefix {st['seq']}: row {i!r} is missing (delete+reinsert is forbidden)"
    return state
