"""SQLite receiver for handover work orders.

The receiver holds the live binding table plus the committed progress of at
most one work order.  Real UNIQUE constraints on both number columns are
always on (offline rows store NULL, which SQLite treats as distinct).  Each
step is committed in a single transaction together with its progress record,
so business change and progress either both land or neither does.

Protocol:
  * first acceptance of an order verifies the receiver's whole binding table
    against the order's initial_hash;
  * afterwards, submits are matched by order identity (order_hash) and the
    persisted committed step number;
  * resending an already-committed step of the same order is a verified
    no-op; gaps and steps of a foreign order are rejected;
  * when the last step lands, the whole table is checked against final_hash.
"""
from __future__ import annotations

import sqlite3

from . import core

SCHEMA = """
CREATE TABLE IF NOT EXISTS bindings(
  id TEXT PRIMARY KEY,
  ws TEXT,
  ch TEXT,
  CONSTRAINT ws_unique UNIQUE(ws),
  CONSTRAINT ch_unique UNIQUE(ch)
);
CREATE TABLE IF NOT EXISTS meta(
  k TEXT PRIMARY KEY,
  v TEXT
);
CREATE TABLE IF NOT EXISTS steps(
  seq INTEGER PRIMARY KEY,
  id TEXT NOT NULL,
  op TEXT NOT NULL,
  hash TEXT NOT NULL
);
"""


class Reject(Exception):
    """A submission was rejected; the database was left unchanged."""

    def __init__(self, code, detail):
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}")


class Receiver:
    def __init__(self, path):
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def close(self):
        self.conn.close()

    # ------------------------------------------------------------------ reads

    def bindings(self):
        """Current rows: {id: (ws, ch)}; (None, None) marks an offline row."""
        return {r[0]: (r[1], r[2])
                for r in self.conn.execute("SELECT id, ws, ch FROM bindings")}

    def bindings_hash(self):
        rows = self.conn.execute("SELECT id, ws, ch FROM bindings").fetchall()
        return core.table_hash(rows)

    def status(self):
        m = dict(self.conn.execute("SELECT k, v FROM meta"))
        return {
            "order_hash": m.get("order_hash"),
            "committed": int(m["committed"]) if "committed" in m else 0,
            "total": int(m["total"]) if "total" in m else None,
            "complete": m.get("complete") == "1",
            "bindings_hash": self.bindings_hash(),
        }

    def step_hashes(self):
        return {r[0]: r[1] for r in
                self.conn.execute("SELECT seq, hash FROM steps ORDER BY seq")}

    # ----------------------------------------------------------------- writes

    def load_initial(self, rows):
        """Bulk-load the initial binding table into a fresh receiver."""
        with self.conn:
            if self.conn.execute("SELECT COUNT(*) FROM bindings").fetchone()[0]:
                raise Reject("not_fresh", "bindings table is not empty")
            if dict(self.conn.execute("SELECT k, v FROM meta")):
                raise Reject("not_fresh", "receiver already accepted an order")
            self.conn.executemany(
                "INSERT INTO bindings (id, ws, ch) VALUES (?, ?, ?)",
                [(i, w, c) for i, (w, c) in rows.items()])

    def begin(self, order):
        """Register an order without submitting a step (empty orders)."""
        with self.conn:
            self._begin_locked(order)
        return self.status()

    def _begin_locked(self, order):
        m = dict(self.conn.execute("SELECT k, v FROM meta"))
        if "order_hash" in m:
            if m["order_hash"] != order["order_hash"]:
                raise Reject("foreign_order",
                             "a different work order is already in progress")
            return
        # First acceptance: verify the whole initial binding table.
        if self.bindings_hash() != order["initial_hash"]:
            raise Reject("initial_mismatch",
                         "receiver bindings differ from the order's initial table")
        total = len(order["steps"])
        complete = "0"
        if total == 0:
            if self.bindings_hash() != order["final_hash"]:
                raise Reject("final_mismatch",
                             "empty order but receiver does not match target table")
            complete = "1"
        for k, v in (("order_hash", order["order_hash"]),
                     ("committed", "0"),
                     ("total", str(total)),
                     ("complete", complete),
                     ("initial_hash", order["initial_hash"]),
                     ("final_hash", order["final_hash"])):
            self.conn.execute("INSERT INTO meta (k, v) VALUES (?, ?)", (k, v))

    def submit(self, order, seq):
        """Submit step `seq` of `order`.  Exactly one step is committed.

        Returns a result dict; raises Reject (leaving the db unchanged) for
        foreign orders, gaps, mismatched resends, state mismatches and
        unique-constraint violations."""
        steps = order["steps"]
        if not isinstance(seq, int) or seq < 0 or seq >= len(steps):
            raise Reject("bad_seq", f"no step {seq} in this order")
        step = steps[seq]
        if step.get("seq") != seq:
            raise Reject("bad_order", "step seq does not match its position")
        try:
            with self.conn:  # one transaction: business change + progress
                self._begin_locked(order)
                committed = int(dict(
                    self.conn.execute("SELECT k, v FROM meta"))["committed"])
                if seq < committed:
                    stored = self.conn.execute(
                        "SELECT hash FROM steps WHERE seq = ?", (seq,)).fetchone()
                    if stored is None or stored[0] != core.step_hash(step):
                        raise Reject("step_mismatch",
                                     "resent step differs from the committed step")
                    return {"result": "already_committed", "committed": committed}
                if seq > committed:
                    raise Reject("gap",
                                 f"next expected step is {committed}, got {seq}")
                self._apply_locked(step)
                self.conn.execute(
                    "INSERT INTO steps (seq, id, op, hash) VALUES (?, ?, ?, ?)",
                    (seq, step["id"], step["op"], core.step_hash(step)))
                committed += 1
                self.conn.execute("UPDATE meta SET v = ? WHERE k = 'committed'",
                                  (str(committed),))
                complete = committed == len(steps)
                if complete:
                    if self.bindings_hash() != order["final_hash"]:
                        raise Reject("final_mismatch",
                                     "order finished but receiver does not match "
                                     "the target table")
                    self.conn.execute("UPDATE meta SET v = '1' WHERE k = 'complete'")
                return {"result": "committed",
                        "committed": committed, "complete": complete}
        except sqlite3.IntegrityError as e:
            raise Reject("constraint_violation", str(e)) from e

    def _apply_locked(self, step):
        rid = step["id"]
        before, after = step["before"], step["after"]
        row = self.conn.execute(
            "SELECT ws, ch FROM bindings WHERE id = ?", (rid,)).fetchone()
        bs = before["state"]
        if bs == core.ABSENT:
            if row is not None:
                raise Reject("state_mismatch", f"{rid}: expected no row")
        elif bs == core.OFFLINE:
            if row is None or row[0] is not None or row[1] is not None:
                raise Reject("state_mismatch", f"{rid}: expected an offline row")
        else:
            if row is None or row[0] != before["ws"] or row[1] != before["ch"]:
                raise Reject("state_mismatch",
                             f"{rid}: expected online ({before['ws']}, {before['ch']})")
        as_ = after["state"]
        if as_ == core.ABSENT:
            self.conn.execute("DELETE FROM bindings WHERE id = ?", (rid,))
        elif as_ == core.OFFLINE:
            self.conn.execute(
                "UPDATE bindings SET ws = NULL, ch = NULL WHERE id = ?", (rid,))
        elif bs == core.ABSENT:
            self.conn.execute("INSERT INTO bindings (id, ws, ch) VALUES (?, ?, ?)",
                              (rid, after["ws"], after["ch"]))
        else:
            self.conn.execute("UPDATE bindings SET ws = ?, ch = ? WHERE id = ?",
                              (after["ws"], after["ch"], rid))
