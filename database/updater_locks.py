"""Postgres advisory locks that keep the stock database updaters from stepping on each other.

Every updater (each Azure worker in tickzen-db-exporter and the Mac runner) claims a stock before exporting it,
so no stock is ever exported by two processes at once, and a worker number can only have one active run.
Session-level locks on one dedicated connection: they are released automatically if the process dies.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from database.updater_schedule import is_current

logger = logging.getLogger(__name__)

STOCK_LOCKS = 728190    # (STOCK_LOCKS, hashtext(symbol)) - one export of a stock at a time
WORKER_LOCKS = 728191   # (WORKER_LOCKS, index) - one run per worker; (WORKER_LOCKS, -1) - the earnings step
EARNINGS_LOCK = -1


class UpdaterLocks:
    def __init__(self, db):
        self.db = db
        self.conn = db._connect_new()
        self.conn.autocommit = True

    def _call(self, fn: str, namespace: int, key_sql: str, key) -> bool:
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT {fn}(%s, {key_sql})", (namespace, key))
            return bool(cur.fetchone()[0])

    def worker(self, index: int) -> bool:
        return self._call("pg_try_advisory_lock", WORKER_LOCKS, "%s", index)

    def earnings(self) -> bool:
        return self._call("pg_try_advisory_lock", WORKER_LOCKS, "%s", EARNINGS_LOCK)

    def claim(self, symbol: str, cutoff: Optional[datetime] = None) -> bool:
        """Lock ``symbol``; with ``cutoff``, also give it back if another updater made it current meanwhile."""
        if not self._call("pg_try_advisory_lock", STOCK_LOCKS, "hashtext(%s)", symbol):
            return False
        if cutoff is not None:
            rows = self.db.table('stocks').select('last_sync_date', 'last_sync_status').eq('symbol', symbol).execute().data
            if rows and is_current(rows[0].get('last_sync_date'), rows[0].get('last_sync_status'), cutoff):
                self.release(symbol)
                return False
        return True

    def release(self, symbol: str) -> None:
        try:
            self._call("pg_advisory_unlock", STOCK_LOCKS, "hashtext(%s)", symbol)
        except Exception as exc:  # the lock dies with the connection anyway
            logger.warning(f"Could not release claim on {symbol}: {exc}")

    def close(self) -> None:
        self.conn.close()
