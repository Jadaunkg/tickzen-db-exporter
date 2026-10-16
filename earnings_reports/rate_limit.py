"""Rate limits for the free data providers (Finnhub free tier, Yahoo via ``yfinance``).

Several processes can call the same provider at once - the web app's cron job, a CLI backfill, the
per-stock exporter - so a purely in-process limiter is not enough.  ``DbRateLimiter`` keeps a shared
fixed-window counter in Postgres (table ``api_rate_limits``, DB clock, so hosts with skewed clocks
agree).  Fixed windows of ``window_seconds`` with at most ``per_window`` calls each bound *any* 60 s
span to ``per_window * (60 / window_seconds + 1)`` calls.

Budgets (deliberately well under the providers' ceilings):

* Finnhub free tier allows 60 calls/min.  7 per 10 s window  -> at most 49 in any minute (~42 steady).
* Yahoo publishes no limit; ~2,000 requests/hour per IP is the commonly observed ceiling before HTTP 429.
  4 per 10 s window -> at most 28 in any minute (~24 steady, ~1,440/hour).

If the database is unreachable the limiter degrades to the in-process one (never blocks forever, never
raises into the caller).
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Callable, List, Optional

logger = logging.getLogger(__name__)

FINNHUB_PER_WINDOW, FINNHUB_WINDOW_SECONDS, FINNHUB_LOCAL_PER_MINUTE = 7, 10, 45
YAHOO_PER_WINDOW, YAHOO_WINDOW_SECONDS, YAHOO_LOCAL_PER_MINUTE = 4, 10, 25
CLEANUP_EVERY = 200


class RateLimiter:
    """In-process rolling window: at most ``per_minute`` calls start in any 60 s (thread-safe)."""

    def __init__(self, per_minute: int = FINNHUB_LOCAL_PER_MINUTE, clock: Optional[Callable[[], float]] = None,
                 sleep: Optional[Callable[[float], None]] = None):
        self.per_minute = max(1, per_minute)
        self._clock = clock or time.monotonic
        self._sleep = sleep or time.sleep
        self._stamps: List[float] = []
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = self._clock()
            self._stamps = [t for t in self._stamps if now - t < 60.0]
            if len(self._stamps) >= self.per_minute:
                pause = 60.0 - (now - self._stamps[0]) + 0.05
                if pause > 0:
                    self._sleep(pause)
                now = self._clock()
                self._stamps = [t for t in self._stamps if now - t < 60.0]
            self._stamps.append(self._clock())


ACQUIRE_SQL = (
    "INSERT INTO public.api_rate_limits AS r (key, window_start, calls) "
    "VALUES (%s, to_timestamp(floor(extract(epoch FROM clock_timestamp()) / %s) * %s), 1) "
    "ON CONFLICT (key, window_start) DO UPDATE SET calls = r.calls + 1 WHERE r.calls < %s "
    "RETURNING calls"
)
UNTIL_NEXT_WINDOW_SQL = "SELECT (%s - mod(extract(epoch FROM clock_timestamp())::numeric, %s))::float AS wait"
CLEANUP_SQL = "DELETE FROM public.api_rate_limits WHERE window_start < NOW() - INTERVAL '1 day'"


class DbRateLimiter:
    """Cross-process fixed-window limiter backed by Postgres, with an in-process limiter in front."""

    def __init__(self, key: str, per_window: int, window_seconds: int, local_per_minute: int,
                 db_factory: Optional[Callable[[], Any]] = None, sleep: Optional[Callable[[float], None]] = None):
        self.key, self.per_window, self.window_seconds = key, per_window, window_seconds
        self._local = RateLimiter(local_per_minute)
        self._db_factory = db_factory
        self._db = None
        self._sleep = sleep or time.sleep
        self._acquired = 0
        self._db_ok = True
        self._lock = threading.Lock()

    def _connection(self):
        if self._db is None:
            if self._db_factory is None:
                from database.azure_postgres_client import AzurePostgresClient
                self._db = AzurePostgresClient()
            else:
                self._db = self._db_factory()
        return self._db

    def _try_acquire(self) -> Optional[float]:
        """None when a slot was granted, else seconds until the next window opens."""
        db = self._connection()
        rows = db.execute_raw_sql(ACQUIRE_SQL, [self.key, self.window_seconds, self.window_seconds, self.per_window])
        if rows:
            self._acquired += 1
            if self._acquired % CLEANUP_EVERY == 0:
                db.execute_raw_sql(CLEANUP_SQL)
            return None
        wait = db.execute_raw_sql(UNTIL_NEXT_WINDOW_SQL, [self.window_seconds, self.window_seconds])
        return max(0.05, float(wait[0]["wait"])) + 0.05 if wait else float(self.window_seconds)

    def wait(self) -> None:
        self._local.wait()
        if not self._db_ok:
            return
        while True:
            try:
                with self._lock:                      # one thread at a time talks to the shared connection
                    pause = self._try_acquire()
            except Exception as exc:  # noqa: BLE001 - never let the limiter break the caller
                logger.warning("Shared rate limiter for %s unavailable, using in-process limit only: %s", self.key, exc)
                self._db_ok = False
                return
            if pause is None:
                return
            self._sleep(pause)


_limiters: dict = {}
_limiters_lock = threading.Lock()


def _db_configured() -> bool:
    return bool(os.getenv("AZURE_POSTGRES_URL") or (os.getenv("AZURE_POSTGRES_HOST") and os.getenv("AZURE_POSTGRES_USER")))


def _build(name: str):
    if name == "finnhub":
        local, shared = FINNHUB_LOCAL_PER_MINUTE, (FINNHUB_PER_WINDOW, FINNHUB_WINDOW_SECONDS)
    elif name == "yahoo":
        local, shared = YAHOO_LOCAL_PER_MINUTE, (YAHOO_PER_WINDOW, YAHOO_WINDOW_SECONDS)
    else:
        raise ValueError(f"unknown limiter {name!r}")
    if os.getenv("TICKZEN_RATE_LIMIT_BACKEND", "db").lower() == "db" and _db_configured():
        return DbRateLimiter(name, shared[0], shared[1], local)
    return RateLimiter(local)


def get_limiter(name: str):
    """Process-wide limiter for ``finnhub`` or ``yahoo`` (shared across processes when a DB is configured)."""
    with _limiters_lock:
        if name not in _limiters:
            _limiters[name] = _build(name)
        return _limiters[name]
