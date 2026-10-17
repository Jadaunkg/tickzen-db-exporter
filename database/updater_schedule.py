"""When the stock database updater may run and what counts as up to date.

Shared by every updater (the Azure workers in tickzen-db-exporter, the Mac runner here) and by the admin
dashboard, so all of them agree on the same rules:

* Exports run only while the US market is closed: on trading days from ``EXPORT_START_ET`` (16:30 New York,
  30 min after the close) to ``EXPORT_STOP_ET`` (09:00, 30 min before the open); weekends and NYSE holidays
  are open all day.
* A stock is **current** when its last *successful* sync happened after the most recent trading-day 16:30 ET
  (``latest_session_cutoff``). One rule covers weekends, holidays and retries of partial/failed stocks.
* ``assign_parts`` splits the monitored stocks between ``worker_count`` workers: ordered by md5(symbol) and
  dealt out in turn, so every worker computes the same split and parts differ by at most one stock.

``stocks.last_sync_date`` is real UTC. Until 2026-10-18 the exporter wrote its IST wall-clock time labelled
UTC (5.5 hours ahead). Reading such an old value as UTC is harmless: scheduled exports end at 09:00 ET, and
09:00 ET plus 5.5 hours is still before the 16:30 ET cutoff, so no stock looks current when it is not.
"""

from __future__ import annotations

import hashlib
from datetime import date, datetime, time, timedelta, timezone
from typing import Dict, Iterable, Optional
from zoneinfo import ZoneInfo

MARKET_TZ = ZoneInfo("America/New_York")
DEFAULT_START = time(16, 30)
DEFAULT_STOP = time(9, 0)
MARKET_OPEN = time(9, 30)
# Option quotes are only trustworthy inside the regular session; skip the noisy first and last minutes.
OPTIONS_QUOTE_START = time(9, 45)
OPTIONS_QUOTE_STOP = time(15, 45)

# NYSE full-day closures.
NYSE_HOLIDAYS = {
    date(2026, 1, 1), date(2026, 1, 19), date(2026, 2, 16), date(2026, 4, 3), date(2026, 5, 25),
    date(2026, 6, 19), date(2026, 7, 3), date(2026, 9, 7), date(2026, 11, 26), date(2026, 12, 25),
    date(2027, 1, 1), date(2027, 1, 18), date(2027, 2, 15), date(2027, 3, 26), date(2027, 5, 31),
    date(2027, 6, 18), date(2027, 7, 5), date(2027, 9, 6), date(2027, 11, 25), date(2027, 12, 24),
}


def is_trading_day(d: date) -> bool:
    return d.weekday() < 5 and d not in NYSE_HOLIDAYS


def next_trading_day(d: date) -> date:
    d += timedelta(days=1)
    while not is_trading_day(d):
        d += timedelta(days=1)
    return d


def parse_hhmm(value: Optional[str], default: time) -> time:
    return datetime.strptime(value.strip(), "%H:%M").time() if value and value.strip() else default


def in_export_window(now: datetime, start: time = DEFAULT_START, stop: time = DEFAULT_STOP) -> bool:
    """True while the market is closed: trading days after ``start`` or before ``stop``, every other day all day."""
    now_et = now.astimezone(MARKET_TZ)
    if not is_trading_day(now_et.date()):
        return True
    t = now_et.time()
    return t >= start or t < stop


def in_options_session(now: datetime, start: time = OPTIONS_QUOTE_START, stop: time = OPTIONS_QUOTE_STOP) -> bool:
    """True on trading days between ``start`` and ``stop`` New York, when Yahoo serves live option quotes.
    (Half-day closes are caught by the chain's own ``marketState``.)"""
    now_et = now.astimezone(MARKET_TZ)
    return is_trading_day(now_et.date()) and start <= now_et.time() < stop


def latest_session_cutoff(now: datetime, start: time = DEFAULT_START) -> datetime:
    """Most recent trading-day ``start`` (16:30 ET) at or before ``now``: a sync after it is current."""
    now_et = now.astimezone(MARKET_TZ)
    day = now_et.date()
    while True:
        candidate = datetime.combine(day, start, tzinfo=MARKET_TZ)
        if is_trading_day(day) and candidate <= now_et:
            return candidate
        day -= timedelta(days=1)


def session_deadline(cutoff: datetime, stop: time = DEFAULT_STOP) -> datetime:
    """Updates for the session that closed at ``cutoff`` must finish by ``stop`` on the next trading day."""
    return datetime.combine(next_trading_day(cutoff.astimezone(MARKET_TZ).date()), stop, tzinfo=MARKET_TZ)


def next_window_open(now: datetime, start: time = DEFAULT_START, stop: time = DEFAULT_STOP) -> datetime:
    """When exports may start again (``now`` itself if the window is open)."""
    if in_export_window(now, start, stop):
        return now
    now_et = now.astimezone(MARKET_TZ)
    return datetime.combine(now_et.date(), start, tzinfo=MARKET_TZ)


def window_close(now: datetime, start: time = DEFAULT_START, stop: time = DEFAULT_STOP) -> datetime:
    """End of the window that contains (or follows) ``now``: the next trading day's ``stop``."""
    now_et = now.astimezone(MARKET_TZ)
    day = now_et.date()
    if is_trading_day(day) and now_et.time() < stop:
        return datetime.combine(day, stop, tzinfo=MARKET_TZ)
    return datetime.combine(next_trading_day(day), stop, tzinfo=MARKET_TZ)


def sync_instant(last_sync) -> Optional[datetime]:
    """``stocks.last_sync_date`` as an aware UTC instant (naive values are taken as UTC)."""
    if not last_sync:
        return None
    if isinstance(last_sync, str):
        last_sync = datetime.fromisoformat(last_sync.replace('Z', '+00:00'))
    if last_sync.tzinfo is None:
        last_sync = last_sync.replace(tzinfo=timezone.utc)
    return last_sync.astimezone(timezone.utc)


def is_current(last_sync, last_status: Optional[str], cutoff: datetime) -> bool:
    synced = sync_instant(last_sync)
    return last_status == 'success' and synced is not None and synced >= cutoff


def assign_parts(symbols: Iterable[str], worker_count: int) -> Dict[str, int]:
    """symbol -> home worker (0-based)."""
    ranked = sorted(set(symbols), key=lambda s: hashlib.md5(s.encode()).hexdigest())
    return {symbol: rank % max(1, worker_count) for rank, symbol in enumerate(ranked)}
