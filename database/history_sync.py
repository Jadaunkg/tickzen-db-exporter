"""Keep stored price and indicator history equal to today's download (not only the newest week).

Why: the export used to upsert only rows newer than the last stored date (minus a 7-day overlap). Two kinds of stale
history followed:

* Prices. Yahoo's adjusted series changes its past after every split and dividend. Stored rows kept the old values:
  KLAC showed a fall from $2,139 to $214 on its 10-for-1 split day (2026-06-10), and every dividend payer's older rows
  drifted from the series the indicators are computed on.
* Indicators. Rows written before the trading-session fix (2026-10) kept the calendar-day values (a "200-day" average
  of ~138 sessions): ~90% of technical_indicators rows, so charts showed a fake drop where the fix began.

Each sync now compares the rows it computed with the stored rows over the same dates and rewrites only the rows that
are new or differ. Unchanged history costs one read and no writes. Pure helpers here; the export calls them
(database/export_to_supabase.py) and tests live in tests/data/test_history_sync.py.
"""

from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

# Stored numerics have 4 decimals: a difference under this is rounding, not a change.
ABS_TOL = 0.00011
REL_TOL = 1e-6

PRICE_COMPARE: Tuple[str, ...] = ('open_price', 'high_price', 'low_price', 'close_price', 'adjusted_close', 'volume')

# Cumulative series (on-balance volume, volume-price trend) restart at the first downloaded day, so their level moves a
# little every sync. They are written with a row but never by themselves make a row "changed".
INDICATOR_SKIP = frozenset({'stock_id', 'date', 'id', 'created_at', 'updated_at', 'obv', 'vpt'})

# A download that does not reach back to the stored window (or misses many stored sessions) is not trusted to rewrite
# history: only the newest rows are written, as before.
MIN_COVERAGE = 0.9
START_SLACK_DAYS = 10


def _day(v: Any) -> str:
    if isinstance(v, (datetime, date)):
        return v.strftime('%Y-%m-%d')
    return str(v)[:10]


def _num(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def values_differ(new: Any, old: Any) -> bool:
    """True when two stored values are really different (None vs a value counts; rounding to 4 decimals does not)."""
    if isinstance(new, str) or isinstance(old, str):
        return (new or None) != (old or None)
    a, b = _num(new), _num(old)
    if a is None and b is None:
        return False
    if a is None or b is None:
        return True
    return abs(a - b) > max(ABS_TOL, REL_TOL * max(abs(a), abs(b)))


def changed_records(
    new_records: Iterable[Dict[str, Any]],
    stored_rows: Iterable[Dict[str, Any]],
    compare: Optional[Sequence[str]] = None,
    skip: frozenset = INDICATOR_SKIP,
) -> List[Dict[str, Any]]:
    """The new records that are missing from `stored_rows` (by date) or differ in a compared column."""
    stored = {_day(r.get('date')): r for r in stored_rows}
    out: List[Dict[str, Any]] = []
    for rec in new_records:
        old = stored.get(_day(rec.get('date')))
        if old is None:
            out.append(rec)
            continue
        cols = compare if compare is not None else [k for k in rec.keys() if k not in skip]
        if any(values_differ(rec.get(c), old.get(c)) for c in cols):
            out.append(rec)
    return out


def coverage_ok(new_dates: Iterable[Any], stored_dates: Iterable[Any], window_start: Any) -> Tuple[bool, str]:
    """
    Whether a download may rewrite the stored rows from `window_start` on: it must start within START_SLACK_DAYS of
    the window and contain at least MIN_COVERAGE of the stored sessions in it. Returns (ok, reason).
    """
    new = sorted({_day(d) for d in new_dates})
    start = _day(window_start)
    stored = {_day(d) for d in stored_dates if _day(d) >= start}
    if not new:
        return False, 'empty download'
    if not stored:
        return True, 'nothing stored in the window'
    first = datetime.strptime(new[0], '%Y-%m-%d').date()
    if (first - datetime.strptime(start, '%Y-%m-%d').date()).days > START_SLACK_DAYS:
        return False, f'download starts {new[0]}, after the stored window start {start}'
    present = len(stored & set(new)) / len(stored)
    if present < MIN_COVERAGE:
        return False, f'download has only {present:.0%} of the stored sessions since {start}'
    return True, 'ok'


def rows_from(records: Iterable[Dict[str, Any]], start: Any) -> List[Dict[str, Any]]:
    """Records dated on or after `start`."""
    s = _day(start)
    return [r for r in records if _day(r.get('date')) >= s]


PRICE_SQL = ('SELECT date, open_price, high_price, low_price, close_price, adjusted_close, volume '
             'FROM daily_price_data WHERE stock_id = %s AND date >= %s')
INDICATOR_SQL = 'SELECT * FROM technical_indicators WHERE stock_id = %s AND date >= %s'


def _plan(run_sql, sql: str, stock_id: int, records: List[Dict[str, Any]], compare,
          within_stored: bool = False) -> Tuple[Optional[List[Dict[str, Any]]], str]:
    if not records:
        return None, 'nothing computed'
    window_start = min(_day(r['date']) for r in records)
    stored = run_sql(sql, [stock_id, window_start]) or []
    if not stored:
        return None, 'nothing stored yet (first load writes everything)'
    if within_stored:
        # Correct what is stored and add new days; older years are not filled in here (database/price_history.py).
        first_stored = min(_day(r['date']) for r in stored)
        records = rows_from(records, first_stored)
        window_start = max(window_start, first_stored)
    ok, why = coverage_ok([r['date'] for r in records], [r['date'] for r in stored], window_start)
    if not ok:
        return None, why
    return changed_records(records, stored, compare), 'ok'


def plan_price_rows(run_sql, mapper, stock_id: int, price_frame) -> Tuple[Optional[List[Dict[str, Any]]], str]:
    """
    The daily_price_data rows to upsert so the stored prices equal today's (adjusted) download over the dates it
    covers, or (None, reason) when the download is not trusted to rewrite history (the caller then writes only the
    newest rows). The oldest downloaded day has no previous close (no daily change) and is never rewritten.
    """
    records = mapper.map_daily_prices(stock_id, price_frame)
    return _plan(run_sql, PRICE_SQL, stock_id, records[1:], PRICE_COMPARE, within_stored=True)


def plan_indicator_rows(run_sql, mapper, stock_id: int, session_frame) -> Tuple[Optional[List[Dict[str, Any]]], str]:
    """Same for technical_indicators, from indicators computed on trading sessions (the 2-year export window)."""
    records = mapper.map_technical_indicators(stock_id, session_frame)
    return _plan(run_sql, INDICATOR_SQL, stock_id, records, None)


def is_new(record: Dict[str, Any], latest_stored: Any) -> bool:
    """Dated after the newest stored row (for the log: new rows vs corrected history)."""
    return latest_stored is None or _day(record.get('date')) > _day(latest_stored)
