"""Market benchmark series, full price history and nightly price checks for monitored stocks.

* ``update_market_index``  daily ^GSPC and SPY bars from Yahoo into ``market_index_daily`` (2 requests a day; the first
  call loads everything since ``PRICE_HISTORY_START``). The single source for "vs market" comparisons: FRED's SP500
  lags several days, and copying its latest value onto today gave 25 of 93 recent dates two different S&P values.
* ``backfill_price_history``  one Yahoo request per stock loads the years before its first stored session and marks
  ``stocks.price_history_complete_at``. The new-stock loader runs it right after a first load; the nightly workers
  catch up older stocks a few per night (``PRICE_BACKFILL_PER_NIGHT``).
* ``run_price_checks``  flags flat zero-volume runs and missing trading days into ``price_data_issues``
  (shown on /admin/db-updater). No Yahoo requests.

Every Yahoo request goes through the process-wide pacer when the runner installed it. All steps are best-effort:
they log and return, never fail an export run.
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

INDEX_SYMBOLS = ("^GSPC", "SPY")
BENCHMARK = "^GSPC"
# 8-K item codes start in August 2004; a little earlier gives every event a year of "before" prices.
PRICE_HISTORY_START = os.getenv("PRICE_HISTORY_START", "2003-01-01")
FLAT_RUN_MIN_DAYS = 3
CHECK_LOOKBACK_DAYS = 120
MISSING_DAYS_MIN = 1


def _yf():
    import yfinance as yf
    return yf


def _bars(frame: pd.DataFrame) -> pd.DataFrame:
    """Yahoo history frame -> rows with Date (trading day, no time zone) and OHLCV, Close not null."""
    if frame is None or frame.empty:
        return pd.DataFrame()
    df = frame.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df.reset_index().rename(columns={"index": "Date", "Datetime": "Date"})
    # Yahoo stamps daily bars at midnight New York time; the calendar date is the trading day.
    df["Date"] = pd.to_datetime(df["Date"]).map(lambda ts: ts.tz_localize(None) if ts.tzinfo else ts).dt.normalize()
    return df.dropna(subset=["Close"]).sort_values("Date").reset_index(drop=True)


def _num(v):
    return None if v is None or pd.isna(v) else round(float(v), 4)


# ---------------------------------------------------------------- market benchmark
def latest_index_date(db, symbol: str = BENCHMARK) -> Optional[date]:
    rows = db.execute_raw_sql("SELECT max(date) AS d FROM market_index_daily WHERE symbol = %s", [symbol])
    return rows[0]["d"] if rows else None


def update_market_index(db, fresh_after: Optional[datetime] = None, start: str = PRICE_HISTORY_START) -> Dict[str, int]:
    """Store new ^GSPC / SPY bars. Skips when the rows were written after ``fresh_after`` (the last session's cutoff),
    so the hourly runs of a night cost 2 Yahoo requests in total. Re-reads the last 10 days to pick up corrections."""
    written: Dict[str, int] = {}
    if fresh_after is not None:
        rows = db.execute_raw_sql("SELECT min(u) AS u FROM (SELECT max(updated_at) AS u FROM market_index_daily "
                                  "WHERE symbol = ANY(%s) GROUP BY symbol) t "
                                  "HAVING count(*) = %s", [list(INDEX_SYMBOLS), len(INDEX_SYMBOLS)])
        if rows and rows[0]["u"] and rows[0]["u"] >= fresh_after:
            return written
    for symbol in INDEX_SYMBOLS:
        try:
            last = latest_index_date(db, symbol)
            since = (last - timedelta(days=10)).isoformat() if last else start
            bars = _bars(_yf().Ticker(symbol).history(start=since, auto_adjust=False, actions=False))
            if bars.empty:
                logger.warning(f"Market index {symbol}: Yahoo returned no rows since {since}")
                continue
            records = [{
                "symbol": symbol, "date": d.strftime("%Y-%m-%d"),
                "open_price": _num(o), "high_price": _num(h), "low_price": _num(lo), "close_price": _num(c),
                "adjusted_close": _num(a), "volume": int(v) if v is not None and not pd.isna(v) else None,
                "source": "yahoo", "updated_at": datetime.now(timezone.utc).isoformat(),
            } for d, o, h, lo, c, a, v in zip(bars["Date"], bars.get("Open"), bars.get("High"), bars.get("Low"),
                                              bars["Close"], bars.get("Adj Close", bars["Close"]), bars.get("Volume"))]
            for i in range(0, len(records), 1000):
                db.table("market_index_daily").upsert(records[i:i + 1000], on_conflict="symbol,date").execute()
            written[symbol] = len(records)
            logger.info(f"Market index {symbol}: {len(records)} rows stored (through {records[-1]['date']})")
        except Exception as e:
            logger.warning(f"Market index {symbol} not updated: {e}")
    return written


def benchmark_close_on(db, day: str, symbol: str = BENCHMARK) -> Optional[float]:
    """Close of the benchmark on ``day`` (YYYY-MM-DD) or None. Never a value from another date."""
    try:
        rows = db.execute_raw_sql("SELECT close_price FROM market_index_daily WHERE symbol = %s AND date = %s",
                                  [symbol, day])
        return float(rows[0]["close_price"]) if rows else None
    except Exception as e:
        logger.warning(f"Benchmark close for {day} unavailable: {e}")
        return None


def benchmark_series(db, start, symbol: str = BENCHMARK) -> Optional[pd.Series]:
    """Benchmark closes by trading date (DatetimeIndex) from ``start``, or None when the table is empty/missing."""
    try:
        rows = db.execute_raw_sql("SELECT date, close_price FROM market_index_daily WHERE symbol = %s AND date >= %s "
                                  "ORDER BY date", [symbol, start])
    except Exception as e:
        logger.warning(f"Benchmark series unavailable: {e}")
        return None
    if not rows:
        return None
    return pd.Series([float(r["close_price"]) for r in rows], index=pd.to_datetime([r["date"] for r in rows]))


def align_benchmark(db, sessions: pd.DataFrame) -> Optional[pd.Series]:
    """Benchmark close for each row of ``sessions`` (by its Date), same index; None if no stored series covers it."""
    if sessions is None or sessions.empty or "Date" not in sessions.columns:
        return None
    dates = pd.to_datetime(sessions["Date"]).dt.normalize()
    series = benchmark_series(db, dates.min().date().isoformat())
    if series is None:
        return None
    aligned = pd.Series(dates.map(series).values, index=sessions.index, dtype=float)
    # Beta pairs returns row by row, so every session needs a benchmark close (today's is missing until the close).
    return aligned if aligned.notna().all() else None


# ---------------------------------------------------------------- full history backfill
def pending_backfill(db, symbols: Iterable[str], limit: int) -> List[Dict]:
    """Monitored stocks (among ``symbols``) with prices stored but older years not loaded yet, oldest added first."""
    symbols = [s.upper() for s in symbols]
    if not symbols or limit <= 0:
        return []
    return db.execute_raw_sql(
        "SELECT s.id AS stock_id, upper(s.symbol) AS symbol, min(d.date) AS first_date "
        "FROM stocks s JOIN daily_price_data d ON d.stock_id = s.id "
        "WHERE upper(s.symbol) = ANY(%s) AND s.price_history_complete_at IS NULL "
        "GROUP BY s.id, s.symbol, s.created_at ORDER BY s.created_at, s.symbol LIMIT %s", [symbols, limit])


def backfill_price_history(db, mapper, stock_id: int, symbol: str, first_date=None,
                           start: str = PRICE_HISTORY_START) -> Optional[int]:
    """Load the sessions before the stock's first stored date (one Yahoo request) and mark the history complete.

    The first stored session is rewritten too, so its daily change is computed from the new previous close.
    Returns rows written, or None when nothing was marked (Yahoo error: try again another night)."""
    if first_date is None:
        rows = db.execute_raw_sql("SELECT min(date) AS d FROM daily_price_data WHERE stock_id = %s", [stock_id])
        first_date = rows[0]["d"] if rows else None
    if first_date is None:
        return None
    first = pd.Timestamp(first_date)
    written = 0
    if first > pd.Timestamp(start) + pd.Timedelta(days=7):
        try:
            bars = _bars(_yf().Ticker(symbol).history(start=start, end=(first + pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
                                                      auto_adjust=False, actions=False))
        except Exception as e:
            logger.warning(f"{symbol}: history backfill failed, retry next night: {e}")
            return None
        bars = bars[bars["Date"] <= first]
        if len(bars) > 1:
            records = mapper.map_daily_prices(stock_id, bars)
            for i in range(0, len(records), 1000):
                db.table("daily_price_data").upsert(records[i:i + 1000], on_conflict="stock_id,date").execute()
            written = len(records) - 1
    db.execute_raw_sql("UPDATE stocks SET price_history_complete_at = now() WHERE id = %s", [stock_id])
    logger.info(f"{symbol}: price history complete ({written} older sessions added before {first.date()})")
    return written


def run_backfill(db, mapper, symbols: Iterable[str], limit: int, stop=None) -> Dict[str, int]:
    """Backfill up to ``limit`` stocks; ``stop()`` returning True (rate limits, time budget) ends the step early."""
    done: Dict[str, int] = {}
    try:
        todo = pending_backfill(db, symbols, limit)
    except Exception as e:
        logger.warning(f"History backfill skipped: {e}")
        return done
    for row in todo:
        if stop and stop():
            break
        n = backfill_price_history(db, mapper, row["stock_id"], row["symbol"], row["first_date"])
        if n is not None:
            done[row["symbol"]] = n
    return done


def backfilled_since(db, since: datetime) -> int:
    rows = db.execute_raw_sql("SELECT count(*) AS n FROM stocks WHERE price_history_complete_at >= %s", [since])
    return int(rows[0]["n"]) if rows else 0


# ---------------------------------------------------------------- nightly price checks
CHECK_SQL = """
WITH m AS (
    SELECT DISTINCT s.id, upper(s.symbol) AS symbol FROM monitored_tickers t
    JOIN stocks s ON upper(s.symbol) = upper(t.symbol) WHERE t.is_active
), cal AS (
    SELECT date FROM market_index_daily WHERE symbol = %(bench)s AND date >= %(since)s
), p AS (
    SELECT m.symbol, d.date, d.close_price, d.volume,
           d.close_price IS NOT DISTINCT FROM lag(d.close_price) OVER w AND coalesce(d.volume, 0) = 0 AS flat
    FROM m JOIN daily_price_data d ON d.stock_id = m.id AND d.date >= %(since)s::date - 30
    WINDOW w AS (PARTITION BY m.symbol ORDER BY d.date)
), runs AS (
    SELECT symbol, date, flat,
           count(*) FILTER (WHERE NOT flat) OVER (PARTITION BY symbol ORDER BY date) AS grp
    FROM p WHERE date >= %(since)s
), flat_runs AS (
    SELECT symbol, min(date) AS first_date, max(date) AS last_date, count(*) AS days
    FROM runs WHERE flat GROUP BY symbol, grp HAVING count(*) >= %(flat_min)s
), span AS (
    SELECT m.symbol, min(d.date) AS lo, max(d.date) AS hi FROM m
    JOIN daily_price_data d ON d.stock_id = m.id AND d.date >= %(since)s GROUP BY m.symbol
), missing AS (
    SELECT sp.symbol, min(c.date) AS first_date, max(c.date) AS last_date, count(*) AS days
    FROM span sp JOIN cal c ON c.date BETWEEN sp.lo AND sp.hi
    JOIN m ON m.symbol = sp.symbol
    WHERE NOT EXISTS (SELECT 1 FROM daily_price_data d WHERE d.stock_id = m.id AND d.date = c.date)
    GROUP BY sp.symbol HAVING count(*) >= %(missing_min)s
)
SELECT symbol, 'flat_zero_volume' AS issue, first_date, last_date, days::int FROM (
    SELECT DISTINCT ON (symbol) * FROM flat_runs ORDER BY symbol, last_date DESC) f
UNION ALL
SELECT symbol, 'missing_days', first_date, last_date, days::int FROM missing
"""


def find_price_issues(db, today: Optional[date] = None) -> List[Dict]:
    since = (today or datetime.now(timezone.utc).date()) - timedelta(days=CHECK_LOOKBACK_DAYS)
    rows = db.execute_raw_sql(CHECK_SQL, {"bench": BENCHMARK, "since": since, "flat_min": FLAT_RUN_MIN_DAYS,
                                          "missing_min": MISSING_DAYS_MIN})
    for r in rows:
        if r["issue"] == "flat_zero_volume":
            r["detail"] = f"{r['days']} sessions in a row with the same close and no volume"
        else:
            r["detail"] = f"{r['days']} trading days missing between {r['first_date']} and {r['last_date']}"
    return rows


def run_price_checks(db) -> Optional[int]:
    """Replace ``price_data_issues`` with tonight's findings. Needs ``market_index_daily`` as the trading calendar."""
    try:
        if latest_index_date(db) is None:
            logger.info("Price checks skipped: no market calendar stored yet")
            return None
        issues = find_price_issues(db)
        now = datetime.now(timezone.utc).isoformat()
        db.execute_raw_sql("DELETE FROM price_data_issues")
        if issues:
            db.table("price_data_issues").upsert(
                [{**{k: r[k] for k in ("symbol", "issue", "first_date", "last_date", "days", "detail")},
                  "checked_at": now} for r in issues], on_conflict="symbol,issue").execute()
        logger.info(f"Price checks: {len(issues)} issue(s) stored")
        return len(issues)
    except Exception as e:
        logger.warning(f"Price checks skipped: {e}")
        return None


def checks_ran_since(db, since: datetime) -> bool:
    try:
        rows = db.execute_raw_sql("SELECT max(checked_at) AS t FROM price_data_issues")
        last = rows[0]["t"] if rows else None
        if last is None:   # empty table: no issues last night, or never ran - the worker_runs marker decides
            rows = db.execute_raw_sql("SELECT last_outcome FROM updater_workers WHERE worker_name = 'price-checks' "
                                      "AND last_seen_at >= %s", [since])
            return bool(rows)
        return last >= since
    except Exception:
        return False


def mark_checks_ran(db) -> None:
    try:
        db.execute_raw_sql("INSERT INTO updater_workers (worker_name, last_seen_at, last_outcome) "
                           "VALUES ('price-checks', now(), 'price checks done') ON CONFLICT (worker_name) "
                           "DO UPDATE SET last_seen_at = now(), last_outcome = EXCLUDED.last_outcome")
    except Exception as e:
        logger.warning(f"Could not record the price-check run: {e}")


# ---------------------------------------------------------------- runner hook
def nightly_step(db, mapper, own_symbols: Iterable[str], session_cutoff: datetime, worker_index: int = 0,
                 stop=None) -> None:
    """Once a worker's own part is current: a few history backfills from its own part, and the price checks
    (worker 0, once per session). The benchmark is updated at the start of each run (``update_market_index``) so the
    night's snapshots find their date. Never raises."""
    try:
        per_night = int(os.getenv("PRICE_BACKFILL_PER_NIGHT", "20"))
        room = per_night - backfilled_since(db, session_cutoff)  # shared count is fine: a soft nightly cap
        if room > 0 and not (stop and stop()):
            done = run_backfill(db, mapper, own_symbols, min(room, per_night), stop=stop)
            if done:
                logger.info(f"History backfill: {len(done)} stocks, {sum(done.values())} older sessions")
        if worker_index == 0 and not checks_ran_since(db, session_cutoff):
            if run_price_checks(db) is not None:
                mark_checks_ran(db)
    except Exception as e:
        logger.warning(f"Nightly price step skipped: {e}")


def complete_new_stock(db, mapper, symbol: str) -> Optional[int]:
    """Right after a stock's first load: add the years before it (one Yahoo request). Never raises."""
    try:
        rows = db.execute_raw_sql("SELECT id FROM stocks WHERE upper(symbol) = %s AND price_history_complete_at IS NULL",
                                  [symbol.upper()])
        return backfill_price_history(db, mapper, rows[0]["id"], symbol.upper()) if rows else None
    except Exception as e:
        logger.warning(f"{symbol}: history backfill skipped: {e}")
        return None


def refresh_market_index(db, session_cutoff: datetime) -> None:
    """Start of a run: store the last session's benchmark before any snapshot needs it. Never raises."""
    try:
        update_market_index(db, fresh_after=session_cutoff)
    except Exception as e:
        logger.warning(f"Market index not refreshed: {e}")
