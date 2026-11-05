"""Market benchmark series, full price history and nightly price checks for monitored stocks.

* ``update_market_index``  daily ^GSPC and SPY bars from Yahoo into ``market_index_daily`` (2 requests a day; the first
  call loads everything since ``PRICE_HISTORY_START``). The single source for "vs market" comparisons: FRED's SP500
  lags several days, and copying its latest value onto today gave 25 of 93 recent dates two different S&P values.
* ``backfill_price_history``  one Yahoo request per stock loads the years before its first stored session and marks
  ``stocks.price_history_complete_at``. The new-stock loader runs it right after a first load; the nightly workers
  catch up older stocks (``PRICE_BACKFILL_PER_NIGHT``) and redo every stock's every ``FULL_HISTORY_REFRESH_DAYS``.
* ``confirm_final_bars``  after ``FINAL_BAR_AFTER_ET`` re-reads the last month for stocks whose last-session row was
  stored from Yahoo's unfinished evening bar, and rewrites the days that differ (one request per stock).
* ``run_price_checks``  flags flat zero-volume runs, missing trading days and stocks with no new prices into
  ``price_data_issues`` (shown on /admin/db-updater). No Yahoo requests.

Every Yahoo request goes through the process-wide pacer when the runner installed it. All steps are best-effort:
they log and return, never fail an export run.
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, time, timedelta, timezone
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
# A stock this many sessions behind the benchmark has stopped updating (KVUE: last close 30 Oct, bought out 2 Nov).
# One session behind is normal while the other worker's part is still syncing.
NO_NEW_PRICES_MIN = 2


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
# Rows older than the daily download only change through this job. The export re-queues a stock when the oldest
# downloaded day disagrees with its stored row (a split or a dividend above 0.2%); smaller dividends are not seen
# there, so every stock's full history is also redone after this many days (~4 stocks a night for 330).
FULL_HISTORY_REFRESH_DAYS = int(os.getenv("FULL_HISTORY_REFRESH_DAYS", "90"))


def pending_backfill(db, symbols: Iterable[str], limit: int) -> List[Dict]:
    """Monitored stocks (among ``symbols``) whose full history is not loaded yet (oldest added first), then those
    last done more than FULL_HISTORY_REFRESH_DAYS ago (oldest first)."""
    symbols = [s.upper() for s in symbols]
    if not symbols or limit <= 0:
        return []
    return db.execute_raw_sql(
        "SELECT s.id AS stock_id, upper(s.symbol) AS symbol, min(d.date) AS first_date "
        "FROM stocks s JOIN daily_price_data d ON d.stock_id = s.id "
        "WHERE upper(s.symbol) = ANY(%s) AND (s.price_history_complete_at IS NULL "
        "OR s.price_history_complete_at < now() - make_interval(days => %s)) "
        "GROUP BY s.id, s.symbol, s.created_at, s.price_history_complete_at "
        "ORDER BY s.price_history_complete_at NULLS FIRST, s.created_at, s.symbol LIMIT %s",
        [symbols, FULL_HISTORY_REFRESH_DAYS, limit])


# The daily download covers ~730 days; rows older than this are rewritten here (a few days of overlap).
REWRITE_UPTO_DAYS = 720


def backfill_price_history(db, mapper, stock_id: int, symbol: str, first_date=None,
                           start: str = PRICE_HISTORY_START, today: Optional[date] = None) -> Optional[int]:
    """Load the sessions before the daily download window (one Yahoo request) and mark the history complete.

    Covers the years before the stock's first stored date and rewrites the stored sessions older than the daily
    2-year download (up to REWRITE_UPTO_DAYS ago): the export only rewrites days it downloads, so older rows kept an
    old split/dividend adjustment (KLAC $654 on 2024-11-04 next to $66 the day after). The export clears
    price_history_complete_at when it sees that (database/history_sync.py), so this runs again for that stock.
    Prices are split- and dividend-adjusted like the daily download (auto_adjust=True), so the two series join.
    Returns rows written, or None when nothing was marked (Yahoo error: try again another night)."""
    if first_date is None:
        rows = db.execute_raw_sql("SELECT min(date) AS d FROM daily_price_data WHERE stock_id = %s", [stock_id])
        first_date = rows[0]["d"] if rows else None
    if first_date is None:
        return None
    first = pd.Timestamp(first_date)
    upto = max(first, pd.Timestamp(today or date.today()) - pd.Timedelta(days=REWRITE_UPTO_DAYS))
    written = 0
    if first > pd.Timestamp(start) + pd.Timedelta(days=7) or upto > first:
        try:
            bars = _bars(_yf().Ticker(symbol).history(start=start, end=(upto + pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
                                                      auto_adjust=True, actions=False))
        except Exception as e:
            logger.warning(f"{symbol}: history backfill failed, retry next night: {e}")
            return None
        bars = bars[bars["Date"] <= upto]
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


# Every writer stores split- and dividend-adjusted prices (close_price = adjusted_close). Rows where they differ came
# from the first version of this job (auto_adjust=False until 2026-11-03): MSFT's 2010-01-04 close was $30.95 instead
# of $22.98, and those stocks were already marked complete. They go back into the queue (once per session, worker 0).
UNADJUSTED_REL_TOL = 0.002
REQUEUE_UNADJUSTED_SQL = """
UPDATE stocks s SET price_history_complete_at = NULL
WHERE s.price_history_complete_at IS NOT NULL
  AND EXISTS (SELECT 1 FROM monitored_tickers t WHERE upper(t.symbol) = upper(s.symbol) AND t.is_active)
  AND EXISTS (SELECT 1 FROM daily_price_data d WHERE d.stock_id = s.id AND d.adjusted_close > 0
              AND abs(d.close_price / d.adjusted_close - 1) > %s)
RETURNING upper(s.symbol) AS symbol
"""


def requeue_unadjusted(db) -> List[str]:
    """Send stocks with unadjusted stored rows back to the full-history queue. Returns their symbols."""
    rows = db.execute_raw_sql(REQUEUE_UNADJUSTED_SQL, [UNADJUSTED_REL_TOL]) or []
    symbols = sorted(r["symbol"] for r in rows)
    if symbols:
        logger.info(f"Full history queued again (unadjusted rows stored): {', '.join(symbols)}")
    return symbols


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
), last_day AS (
    SELECT m.symbol, max(d.date) AS hi FROM m JOIN daily_price_data d ON d.stock_id = m.id GROUP BY m.symbol
), behind AS (
    SELECT l.symbol, l.hi AS first_date, max(c.date) AS last_date, count(*) AS days
    FROM last_day l JOIN cal c ON c.date > l.hi GROUP BY l.symbol, l.hi HAVING count(*) >= %(behind_min)s
)
SELECT symbol, 'flat_zero_volume' AS issue, first_date, last_date, days::int FROM (
    SELECT DISTINCT ON (symbol) * FROM flat_runs ORDER BY symbol, last_date DESC) f
UNION ALL
SELECT symbol, 'missing_days', first_date, last_date, days::int FROM missing
UNION ALL
SELECT symbol, 'no_new_prices', first_date, last_date, days::int FROM behind
"""


def find_price_issues(db, today: Optional[date] = None) -> List[Dict]:
    since = (today or datetime.now(timezone.utc).date()) - timedelta(days=CHECK_LOOKBACK_DAYS)
    rows = db.execute_raw_sql(CHECK_SQL, {"bench": BENCHMARK, "since": since, "flat_min": FLAT_RUN_MIN_DAYS,
                                          "missing_min": MISSING_DAYS_MIN, "behind_min": NO_NEW_PRICES_MIN})
    for r in rows:
        if r["issue"] == "flat_zero_volume":
            r["detail"] = f"{r['days']} sessions in a row with the same close and no volume"
        elif r["issue"] == "no_new_prices":
            r["detail"] = (f"no new prices since {r['first_date']} ({r['days']} trading days through "
                           f"{r['last_date']}): bought out, delisted, renamed, or the download fails")
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


# ---------------------------------------------------------------- final bars of the last session
# The evening sync stores Yahoo's bar as it is then, and for thin and OTC stocks it is not final yet: AUMN's 4 Nov row
# was written at 19:25 New York as $0.1998 / 7,520 shares, Yahoo's final bar is $0.20 / 17,500 (every stock's last-day
# volume was 1-9% low). The next evening's sync corrects it (it compares the downloaded days with the stored ones),
# so the page showed the unfinished day for a whole day. After FINAL_BAR_AFTER_ET the workers re-read the last month
# of each stock whose last-session row was stored before then, rewrite the days that differ and recompute its
# indicators from the stored prices. The session row is written even when equal: its updated_at marks it final.
FINAL_BAR_AFTER_ET = os.getenv("FINAL_BAR_AFTER_ET", "02:00")
FINAL_BAR_PERIOD = "1mo"
PROVISIONAL_SQL = (
    "SELECT s.id AS stock_id, upper(s.symbol) AS symbol FROM stocks s "
    "JOIN daily_price_data d ON d.stock_id = s.id AND d.date = %s "
    "WHERE upper(s.symbol) = ANY(%s) AND d.updated_at < %s ORDER BY s.symbol")


def final_bar_time(session_cutoff: datetime) -> datetime:
    """When the bars of the session that closed at ``session_cutoff`` count as final (the next calendar day)."""
    from database.updater_schedule import MARKET_TZ, parse_hhmm
    day = session_cutoff.astimezone(MARKET_TZ).date() + timedelta(days=1)
    return datetime.combine(day, parse_hhmm(FINAL_BAR_AFTER_ET, time(2, 0)), tzinfo=MARKET_TZ)


def rewrite_indicators(db, mapper, stock_id: int) -> Optional[int]:
    """technical_indicators from the stored prices, as the export does after its price step. Rows written or None."""
    from database.history_sync import DELETE_INDICATOR_SQL, plan_indicator_rewrite
    rows, stray, why = plan_indicator_rewrite(db.execute_raw_sql, mapper, stock_id)
    if rows is None:
        logger.warning(f"stock {stock_id}: indicators not recomputed ({why})")
        return None
    for i in range(0, len(rows), 1000):
        db.table("technical_indicators").upsert(rows[i:i + 1000], on_conflict="stock_id,date").execute()
    if stray:
        db.execute_raw_sql(DELETE_INDICATOR_SQL, [stock_id, stray])
    return len(rows)


def apply_final_bars(db, mapper, stock_id: int, bars: pd.DataFrame, session_day: date) -> Optional[int]:
    """Write the downloaded days that differ from the stored rows (up to ``session_day``) and mark the session row
    final. Returns the days corrected, or None when the download is not trusted (no bar for the session, or it misses
    stored days): the row stays unconfirmed and the next run tries again."""
    from database.history_sync import PRICE_COMPARE, PRICE_SQL, changed_records, coverage_ok
    if bars.empty:
        return None
    bars = bars[bars["Date"] <= pd.Timestamp(session_day)]
    records = mapper.map_daily_prices(stock_id, bars)[1:]   # the first day has no previous close
    if not records or records[-1]["date"] != session_day.isoformat():
        return None
    stored = db.execute_raw_sql(PRICE_SQL, [stock_id, records[0]["date"]]) or []
    ok, _ = coverage_ok([r["date"] for r in records], [r["date"] for r in stored], records[0]["date"])
    if not ok:
        return None
    changed = changed_records(records, stored, PRICE_COMPARE)
    session_row = records[-1]
    rows = changed if any(r is session_row for r in changed) else changed + [session_row]
    db.table("daily_price_data").upsert(rows, on_conflict="stock_id,date").execute()
    if changed:
        rewrite_indicators(db, mapper, stock_id)
    return len(changed)


def confirm_final_bars(db, mapper, symbols: Iterable[str], session_cutoff: datetime, now: Optional[datetime] = None,
                       stop=None) -> Optional[Dict[str, Optional[int]]]:
    """Re-read the stocks (among ``symbols``) whose last-session row predates final_bar_time. Returns
    {symbol: days corrected or None}, or None before the final time."""
    from database.updater_schedule import MARKET_TZ
    final_after = final_bar_time(session_cutoff)
    if (now or datetime.now(timezone.utc)) < final_after:
        return None
    symbols = [s.upper() for s in symbols]
    if not symbols or mapper is None:
        return {}
    session_day = session_cutoff.astimezone(MARKET_TZ).date()
    done: Dict[str, Optional[int]] = {}
    for row in db.execute_raw_sql(PROVISIONAL_SQL, [session_day, symbols, final_after]) or []:
        if stop and stop():
            break
        try:
            bars = _bars(_yf().Ticker(row["symbol"]).history(period=FINAL_BAR_PERIOD, auto_adjust=True, actions=False))
            done[row["symbol"]] = apply_final_bars(db, mapper, row["stock_id"], bars, session_day)
        except Exception as e:
            logger.warning(f"{row['symbol']}: final-bar check failed, next run tries again: {e}")
    return done


# ---------------------------------------------------------------- runner hook
def nightly_step(db, mapper, own_symbols: Iterable[str], session_cutoff: datetime, worker_index: int = 0,
                 stop=None) -> None:
    """Once a worker's own part is current: the final bars of the last session (after FINAL_BAR_AFTER_ET), history
    backfills from its own part, and (worker 0, once per session) the unadjusted-history re-queue and the price
    checks. The runner also calls it when
    every stock is already current. The benchmark is updated at the start of each run (``update_market_index``) so
    the night's snapshots find their date. Never raises."""
    own_symbols = list(own_symbols)
    try:
        confirmed = confirm_final_bars(db, mapper, own_symbols, session_cutoff, stop=stop)
        if confirmed:
            fixed = {s: n for s, n in confirmed.items() if n}
            logger.info(f"Final bars: {len(confirmed)} stocks re-read, {sum(fixed.values())} days corrected"
                        + (f" ({', '.join(sorted(fixed))})" if fixed else ""))
    except Exception as e:
        logger.warning(f"Final-bar check skipped: {e}")
    try:
        if worker_index == 0 and not checks_ran_since(db, session_cutoff):
            requeue_unadjusted(db)
    except Exception as e:
        logger.warning(f"Unadjusted-history check skipped: {e}")
    try:
        per_night = int(os.getenv("PRICE_BACKFILL_PER_NIGHT", "100"))
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
