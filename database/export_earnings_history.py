#!/usr/bin/env python3
"""Export earnings history, price reactions and implied moves to Azure PostgreSQL.

Feeds the earnings pages (Flask ``/stock-analysis/earnings-calendar/*`` and Next.js
``/stocks/<ticker>/earnings``).  Free sources only: Yahoo (``yfinance``) for deep history, Finnhub
(free tier, rate-limited) for report times, fresh results, revenue and a beat/miss cross-check.

Usage::

    python database/export_earnings_history.py --migrate                 # create tables/columns (idempotent)
    python database/export_earnings_history.py --tickers AAPL MSFT       # specific stocks
    python database/export_earnings_history.py --all --skip-fresh-hours 168   # backfill / weekly sweep
    python database/export_earnings_history.py --recent-days 4           # stocks that just reported (daily)
    python database/export_earnings_history.py --stalest 40              # batch: least recently checked first
    python database/export_earnings_history.py --implied --window-days 14    # options-implied moves (market hours only)
    python database/export_earnings_history.py --runup-backfill --limit 50   # fill pre-report run-ups for existing rows

The same functions are called by the per-stock exporter (``export_to_supabase.py``) and by the cron
endpoint ``/api/cron/refresh-earnings-insights``.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from earnings_reports import earnings_history as eh  # noqa: E402

logger = logging.getLogger(__name__)
_as_date = eh._as_date

MIGRATION_PATH = Path(__file__).with_name("migrations") / "20261004_earnings_history.sql"
RUNUP_MIGRATION_PATH = Path(__file__).with_name("migrations") / "20261005_earnings_runup.sql"
IMPLIED_STATUS_MIGRATION_PATH = Path(__file__).with_name("migrations") / "20261025_implied_move_status.sql"
MIGRATION_PATHS = (MIGRATION_PATH, RUNUP_MIGRATION_PATH, IMPLIED_STATUS_MIGRATION_PATH)
CALENDAR_LOOKBACK_DAYS = 400   # calendar rows older than this are never needed for the merge


# --------------------------------------------------------------------------- schema
def apply_migration(db) -> None:
    """Run the additive, idempotent migration (CREATE ... IF NOT EXISTS / ADD COLUMN IF NOT EXISTS)."""
    for path in MIGRATION_PATHS:
        db.execute_raw_sql(path.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------- reads
def active_stocks(db, tickers: Optional[Sequence[str]] = None) -> List[Dict[str, Any]]:
    rows = db.execute_raw_sql(
        "SELECT id, COALESCE(symbol, ticker) AS symbol FROM public.stocks "
        "WHERE is_active = true AND COALESCE(symbol, ticker) IS NOT NULL ORDER BY COALESCE(symbol, ticker)"
    )
    if tickers:
        wanted = {t.strip().upper() for t in tickers}
        rows = [r for r in rows if str(r["symbol"]).upper() in wanted]
    return [{"id": int(r["id"]), "symbol": str(r["symbol"]).upper()} for r in rows]


def calendar_rows_for(db, symbol: str, since: date) -> List[Dict[str, Any]]:
    return db.execute_raw_sql(
        "SELECT earnings_date, time_of_day, eps_estimate, eps_actual, revenue_estimate, revenue_actual "
        "FROM public.earnings_calendar WHERE ticker = %s AND earnings_date >= %s ORDER BY earnings_date",
        [symbol, since],
    )


def recently_refreshed_ids(db, hours: int) -> set:
    if hours <= 0:
        return set()
    rows = db.execute_raw_sql(
        "SELECT stock_id FROM public.earnings_history_status WHERE checked_at > NOW() - (%s * INTERVAL '1 hour')",
        [hours],
    )
    return {int(r["stock_id"]) for r in rows}


def select_stale_stocks(db, limit: int, min_age_hours: int = 144) -> List[Dict[str, Any]]:
    """Active stocks never checked, then the ones checked longest ago (older than ``min_age_hours``)."""
    rows = db.execute_raw_sql(
        "SELECT s.id, COALESCE(s.symbol, s.ticker) AS symbol FROM public.stocks s "
        "LEFT JOIN public.earnings_history_status h ON h.stock_id = s.id "
        "WHERE s.is_active = true AND COALESCE(s.symbol, s.ticker) IS NOT NULL "
        "AND (h.checked_at IS NULL OR h.checked_at < NOW() - (%s * INTERVAL '1 hour')) "
        "ORDER BY h.checked_at NULLS FIRST, s.id LIMIT %s",
        [min_age_hours, limit],
    )
    return [{"id": int(r["id"]), "symbol": str(r["symbol"]).upper()} for r in rows]


def record_status(db, stock_id: int, rows: int, note: Optional[str] = None, daily_vol: Optional[float] = None) -> None:
    """``daily_vol`` (percent points) is stored for the single-expiry implied move; None keeps the stored one."""
    if daily_vol is None:
        db.execute_raw_sql(
            "INSERT INTO public.earnings_history_status (stock_id, checked_at, rows, note) VALUES (%s, NOW(), %s, %s) "
            "ON CONFLICT (stock_id) DO UPDATE SET checked_at = NOW(), rows = EXCLUDED.rows, note = EXCLUDED.note",
            [stock_id, rows, note],
        )
        return
    db.execute_raw_sql(
        "INSERT INTO public.earnings_history_status (stock_id, checked_at, rows, note, daily_vol_pct, daily_vol_at) "
        "VALUES (%s, NOW(), %s, %s, %s, NOW()) "
        "ON CONFLICT (stock_id) DO UPDATE SET checked_at = NOW(), rows = EXCLUDED.rows, note = EXCLUDED.note, "
        "daily_vol_pct = EXCLUDED.daily_vol_pct, daily_vol_at = NOW()",
        [stock_id, rows, note, daily_vol],
    )


def refresh_if_stale(db, stock: Dict[str, Any], max_age_hours: int = 24, **kwargs: Any) -> Optional[Dict[str, Any]]:
    """Per-stock exporter hook: rebuild history unless it was checked within ``max_age_hours``."""
    if stock["id"] in recently_refreshed_ids(db, max_age_hours):
        return None
    return refresh_stock_history(db, stock, **kwargs)


def recent_reporters(db, today: date, days: int) -> List[str]:
    """Active-stock symbols whose calendar date falls in [today - days, today] (they reported recently)."""
    rows = db.execute_raw_sql(
        "SELECT DISTINCT c.ticker FROM public.earnings_calendar c "
        "JOIN public.stocks s ON COALESCE(s.symbol, s.ticker) = c.ticker AND s.is_active = true "
        "WHERE c.earnings_date BETWEEN %s AND %s",
        [today - timedelta(days=days), today],
    )
    return [str(r["ticker"]) for r in rows]


# --------------------------------------------------------------------------- history
def refresh_stock_history(
    db,
    stock: Dict[str, Any],
    *,
    today: Optional[date] = None,
    use_finnhub: bool = True,
    fetch_events: Callable[[str], List[Dict[str, Any]]] = eh.fetch_events,
    fetch_bars: Callable[[str], List[Dict[str, Any]]] = eh.fetch_bars,
    fetch_surprises: Callable[[str], List[Dict[str, Any]]] = eh.fetch_finnhub_surprises,
) -> Dict[str, Any]:
    """Rebuild and upsert one stock's earnings history.  Returns a small summary (never raises for data gaps)."""
    today = today or datetime.now(timezone.utc).date()
    symbol, stock_id = stock["symbol"], stock["id"]

    events = fetch_events(symbol)
    calendar = calendar_rows_for(db, symbol, today - timedelta(days=CALENDAR_LOOKBACK_DAYS))
    events = eh.merge_calendar_events(events, calendar)
    if not any(eh.finite(e.get("eps_actual")) is not None for e in events):
        record_status(db, stock_id, 0, "no reported events")
        return {"symbol": symbol, "rows": 0, "reason": "no reported events"}

    bars = fetch_bars(symbol)
    records = eh.build_history_records(stock_id, events, bars, today=today)
    if not records:
        record_status(db, stock_id, 0, "no usable events")
        return {"symbol": symbol, "rows": 0, "reason": "no usable events"}

    # Carry earlier conflict flags forward (Finnhub only covers the last four quarters), then let any
    # fresh Finnhub evidence confirm or clear the quarters it covers.
    previously_flagged = {
        _as_date(r["earnings_date"])
        for r in db.execute_raw_sql(
            "SELECT earnings_date FROM public.earnings_history WHERE stock_id = %s AND eps_source_conflict", [stock_id])
    }
    for record in records:
        record["eps_source_conflict"] = record["earnings_date"] in previously_flagged
    if use_finnhub:
        eh.flag_source_conflicts(records, fetch_surprises(symbol), strict_actuals=eh.is_reit(symbol))
    conflicts = sum(1 for r in records if r["eps_source_conflict"])

    now = datetime.now(timezone.utc)
    for record in records:
        record["updated_at"] = now
    db.table("earnings_history").upsert(records, on_conflict="stock_id,earnings_date").execute()
    # The same bars give the normal daily movement the single-expiry implied move needs (no extra request).
    record_status(db, stock_id, len(records),
                  daily_vol=eh.normal_daily_vol(bars, [r.get("reaction_date") for r in records]))
    return {
        "symbol": symbol,
        "rows": len(records),
        "with_reaction": sum(1 for r in records if r["reaction_1d_pct"] is not None),
        "conflicts": conflicts,
    }


MAX_CONSECUTIVE_FAILURES = 8     # a provider that fails this many in a row is unhealthy: stop, retry next run


def refresh_histories(
    db,
    stocks: Sequence[Dict[str, Any]],
    *,
    pause_seconds: float = 0.0,
    max_consecutive_failures: int = MAX_CONSECUTIVE_FAILURES,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Refresh many stocks.  One failing ticker never stops the run, but persistent throttling or a run of
    consecutive failures does (circuit breaker): hammering a provider that is refusing us only prolongs the ban.
    Request pacing is done by the shared limiters inside the fetchers, not here."""
    ok = failed = rows = streak = 0
    failures: List[str] = []
    aborted: Optional[str] = None
    for stock in stocks:
        try:
            summary = refresh_stock_history(db, stock, **kwargs)
            rows += summary.get("rows", 0)
            ok += 1
            streak = 0
            logger.info("earnings history %s: %s", stock["symbol"], summary)
        except eh.ProviderThrottled as exc:
            aborted = f"provider throttled at {stock['symbol']}: {exc}"
            logger.error("earnings history aborted: %s", aborted)
            break
        except Exception as exc:  # noqa: BLE001 - provider/network errors are per-ticker
            failed += 1
            streak += 1
            failures.append(stock["symbol"])
            logger.warning("earnings history %s failed: %s", stock["symbol"], exc)
            if streak >= max_consecutive_failures:
                aborted = f"{streak} consecutive failures (last: {stock['symbol']})"
                logger.error("earnings history aborted: %s", aborted)
                break
        if pause_seconds:
            time.sleep(pause_seconds)
    out = {"stocks": len(stocks), "ok": ok, "failed": failed, "rows": rows, "failures": failures[:50]}
    if aborted:
        out["aborted"] = aborted
    return out


# --------------------------------------------------------------------------- run-up backfill
def runup_backfill_stocks(db, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """Active stocks that have measured reactions but no run-up stored yet (one price-history call each)."""
    sql = (
        "SELECT DISTINCT s.id, COALESCE(s.symbol, s.ticker) AS symbol FROM public.earnings_history h "
        "JOIN public.stocks s ON s.id = h.stock_id "
        "WHERE s.is_active = true AND h.reaction_date IS NOT NULL "
        "AND h.runup_5d_pct IS NULL AND h.runup_20d_pct IS NULL ORDER BY s.id"
    )
    rows = db.execute_raw_sql(sql)
    stocks = [{"id": int(r["id"]), "symbol": str(r["symbol"]).upper()} for r in rows]
    return stocks[:limit] if limit else stocks


def backfill_stock_runups(
    db,
    stock: Dict[str, Any],
    *,
    fetch_bars: Callable[[str], List[Dict[str, Any]]] = eh.fetch_bars,
) -> Dict[str, Any]:
    """Fill ``runup_5d_pct`` / ``runup_20d_pct`` for one stock from its price history, using the stored report
    dates and report times.  Only those two columns are written; reactions and EPS are left untouched."""
    rows = db.execute_raw_sql(
        "SELECT earnings_date, time_of_day FROM public.earnings_history WHERE stock_id = %s AND reaction_date IS NOT NULL",
        [stock["id"]],
    )
    bars = eh.normalize_bars(fetch_bars(stock["symbol"]))
    dates: List[date] = []
    short: List[Optional[float]] = []
    long_: List[Optional[float]] = []
    for row in rows:
        report = _as_date(row["earnings_date"])
        measured = eh.measure_reaction(bars, report, row.get("time_of_day")) if report else None
        if not measured or (measured["runup_5d_pct"] is None and measured["runup_20d_pct"] is None):
            continue
        dates.append(report)
        short.append(measured["runup_5d_pct"])
        long_.append(measured["runup_20d_pct"])
    if dates:
        db.execute_raw_sql(
            "UPDATE public.earnings_history e SET runup_5d_pct = v.r5, runup_20d_pct = v.r20 "
            "FROM (SELECT unnest(%s::date[]) AS d, unnest(%s::numeric[]) AS r5, unnest(%s::numeric[]) AS r20) v "
            "WHERE e.stock_id = %s AND e.earnings_date = v.d",
            [dates, short, long_, stock["id"]],
        )
    return {"symbol": stock["symbol"], "rows": len(rows), "updated": len(dates)}


def backfill_runups(
    db,
    stocks: Sequence[Dict[str, Any]],
    *,
    max_consecutive_failures: int = MAX_CONSECUTIVE_FAILURES,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Backfill many stocks with the same circuit breaker as ``refresh_histories`` (pacing comes from the shared
    Yahoo limiter inside ``fetch_bars``)."""
    ok = failed = updated = streak = 0
    aborted: Optional[str] = None
    for stock in stocks:
        try:
            summary = backfill_stock_runups(db, stock, **kwargs)
            updated += summary["updated"]
            ok += 1
            streak = 0
            logger.info("run-up backfill %s: %s", stock["symbol"], summary)
        except eh.ProviderThrottled as exc:
            aborted = f"provider throttled at {stock['symbol']}: {exc}"
            break
        except Exception as exc:  # noqa: BLE001 - provider/network errors are per-ticker
            failed += 1
            streak += 1
            logger.warning("run-up backfill %s failed: %s", stock["symbol"], exc)
            if streak >= max_consecutive_failures:
                aborted = f"{streak} consecutive failures (last: {stock['symbol']})"
                break
    out: Dict[str, Any] = {"stocks": len(stocks), "ok": ok, "failed": failed, "rows_updated": updated}
    if aborted:
        out["aborted"] = aborted
    return out


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


# --------------------------------------------------------------------------- implied moves
NO_OPTIONS_RETRY_HOURS = 7 * 24      # a stock with no listed options is asked again after a week
CLOSE_REPORT_DAYS = 3                # a miss this close to its report is retried at the short ``close_retry_hours``
DAILY_VOL_MAX_AGE_DAYS = 14          # a stored normal daily movement older than this is recomputed when needed


def _daily_vol_source(db, row: Dict[str, Any], fetch_bars: Callable[..., List[Dict[str, Any]]]) -> Callable[[], Optional[float]]:
    """Normal daily movement for the single-expiry estimate: the value stored by the last history refresh, or (when
    missing or old) one price-history request, stored back for the next run.  Only called when that estimate is
    actually needed."""
    def get() -> Optional[float]:
        stamp = row.get("daily_vol_at")
        if row.get("daily_vol_pct") is not None and stamp is not None \
                and _aware(stamp) > datetime.now(timezone.utc) - timedelta(days=DAILY_VOL_MAX_AGE_DAYS):
            return eh.finite(row["daily_vol_pct"])
        try:
            bars = fetch_bars(str(row["ticker"]), years=1)
            reactions = db.execute_raw_sql(
                "SELECT reaction_date FROM public.earnings_history WHERE stock_id = %s AND reaction_date IS NOT NULL",
                [row.get("stock_id")]) if row.get("stock_id") is not None else []
            vol = eh.normal_daily_vol(bars, [r["reaction_date"] for r in reactions])
        except eh.ProviderThrottled:
            raise
        except Exception as exc:  # noqa: BLE001 - no normal movement: the estimate is simply not made
            logger.warning("daily vol %s failed: %s", row.get("ticker"), exc)
            return None
        if vol is not None and row.get("stock_id") is not None:
            db.execute_raw_sql(
                "UPDATE public.earnings_history_status SET daily_vol_pct = %s, daily_vol_at = NOW() WHERE stock_id = %s",
                [vol, row["stock_id"]])
        return vol
    return get


def refresh_implied_moves(
    db,
    *,
    today: Optional[date] = None,
    window_days: int = 14,
    fetch: Callable[..., Optional[Dict[str, Any]]] = eh.fetch_implied_move,
    pause_seconds: float = 0.0,
    max_consecutive_failures: int = MAX_CONSECUTIVE_FAILURES,
    min_age_hours: float = 0.0,
    retry_hours: Optional[float] = None,
    close_retry_hours: Optional[float] = None,
    no_options_retry_hours: float = NO_OPTIONS_RETRY_HOURS,
    max_requests: Optional[int] = None,
    max_minutes: Optional[float] = None,
    requests_sent: Callable[[], int] = eh.yahoo_requests_sent,
    fetch_bars: Callable[..., List[Dict[str, Any]]] = eh.fetch_bars,
) -> Dict[str, Any]:
    """Options-implied move for tracked stocks reporting in the next ``window_days`` (paced by the shared
    Yahoo limiter inside ``fetch``; same circuit breaker as ``refresh_histories``).

    Run it only during the regular session (``earnings_local_update.run_implied``): Yahoo zeroes option quotes
    outside it.  A fetch that raises ``OptionsMarketClosed`` stops the run without writing anything.

    Which reports are due (from ``implied_move_checked_at``, the last attempt; older rows only have
    ``implied_move_updated_at``):
    * a stored move is re-quoted after ``min_age_hours``;
    * a miss is retried after ``retry_hours`` (default: ``min_age_hours``), or after ``close_retry_hours`` (default:
      ``retry_hours``) once the report is ``CLOSE_REPORT_DAYS`` days away, when option trading picks up;
    * a stock with no listed options is asked again after ``no_options_retry_hours``.
    Reports dated today are skipped unless they come after the close: a BMO, intraday or unknown-time report may
    already be out, and a straddle quoted after it is no expectation.  The nearest reports go first (never-tried
    ones first within a day), so a run cut short by ``max_requests`` (Yahoo requests, counted by ``requests_sent``)
    or ``max_minutes`` has done the reports that matter soonest; the rest are due on the next run.

    A miss (no reliable quote now) never overwrites a stored move whose expiry still covers the reaction: the row
    keeps that earlier live quote and only records the attempt.  Every attempt stores its reason
    (``implied_move_status``) and every stored move its ``implied_move_method``."""
    today = today or datetime.now(timezone.utc).date()
    rows = db.execute_raw_sql(
        "SELECT c.ticker, c.earnings_date, c.time_of_day, c.implied_move_pct, c.implied_move_expiry, "
        "c.implied_move_updated_at, c.implied_move_checked_at, c.implied_move_status, "
        "s.id AS stock_id, h.daily_vol_pct, h.daily_vol_at "
        "FROM public.earnings_calendar c "
        "JOIN public.stocks s ON COALESCE(s.symbol, s.ticker) = c.ticker AND s.is_active = true "
        "LEFT JOIN public.earnings_history_status h ON h.stock_id = s.id "
        "WHERE c.earnings_date BETWEEN %s AND %s ORDER BY c.earnings_date, c.ticker",
        [today, today + timedelta(days=window_days)],
    )
    before = len(rows)
    rows = [r for r in rows if r["earnings_date"] != today or eh.session_from_calendar_hour(r.get("time_of_day")) == eh.AMC]
    retry_hours = min_age_hours if retry_hours is None else retry_hours
    close_retry_hours = retry_hours if close_retry_hours is None else close_retry_hours
    now = datetime.now(timezone.utc)

    def last_attempt(r: Dict[str, Any]) -> Optional[datetime]:
        stamp = r.get("implied_move_checked_at") or r.get("implied_move_updated_at")
        return _aware(stamp) if stamp else None

    def wait_hours(r: Dict[str, Any]) -> float:
        if r.get("implied_move_pct") is not None:
            return min_age_hours
        if r.get("implied_move_status") == "no_options":
            return no_options_retry_hours
        days_left = (_as_date(r["earnings_date"]) - today).days
        return close_retry_hours if days_left <= CLOSE_REPORT_DAYS else retry_hours

    def due(r: Dict[str, Any]) -> bool:
        stamp, hours = last_attempt(r), wait_hours(r)
        return not (stamp and hours > 0 and stamp > now - timedelta(hours=hours))

    rows = sorted((r for r in rows if due(r)), key=lambda r: (_as_date(r["earnings_date"]), last_attempt(r) is not None))
    logger.info("implied moves: %d of %d reports need a refresh (min age %sh, retry %sh, near the report %sh)",
                len(rows), before, min_age_hours, retry_hours, close_retry_hours)
    started, first_request = time.monotonic(), requests_sent()
    stored = cleared = kept = failed = streak = done = 0
    by_status: Dict[str, int] = {}
    by_method: Dict[str, int] = {}
    aborted: Optional[str] = None
    stopped: Optional[str] = None
    for row in rows:
        if max_requests is not None and requests_sent() - first_request >= max_requests:
            stopped = f"request budget ({max_requests}) used"
            break
        if max_minutes is not None and time.monotonic() - started >= max_minutes * 60:
            stopped = f"time budget ({max_minutes:g} min) used"
            break
        ticker, report = str(row["ticker"]), row["earnings_date"]
        session = eh.session_from_calendar_hour(row.get("time_of_day"))
        try:
            result = fetch(ticker, report, session, daily_vol=_daily_vol_source(db, row, fetch_bars))
            streak = 0
        except eh.OptionsMarketClosed as exc:
            aborted = f"options market closed at {ticker}: {exc}"
            logger.warning("implied moves stopped: %s", aborted)
            break
        except eh.ProviderThrottled as exc:
            aborted = f"provider throttled at {ticker}: {exc}"
            logger.error("implied moves aborted: %s", aborted)
            break
        except Exception as exc:  # noqa: BLE001
            failed += 1
            streak += 1
            logger.warning("implied move %s failed: %s", ticker, exc)
            if streak >= max_consecutive_failures:
                aborted = f"{streak} consecutive failures (last: {ticker})"
                logger.error("implied moves aborted: %s", aborted)
                break
            continue
        done += 1
        move = result.get("implied_move_pct") if result else None
        expiry = result.get("expiry") if result else None
        method = result.get("method") if move is not None else None
        status = (result or {}).get("status") or ("ok" if move is not None else "no_reliable_quote")
        by_status[status] = by_status.get(status, 0) + 1
        if move is None and row.get("implied_move_pct") is not None \
                and eh.expiry_covers(eh._as_date(row.get("implied_move_expiry")), report, session,
                                     max_days=eh.TERM_MAX_FIRST_EXPIRY_DAYS):
            kept += 1          # a miss never erases an earlier live quote that still covers this report
            db.execute_raw_sql(
                "UPDATE public.earnings_calendar SET implied_move_status = %s, implied_move_checked_at = NOW() "
                "WHERE ticker = %s AND earnings_date = %s",
                [status, ticker, report],
            )
            continue
        db.execute_raw_sql(
            "UPDATE public.earnings_calendar SET implied_move_pct = %s, implied_move_expiry = %s, "
            "implied_move_method = %s, implied_move_status = %s, implied_move_checked_at = NOW(), "
            "implied_move_updated_at = NOW() WHERE ticker = %s AND earnings_date = %s",
            [move, expiry, method, status, ticker, report],
        )
        if move is not None:
            stored += 1
            by_method[method or "unknown"] = by_method.get(method or "unknown", 0) + 1
        else:
            cleared += 1
        if pause_seconds:
            time.sleep(pause_seconds)
    out: Dict[str, Any] = {"candidates": len(rows), "done": done, "stored": stored, "no_reliable_quote": cleared,
                           "kept_previous": kept, "failed": failed, "requests": requests_sent() - first_request,
                           "minutes": round((time.monotonic() - started) / 60, 1), "status": by_status,
                           "method": by_method}
    if stopped:
        out["stopped"] = stopped
        logger.info("implied moves stopped early: %s; %d reports left for the next run", stopped, len(rows) - done - failed)
    if aborted:
        out["aborted"] = aborted
    return out


# --------------------------------------------------------------------------- CLI
def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--migrate", action="store_true", help="apply the additive migration first")
    parser.add_argument("--tickers", nargs="*", help="refresh these stocks")
    parser.add_argument("--all", action="store_true", help="refresh every active stock")
    parser.add_argument("--recent-days", type=int, help="refresh stocks whose report date was in the last N days")
    parser.add_argument("--skip-fresh-hours", type=int, default=0, help="with --all: skip stocks refreshed within N hours")
    parser.add_argument("--stalest", type=int, help="refresh the N least recently checked stocks (batch mode)")
    parser.add_argument("--limit", type=int, help="process at most N stocks")
    parser.add_argument("--no-finnhub", action="store_true", help="skip the Finnhub beat/miss cross-check")
    parser.add_argument("--runup-backfill", action="store_true", help="fill the pre-report run-up columns for stocks that lack them (1 Yahoo call per stock)")
    parser.add_argument("--implied", action="store_true", help="refresh options-implied moves for upcoming reports")
    parser.add_argument("--window-days", type=int, default=14)
    parser.add_argument("--implied-min-age-hours", type=float, default=0.0, help="skip implied moves attempted within N hours")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    from dotenv import load_dotenv

    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
    from database.azure_postgres_client import AzurePostgresClient

    db = AzurePostgresClient()
    try:
        if args.migrate:
            apply_migration(db)
            logger.info("migration applied")

        stocks: List[Dict[str, Any]] = []
        if args.runup_backfill:
            todo = runup_backfill_stocks(db, args.limit)
            if args.tickers:
                wanted = {t.strip().upper() for t in args.tickers}
                todo = [s for s in todo if s["symbol"] in wanted]
            logger.info("run-up backfill for %d stocks", len(todo))
            print(backfill_runups(db, todo))
            return 0
        if args.tickers:
            stocks = active_stocks(db, args.tickers)
        elif args.all:
            stocks = active_stocks(db)
            skip = recently_refreshed_ids(db, args.skip_fresh_hours)
            stocks = [s for s in stocks if s["id"] not in skip]
        elif args.stalest:
            stocks = select_stale_stocks(db, args.stalest, args.skip_fresh_hours or 144)
        elif args.recent_days is not None:
            stocks = active_stocks(db, recent_reporters(db, datetime.now(timezone.utc).date(), args.recent_days))
            skip = recently_refreshed_ids(db, args.skip_fresh_hours)
            stocks = [s for s in stocks if s["id"] not in skip]
        if args.limit:
            stocks = stocks[: args.limit]
        if stocks:
            logger.info("refreshing earnings history for %d stocks", len(stocks))
            print(refresh_histories(db, stocks, use_finnhub=not args.no_finnhub))

        if args.implied:
            print(refresh_implied_moves(db, window_days=args.window_days, min_age_hours=args.implied_min_age_hours))
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
