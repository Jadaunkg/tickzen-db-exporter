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
    python database/export_earnings_history.py --implied --window-days 10    # options-implied moves (market hours only)
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
MIGRATION_PATHS = (MIGRATION_PATH, RUNUP_MIGRATION_PATH)
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


def record_status(db, stock_id: int, rows: int, note: Optional[str] = None) -> None:
    db.execute_raw_sql(
        "INSERT INTO public.earnings_history_status (stock_id, checked_at, rows, note) VALUES (%s, NOW(), %s, %s) "
        "ON CONFLICT (stock_id) DO UPDATE SET checked_at = NOW(), rows = EXCLUDED.rows, note = EXCLUDED.note",
        [stock_id, rows, note],
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
        eh.flag_source_conflicts(records, fetch_surprises(symbol))
    conflicts = sum(1 for r in records if r["eps_source_conflict"])

    now = datetime.now(timezone.utc)
    for record in records:
        record["updated_at"] = now
    db.table("earnings_history").upsert(records, on_conflict="stock_id,earnings_date").execute()
    record_status(db, stock_id, len(records))
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
def refresh_implied_moves(
    db,
    *,
    today: Optional[date] = None,
    window_days: int = 10,
    fetch: Callable[[str, date, Optional[str]], Optional[Dict[str, Any]]] = eh.fetch_implied_move,
    pause_seconds: float = 0.0,
    max_consecutive_failures: int = MAX_CONSECUTIVE_FAILURES,
    min_age_hours: float = 0.0,
    retry_hours: Optional[float] = None,
) -> Dict[str, Any]:
    """Options-implied move for tracked stocks reporting in the next ``window_days`` (paced by the shared
    Yahoo limiter inside ``fetch``; same circuit breaker as ``refresh_histories``).

    Run it only during the regular session (``earnings_local_update.run_implied``): Yahoo zeroes option quotes
    outside it.  A fetch that raises ``OptionsMarketClosed`` stops the run without writing anything.

    ``min_age_hours`` > 0 skips rows whose stored move is that recent; ``retry_hours`` (default: the same)
    skips rows whose last attempt found no reliable quote that recently, so an hourly job does not re-request
    Yahoo for the same reports.  Reports dated today are skipped unless they come after the close: a BMO,
    intraday or unknown-time report may already be out, and a straddle quoted after it is no expectation.

    A miss (no reliable quote now) never overwrites a stored move whose expiry still covers the reaction: the row
    keeps that earlier live quote and its own timestamp, and is retried on the next run."""
    today = today or datetime.now(timezone.utc).date()
    rows = db.execute_raw_sql(
        "SELECT c.ticker, c.earnings_date, c.time_of_day, c.implied_move_pct, c.implied_move_expiry, "
        "c.implied_move_updated_at "
        "FROM public.earnings_calendar c "
        "JOIN public.stocks s ON COALESCE(s.symbol, s.ticker) = c.ticker AND s.is_active = true "
        "WHERE c.earnings_date BETWEEN %s AND %s ORDER BY c.earnings_date, c.ticker",
        [today, today + timedelta(days=window_days)],
    )
    before = len(rows)
    rows = [r for r in rows if r["earnings_date"] != today or eh.session_from_calendar_hour(r.get("time_of_day")) == eh.AMC]
    retry_hours = min_age_hours if retry_hours is None else retry_hours
    if min_age_hours > 0 or retry_hours > 0:
        now = datetime.now(timezone.utc)

        def recent(r: Dict[str, Any]) -> bool:
            hours = min_age_hours if r.get("implied_move_pct") is not None else retry_hours
            stamp = r.get("implied_move_updated_at")
            return bool(stamp and hours > 0 and _aware(stamp) > now - timedelta(hours=hours))

        rows = [r for r in rows if not recent(r)]
    logger.info("implied moves: %d of %d reports need a refresh (min age %sh, retry %sh)",
                len(rows), before, min_age_hours, retry_hours)
    stored = cleared = kept = failed = streak = 0
    aborted: Optional[str] = None
    for row in rows:
        ticker, report = str(row["ticker"]), row["earnings_date"]
        session = eh.session_from_calendar_hour(row.get("time_of_day"))
        try:
            result = fetch(ticker, report, session)
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
        move = result["implied_move_pct"] if result else None
        expiry = result["expiry"] if result else None
        if move is None and row.get("implied_move_pct") is not None \
                and eh.expiry_covers(eh._as_date(row.get("implied_move_expiry")), report, session):
            kept += 1          # a miss never erases an earlier live quote that still covers this report
            continue
        db.execute_raw_sql(
            "UPDATE public.earnings_calendar SET implied_move_pct = %s, implied_move_expiry = %s, "
            "implied_move_updated_at = NOW() WHERE ticker = %s AND earnings_date = %s",
            [move, expiry, ticker, report],
        )
        stored += 1 if move is not None else 0
        cleared += 1 if move is None else 0
        if pause_seconds:
            time.sleep(pause_seconds)
    out = {"candidates": len(rows), "stored": stored, "no_reliable_quote": cleared, "kept_previous": kept,
           "failed": failed}
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
    parser.add_argument("--window-days", type=int, default=10)
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
