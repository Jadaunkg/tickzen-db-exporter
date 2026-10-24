#!/usr/bin/env python3
"""Yahoo-backed earnings updates: a nightly history safety net and a market-hours implied-move job.

Everything that asks Yahoo for data runs on the updaters (the Azure workers in tickzen-db-exporter, the Mac
runner here) and never on the web app:

* per-stock earnings history is refreshed by ``export_stock_data`` itself (one request pair per stock, gated to once
  per 24 h), so the nightly DB update already covers every stock;
* ``run`` (called by ``cron_update_runner.py`` after its exports, outside market hours) adds a safety-net refresh for
  stocks that reported in the last few days and were not refreshed recently;
* ``run_implied`` refreshes the options-implied move for reports in the next ``window_days`` (two to four Yahoo
  requests per report, ~2.5 on average).  It only works **during the regular session**: after the close Yahoo returns
  bid = ask = openInterest = 0 on every contract (seen on NFLX, META, SPOT and GE at 05:30 New York, 2026-10-16), so
  the nightly runs stored NULL for 224 of 232 attempts.  It therefore runs as its own job (Azure Container Apps job
  ``tickzen-implied-moves`` in tickzen-db-exporter, hourly in the session) and never from the nightly update, inside a
  per-run and a per-day Yahoo request budget.  It only reads option chains (plus, rarely, one price history for the
  single-expiry estimate) and writes ``earnings_calendar.implied_move_*``; it never writes prices.

Both steps are freshness-gated and go through the shared Yahoo limiter (earnings_reports/rate_limit.py) with the
circuit breaker.  The cloud job (earnings_reports/insights_job.py) only calls Finnhub.

    python database/earnings_local_update.py                 # what the nightly runner does (history only)
    python database/earnings_local_update.py --implied-only  # the market-hours job; exits at once outside the session
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)

HISTORY_RECENT_DAYS = 4          # stocks whose report date fell in the last N days
HISTORY_MIN_AGE_HOURS = 12       # ...and that were not refreshed within this many hours
IMPLIED_WINDOW_DAYS = 14         # reports in the next N days get an implied move (the expected-moves page shows 14)
IMPLIED_MIN_AGE_HOURS = 20       # a stored move is re-quoted once per trading day
IMPLIED_RETRY_HOURS = 20         # a miss (thin or wide quotes) is retried once per trading day...
IMPLIED_CLOSE_RETRY_HOURS = 1.9  # ...and every other hourly run in the last days before its report
IMPLIED_RUN_MAX_REQUESTS = 1000  # Yahoo requests per run (~50 min at the workers' 1,200/hour pace)
IMPLIED_RUN_MAX_MINUTES = 50     # runs start hourly: stop cleanly before the next one and the 1 h replica timeout
IMPLIED_DAILY_MAX_REQUESTS = 3500  # all runs of one New York day together
IMPLIED_DB_RETRY_SECONDS = 60    # one retry when the database refuses the first connection
CRON_JOB = "implied-moves"       # cron_runs.job (Control Room)


def run(db, *, history_recent_days: int = HISTORY_RECENT_DAYS, history_min_age_hours: int = HISTORY_MIN_AGE_HOURS,
        today=None) -> Dict[str, Any]:
    """Nightly step: history safety net for recent reporters.  Never raises for a provider problem."""
    from database import export_earnings_history as ex

    today = today or datetime.now(timezone.utc).date()
    try:
        stocks = ex.active_stocks(db, ex.recent_reporters(db, today, history_recent_days))
        fresh = ex.recently_refreshed_ids(db, history_min_age_hours)
        stale = [s for s in stocks if s["id"] not in fresh]
        history = ex.refresh_histories(db, stale) if stale else {"stocks": 0, "note": "all recent reporters fresh"}
    except Exception as exc:  # noqa: BLE001
        logger.warning("earnings history step failed: %s", exc)
        history = {"error": str(exc)}
    return {"history": history}


def yahoo_request_counter() -> Callable[[], int]:
    """Yahoo requests sent by this process: the exporter's HTTP-level pacer when it is installed (it also sees
    yfinance's own cookie requests), else the count kept by ``yahoo_call``."""
    from earnings_reports import earnings_history as eh

    try:
        from database import yahoo_pacer        # only in tickzen-db-exporter
    except ImportError:
        return eh.yahoo_requests_sent
    pacer = getattr(yahoo_pacer, "_pacer", None)
    return (lambda: pacer.total) if pacer is not None else eh.yahoo_requests_sent


def requests_used_today(db, now: datetime) -> int:
    """Yahoo requests the implied-move runs of this New York day already used (from their cron_runs rows)."""
    from database.updater_schedule import MARKET_TZ

    day_start = datetime.combine(now.astimezone(MARKET_TZ).date(), datetime.min.time(), tzinfo=MARKET_TZ)
    rows = db.execute_raw_sql(
        "SELECT COALESCE(SUM((summary->>'requests')::int), 0) AS n FROM public.cron_runs "
        "WHERE job = %s AND started_at >= %s AND summary ? 'requests'", [CRON_JOB, day_start])
    return int(rows[0]["n"]) if rows else 0


def _cron_start(db) -> Optional[int]:
    """A 'running' cron_runs row, so a run that dies half-way stays visible in the Control Room."""
    try:
        rows = db.execute_raw_sql(
            "INSERT INTO public.cron_runs (job, started_at, status) VALUES (%s, NOW(), 'running') RETURNING id", [CRON_JOB])
        return int(rows[0]["id"]) if rows else None
    except Exception as exc:  # noqa: BLE001 - logging a run must never stop the run
        logger.warning("cron_runs start failed: %s", exc)
        return None


def _cron_finish(db, run_id: Optional[int], implied: Dict[str, Any]) -> None:
    if run_id is None:
        return
    status = "failed" if "error" in implied else "partial" if implied.get("aborted") or implied.get("stopped") else "ok"
    error = implied.get("error") or implied.get("aborted")
    try:
        db.execute_raw_sql(
            "UPDATE public.cron_runs SET finished_at = NOW(), status = %s, summary = %s::jsonb, error = %s WHERE id = %s",
            [status, json.dumps({k: v for k, v in implied.items() if k != "error"}, default=str), error, run_id])
    except Exception as exc:  # noqa: BLE001
        logger.warning("cron_runs finish failed: %s", exc)


def _is_connection_error(exc: BaseException) -> bool:
    text = str(exc).lower()
    return "connection refused" in text or "could not connect" in text or "connection failed" in text


def run_implied(db, *, now: Optional[datetime] = None, force: bool = False, window_days: int = IMPLIED_WINDOW_DAYS,
                min_age_hours: float = IMPLIED_MIN_AGE_HOURS, retry_hours: float = IMPLIED_RETRY_HOURS,
                close_retry_hours: float = IMPLIED_CLOSE_RETRY_HOURS, run_max_requests: int = IMPLIED_RUN_MAX_REQUESTS,
                run_max_minutes: float = IMPLIED_RUN_MAX_MINUTES, daily_max_requests: int = IMPLIED_DAILY_MAX_REQUESTS,
                db_retry_seconds: float = IMPLIED_DB_RETRY_SECONDS,
                sleep: Callable[[float], None] = time.sleep) -> Dict[str, Any]:
    """Market-hours step: options-implied moves.  Outside the regular session it returns at once without a single
    Yahoo request (``force`` skips that clock check; the chain's own marketState still stops a closed market).

    Each run uses at most ``run_max_requests`` Yahoo requests and ``run_max_minutes``, and all runs of one New York
    day at most ``daily_max_requests`` together; the run is logged in ``cron_runs`` (job ``implied-moves``) with its
    request count.  A database that refuses the first connection is retried once after ``db_retry_seconds``."""
    from database import export_earnings_history as ex
    from database.updater_schedule import MARKET_TZ, in_options_session

    now = now or datetime.now(timezone.utc)
    if not force and not in_options_session(now):
        return {"implied": {"skipped": f"outside the options session ({now.astimezone(MARKET_TZ):%a %H:%M} New York)"}}
    counter = yahoo_request_counter()
    for attempt in (1, 2):
        try:
            used = requests_used_today(db, now)
            break
        except Exception as exc:  # noqa: BLE001
            if attempt == 2 or not _is_connection_error(exc):
                logger.warning("implied move step failed before it started: %s", exc)
                return {"implied": {"error": str(exc)}}
            logger.warning("database refused the connection (%s); retrying in %ss", exc, db_retry_seconds)
            sleep(db_retry_seconds)
    budget = min(run_max_requests, daily_max_requests - used)
    if budget <= 0:
        return {"implied": {"skipped": f"daily Yahoo budget used ({used} of {daily_max_requests} requests)"}}
    run_id = _cron_start(db)
    try:
        implied = ex.refresh_implied_moves(db, today=now.astimezone(MARKET_TZ).date(), window_days=window_days,
                                           min_age_hours=min_age_hours, retry_hours=retry_hours,
                                           close_retry_hours=close_retry_hours, max_requests=budget,
                                           max_minutes=run_max_minutes, requests_sent=counter)
    except Exception as exc:  # noqa: BLE001
        logger.warning("implied move step failed: %s", exc)
        implied = {"error": str(exc)}
    _cron_finish(db, run_id, implied)
    return {"implied": implied}


def run_after_batch() -> Optional[Dict[str, Any]]:
    """Entry point for cron_update_runner: opens its own connection and swallows every error (the stock export
    that just ran must never be failed by an earnings problem)."""
    try:
        from database.azure_postgres_client import AzurePostgresClient

        db = AzurePostgresClient()
        try:
            result = run(db)
        finally:
            db.close()
        logger.info("earnings local update: %s", result)
        return result
    except Exception as exc:  # noqa: BLE001
        logger.warning("earnings local update skipped: %s", exc)
        return None


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--implied-only", action="store_true",
                        help="the market-hours job: only refresh options-implied moves")
    parser.add_argument("--force", action="store_true", help="with --implied-only: skip the session clock check")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.implied_only and not args.force:
        from database.updater_schedule import in_options_session

        if not in_options_session(datetime.now(timezone.utc)):     # decide before opening a DB connection
            print(run_implied(None))
            return 0
    from dotenv import load_dotenv

    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
    from database.azure_postgres_client import AzurePostgresClient

    db = AzurePostgresClient()
    try:
        print(run_implied(db, force=args.force) if args.implied_only else run(db))
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
