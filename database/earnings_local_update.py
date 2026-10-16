#!/usr/bin/env python3
"""Yahoo-backed earnings updates: a nightly history safety net and a market-hours implied-move job.

Everything that asks Yahoo for data runs on the updaters (the Azure workers in tickzen-db-exporter, the Mac
runner here) and never on the web app:

* per-stock earnings history is refreshed by ``export_stock_data`` itself (one request pair per stock, gated to once
  per 24 h), so the nightly DB update already covers every stock;
* ``run`` (called by ``cron_update_runner.py`` after its exports, outside market hours) adds a safety-net refresh for
  stocks that reported in the last few days and were not refreshed recently;
* ``run_implied`` refreshes the options-implied move for reports in the next ``window_days`` (two Yahoo requests per
  report).  It only works **during the regular session**: after the close Yahoo returns bid = ask = openInterest = 0
  on every contract (seen on NFLX, META, SPOT and GE at 05:30 New York, 2026-10-16), so the nightly runs stored
  NULL for 224 of 232 attempts.  It therefore runs as its own job (Azure Container Apps job ``tickzen-implied-moves``
  in tickzen-db-exporter, a few times per trading day) and never from the nightly update.  It only reads option
  chains and writes ``earnings_calendar.implied_move_*``; it never writes prices.

Both steps are freshness-gated and go through the shared Yahoo limiter (earnings_reports/rate_limit.py) with the
circuit breaker.  The cloud job (earnings_reports/insights_job.py) only calls Finnhub.

    python database/earnings_local_update.py                 # what the nightly runner does (history only)
    python database/earnings_local_update.py --implied-only  # the market-hours job; exits at once outside the session
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)

HISTORY_RECENT_DAYS = 4          # stocks whose report date fell in the last N days
HISTORY_MIN_AGE_HOURS = 12       # ...and that were not refreshed within this many hours
IMPLIED_WINDOW_DAYS = 10         # reports in the next N days get an implied move
IMPLIED_MIN_AGE_HOURS = 20       # a stored move is re-quoted once per trading day
IMPLIED_RETRY_HOURS = 1.5        # a report with no reliable quote is retried by the next run (runs are 2 h apart)


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


def run_implied(db, *, now: Optional[datetime] = None, force: bool = False, window_days: int = IMPLIED_WINDOW_DAYS,
                min_age_hours: float = IMPLIED_MIN_AGE_HOURS, retry_hours: float = IMPLIED_RETRY_HOURS) -> Dict[str, Any]:
    """Market-hours step: options-implied moves.  Outside the regular session it returns at once without a single
    Yahoo request (``force`` skips that clock check; the chain's own marketState still stops a closed market)."""
    from database import export_earnings_history as ex
    from database.updater_schedule import MARKET_TZ, in_options_session

    now = now or datetime.now(timezone.utc)
    if not force and not in_options_session(now):
        return {"implied": {"skipped": f"outside the options session ({now.astimezone(MARKET_TZ):%a %H:%M} New York)"}}
    try:
        implied = ex.refresh_implied_moves(db, today=now.astimezone(MARKET_TZ).date(), window_days=window_days,
                                           min_age_hours=min_age_hours, retry_hours=retry_hours)
    except Exception as exc:  # noqa: BLE001
        logger.warning("implied move step failed: %s", exc)
        implied = {"error": str(exc)}
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
