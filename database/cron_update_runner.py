#!/usr/bin/env python3
"""
Automated Cron Update Runner (cloud edition)
============================================

Runs the same per-stock export as tickzen-github's ``database/cron_update_runner.py`` (the macOS launchd
job), as one of several workers on Azure. The data written per stock is identical; the shared rules live in
modules synced from tickzen-github:

* ``database/updater_schedule.py`` - **when** (only while the US market is closed: trading days 16:30-09:00
  New York time, weekends and NYSE holidays all day) and **what is stale** (no successful sync since the
  latest trading-day 16:30 ET), plus the split of the stock list into ``WORKER_COUNT`` equal parts.
* ``database/updater_locks.py`` - per-stock advisory locks, one run per worker, one earnings step at a time.
* ``database/updater_run_log.py`` - the run log shown on the admin dashboard (/admin/db-updater).

Worker ``WORKER_INDEX`` exports its own part first, then helps with stale stocks of the other parts, so a
worker whose IP gets blocked does not leave its part stale. Stocks are claimed one at a time, right before each
export, so an idle worker can always take over what another has not reached yet.

**Hourly batches.** The job starts every hour (at :30). Each run exports at most ``STOCKS_PER_RUN`` stocks
(default 70) and stops after ``RUN_TIME_BUDGET_MINUTES`` (default 55), then waits for the next hour. Inside a
run every Yahoo request is spaced evenly to ``YAHOO_REQUESTS_PER_HOUR`` (``database/yahoo_pacer.py``), so
neither the hour nor any single minute sees a burst. A real HTTP 429 pauses the worker for a minute and halves
its pace until the run ends; ``STOP_AFTER_429`` of them end the run early.

    python database/cron_update_runner.py --preview      # show this worker's queue, export nothing
    python database/cron_update_runner.py                # what the scheduled job runs
    python database/cron_update_runner.py --limit 5 --force
"""

import os
import sys
import time
import logging
import requests
from datetime import datetime, timezone
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from database.updater_schedule import (  # noqa: E402
    MARKET_TZ, assign_parts, in_export_window, is_current, latest_session_cutoff, parse_hhmm, sync_instant,
    DEFAULT_START, DEFAULT_STOP,
)

# Set up logging
log_dir = Path(__file__).parent / 'logs'
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_dir / f'cron_update_runner_{datetime.now().strftime("%Y%m%d")}.log')
    ]
)
logger = logging.getLogger(__name__)

NEW_STOCKS_LOCK_INDEX = 1000   # worker lock of the new-stock loader (nightly workers use 0..WORKER_COUNT-1)
RATE_LIMIT_MARKERS = ('rate_limited', 'too many requests', 'rate limit', '429', 'invalid crumb', 'crumb', 'unauthorized')


def env_int(name: str, default: int) -> int:
    value = os.getenv(name, "").strip()
    return int(value) if value else default


def internet_and_ip(timeout=8):
    """(online, egress IP) from Cloudflare's trace endpoint.

    Deliberately not a Yahoo URL: Yahoo answers a plain ``requests`` call with HTTP 429, which the pacer took as
    throttling and halved the worker's speed for the whole night (2026-10-15/16). The egress IP is what Yahoo
    throttles on, so it is logged and shown on the dashboard.
    """
    try:
        response = requests.get("https://www.cloudflare.com/cdn-cgi/trace", timeout=timeout)
        fields = dict(line.split("=", 1) for line in response.text.splitlines() if "=" in line)
        return response.ok, fields.get("ip")
    except requests.RequestException as exc:
        logger.warning(f"HTTPS connectivity check failed: {exc}")
        return False, None


def get_exporter_and_client():
    """Load the Azure exporter and the DB client it owns."""
    if not (os.getenv("AZURE_POSTGRES_URL") or os.getenv("DATABASE_URL") or os.getenv("AZURE_POSTGRES_HOST")):
        logger.error("Azure Postgres is not configured (set AZURE_POSTGRES_URL or AZURE_POSTGRES_HOST/USER/PASSWORD/DB).")
        return None, None
    try:
        from database.export_to_azure_postgres import AzurePostgresDataExporter
        exporter = AzurePostgresDataExporter()
        # Use the exact client owned by the exporter. This avoids creating
        # a second Azure connection for the scheduler's stale-stock query.
        return exporter, exporter.db
    except Exception as e:
        logger.error(f"Failed to load AzurePostgresDataExporter: {e}")
        return None, None


def load_tickers_from_db(db) -> list:
    """Active symbols in monitored_tickers, oldest additions first (deduplicated, upper-case)."""
    rows = db.table('monitored_tickers').select('symbol', 'created_at').eq('is_active', True).order('created_at').execute().data
    seen = set()
    return [s for s in (str(r.get('symbol') or '').strip().upper() for r in rows) if s and not (s in seen or seen.add(s))]


def build_queue(db, tickers: list, cutoff: datetime, worker_index: int, worker_count: int, new_only: bool = False):
    """Returns (own, others, counts): stale stocks of this worker's part and of the other parts.

    Each list: stocks not yet in ``stocks`` first, then the rest by oldest successful sync. With ``new_only`` (the
    new-stock loader) every never-synced stock is "own" and nothing else is queued.
    """
    db_map = {s['symbol'].upper(): s for s in
              db.table('stocks').select('symbol', 'last_sync_date', 'last_sync_status').execute().data}
    oldest = datetime.min.replace(tzinfo=timezone.utc)

    def order(symbols):
        new = [s for s in symbols if s not in db_map]
        old = sorted((s for s in symbols if s in db_map),
                     key=lambda s: sync_instant(db_map[s].get('last_sync_date')) or oldest)
        return new + old

    home = assign_parts(tickers, worker_count)
    if new_only:
        # Never synced: not in ``stocks`` yet, or only an earnings-page row without any export.
        stale = [s for s in tickers if s not in db_map or not db_map[s].get('last_sync_date')]
        home = {s: worker_index for s in tickers}
    else:
        stale = [s for s in tickers if s not in db_map
                 or not is_current(db_map[s].get('last_sync_date'), db_map[s].get('last_sync_status'), cutoff)]
    own = [s for s in stale if home[s] == worker_index]
    others = [s for s in stale if home[s] != worker_index]
    counts = {
        'total': len(tickers),
        'own_total': sum(1 for s in tickers if home[s] == worker_index),
        'current': len(tickers) - len(stale),
        'new': sum(1 for s in stale if s not in db_map),
        'own_stale': len(own),
        'others_stale': len(others),
    }
    return order(own), order(others), counts


def recently_failed(db, hours: int) -> set:
    """Symbols whose latest attempt in the last ``hours`` failed (the new-stock loader does not retry them yet)."""
    try:
        rows = db.execute_raw_sql(
            "SELECT DISTINCT ON (symbol) symbol, status FROM updater_run_stocks "
            "WHERE finished_at > now() - make_interval(hours => %s) ORDER BY symbol, finished_at DESC", [hours])
        return {r['symbol'].upper() for r in rows if r['status'] not in ('success', 'partial')}
    except Exception as exc:  # the run log is optional
        logger.warning(f"Could not read recent failures: {exc}")
        return set()


def is_rate_limited(errors) -> bool:
    return any(marker in str(err).lower() for err in (errors or []) for marker in RATE_LIMIT_MARKERS)


def run_earnings_step(locks) -> None:
    """Recent-reporter history refresh. Freshness-gated, never fatal, one worker at a time. (Implied moves need live
    option quotes, so they run in the market-hours job database/implied_moves_job.py, not here.)"""
    if not locks.earnings():
        logger.info("Earnings step is running on another worker - skipping.")
        return
    try:
        from database.earnings_local_update import run_after_batch
        run_after_batch()
    except Exception as e:
        logger.warning(f"Earnings local update skipped: {e}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Cron update runner for TickZen stock data.")
    parser.add_argument("--preview", action="store_true", help="Show this worker's queue without exporting anything.")
    parser.add_argument("--limit", type=int, default=env_int("STOCKS_PER_RUN", 70),
                        help="Max stocks this run exports (default 70, env STOCKS_PER_RUN; 0 = no cap).")
    parser.add_argument("--max-minutes", type=int, default=env_int("RUN_TIME_BUDGET_MINUTES", 55),
                        help="Stop starting new stocks after this many minutes (default 55; 0 = until the window closes).")
    parser.add_argument("--force", action="store_true", help="Run even while the US market is open.")
    args = parser.parse_args()

    worker_index = env_int("WORKER_INDEX", 0)
    worker_count = max(1, env_int("WORKER_COUNT", 1))
    # New-stock loader: a separate frequent job that gives newly added stocks their first full load at any hour.
    new_only = os.getenv("NEW_STOCKS_ONLY", "").strip().lower() in {"1", "true", "yes", "on"}
    retry_failed_after_hours = env_int("NEW_STOCKS_RETRY_HOURS", 6)
    help_others = os.getenv("HELP_OTHER_WORKERS", "true").strip().lower() in {"1", "true", "yes", "on"}
    max_rate_limited = env_int("MAX_CONSECUTIVE_RATE_LIMITED", 3)
    stop_after_429 = env_int("STOP_AFTER_429", 3)
    yahoo_per_hour = env_int("YAHOO_REQUESTS_PER_HOUR", 1200)
    start_et = parse_hhmm(os.getenv("EXPORT_START_ET"), DEFAULT_START)
    stop_et = parse_hhmm(os.getenv("EXPORT_STOP_ET"), DEFAULT_STOP)
    if not 0 <= worker_index < worker_count:
        logger.error(f"WORKER_INDEX={worker_index} must be between 0 and WORKER_COUNT-1 ({worker_count - 1}).")
        sys.exit(1)

    from database.updater_run_log import RunLog
    default_name = "azure-new-stocks" if new_only else f"azure-{worker_index}"
    log_meta = dict(worker_name=os.getenv("WORKER_NAME") or default_name, worker_index=worker_index,
                    worker_count=worker_count, region=os.getenv("WORKER_REGION"), image_tag=os.getenv("IMAGE_TAG"),
                    pace_per_hour=yahoo_per_hour)

    def window_open() -> bool:
        # A first load runs at any hour: a bar stored during the session is replaced by that night's update
        # (the price export re-reads the last 7 days), because the stock is stale again after the close.
        return args.force or new_only or in_export_window(datetime.now(MARKET_TZ), start_et, stop_et)

    now_et = datetime.now(MARKET_TZ)
    if not window_open():
        logger.info(f"Execution skipped: US market hours ({now_et.strftime('%a %H:%M')} New York). "
                    f"Exports run {start_et:%H:%M}-{stop_et:%H:%M} ET on trading days and all day otherwise. "
                    "Use --force to override.")
        if not args.preview:
            try:
                from database.azure_postgres_client import AzurePostgresClient
                quick_db = AzurePostgresClient(max_retries=1, timeout=10)
                RunLog(quick_db, **log_meta).seen("market open - waiting for the close")
                quick_db.close()
            except Exception as e:
                logger.warning(f"Could not record worker heartbeat: {e}")
        sys.exit(0)
    cutoff = latest_session_cutoff(now_et, start_et)
    logger.info(f"Worker {worker_index + 1}/{worker_count}. Stocks are current if synced after "
                f"{cutoff.strftime('%a %Y-%m-%d %H:%M %Z')}.")

    # 1. Connectivity Check (not against Yahoo, see internet_and_ip)
    online, egress_ip = internet_and_ip()
    if not online:
        logger.warning("❌ No active internet connection detected. Skipping execution.")
        sys.exit(0)
    logger.info(f"✓ Internet connection verified. Egress public IP: {egress_ip}")

    from database import yahoo_pacer
    pacer = yahoo_pacer.install(yahoo_per_hour)
    logger.info(f"Yahoo requests paced to {yahoo_per_hour}/hour (one every {3600 / yahoo_per_hour:.1f}s); "
                f"this run exports at most {args.limit or 'all'} stocks"
                + (f" within {args.max_minutes} minutes." if args.max_minutes else "."))

    # 2. Exporter and DB Setup
    exporter, db = get_exporter_and_client()
    if not exporter or not db:
        logger.error("❌ Failed to set up database connection or exporter. Exiting.")
        sys.exit(1)
    logger.info("✓ Using AZURE database configuration.")
    run_log = RunLog(db, egress_ip=egress_ip, **log_meta)

    # 3. Load Tickers from the database
    try:
        all_tickers = load_tickers_from_db(db)
    except Exception as e:
        logger.error(f"❌ Failed to read monitored_tickers: {e}")
        run_log.seen(f"error: could not read monitored_tickers ({e})")
        sys.exit(1)
    if not all_tickers:
        logger.error("❌ No active tickers in monitored_tickers. Exiting.")
        run_log.seen("no active tickers in monitored_tickers")
        sys.exit(1)

    # 4. Initial queue
    try:
        own, others, counts = build_queue(db, all_tickers, cutoff, worker_index, worker_count, new_only=new_only)
    except Exception as e:
        logger.error(f"❌ Failed to fetch stocks from database: {e}")
        run_log.seen(f"error: could not read stocks ({e})")
        sys.exit(1)
    skipped: set = set()
    if new_only:
        skipped = recently_failed(db, retry_failed_after_hours) & set(own)
        own = [s for s in own if s not in skipped]
        if skipped:
            logger.info(f"Not retrying yet (failed in the last {retry_failed_after_hours} h): {', '.join(sorted(skipped))}")
        if not own:
            logger.info("✓ No new stocks waiting for their first load.")
            if not args.preview:
                run_log.seen("no new stocks to load" + (f" ({len(skipped)} recently failed)" if skipped else ""))
            sys.exit(0)
    logger.info("Queue Status:")
    logger.info(f"  - Monitored stocks: {counts['total']} (this worker's part: {counts['own_total']})")
    logger.info(f"  - Current since the last close: {counts['current']}")
    logger.info(f"  - New Tickers (Not in DB): {counts['new']}")
    logger.info(f"  - Stale in this worker's part: {counts['own_stale']}")
    logger.info(f"  - Stale in other parts: {counts['others_stale']}" + ("" if help_others else " (helping disabled)"))

    if args.preview:
        logger.info(f"Own part, in order: {', '.join(own) or '-'}")
        if help_others:
            logger.info(f"Then helping with: {', '.join(others) or '-'}")
        logger.info("Preview mode active. Exiting without execution.")
        sys.exit(0)

    if not own and not (help_others and others):
        logger.info("✓ All stocks are up to date since the last close. No execution needed.")
        run_log.seen("all stocks current")
        sys.exit(0)

    from database.updater_locks import UpdaterLocks
    locks = UpdaterLocks(db)
    lock_index = NEW_STOCKS_LOCK_INDEX if new_only else worker_index
    if not locks.worker(lock_index):
        logger.info(f"{'New-stock loader' if new_only else f'Worker {worker_index}'} is already running elsewhere. Exiting.")
        sys.exit(0)

    # 5. Export loop: plan the next stocks, claim each one right before exporting it.
    run_log.start_run(session_cutoff=cutoff)
    start_time = datetime.now()
    deadline = time.monotonic() + args.max_minutes * 60 if args.max_minutes > 0 else None
    attempted = set()   # never retry a stock twice in one run (a partial stock would loop forever)
    success_count = fail_count = partial_count = helped = 0
    consecutive_rate_limited = 0
    earnings_done = new_only   # the earnings step belongs to the nightly workers
    plan: list = []
    own_set = set(own)
    first_plan = True
    stop_reason = None
    pacer.since_mark()

    from database.export_to_azure_postgres import prepare_bulk_prices

    def check_stop():
        if not window_open():
            return 'window_closed'
        if deadline is not None and time.monotonic() >= deadline:
            return 'time_budget'
        if args.limit > 0 and success_count + partial_count + fail_count >= args.limit:
            return 'batch_done'
        if consecutive_rate_limited >= max_rate_limited > 0 or pacer.throttled >= stop_after_429 > 0:
            return 'rate_limited'
        return None

    while True:
        stop_reason = check_stop()
        if stop_reason:
            break
        if not plan:
            if not first_plan:
                own, others, _ = build_queue(db, all_tickers, cutoff, worker_index, worker_count, new_only=new_only)
                if new_only:
                    own = [s for s in own if s not in skipped]
                own_set = set(own)
            first_plan = False
            own_left = [s for s in own if s not in attempted]
            if not own_left and not earnings_done:
                # This worker's part is done: run the earnings step before helping other parts.
                run_earnings_step(locks)
                earnings_done = True
                stop_reason = check_stop()
                if stop_reason:
                    break
            # Own part first; other parts only once it is done (the earnings step runs in between).
            candidates = own_left or ([s for s in others if s not in attempted] if help_others else [])
            if not candidates:
                stop_reason = 'completed'
                break
            room = args.limit - (success_count + partial_count + fail_count) if args.limit > 0 else len(candidates)
            plan = candidates[:room]
            # One bulk price download for the planned stocks of this worker's own part (others' owners fetch theirs).
            prefetch = [s for s in plan if s in own_set]
            if prefetch:
                try:
                    prepare_bulk_prices(prefetch)
                    logger.info(f"Bulk price download for {len(prefetch)} stocks: {pacer.since_mark()} Yahoo requests")
                except Exception as e:
                    logger.warning(f"Failed to prepare bulk prices: {e}")

        ticker = plan.pop(0)
        attempted.add(ticker)
        if not locks.claim(ticker, cutoff):
            continue   # being exported by another worker, or already current
        is_helping = ticker not in own_set
        logger.info(f"[{success_count + partial_count + fail_count + 1}] Processing {ticker}"
                    + (" (helping another part)" if is_helping else "") + "...")
        stock_started = datetime.now(timezone.utc)
        status, errors = 'failed', []
        try:
            result = exporter.export_stock_data(ticker)
            status = result.get('status', 'failed')
            errors = result.get('errors') or []
            if status == 'success':
                success_count += 1
            elif status == 'partial':
                partial_count += 1
            else:
                fail_count += 1
                logger.error(f"Failed to export {ticker}: {errors}")
        except Exception as e:
            fail_count += 1
            errors = [str(e)]
            logger.error(f"Exception during export of {ticker}: {e}", exc_info=True)
        finally:
            locks.release(ticker)
        stock_requests = pacer.since_mark()
        helped += is_helping
        run_log.stock(ticker, status, stock_started, own_part=not is_helping,
                      yahoo_requests=stock_requests, errors=errors,
                      run_yahoo_requests=pacer.total, run_yahoo_429=pacer.throttled,
                      run_pace_per_hour=round(pacer.per_hour))
        logger.info(f"  {ticker}: {stock_requests} Yahoo requests "
                    f"(run total {pacer.total}, 429s {pacer.throttled}, pace {pacer.per_hour:.0f}/hour)")
        consecutive_rate_limited = consecutive_rate_limited + 1 if is_rate_limited(errors) else 0

    if stop_reason == 'rate_limited':
        logger.warning("Stopping run: Yahoo rate limits. Unfinished stocks stay queued for the next hourly run "
                       "and for the other workers.")
    elif stop_reason == 'completed' and not earnings_done and window_open():
        run_earnings_step(locks)

    locks.close()
    duration = (datetime.now() - start_time).total_seconds()
    processed = success_count + partial_count + fail_count
    run_log.finish(stop_reason or 'completed', yahoo_requests=pacer.total, yahoo_429=pacer.throttled,
                   pace_per_hour=round(pacer.per_hour))

    logger.info("\n" + "="*80)
    logger.info(f"BATCH EXPORT RUN COMPLETE ({stop_reason})")
    logger.info("="*80)
    logger.info(f"Duration: {duration/60:.1f} minutes")
    logger.info(f"Processed: {processed} ({helped} helped from other parts)")
    logger.info(f"Success: {success_count}")
    logger.info(f"Partial: {partial_count}")
    logger.info(f"Failed: {fail_count}")
    logger.info(f"Yahoo requests: {pacer.total} ({pacer.total / processed:.1f} per stock), 429 responses: {pacer.throttled}"
                if processed else f"Yahoo requests: {pacer.total}, 429 responses: {pacer.throttled}")
    logger.info("="*80 + "\n")

    sys.exit(0 if fail_count == 0 and stop_reason != 'rate_limited' else 1)


if __name__ == "__main__":
    main()
