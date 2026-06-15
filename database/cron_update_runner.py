#!/usr/bin/env python3
"""
Automated Cron Update Runner & Web Service
============================================

Daemon batch runner designed for Render free-tier Web Services.
Features:
1. Starts a lightweight background HTTP server binding to PORT (default 10000) to pass Render's port scan.
2. Serves a live, real-time status dashboard ('dashboard.html') updated after each stock sync.
3. Automatically runs in a persistent loop, sleeping during active market hours to avoid rate conflicts.
"""

import os
import sys
import json
import logging
import socket
import threading
import time
from datetime import datetime, date, timezone, timedelta
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

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

def check_internet(host="8.8.8.8", port=53, timeout=3) -> bool:
    """Check if the system has an active internet connection using a DNS socket test."""
    try:
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
        return True
    except socket.error:
        return False

def get_exporter_and_client():
    """Dynamically load the appropriate database client and exporter."""
    from analysis_scripts.peer_comparison import get_db_client
    db_client = get_db_client()
    
    if db_client is None:
        logger.error("Could not initialize database client.")
        return None, None, None

    azure_url = os.getenv("AZURE_POSTGRES_URL") or os.getenv("DATABASE_URL")
    if azure_url or os.getenv("AZURE_POSTGRES_HOST"):
        try:
            from database.export_to_azure_postgres import AzurePostgresDataExporter
            exporter = AzurePostgresDataExporter()
            return exporter, db_client, "azure"
        except Exception as e:
            logger.error(f"Failed to load AzurePostgresDataExporter: {e}")
            
    try:
        from database.export_to_supabase import SupabaseDataExporter
        exporter = SupabaseDataExporter()
        return exporter, db_client, "supabase"
    except Exception as e:
        logger.error(f"Failed to load SupabaseDataExporter: {e}")

    return None, None, None

def load_tickers_list() -> list:
    """Load tickers from stock_tickers_list.json."""
    json_path = Path(__file__).parent / "stock_tickers_list.json"
    try:
        with open(json_path, "r") as f:
            data = json.load(f)
            tickers = data.get("tickers", [])
            seen = set()
            deduped = [x.upper() for x in tickers if not (x.upper() in seen or seen.add(x.upper()))]
            return deduped
    except Exception as e:
        logger.error(f"Failed to load stock_tickers_list.json: {e}")
        return []

def load_tickers_from_db(db) -> list:
    """Fetch active tickers from monitored_tickers table."""
    try:
        res = db.table('monitored_tickers').select('symbol').eq('is_active', True).execute()
        if hasattr(res, 'data') and res.data:
            tickers = [r['symbol'].upper() for r in res.data if r.get('symbol')]
            tickers.sort()
            return tickers
        return []
    except Exception as e:
        logger.warning(f"Could not load tickers from database 'monitored_tickers' table (using fallback): {e}")
        return []

def update_dashboard():
    """Trigger dashboard HTML regeneration."""
    try:
        from database.generate_dashboard import main as run_dashboard_gen
        run_dashboard_gen()
        logger.info("Dashboard HTML page updated successfully.")
    except Exception as e:
        logger.warning(f"Failed to regenerate dashboard HTML: {e}")

def start_dashboard_web_server():
    """Start a background HTTP server serving dashboard.html to pass Render's port checks."""
    import http.server
    import socketserver
    
    port = int(os.getenv("PORT", 10000))
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    class DashboardHandler(http.server.SimpleHTTPRequestHandler):
        def do_GET(self):
            if self.path == '/' or self.path == '/index.html':
                dashboard_path = os.path.join(project_root, 'dashboard.html')
                if os.path.exists(dashboard_path):
                    self.send_response(200)
                    self.send_header('Content-type', 'text/html')
                    self.end_headers()
                    with open(dashboard_path, 'rb') as f:
                        self.wfile.write(f.read())
                    return
            return super().do_GET()
            
        def log_message(self, format, *args):
            # Suppress HTTP access logging to keep application logs clean
            pass

    socketserver.TCPServer.allow_reuse_address = True
    try:
        server = socketserver.TCPServer(('0.0.0.0', port), DashboardHandler)
        logger.info(f"✓ Dashboard Web Server started successfully on port {port}")
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        return server
    except Exception as e:
        logger.error(f"❌ Failed to start dashboard web server on port {port}: {e}")
        return None

def handle_ticker_sync_result(db, ticker, result):
    """
    Update ticker's consecutive failure count in monitored_tickers.
    If it hits 3 consecutive failures, delete the ticker.
    """
    ticker_upper = ticker.upper().strip()
    status = result.get('status', 'failed')
    errors = result.get('errors', [])
    
    # Check if this was a temporary rate limit or connection issue
    is_temporary = any(
        keyword in str(e).lower()
        for e in errors
        for keyword in ['rate_limited', 'too many requests', 'rate limit', 'invalid crumb', 'unauthorized', 'cooldown']
    )
    
    # If the sync succeeded or was partial without rate limit errors, we reset failures to 0
    is_success = (status in ('success', 'partial')) and not is_temporary
    
    try:
        # 1. Fetch current consecutive_failures count
        res = db.table('monitored_tickers').select('consecutive_failures').eq('symbol', ticker_upper).execute()
        if not res.data:
            return # Ticker not in monitored_tickers
            
        current_failures = res.data[0].get('consecutive_failures') or 0
        
        if is_success:
            if current_failures > 0:
                db.table('monitored_tickers').update({'consecutive_failures': 0}).eq('symbol', ticker_upper).execute()
                logger.info(f"✓ Reset consecutive failures for {ticker_upper} to 0")
        else:
            # It's a hard data availability failure
            new_failures = current_failures + 1
            logger.warning(f"⚠ Ticker {ticker_upper} failed data sync (Hard failure {new_failures}/3)")
            
            if new_failures >= 3:
                # Delete the ticker from monitored_tickers
                db.table('monitored_tickers').delete().eq('symbol', ticker_upper).execute()
                logger.error(f"❌ Ticker {ticker_upper} deleted from monitored_tickers after 3 consecutive hard failures (likely delisted or invalid).")
            else:
                db.table('monitored_tickers').update({'consecutive_failures': new_failures}).eq('symbol', ticker_upper).execute()
                
    except Exception as db_err:
        logger.warning(f"Could not update monitored_tickers failure count for {ticker_upper}: {db_err}")

def run_sync_cycle(exporter, db, db_type, limit_val, offset_val, instance_index, total_instances, sub_batch_size, sleep_interval_seconds, preview_mode):
    """Executes a single cycle of checking and updating tickers in sub-batches."""
    logger.info("Beginning database sync cycle...")

    # Ensure monitored_tickers has consecutive_failures column (Azure Postgres fallback)
    if db_type == "azure" and hasattr(db, 'execute_raw_sql'):
        try:
            db.execute_raw_sql("ALTER TABLE monitored_tickers ADD COLUMN IF NOT EXISTS consecutive_failures INT DEFAULT 0;")
        except Exception as e:
            logger.warning(f"Could not ensure consecutive_failures column exists: {e}")

    # Load Tickers
    all_tickers = load_tickers_from_db(db)
    fallback_used = False
    if not all_tickers:
        logger.warning("No active tickers found in database monitored_tickers. Falling back to JSON.")
        all_tickers = load_tickers_list()
        fallback_used = True

    if not all_tickers:
        logger.error("No stock tickers found. Sync cycle aborted.")
        return False

    # Apply Alphabetical Workload Slicing
    if instance_index is not None:
        chunk_size = (len(all_tickers) + total_instances - 1) // total_instances
        start_idx = instance_index * chunk_size
        end_idx = min(start_idx + chunk_size, len(all_tickers))
        sliced_tickers = all_tickers[start_idx:end_idx]
        logger.info(f"Instance Partition active: {instance_index}/{total_instances} (Slice range index {start_idx} to {end_idx}).")
        logger.info(f"Assigned workload: {len(sliced_tickers)} stocks.")
        run_limit = limit_val if limit_val is not None else len(sliced_tickers)
    else:
        offset_val = max(0, offset_val)
        sliced_tickers = all_tickers[offset_val:]
        logger.info(f"Direct slicing active: Offset {offset_val}.")
        logger.info(f"Assigned workload: {len(sliced_tickers)} stocks.")
        run_limit = limit_val if limit_val is not None else 70

    # Query DB to check today's sync statuses
    try:
        db_stocks = db.table('stocks').select('symbol', 'last_sync_date').execute()
        db_map = {}
        for s in db_stocks.data:
            symbol = s['symbol'].upper()
            last_sync = s.get('last_sync_date')
            db_map[symbol] = last_sync
    except Exception as e:
        logger.error(f"Failed to fetch stock sync dates from database: {e}")
        return False

    # Filter and Prioritize Queue
    today_str = date.today().isoformat()
    new_stocks = []
    pending_stocks = []
    synced_count = 0

    for ticker in sliced_tickers:
        ticker_upper = ticker.upper()
        if ticker_upper not in db_map:
            new_stocks.append(ticker_upper)
        else:
            last_sync = db_map[ticker_upper]
            if last_sync:
                if hasattr(last_sync, 'strftime'):
                    last_sync_date_str = last_sync.strftime('%Y-%m-%d')
                else:
                    last_sync_date_str = str(last_sync)

                if last_sync_date_str.startswith(today_str):
                    synced_count += 1
                else:
                    pending_stocks.append((ticker_upper, last_sync_date_str))
            else:
                pending_stocks.append((ticker_upper, ""))

    pending_stocks.sort(key=lambda x: x[1] or "")
    pending_tickers = [x[0] for x in pending_stocks]
    queue = new_stocks + pending_tickers

    logger.info(f"Queue Status: Synced Today: {synced_count}, New: {len(new_stocks)}, Outdated: {len(pending_tickers)}, Total Pending: {len(queue)}")

    if not queue:
        logger.info("✓ All assigned stocks are already synced and up-to-date today.")
        return True

    # Sub-Batch Split
    target_tickers = queue[:run_limit]
    chunks = [target_tickers[i:i + sub_batch_size] for i in range(0, len(target_tickers), sub_batch_size)]
    
    logger.info(f"Syncing {len(target_tickers)} stocks in {len(chunks)} sub-batches of size {sub_batch_size} (sleep {sleep_interval_seconds}s).")
    
    if preview_mode:
        for chunk_idx, chunk in enumerate(chunks, 1):
            logger.info(f"Preview [Sub-batch {chunk_idx}/{len(chunks)}]: {', '.join(chunk)}")
        return True

    # Sync Loop
    start_time = datetime.now()
    total_processed = 0
    total_success = 0
    total_fail = 0
    total_partial = 0

    for chunk_idx, chunk in enumerate(chunks, 1):
        logger.info(f"\n================================================================================")
        logger.info(f"STARTING SUB-BATCH {chunk_idx}/{len(chunks)} ({len(chunk)} tickers)")
        logger.info(f"================================================================================\n")
        
        if db_type == "azure":
            try:
                from database.export_to_azure_postgres import prepare_bulk_prices
                prepare_bulk_prices(chunk)
            except Exception as e:
                logger.warning(f"Failed to prepare bulk prices: {e}")

        chunk_success = 0
        chunk_fail = 0
        chunk_partial = 0
        consecutive_failures = 0
        aborted_due_to_errors = False

        for idx, ticker in enumerate(chunk, 1):
            # Check if yfinance rate limit cooldown is active
            from data_processing_scripts.data_collection import _rate_limit_cooldown_remaining_seconds
            cooldown_remaining = _rate_limit_cooldown_remaining_seconds()
            if cooldown_remaining > 0:
                logger.warning(f"Aborting sync cycle early: yfinance provider rate limit cooldown is active ({int(cooldown_remaining)}s remaining) to avoid continuous failure loop.")
                aborted_due_to_errors = True
                break

            logger.info(f"[{chunk_idx}/{len(chunks)}][{idx}/{len(chunk)}] Processing {ticker}...")
            result = None
            try:
                result = exporter.export_stock_data(ticker)
                status = result.get('status', 'failed')
                if status == 'success':
                    chunk_success += 1
                    consecutive_failures = 0
                elif status == 'partial':
                    chunk_partial += 1
                    # Check if the partial success has rate-limiting related errors
                    has_rate_limit_error = any(
                        'rate_limited' in str(e).lower() or 
                        'too many requests' in str(e).lower() or 
                        'rate limit' in str(e).lower() or
                        'invalid crumb' in str(e).lower() or
                        'unauthorized' in str(e).lower()
                        for e in result.get('errors', [])
                    )
                    if has_rate_limit_error:
                        consecutive_failures += 1
                    else:
                        consecutive_failures = 0
                else:
                    chunk_fail += 1
                    # Check if the failure was due to rate limits or crumb errors
                    has_rate_limit_error = any(
                        'rate_limited' in str(e).lower() or 
                        'too many requests' in str(e).lower() or 
                        'rate limit' in str(e).lower() or
                        'invalid crumb' in str(e).lower() or
                        'unauthorized' in str(e).lower()
                        for e in result.get('errors', [])
                    )
                    if has_rate_limit_error:
                        consecutive_failures += 1
                    else:
                        consecutive_failures = 0
                    logger.error(f"Failed to export {ticker}: {result.get('errors')}")
            except Exception as e:
                chunk_fail += 1
                # Check if the exception indicates rate limits
                has_rate_limit_error = any(
                    k in str(e).lower() 
                    for k in ['rate_limited', 'too many requests', 'rate limit', 'invalid crumb', 'unauthorized']
                )
                if has_rate_limit_error:
                    consecutive_failures += 1
                else:
                    consecutive_failures = 0
                logger.error(f"Exception during export of {ticker}: {e}", exc_info=True)
                result = {'status': 'failed', 'errors': [str(e)]}
            
            # Handle failure count and pruning logic
            if result:
                handle_ticker_sync_result(db, ticker, result)
            
            # Regenerate live HTML status dashboard after each ticker update
            update_dashboard()

            if consecutive_failures >= 3:
                logger.warning("Aborting sync cycle early: Hit 3 consecutive failures. Yahoo Finance may be rate-limiting us or crumb is invalid.")
                aborted_due_to_errors = True
                break

            # Sleep 30 seconds between tickers to avoid rate limiting and spamming Yahoo Finance
            if idx < len(chunk):
                logger.info(f"Sleeping for 30 seconds before processing next ticker...")
                time.sleep(30)

        total_processed += len(chunk)
        total_success += chunk_success
        total_fail += chunk_fail
        total_partial += chunk_partial

        logger.info(f"Finished Sub-batch {chunk_idx}/{len(chunks)}. Success: {chunk_success}, Failed: {chunk_fail}")

        if aborted_due_to_errors:
            logger.warning("Stopping entire batch sync because abort flag was raised.")
            break

        if chunk_idx < len(chunks):
            logger.info(f"Sleeping for {sleep_interval_seconds / 60:.1f} minutes to bypass rate limits...")
            time.sleep(sleep_interval_seconds)

    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"Batch completed in {duration/60:.1f} minutes. Success: {total_success}, Failed: {total_fail}")
    return total_fail == 0 and not aborted_due_to_errors

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Cron update runner for TickZen stock data.")
    parser.add_argument("--preview", action="store_true", help="Preview pending stocks without running the export.")
    parser.add_argument("--limit", type=int, default=None, help="Number of stocks to process in this batch (default: 70 when not partitioning, or entire slice size when partitioning).")
    parser.add_argument("--offset", type=int, help="Offset to start slicing the ticker list (overrides env TICKER_BATCH_OFFSET).")
    parser.add_argument("--instance-index", type=int, help="Index of this instance (0 to total-instances - 1, overrides env INSTANCE_INDEX).")
    parser.add_argument("--total-instances", type=int, help="Total number of parallel instances (overrides env TOTAL_INSTANCES).")
    parser.add_argument("--sub-batch-size", type=int, help="Number of stocks to process per sub-batch to avoid rate limits (overrides env SUB_BATCH_SIZE).")
    parser.add_argument("--batch-sleep", type=int, help="Seconds to sleep between sub-batches (overrides env BATCH_SLEEP_SECONDS).")
    parser.add_argument("--once", action="store_true", help="Run once and exit instead of starting a persistent HTTP daemon.")
    parser.add_argument("--force", action="store_true", help="Force execution even outside allowed hours.")
    args = parser.parse_args()

    # Determine execution window check for single runs
    if args.once and not args.force:
        now = datetime.now()
        current_hour = now.hour
        current_minute = now.minute
        is_allowed = 7 <= current_hour < 18 or (current_hour == 18 and current_minute <= 30)
        if not is_allowed:
            logger.info(f"Outside allowed database export window (07:00 to 18:30 IST). Execution skipped. Use --force to override.")
            sys.exit(0)

    # 1. Connectivity Check
    if not check_internet():
        logger.warning("❌ No active internet connection detected. Skipping execution.")
        sys.exit(0)
    logger.info("✓ Internet connection verified.")

    # 2. Exporter and DB Setup
    exporter, db, db_type = get_exporter_and_client()
    if not exporter or not db:
        logger.error("❌ Failed to set up database connection or exporter. Exiting.")
        sys.exit(1)
    logger.info(f"✓ Using {db_type.upper()} database configuration.")

    # Resolve slicing parameters from CLI arguments or Environment Variables
    env_limit = os.getenv("TICKER_BATCH_LIMIT")
    env_offset = os.getenv("TICKER_BATCH_OFFSET")
    env_instance_index = os.getenv("INSTANCE_INDEX")
    env_total_instances = os.getenv("TOTAL_INSTANCES")
    env_sub_batch_size = os.getenv("SUB_BATCH_SIZE")
    env_batch_sleep = os.getenv("BATCH_SLEEP_SECONDS")

    limit_val = args.limit if args.limit is not None else (int(env_limit) if env_limit else None)
    offset_val = args.offset if args.offset is not None else (int(env_offset) if env_offset else 0)
    instance_index = args.instance_index if args.instance_index is not None else (int(env_instance_index) if env_instance_index is not None else None)
    total_instances = args.total_instances if args.total_instances is not None else (int(env_total_instances) if env_total_instances else 5)
    
    sub_batch_size = args.sub_batch_size if args.sub_batch_size is not None else (int(env_sub_batch_size) if env_sub_batch_size else 70)
    sleep_interval_seconds = args.batch_sleep if args.batch_sleep is not None else (int(env_batch_sleep) if env_batch_sleep else 4200)

    if args.once:
        # Run once and exit (e.g. for Cron run)
        success = run_sync_cycle(exporter, db, db_type, limit_val, offset_val, instance_index, total_instances, sub_batch_size, sleep_interval_seconds, args.preview)
        sys.exit(0 if success else 1)
    
    # Render Web Service (Daemon mode)
    # Start web server
    server = start_dashboard_web_server()
    # Generate initial dashboard immediately
    update_dashboard()

    logger.info("Entering persistent daemon scheduler loop...")
    while True:
        # Check time window (07:00 to 18:30 IST)
        # Convert UTC to IST
        utc_now = datetime.now(timezone.utc)
        ist_now = utc_now + timedelta(hours=5, minutes=30)
        
        current_hour = ist_now.hour
        current_minute = ist_now.minute
        
        is_allowed = 7 <= current_hour < 18 or (current_hour == 18 and current_minute <= 30)
        
        if args.force:
            is_allowed = True
            
        if is_allowed:
            run_sync_cycle(exporter, db, db_type, limit_val, offset_val, instance_index, total_instances, sub_batch_size, sleep_interval_seconds, args.preview)
            
            logger.info("Sync cycle complete. Sleeping for 1 hour...")
            time.sleep(3600)
        else:
            logger.info(f"Allowed IST window (07:00 to 18:30) is currently closed. Current IST: {ist_now.strftime('%H:%M')}. Re-checking in 15 minutes...")
            time.sleep(900)

if __name__ == "__main__":
    main()
