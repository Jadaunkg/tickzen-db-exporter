#!/usr/bin/env python3
"""
Automated Cron Update Runner
============================

Automated daily batch runner designed for macOS cron/launchd loops.
Features:
1. Internet connection check before starting.
2. DB query to find which tickers from `stock_tickers_list.json` are pending.
3. Priority sorting: new stocks (not in DB) processed first, then oldest/unsynced stocks.
4. Auto-chunking: takes the first 70 stocks and updates them.
5. Runs either Azure Postgres or Supabase exporter based on configuration.
"""

import os
import sys
import json
import logging
import socket
from datetime import datetime, date
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
            # Deduplicate while preserving order
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
    parser.add_argument("--force", action="store_true", help="Force execution even outside allowed hours.")
    args = parser.parse_args()

    # Time window check: Only run database export during off-market hours (07:00 to 18:30 IST)
    # to prevent hitting yfinance rate limits concurrently with the active trading daemon.
    if not args.force:
        now = datetime.now()
        current_hour = now.hour
        current_minute = now.minute
        
        is_allowed = False
        if 7 <= current_hour < 18:
            is_allowed = True
        elif current_hour == 18 and current_minute <= 30:
            is_allowed = True
            
        if not is_allowed:
            logger.info(f"Execution skipped: Current time ({now.strftime('%H:%M')}) is outside the allowed database export window (07:00 to 18:30 IST) to avoid yfinance rate limit conflicts with the active trading daemon. Use --force to override.")
            sys.exit(0)

    logger.info("Starting automated data sync check...")
    
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

    # 3. Load Tickers
    all_tickers = load_tickers_from_db(db)
    fallback_used = False
    if not all_tickers:
        logger.warning("No active tickers found in 'monitored_tickers' table. Falling back to stock_tickers_list.json.")
        all_tickers = load_tickers_list()
        fallback_used = True

    if not all_tickers:
        logger.error("❌ No tickers found in database or stock_tickers_list.json. Exiting.")
        sys.exit(1)

    if fallback_used:
        logger.info(f"Loaded {len(all_tickers)} total tickers from stock_tickers_list.json (fallback).")
    else:
        logger.info(f"Loaded {len(all_tickers)} active tickers from monitored_tickers table.")

    # Apply Slicing
    if instance_index is not None:
        chunk_size = (len(all_tickers) + total_instances - 1) // total_instances
        start_idx = instance_index * chunk_size
        end_idx = min(start_idx + chunk_size, len(all_tickers))
        sliced_tickers = all_tickers[start_idx:end_idx]
        logger.info(f"Partitioning active: Instance {instance_index}/{total_instances} (Slice index {start_idx} to {end_idx}).")
        logger.info(f"Sliced list to {len(sliced_tickers)} tickers for this instance.")
        run_limit = limit_val if limit_val is not None else len(sliced_tickers)
    else:
        offset_val = max(0, offset_val)
        sliced_tickers = all_tickers[offset_val:]
        logger.info(f"Direct slicing active: Offset {offset_val}.")
        logger.info(f"Sliced list has {len(sliced_tickers)} tickers.")
        run_limit = limit_val if limit_val is not None else 70

    # 4. Query DB for Last Sync Dates
    try:
        # Select symbol and last_sync_date
        db_stocks = db.table('stocks').select('symbol', 'last_sync_date').execute()
        db_map = {}
        for s in db_stocks.data:
            symbol = s['symbol'].upper()
            last_sync = s.get('last_sync_date')
            db_map[symbol] = last_sync
    except Exception as e:
        logger.error(f"❌ Failed to fetch stocks from database: {e}")
        sys.exit(1)

    # 5. Filter and Prioritize Queue
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
                # convert to string to check or handle object
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

    # Sort pending stocks: oldest last_sync first (nulls/empty first)
    pending_stocks.sort(key=lambda x: x[1] or "")
    pending_tickers = [x[0] for x in pending_stocks]

    # Combine queue: New stocks first, then pending stocks
    queue = new_stocks + pending_tickers

    logger.info(f"Queue Status:")
    logger.info(f"  - Already Synced Today: {synced_count}")
    logger.info(f"  - New Tickers (Not in DB): {len(new_stocks)} ({', '.join(new_stocks[:10])}...)" if new_stocks else "  - New Tickers (Not in DB): 0")
    logger.info(f"  - Outdated Tickers: {len(pending_tickers)}")
    logger.info(f"  - Total Pending Queue: {len(queue)}")

    if not queue:
        logger.info("✓ All stocks are up to date for today. No execution needed.")
        sys.exit(0)

    # 6. Apply Batch Limit and Split into Sub-batches
    target_tickers = queue[:run_limit]
    
    # Split target_tickers into chunks of sub_batch_size
    chunks = [target_tickers[i:i + sub_batch_size] for i in range(0, len(target_tickers), sub_batch_size)]
    
    logger.info(f"Target Queue of {len(target_tickers)} tickers split into {len(chunks)} sub-batches (size={sub_batch_size}, sleep={sleep_interval_seconds}s).")
    
    if args.preview:
        for chunk_idx, chunk in enumerate(chunks, 1):
            logger.info(f"Preview [Sub-batch {chunk_idx}/{len(chunks)}]: {', '.join(chunk)}")
        logger.info("Preview mode active. Exiting without execution.")
        sys.exit(0)

    # 7. Execute Export in Sub-batches
    start_time = datetime.now()
    total_processed = 0
    total_success = 0
    total_fail = 0
    total_partial = 0

    for chunk_idx, chunk in enumerate(chunks, 1):
        logger.info(f"\n================================================================================")
        logger.info(f"STARTING SUB-BATCH {chunk_idx}/{len(chunks)} ({len(chunk)} tickers)")
        logger.info(f"================================================================================\n")
        
        # If Azure, run bulk download for prices first
        if db_type == "azure":
            try:
                from database.export_to_azure_postgres import prepare_bulk_prices
                prepare_bulk_prices(chunk)
            except Exception as e:
                logger.warning(f"Failed to prepare bulk prices: {e}")

        chunk_success = 0
        chunk_fail = 0
        chunk_partial = 0

        for idx, ticker in enumerate(chunk, 1):
            logger.info(f"[{chunk_idx}/{len(chunks)}][{idx}/{len(chunk)}] Processing {ticker}...")
            try:
                result = exporter.export_stock_data(ticker)
                status = result.get('status', 'failed')
                if status == 'success':
                    chunk_success += 1
                elif status == 'partial':
                    chunk_partial += 1
                else:
                    chunk_fail += 1
                    logger.error(f"Failed to export {ticker}: {result.get('errors')}")
            except Exception as e:
                chunk_fail += 1
                logger.error(f"Exception during export of {ticker}: {e}", exc_info=True)

        total_processed += len(chunk)
        total_success += chunk_success
        total_fail += chunk_fail
        total_partial += chunk_partial

        logger.info(f"Finished Sub-batch {chunk_idx}/{len(chunks)}. Success: {chunk_success}, Partial: {chunk_partial}, Failed: {chunk_fail}")

        # Sleep between sub-batches
        if chunk_idx < len(chunks):
            logger.info(f"Sleeping for {sleep_interval_seconds / 60:.1f} minutes before next sub-batch to prevent API rate limits...")
            import time
            time.sleep(sleep_interval_seconds)

    duration = (datetime.now() - start_time).total_seconds()
    
    logger.info("\n" + "="*80)
    logger.info("BATCH EXPORT RUN COMPLETE")
    logger.info("="*80)
    logger.info(f"Duration: {duration/60:.1f} minutes")
    logger.info(f"Processed: {total_processed}")
    logger.info(f"Success: {total_success}")
    logger.info(f"Partial: {total_partial}")
    logger.info(f"Failed: {total_fail}")
    logger.info("="*80 + "\n")

    sys.exit(0 if total_fail == 0 else 1)

if __name__ == "__main__":
    main()
