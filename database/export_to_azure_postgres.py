#!/usr/bin/env python3
"""
Azure PostgreSQL Data Exporter
==============================

Separate Azure migration test entrypoint. The existing Supabase exporter remains
unchanged; this class reuses its export workflow and swaps only the destination
client to Azure PostgreSQL.

Usage:
-----
    python database/export_to_azure_postgres.py --test-connection
    python database/export_to_azure_postgres.py AAPL
    python database/export_to_azure_postgres.py AAPL GOOGL MSFT
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from database.azure_postgres_client import AzurePostgresClient, test_connection


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"azure_postgres_export_{datetime.now().strftime('%Y%m%d')}.log"),
    ],
)

logger = logging.getLogger(__name__)


class AzurePostgresDataExporter:
    """Export TickZen stock data to Azure PostgreSQL using the existing pipeline."""

    def __init__(
        self,
        host: str = None,
        database: str = None,
        user: str = None,
        password: str = None,
        port: int = None,
        sslmode: str = None,
        connection_url: str = None,
    ):
        from analysis_scripts.risk_analysis import RiskAnalyzer
        from database.data_mapper import DataMapper
        from database.export_to_supabase import SupabaseDataExporter
        from database.pipeline_data_collector import PipelineDataCollector

        delegate = object.__new__(SupabaseDataExporter)
        delegate.collector = PipelineDataCollector()
        delegate.mapper = DataMapper()
        delegate.db = AzurePostgresClient(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port,
            sslmode=sslmode,
            connection_url=connection_url,
        )
        delegate.collector.db = delegate.db
        delegate.risk_analyzer = RiskAnalyzer()
        
        # Redirect the logger of export_to_supabase to print with our name
        import database.export_to_supabase
        database.export_to_supabase.logger = logging.getLogger("database.export_to_azure_postgres")
        
        self._delegate = delegate

        logger.info("AzurePostgresDataExporter initialized successfully")

    def __getattr__(self, name):
        return getattr(self._delegate, name)


def prepare_bulk_prices(tickers: List[str]):
    """Download daily prices for all tickers in a single yfinance request and cache them."""
    if not tickers:
        return
        
    logger.info(f"Bulk downloading daily price data for {len(tickers)} tickers: {tickers}...")
    try:
        import yfinance as yf
        import pandas as pd
        from data_processing_scripts.data_collection import set_bulk_price_data
        
        # Download 2 years of daily data (the cutoff limit) to keep it fast
        df = yf.download(tickers, period="2y", group_by="ticker", progress=False)
        if df.empty:
            return
            
        prices_dict = {}
        current_prices_dict = {}
        
        # Iterate over df level 0 columns (tickers)
        for ticker in tickers:
            ticker_upper = ticker.upper()
            if ticker_upper in df.columns.levels[0]:
                ticker_df = df[ticker_upper].copy()
                # Drop rows with NaN Close to be clean
                ticker_df = ticker_df.dropna(subset=['Close'])
                if ticker_df.empty:
                    continue
                
                # Reset index to make Date a column
                ticker_df = ticker_df.reset_index()
                # Ensure the date column is named 'Date'
                ticker_df.rename(columns={'index': 'Date', 'DATE': 'Date', 'Date': 'Date'}, inplace=True)
                
                # Check for Adj Close and map it to Adj Close if it was named something else
                if 'Adj Close' in ticker_df.columns:
                    ticker_df.rename(columns={'Adj Close': 'Adj Close'}, inplace=True)
                
                prices_dict[ticker] = ticker_df
                
                # Construct current price quote from last row
                last_row = ticker_df.iloc[-1]
                prev_row = ticker_df.iloc[-2] if len(ticker_df) > 1 else last_row
                
                current_price = float(last_row['Close'])
                previous_close = float(prev_row['Close'])
                change = current_price - previous_close
                change_percent = (change / previous_close) * 100 if previous_close != 0 else 0
                
                current_prices_dict[ticker] = {
                    'ticker': ticker,
                    'current_price': current_price,
                    'previous_close': previous_close,
                    'change': change,
                    'change_percent': change_percent,
                    'market_state': 'CLOSED',
                    'last_updated': datetime.now().isoformat(),
                    'currency': 'USD'
                }
                
        set_bulk_price_data(prices_dict, current_prices_dict)
        logger.info(f"Successfully cached bulk price data for {len(prices_dict)} tickers")
    except Exception as e:
        logger.warning(f"Failed to bulk download prices: {e}. Falling back to individual stock downloads.")


def export_multiple_stocks(exporter: AzurePostgresDataExporter, tickers: List[str]) -> Dict:
    results = {
        "total": len(tickers),
        "successful": 0,
        "partial": 0,
        "failed": 0,
        "details": {},
    }

    for ticker in tickers:
        result = exporter.export_stock_data(ticker)
        results["details"][ticker] = result

        if result["status"] == "success":
            results["successful"] += 1
        elif result["status"] == "partial":
            results["partial"] += 1
        else:
            results["failed"] += 1

    logger.info("\n%s", "=" * 80)
    logger.info("AZURE POSTGRES BATCH EXPORT SUMMARY")
    logger.info("%s", "=" * 80)
    logger.info("Total Stocks: %s", results["total"])
    logger.info("Successful: %s", results["successful"])
    logger.info("Partial: %s", results["partial"])
    logger.info("Failed: %s", results["failed"])
    logger.info("%s\n", "=" * 80)

    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export stock data to Azure PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Test connection only:
    python database/export_to_azure_postgres.py --test-connection

  Export single stock:
    python database/export_to_azure_postgres.py AAPL

  Export multiple stocks:
    python database/export_to_azure_postgres.py AAPL GOOGL MSFT TSLA
        """,
    )

    parser.add_argument("tickers", nargs="*", help="Stock ticker symbols to export")
    parser.add_argument("--all", action="store_true", help="Export all active tickers from monitored_tickers table (with JSON fallback)")
    parser.add_argument("--test-connection", action="store_true", help="Only test Azure PostgreSQL connectivity")
    parser.add_argument("--url", help="Full Azure PostgreSQL connection URL")
    parser.add_argument("--host", help="Azure PostgreSQL host")
    parser.add_argument("--db", help="Azure PostgreSQL database name")
    parser.add_argument("--user", help="Azure PostgreSQL user")
    parser.add_argument("--password", help="Azure PostgreSQL password")
    parser.add_argument("--port", type=int, help="Azure PostgreSQL port")
    parser.add_argument("--sslmode", help="PostgreSQL SSL mode")
    parser.add_argument("--limit", type=int, help="Limit the number of tickers exported (use with --all)")
    parser.add_argument("--offset", type=int, default=None, help="Offset to start exporting tickers from (use with --all)")
    parser.add_argument("--instance-index", type=int, help="Index of this instance (0 to total-instances - 1, overrides env INSTANCE_INDEX)")
    parser.add_argument("--total-instances", type=int, help="Total number of parallel instances (overrides env TOTAL_INSTANCES)")
    parser.add_argument("--preview", action="store_true", help="Preview tickers to export without running actual exports")
 
    args = parser.parse_args()
 
    if args.test_connection:
        ok = test_connection(
            connection_url=args.url,
            host=args.host,
            database=args.db,
            user=args.user,
            password=args.password,
            port=args.port,
            sslmode=args.sslmode,
        )
        sys.exit(0 if ok else 1)
 
    import json
 
    try:
        exporter = AzurePostgresDataExporter(
            connection_url=args.url,
            host=args.host,
            database=args.db,
            user=args.user,
            password=args.password,
            port=args.port,
            sslmode=args.sslmode,
        )
    except Exception as error:
        logger.error(f"Fatal Azure PostgreSQL initialization error: {error}")
        sys.exit(1)
 
    tickers = args.tickers
    if args.all:
        db_tickers = []
        try:
            res = exporter.db.table('monitored_tickers').select('symbol').eq('is_active', True).execute()
            if hasattr(res, 'data') and res.data:
                db_tickers = [r['symbol'].upper() for r in res.data if r.get('symbol')]
                db_tickers.sort()
                logger.info(f"Loaded {len(db_tickers)} active tickers from monitored_tickers table.")
        except Exception as e:
            logger.warning(f"Could not load tickers from database 'monitored_tickers' table: {e}")
 
        if not db_tickers:
            json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stock_tickers_list.json")
            try:
                with open(json_path, "r") as f:
                    tickers_data = json.load(f)
                    db_tickers = tickers_data.get("tickers", [])
                seen = set()
                db_tickers = [x.upper() for x in db_tickers if not (x.upper() in seen or seen.add(x.upper()))]
                db_tickers.sort()
                logger.info(f"Loaded {len(db_tickers)} tickers from stock_tickers_list.json (fallback).")
            except Exception as e:
                parser.error(f"Failed to load stock_tickers_list.json: {e}")
 
        # Resolve slicing parameters from CLI arguments or Environment Variables
        env_limit = os.getenv("TICKER_BATCH_LIMIT")
        env_offset = os.getenv("TICKER_BATCH_OFFSET")
        env_instance_index = os.getenv("INSTANCE_INDEX")
        env_total_instances = os.getenv("TOTAL_INSTANCES")
 
        limit_val = args.limit if args.limit is not None else (int(env_limit) if env_limit else None)
        offset_val = args.offset if args.offset is not None else (int(env_offset) if env_offset else 0)
        instance_index = args.instance_index if args.instance_index is not None else (int(env_instance_index) if env_instance_index is not None else None)
        total_instances = args.total_instances if args.total_instances is not None else (int(env_total_instances) if env_total_instances else 5)
 
        # Apply slicing
        if instance_index is not None:
            chunk_size = (len(db_tickers) + total_instances - 1) // total_instances
            start_idx = instance_index * chunk_size
            end_idx = min(start_idx + chunk_size, len(db_tickers))
            tickers = db_tickers[start_idx:end_idx]
            logger.info(f"Partitioning active: Instance {instance_index}/{total_instances} (Slice index {start_idx} to {end_idx}).")
            logger.info(f"Sliced list to {len(tickers)} tickers for this instance.")
            run_limit = limit_val if limit_val is not None else len(tickers)
            tickers = tickers[:run_limit]
        else:
            offset_val = max(0, offset_val)
            tickers = db_tickers[offset_val:]
            logger.info(f"Direct slicing active: Offset {offset_val}.")
            run_limit = limit_val if limit_val is not None else len(tickers)
            tickers = tickers[:run_limit]
            logger.info(f"Sliced list has {len(tickers)} tickers.")
 
    if not tickers:
        parser.error("Provide at least one ticker, use --all, or use --test-connection.")
 
    if args.preview:
        logger.info(f"Preview mode active. Tickers to export: {tickers}")
        sys.exit(0)
 
    try:
        # Download all price data in one bulk request first
        prepare_bulk_prices(tickers)
 
        if len(tickers) == 1:
            result = exporter.export_stock_data(tickers[0])
            sys.exit(0 if result["status"] in ["success", "partial"] else 1)
 
        results = export_multiple_stocks(exporter, tickers)
        sys.exit(0 if results["failed"] == 0 else 1)
    except Exception as error:
        logger.error(f"Fatal Azure PostgreSQL export error: {error}")
        logger.exception("Traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()
