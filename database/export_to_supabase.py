#!/usr/bin/env python3
"""
Production Supabase Data Exporter
==================================

Complete pipeline for collecting stock data and exporting to Supabase.
Uses existing pipeline for data collection, then transforms and exports
to all relevant Supabase tables.

Workflow:
--------
1. Collect data using PipelineDataCollector
2. Transform data using DataMapper
3. Export to Supabase using SupabaseClient
4. Log sync status and statistics
5. Handle errors gracefully

Usage:
-----
    python export_to_supabase.py AAPL
    python export_to_supabase.py AAPL GOOGL MSFT
"""

import os
import sys
import argparse
import logging
import time
import re
import json
from datetime import datetime, timezone
from typing import List, Dict

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))


from database.pipeline_data_collector import PipelineDataCollector, as_utc_date, refresh_cutoff
from database.data_mapper import DataMapper
from Models.forecast_core import PIPELINE_VERSION as TICKZEN_PROPHET_PIPELINE_VERSION
from database.supabase_client import SupabaseClient
from analysis_scripts.risk_analysis import RiskAnalyzer
from database.financial_contract import (
    SCHEMA_VERSION,
    CALCULATION_VERSION,
    PartialProviderData,
    carry_forward_snapshot_size,
    finite_float,
    next_earnings_date as upcoming_earnings_date,
    normalized_exchange,
    stable_content_hash,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'supabase_export_{datetime.now().strftime("%Y%m%d")}.log')
    ]
)

logger = logging.getLogger(__name__)

UNKNOWN_COLUMN_ERROR_PATTERN = re.compile(
    r"Could not find the '([^']+)' column of '([^']+)' in the schema cache"
)
POSTGRES_UNKNOWN_COLUMN_ERROR_PATTERN = re.compile(
    r'column "([^"]+)" of relation "([^"]+)" does not exist'
)


class SupabaseDataExporter:
    """
    Complete data export pipeline from collection to Supabase storage
    """
    
    def __init__(self, supabase_url: str = None, supabase_key: str = None):
        """
        Initialize exporter with Supabase credentials
        
        Args:
            supabase_url: Supabase project URL (or from env)
            supabase_key: Supabase API key (or from env)
        """
        self.supabase_url = supabase_url or os.getenv('SUPABASE_URL')
        self.supabase_key = supabase_key or os.getenv('SUPABASE_KEY')
        
        if not self.supabase_url or not self.supabase_key:
            raise ValueError("Supabase credentials required. Set SUPABASE_URL and SUPABASE_KEY environment variables.")
        
        # Initialize components
        self.db = SupabaseClient(url=self.supabase_url, key=self.supabase_key)
        self.collector = PipelineDataCollector()
        self.collector.db = self.db
        self.mapper = DataMapper()
        self.risk_analyzer = RiskAnalyzer()
        
        logger.info("SupabaseDataExporter initialized successfully")
    
    def _get_latest_data_date(self, stock_id: int, table_name: str, date_column: str = 'date') -> str:
        """
        Get the latest date we have data for in the specified table
        
        Args:
            stock_id: Database ID of the stock
            table_name: Name of the table to check
            date_column: Name of the date column (default: 'date')
            
        Returns:
            Latest date as string (YYYY-MM-DD) or None if no data exists
        """
        try:
            result = self.db.client.table(table_name)\
                .select(date_column)\
                .eq('stock_id', stock_id)\
                .order(date_column, desc=True)\
                .limit(1)\
                .execute()
            
            if result.data:
                latest_date = result.data[0][date_column]
                logger.info(f"  📅 Latest {table_name} date in DB: {latest_date}")
                return latest_date
            else:
                logger.info(f"  📅 No existing {table_name} data found - full export needed")
                return None
                
        except Exception as e:
            logger.warning(f"Error checking latest date in {table_name}: {e}")
            return None
    
    def _filter_new_data_only(self, data, latest_db_date: str, date_column: str = 'Date'):
        """
        Filter dataframe to only include records newer than the latest database date
        
        Args:
            data: pandas DataFrame with date column
            latest_db_date: Latest date we have in database (YYYY-MM-DD)
            date_column: Name of the date column in data
            
        Returns:
            Filtered DataFrame with only new records
        """
        try:
            import pandas as pd
            from datetime import datetime
            
            if data is None or data.empty or latest_db_date is None:
                return data
                
            # Convert date column to datetime for comparison
            data_dates = pd.to_datetime(data[date_column], errors='coerce')
            latest_db_datetime = pd.to_datetime(latest_db_date)
            
            # Filter to only dates after the latest database date
            mask = data_dates > latest_db_datetime
            new_data = data[mask].copy()
            
            original_count = len(data)
            new_count = len(new_data)
            
            if new_count > 0:
                logger.info(f"  📊 Incremental update: {original_count} total → {new_count} new records (after {latest_db_date})")
            else:
                logger.info(f"  ⏭️  No new data found after {latest_db_date} - skipping export")
            
            return new_data
            
        except Exception as e:
            logger.warning(f"Error filtering new data: {e}")
            return data  # Return original data if filtering fails
    
    def _filter_last_two_years(self, data, column_name='Date'):
        """
        Filter dataframe to only include the last 2 years of data
        
        Args:
            data: pandas DataFrame with date column
            column_name: Name of the date column (default: 'Date')
            
        Returns:
            Filtered DataFrame with last 2 years of data
        """
        try:
            import pandas as pd
            from datetime import datetime, timedelta
            
            if data is None or data.empty:
                return data
                
            # Convert to DataFrame if not already
            if not isinstance(data, pd.DataFrame):
                return data
                
            # Check if date column exists
            if column_name not in data.columns:
                logger.warning(f"Date column '{column_name}' not found, returning full dataset")
                return data
            
            # Calculate cutoff date (2 years ago from today)
            cutoff_date = datetime.now() - timedelta(days=730)  # 2 years = 730 days
            
            # Convert date column to datetime if needed
            date_col = pd.to_datetime(data[column_name], errors='coerce')
            
            # Filter to last 2 years
            mask = date_col >= cutoff_date
            filtered_data = data[mask].copy()
            
            original_count = len(data)
            filtered_count = len(filtered_data)
            
            logger.info(f"  📅 Filtered data: {original_count} → {filtered_count} rows (last 2 years only)")
            
            return filtered_data
            
        except Exception as e:
            logger.warning(f"Error filtering data to last 2 years: {e}")
            return data  # Return original data if filtering fails
    
    def _sanitize_records_for_json(self, records: List[Dict]) -> List[Dict]:
        """
        Sanitize records to remove NaN, Infinity, and other non-JSON-compliant values
        
        Args:
            records: List of dictionaries to sanitize
            
        Returns:
            Sanitized list of dictionaries with NaN/Infinity converted to None
        """
        import numpy as np
        
        if not records:
            return records
        
        sanitized = []
        for record in records:
            if not isinstance(record, dict):
                logger.warning("Skipping non-object record while building canonical snapshot")
                continue
            sanitized_record = {}
            for key, value in record.items():
                if value is None:
                    sanitized_record[key] = None
                elif isinstance(value, float):
                    # Check for NaN or Infinity
                    if np.isnan(value) or np.isinf(value):
                        logger.debug(f"Sanitizing {key}: {value} → None")
                        sanitized_record[key] = None
                    else:
                        sanitized_record[key] = value
                else:
                    sanitized_record[key] = value
            sanitized.append(sanitized_record)
        
        return sanitized

    @staticmethod
    def _snapshot_stock_metadata(ticker: str, info: Dict) -> Dict:
        """Keep listing identity and currencies in the canonical snapshot."""
        quote_currency = (info.get('currency') or '').strip().upper() or None
        financial_currency = (
            info.get('financialCurrency') or info.get('currency') or ''
        ).strip().upper() or None
        exchange = info.get('exchange')
        return {
            'symbol': ticker,
            'company_name': info.get('shortName') or info.get('longName') or ticker,
            'sector': info.get('sector'),
            'industry': info.get('industry'),
            'country': info.get('country'),
            'exchange': exchange,
            'normalized_exchange': normalized_exchange(exchange),
            'quote_currency': quote_currency,
            'financial_currency': financial_currency,
            'security_type': info.get('quoteType'),
        }

    @staticmethod
    def _snapshot_peer_records(peer_records: List[Dict], peer_data: Dict) -> List[Dict]:
        """Attach per-listing currency metadata without changing legacy table rows."""
        metadata_by_ticker = {
            str(ticker).strip().upper(): metrics
            for ticker, metrics in (peer_data or {}).items()
            if isinstance(metrics, dict)
        }
        snapshots = []
        for record in peer_records or []:
            if not isinstance(record, dict):
                continue
            metrics = metadata_by_ticker.get(
                str(record.get('peer_ticker') or '').strip().upper(),
                {},
            )
            raw_currency = record.get('quote_currency') or metrics.get('quote_currency')
            quote_currency = (
                raw_currency.strip().upper()
                if isinstance(raw_currency, str) and raw_currency.strip()
                else None
            )
            exchange = metrics.get('exchange') or record.get('exchange')
            snapshots.append({
                **record,
                'quote_currency': quote_currency,
                'normalized_exchange': normalized_exchange(exchange),
                'exchange': exchange,
                'country': metrics.get('country') or record.get('country'),
            })
        return snapshots

    def _validate_snapshot_payload(self, payload: Dict) -> List[str]:
        """Validate the minimum invariants for a publishable stock snapshot.

        A current read-model must identify the stock, contain a positive finite
        market price, and never expose malformed forecast uncertainty bands.
        Empty optional sections are allowed when the upstream provider did not
        return those facts; present forecast rows, however, must be complete
        and ordered (low <= base <= high).
        """
        errors: List[str] = []
        if not isinstance(payload, dict):
            return ["snapshot payload must be an object"]

        ticker = str(payload.get("ticker") or "").strip().upper()
        stock = payload.get("stock")
        if not ticker:
            errors.append("ticker is required")
        if not isinstance(stock, dict):
            errors.append("stock metadata is required")
        elif str(stock.get("symbol") or "").strip().upper() != ticker:
            errors.append("stock.symbol must match ticker")

        market = payload.get("market_price")
        if not isinstance(market, dict):
            errors.append("market_price is required")
        else:
            current_price = finite_float(market.get("current_price"))
            if current_price is None or current_price <= 0:
                errors.append("market_price.current_price must be a positive finite number")

        forecasts = payload.get("forecast", [])
        if forecasts is None:
            forecasts = []
        if not isinstance(forecasts, list):
            errors.append("forecast must be an array")
        else:
            for index, row in enumerate(forecasts):
                if not isinstance(row, dict):
                    errors.append(f"forecast[{index}] must be an object")
                    continue
                values = {
                    field: finite_float(row.get(field))
                    for field in ("low_price", "base_price", "high_price")
                }
                missing = [field for field, value in values.items() if value is None]
                if missing:
                    errors.append(f"forecast[{index}] missing finite {', '.join(missing)}")
                    continue
                if not (values["low_price"] <= values["base_price"] <= values["high_price"]):
                    errors.append(f"forecast[{index}] uncertainty bands are not ordered")

        # This catches nested provider values that bypass the record sanitizer
        # (for example a Decimal/NumPy scalar converted during serialization).
        try:
            json.dumps(payload, allow_nan=False, default=str)
        except (TypeError, ValueError, OverflowError) as exc:
            errors.append(f"snapshot contains non-JSON values: {exc}")
        return errors

    def _extract_unknown_column(self, error: Exception, expected_table: str) -> str | None:
        """
        Extract unknown column name from Supabase/PostgREST schema-cache errors
        and native PostgreSQL errors.

        Args:
            error: Exception raised by postgrest client or psycopg2
            expected_table: Table name we attempted to write to

        Returns:
            Unknown column name if present for the expected table, else None
        """
        message = str(error)
        
        # Check Supabase/PostgREST format
        match = UNKNOWN_COLUMN_ERROR_PATTERN.search(message)
        if match:
            column_name, table_name = match.groups()
            if table_name == expected_table:
                return column_name
                
        # Check native PostgreSQL format
        pg_match = POSTGRES_UNKNOWN_COLUMN_ERROR_PATTERN.search(message)
        if pg_match:
            column_name, table_name = pg_match.groups()
            if table_name == expected_table:
                return column_name

        return None

    def _upsert_required_schema(self, table_name: str, records: List[Dict], on_conflict: str):
        """Upsert records and fail closed when the deployed schema is incompatible.

        Silently removing an unknown column can publish an internally incomplete
        record while reporting success.  Migrations and readers are versioned, so
        schema drift must stop this batch before it becomes public.
        """
        if not records:
            return
        self.db.client.table(table_name).upsert(
            [dict(record) for record in records],
            on_conflict=on_conflict,
        ).execute()
    
    def _is_forecast_current(self, stock_id: int, ticker: str) -> bool:
        """
        Check if forecast data for this stock was created since the symbol's refresh cutoff

        Args:
            stock_id: Database ID of the stock
            ticker: Stock symbol (sets the staggered 30-day refresh slot)

        Returns:
            True if current forecast data exists, False otherwise
        """
        try:
            cutoff = refresh_cutoff(ticker).isoformat()

            # Check if there's any forecast data for this stock created since the cutoff
            result = self.db.client.table('forecast_data')\
                .select('forecast_date')\
                .eq('stock_id', stock_id)\
                .eq('model_version', TICKZEN_PROPHET_PIPELINE_VERSION)\
                .gte('created_at', cutoff)\
                .limit(1)\
                .execute()

            has_data = bool(result.data)
            if has_data:
                logger.info(f"  ℹ Forecast data already updated since {cutoff} - skipping")
            
            return has_data
            
        except Exception as e:
            logger.warning(f"Error checking forecast update status: {e}")
            return False  # If we can't check, proceed with update to be safe
            
    def _is_analyst_data_updated_this_week(self, stock_id: int) -> bool:
        """Check if analyst targets were updated in the last 7 days."""
        try:
            from datetime import datetime
            res = self.db.client.table('analyst_data').select('updated_at').eq('stock_id', stock_id).execute()
            if res.data and res.data[0].get('updated_at'):
                val = res.data[0]['updated_at']
                if isinstance(val, str):
                    updated_at = datetime.fromisoformat(val.replace('Z', '+00:00'))
                else:
                    updated_at = val
                now = datetime.now(updated_at.tzinfo) if updated_at.tzinfo else datetime.now()
                return (now - updated_at).days < 7
        except Exception as e:
            logger.warning(f"Error checking weekly analyst update status: {e}")
        return False

    def _analyst_earnings_date_is_stale(self, stock_id: int, info: Dict) -> bool:
        """True when the stored next_earnings_date no longer matches what the provider reports now.

        Analyst targets are refreshed weekly, but an earnings date goes stale the day the report
        happens (and moves when the company reschedules). Without this check a recently refreshed row
        keeps a past/outdated date for up to a week. The refresh is the same single-row analyst update,
        so it costs nothing extra: ``info`` is already fetched.
        """
        try:
            fresh = upcoming_earnings_date(info or {})
            res = self.db.client.table('analyst_data').select('next_earnings_date').eq('stock_id', stock_id).execute()
            stored = res.data[0].get('next_earnings_date') if res.data else None
            stored = str(stored)[:10] if stored else None
            return stored != fresh
        except Exception as e:
            logger.warning(f"Error checking earnings-date freshness: {e}")
            return False

    def _is_ownership_data_updated_this_week(self, stock_id: int) -> bool:
        """Check if ownership/short interest data was updated in the last 7 days."""
        try:
            from datetime import datetime
            res = self.db.client.table('ownership_data').select('updated_at').eq('stock_id', stock_id).order('updated_at', desc=True).limit(1).execute()
            if res.data and res.data[0].get('updated_at'):
                val = res.data[0]['updated_at']
                if isinstance(val, str):
                    updated_at = datetime.fromisoformat(val.replace('Z', '+00:00'))
                else:
                    updated_at = val
                now = datetime.now(updated_at.tzinfo) if updated_at.tzinfo else datetime.now()
                return (now - updated_at).days < 7
        except Exception as e:
            logger.warning(f"Error checking weekly ownership update status: {e}")
        return False
    
    def _profile_needs_update(self, ticker: str, stock_row: Dict, collected_data: Dict) -> bool:
        """
        Whether the full company profile should be written to ``stocks``.

        Written when the row has no profile yet, when the collector fetched a fresh profile (even if
        this worker had no cache), or when the row was last updated before the symbol's refresh cutoff.
        """
        if collected_data.get('profile_refreshed'):
            return True
        if not stock_row.get('company_name') or not stock_row.get('updated_at'):
            return True
        # updated_at moves on every sync, so a row blanked by an empty Yahoo profile
        # (company_name = ticker, no sector) must be re-written whenever a profile is at hand.
        if not stock_row.get('sector') or stock_row.get('company_name') == ticker:
            return True
        try:
            return as_utc_date(stock_row['updated_at']) < refresh_cutoff(ticker)
        except Exception as e:
            logger.warning(f"Error checking company profile update status: {e}")
            return True

    def _are_insider_transactions_current(self, stock_id: int, ticker: str) -> bool:
        """
        Check if insider transactions for this stock were written since the symbol's refresh cutoff

        Args:
            stock_id: Database ID of the stock
            ticker: Stock symbol (sets the staggered 30-day refresh slot)

        Returns:
            True if insider transactions were updated since the cutoff, False otherwise
        """
        try:
            cutoff = refresh_cutoff(ticker).isoformat()

            # Check if there are any insider transactions for this stock created since the cutoff
            result = self.db.client.table('insider_transactions')\
                .select('transaction_date')\
                .eq('stock_id', stock_id)\
                .gte('created_at', cutoff)\
                .limit(1)\
                .execute()

            has_data = bool(result.data)
            if has_data:
                logger.info(f"  ℹ Insider transactions already updated since {cutoff} - skipping")
            
            return has_data
            
        except Exception as e:
            logger.warning(f"Error checking insider transactions update status: {e}")
            return False
    

    def _filter_new_records_only(
        self,
        data,
        latest_db_date: str,
        date_column: str = 'Date',
        overlap_days: int = 0,
    ):
        """
        Filter dataframe to only include records newer than the latest database date
        
        Args:
            data: pandas DataFrame with date column
            latest_db_date: Latest date in database (YYYY-MM-DD) or None
            date_column: Name of the date column (default: 'Date')
            
        Returns:
            Filtered DataFrame with only new records
        """
        try:
            import pandas as pd
            from datetime import datetime
            
            if data is None or data.empty:
                return data
                
            if latest_db_date is None:
                # No existing data in DB - return full 2-year filtered dataset
                logger.info(f"  🆕 No existing data in DB - exporting full filtered dataset")
                return data
            
            # Convert to DataFrame if not already
            if not isinstance(data, pd.DataFrame):
                return data
                
            # Check if date column exists
            if date_column not in data.columns:
                logger.warning(f"Date column '{date_column}' not found, returning full dataset")
                return data
            
            # Convert dates for comparison
            data_dates = pd.to_datetime(data[date_column], errors='coerce')
            latest_db_datetime = pd.to_datetime(latest_db_date)
            # Recent upstream history can be corrected. Keep a bounded overlap
            # so the destination upsert can repair those rows by natural key.
            if overlap_days > 0:
                latest_db_datetime -= pd.Timedelta(days=overlap_days)
            
            # Filter to only records newer than latest DB date
            mask = data_dates > latest_db_datetime
            filtered_data = data[mask].copy()
            
            original_count = len(data)
            filtered_count = len(filtered_data)
            
            if filtered_count == 0:
                logger.info(f"  ✅ All data is already in DB - no new records to export")
            else:
                logger.info(f"  🆕 Filtered to new records only: {original_count} → {filtered_count} new records")
            
            return filtered_data
            
        except Exception as e:
            logger.warning(f"Error filtering new records: {e}")
            return data  # Return original data if filtering fails

    def _write_final_sync_markers(self, stock_id: int, status: str, processed_data) -> None:
        """
        Stamp the finished sync on ``stocks`` and move its coverage dates.

        The coverage dates (data_end_date, ...) used to be written only with the 30-day profile refresh, so a
        stock whose profile came from the cache kept the data_end_date of its last refresh. Timestamps are real
        UTC; the Mac and the containers run on IST and a naive now() was stored 5.5 hours ahead.
        """
        now = datetime.now(timezone.utc).isoformat()
        # UPDATE, not UPSERT: only these columns are touched, so no NOT NULL profile column is needed.
        self.db.client.table('stocks').update({
            'updated_at': now,
            'last_sync_date': now,
            'last_sync_status': status,  # success, partial, or failed
            **self.mapper.map_data_coverage(processed_data),
        }).eq('id', stock_id).execute()

    def _verify_stock_sync_persisted(self, stock_id: int, expected_status: str) -> None:
        """Verify the final stock sync marker is readable after the write."""
        verification = self.db.client.table('stocks')\
            .select('id, last_sync_date, last_sync_status')\
            .eq('id', stock_id)\
            .limit(1)\
            .execute()

        if not verification.data:
            raise RuntimeError(
                f"Post-write verification failed: stock ID {stock_id} was not readable"
            )

        row = verification.data[0]
        if not row.get('last_sync_date'):
            raise RuntimeError(
                f"Post-write verification failed: stock ID {stock_id} has no last_sync_date"
            )
        if row.get('last_sync_status') != expected_status:
            raise RuntimeError(
                f"Post-write verification failed: expected status {expected_status!r}, "
                f"got {row.get('last_sync_status')!r} for stock ID {stock_id}"
            )
    
    def _fetch_ttm_cashflow(self, ticker: str):
        """Trailing-twelve-month cash-flow statement (OCF / capex / FCF), or None. One Yahoo request, never fatal."""
        try:
            import yfinance as yf
            from earnings_reports.earnings_history import yahoo_call

            return yahoo_call(lambda: yf.Ticker(ticker).ttm_cashflow)
        except Exception as exc:  # noqa: BLE001 - FCF is left empty rather than failing the stock
            logger.warning(f"  ⚠ TTM cash-flow statement unavailable for {ticker}: {exc}")
            return None

    def _carry_forward_snapshot_size(self, stock_id: int, snapshot_record: Dict) -> None:
        """Fill market cap / EV / share counts that Yahoo left empty today from the last complete snapshot."""
        if all(finite_float(snapshot_record.get(f)) for f in ('market_cap', 'enterprise_value', 'shares_outstanding', 'float_shares')):
            return
        run_sql = getattr(self.db.client, 'execute_raw_sql', None)
        if not callable(run_sql):
            return
        try:
            rows = run_sql(
                "SELECT date, current_price, market_cap, enterprise_value, shares_outstanding, float_shares "
                "FROM market_price_snapshot WHERE stock_id = %s AND market_cap IS NOT NULL AND date < %s "
                "ORDER BY date DESC LIMIT 1",
                [stock_id, snapshot_record.get('date')],
            )
            if rows:
                carry_forward_snapshot_size(snapshot_record, rows[0])
                logger.info("  ℹ Market snapshot size fields were empty; carried forward from the last complete snapshot")
        except Exception as exc:  # noqa: BLE001 - never block the export for a cosmetic fill
            logger.warning(f"  ⚠ Could not carry forward snapshot size fields: {exc}")

    def export_stock_data(self, ticker: str) -> Dict:
        """
        Complete export workflow for a single stock
        
        Args:
            ticker: Stock symbol to export
            
        Returns:
            Dict with export results and statistics
        """
        start_time = time.time()
        
        result = {
            'ticker': ticker,
            'status': 'success',
            'errors': [],
            'records_exported': {},
            'duration_seconds': 0
        }
        
        try:
            logger.info(f"\n{'='*80}")
            logger.info(f"STARTING EXPORT: {ticker}")
            logger.info(f"{'='*80}\n")
            
            # Step 1: Collect data
            logger.info("PHASE 1: DATA COLLECTION")
            logger.info("-" * 40)
            
            collected_data = self.collector.collect_all_data(ticker)
            
            if collected_data['collection_status'] == 'failed':
                result['status'] = 'failed'
                result['errors'] = collected_data['errors']
                logger.error(f"Data collection failed for {ticker}")
                return result
            
            if collected_data['collection_status'] == 'partial':
                result['status'] = 'partial'
                result['errors'] = collected_data['errors']
                logger.warning(f"Data collection partially completed for {ticker}")
            
            # Validate collected data
            is_valid, issues = self.collector.validate_data(collected_data)
            if not is_valid:
                logger.warning(f"Data validation issues: {issues}")
                result['errors'].extend(issues)
                result['status'] = 'partial'

            # A partial collection is not safe to publish table-by-table. Keep
            # the existing public rows untouched; the scheduler will retry it
            # because no successful sync marker is advanced.
            if result['status'] != 'success':
                result['duration_seconds'] = int(time.time() - start_time)
                logger.warning(
                    "Publication blocked for %s: collection/validation status is %s",
                    ticker,
                    result['status'],
                )
                return result
            
            # Step 2: Transform and export data
            logger.info("\nPHASE 2: DATA TRANSFORMATION & EXPORT")
            logger.info("-" * 40)
            
            # Reset mapper stats
            self.mapper.reset_sync_stats()
            
            # 2.1: Export stock metadata (company profile - staggered 30-day refresh)
            logger.info("\n[1/11] Exporting stock metadata...")

            # First, check if the stock already exists and has a valid profile updated since its refresh cutoff
            existing_stock = self.db.client.table('stocks')\
                .select('id, updated_at, company_name, sector')\
                .eq('symbol', ticker)\
                .execute()
            
            stock_id = None
            profile_needs_update = True
            
            if existing_stock.data:
                stock_id = existing_stock.data[0]['id']
                profile_needs_update = self._profile_needs_update(ticker, existing_stock.data[0], collected_data)
            
            # If stock record does not exist in DB, create the basic row first
            if stock_id is None:
                basic_stock_record = {
                    'symbol': ticker,
                    'ticker': ticker,
                    'is_active': True,
                    'sync_enabled': True
                }
                
                stock_result = self.db.client.table('stocks').upsert(
                    basic_stock_record,
                    on_conflict='symbol'
                ).execute()
                
                if not stock_result.data:
                    raise Exception("Failed to insert/update basic stock record")
                
                stock_id = stock_result.data[0]['id']
            
            # Now decide whether to update the full profile or skip
            if not profile_needs_update:
                logger.info("  ⏭️  Company profile already current - skipping profile update")
                
                # Sync markers are written only after all export phases and the
                # sync log succeed; otherwise a later failure could leave a
                # false success marker.
                logger.info("  ✓ Profile skipped; final sync markers deferred")
                result['records_exported']['stocks'] = 0  # Track as skipped
            else:
                # Update full company profile
                stock_record = self.mapper.map_stock_metadata(
                    ticker,
                    collected_data['info'],
                    collected_data['processed_data']
                )
                
                # Ensure we update the correct ID
                stock_record['id'] = stock_id

                if not collected_data.get('info'):
                    # Yahoo sent no profile: keep the stored name/sector/industry, refresh only the coverage dates.
                    logger.warning(f"  ⚠️  Empty company profile for {ticker} - keeping the stored profile")
                    stock_record = {
                        key: stock_record[key]
                        for key in ('id', 'symbol', 'data_start_date', 'data_end_date', 'total_records')
                    }

                stock_result = self.db.client.table('stocks').upsert(
                    stock_record,
                    on_conflict='symbol'
                ).execute()

                logger.info(f"  ✓ Stock profile updated: ID = {stock_id}")
                result['records_exported']['stocks'] = 1
                self.mapper.sync_stats['records_inserted'] += 1
            
            # 2.2: Export daily price data (incremental - only new records)
            logger.info("\n[2/11] Exporting daily price data...")
            
            # Use raw stock_data for daily prices (has all trading days)
            # processed_data is for technical indicators, not all price points
            raw_price_data = collected_data.get('stock_data')
            
            if raw_price_data is None or (hasattr(raw_price_data, 'empty') and raw_price_data.empty):
                logger.info(f"  ℹ️ No raw price data available, using processed_data")
                raw_price_data = collected_data.get('processed_data')
            
            # First filter to last 2 years for database storage policy
            filtered_price_data = self._filter_last_two_years(
                raw_price_data, 
                'Date'
            )
            
            logger.info(f"  📅 Raw data: {len(raw_price_data) if hasattr(raw_price_data, '__len__') else 'N/A'} rows")
            logger.info(f"  📅 Filtered data: {len(filtered_price_data) if hasattr(filtered_price_data, '__len__') else 'N/A'} rows (last 2 years only)")
            
            # Get latest date in database for this stock
            latest_price_date = self._get_latest_data_date(stock_id, 'daily_price_data', 'date')
            logger.info(f"  📅 Latest daily_price_data date in DB: {latest_price_date}")
            
            # Filter to only new records (newer than latest DB date)
            incremental_price_data = self._filter_new_records_only(
                filtered_price_data, 
                latest_price_date, 
                'Date',
                overlap_days=7,
            )
            
            if incremental_price_data is not None and len(incremental_price_data) > 0:
                logger.info(f"  📅 Found {len(incremental_price_data)} new trading days to export")
                price_records = self.mapper.map_daily_prices(
                    stock_id,
                    incremental_price_data
                )
                
                # Batch insert only new price records
                batch_size = 1000
                total_inserted = 0
                for i in range(0, len(price_records), batch_size):
                    batch = price_records[i:i+batch_size]
                    self.db.client.table('daily_price_data').upsert(
                        batch,
                        on_conflict='stock_id,date'
                    ).execute()
                    total_inserted += len(batch)
                    if len(price_records) > batch_size:  # Only show progress for large batches
                        logger.info(f"  Progress: {total_inserted}/{len(price_records)} records...")
                
                logger.info(f"  ✓ Exported {total_inserted} new price records")
                result['records_exported']['daily_price_data'] = total_inserted
                self.mapper.sync_stats['records_inserted'] += total_inserted
            else:
                logger.info(f"  ✅ No new price data to export (all data already in database)")
                result['records_exported']['daily_price_data'] = 0
            
            # 2.3: Export technical indicators (incremental - only new records)
            logger.info("\n[3/11] Exporting technical indicators...")
            
            # Get latest date in database for this stock's technical indicators
            latest_indicators_date = self._get_latest_data_date(stock_id, 'technical_indicators', 'date')
            logger.info(f"  📅 Latest technical_indicators date in DB: {latest_indicators_date}")
            
            # Filter processed_data (has technical indicators) to last 2 years
            processed_filtered = self._filter_last_two_years(
                collected_data.get('processed_data'),
                'Date'
            )
            
            # Filter to only new records (newer than latest DB date)
            # MUST use processed_data which has technical indicator columns (SMA, RSI, MACD, etc.)
            incremental_indicators_data = self._filter_new_records_only(
                processed_filtered,
                latest_indicators_date, 
                'Date',
                overlap_days=7,
            )
            
            if incremental_indicators_data is not None and len(incremental_indicators_data) > 0:
                tech_records = self.mapper.map_technical_indicators(
                    stock_id,
                    incremental_indicators_data
                )
                
                # Batch insert only new indicator records
                batch_size = 1000
                total_inserted = 0
                for i in range(0, len(tech_records), batch_size):
                    batch = tech_records[i:i+batch_size]
                    self._upsert_required_schema(
                        table_name='technical_indicators',
                        records=batch,
                        on_conflict='stock_id,date'
                    )
                    total_inserted += len(batch)
                    if len(tech_records) > batch_size:  # Only show progress for large batches
                        logger.info(f"  Progress: {total_inserted}/{len(tech_records)} records...")
                
                logger.info(f"  ✓ Exported {total_inserted} new indicator records")
                result['records_exported']['technical_indicators'] = total_inserted
                self.mapper.sync_stats['records_inserted'] += total_inserted
            else:
                logger.info(f"  ✅ No new technical indicators to export")
                result['records_exported']['technical_indicators'] = 0
            
            # 2.4: Export analyst data (one record per stock)
            logger.info("\n[4/11] Exporting analyst data...")
            if (
                self._is_analyst_data_updated_this_week(stock_id)
                and not self._analyst_earnings_date_is_stale(stock_id, collected_data['info'])
            ):
                logger.info("  ⏭️  Analyst data already updated this week - skipping")
                result['records_exported']['analyst_data'] = 0
            else:
                analyst_record = self.mapper.map_analyst_data(
                    stock_id,
                    collected_data['info']
                )
                
                try:
                    exist_check = self.db.client.table('analyst_data').select('id').eq('stock_id', stock_id).execute()
                    if exist_check.data:
                        self.db.client.table('analyst_data').update(analyst_record).eq('stock_id', stock_id).execute()
                        logger.info(f"  ✓ Updated analyst information")
                    else:
                        self.db.client.table('analyst_data').insert(analyst_record).execute()
                        logger.info(f"  ✓ Inserted analyst information")
                    result['records_exported']['analyst_data'] = 1
                    self.mapper.sync_stats['records_inserted'] += 1
                except Exception as e:
                    logger.warning(f"  ⚠️  Could not export analyst data: {e}")
                    result['records_exported']['analyst_data'] = 0
            
            # 2.4b: Earnings history + price reactions (Azure only; skipped if checked within 24h).
            # Non-fatal: an earnings-data problem must never fail the stock's main export.
            try:
                if callable(getattr(self.db.client, 'execute_raw_sql', None)):
                    from database.export_earnings_history import refresh_if_stale
                    earnings_summary = refresh_if_stale(self.db.client, {'id': stock_id, 'symbol': ticker.upper()})
                    if earnings_summary is None:
                        logger.info("  ⏭️  Earnings history checked within 24h - skipping")
                        result['records_exported']['earnings_history'] = 0
                    else:
                        logger.info(f"  ✓ Earnings history: {earnings_summary.get('rows', 0)} quarters "
                                    f"({earnings_summary.get('with_reaction', 0)} with price reaction)")
                        result['records_exported']['earnings_history'] = earnings_summary.get('rows', 0)
                        self.mapper.sync_stats['records_inserted'] += earnings_summary.get('rows', 0)
            except Exception as e:
                logger.warning(f"  ⚠️  Could not export earnings history: {e}")
                result['records_exported']['earnings_history'] = 0

            # 2.4c: Analyst detail behind the forecast tab (rating breakdown, firm actions, EPS/revenue estimates).
            # Azure only; skipped if fetched within the week. Non-fatal for the same reason as 2.4b.
            try:
                if callable(getattr(self.db.client, 'execute_raw_sql', None)):
                    from database.export_analyst_detail import refresh_if_stale as refresh_analyst_detail
                    detail_summary = refresh_analyst_detail(self.db.client, {'id': stock_id, 'symbol': ticker.upper()})
                    if detail_summary is None:
                        logger.info("  ⏭️  Analyst detail fetched this week - skipping")
                        result['records_exported']['analyst_detail_data'] = 0
                    else:
                        logger.info(f"  ✓ Analyst detail: {', '.join(detail_summary.get('datasets', [])) or 'nothing available'}")
                        result['records_exported']['analyst_detail_data'] = 1
                        self.mapper.sync_stats['records_inserted'] += 1
            except Exception as e:
                logger.warning(f"  ⚠️  Could not export analyst detail: {e}")
                result['records_exported']['analyst_detail_data'] = 0

            # 2.5: Export forecast data (12 monthly forecasts - staggered 30-day refresh)
            logger.info("\n[5/11] Exporting forecast data...")
            
            # Check if forecast data is already current
            if self._is_forecast_current(stock_id, ticker):
                logger.info("  ⏭️  Forecast data already current - skipping")
                result['records_exported']['forecast_data'] = 0  # Track as skipped
            elif collected_data.get('forecast_data') is not None:
                forecast_records = self.mapper.map_forecast_data(
                    stock_id,
                    collected_data['forecast_data'],
                    collected_data['info'],
                    collected_data.get('forecast_metadata'),
                )
                
                if forecast_records:
                    # Azure replacement is a single transaction so a failed
                    # insert cannot expose an empty forecast table.
                    replace_atomic = getattr(self.db.client, 'replace_rows_atomic', None)
                    if callable(replace_atomic):
                        replace_atomic('forecast_data', {'stock_id': stock_id}, forecast_records)
                    else:
                        # Legacy PostgREST fallback is non-destructive: publish
                        # the new version without deleting the last-known-good.
                        self.db.client.table('forecast_data').upsert(
                            forecast_records,
                            on_conflict='stock_id,forecast_date,forecast_period',
                        ).execute()
                    logger.info(f"  ✓ Exported {len(forecast_records)} forecast records")
                    result['records_exported']['forecast_data'] = len(forecast_records)
                    self.mapper.sync_stats['records_inserted'] += len(forecast_records)
                else:
                    logger.info("  ℹ No forecast data to export")
            else:
                logger.info("  ℹ Forecast data not available")
            
            # 2.6: Export fundamental data
            logger.info("\n[6/12] Exporting fundamental data...")
            try:
                fundamental_record = self.mapper.map_fundamental_data(
                    stock_id,
                    collected_data['info'],
                    collected_data.get('balance_sheet'),
                    collected_data.get('financials'),
                    ticker=ticker,
                    ttm_cashflow=self._fetch_ttm_cashflow(ticker),
                )
            except PartialProviderData as exc:
                # A stub payload must not replace the last good row (it used to: AVGO/JPM/DHR were blanked).
                logger.warning(f"  ⚠ Skipping fundamental data, nothing written: {exc}")
                fundamental_record = None

            if fundamental_record is not None:
                self.db.client.table('fundamental_data').upsert(
                    [fundamental_record],
                    on_conflict='stock_id,period_date,period_type'
                ).execute()
                logger.info(f"  ✓ Exported fundamental metrics")
                result['records_exported']['fundamental_data'] = 1
                self.mapper.sync_stats['records_inserted'] += 1
            
            # 2.6b: Export quarterly fundamental data
            logger.info("\n[6b/12] Exporting quarterly fundamental data...")
            if collected_data.get('quarterly_earnings') is not None:
                quarterly_records = self.mapper.map_quarterly_fundamental_data(
                    stock_id,
                    collected_data['quarterly_earnings']
                )
                
                if quarterly_records:
                    self.db.client.table('fundamental_data').upsert(
                        quarterly_records,
                        on_conflict='stock_id,period_date,period_type'
                    ).execute()
                    logger.info(f"  ✓ Exported {len(quarterly_records)} quarterly fundamental records")
                    result['records_exported']['quarterly_fundamental_data'] = len(quarterly_records)
                    self.mapper.sync_stats['records_inserted'] += len(quarterly_records)
                else:
                    logger.info("  ℹ No quarterly fundamental data to export")
            else:
                logger.info("  ℹ Quarterly data not available")
            
            # 2.7: Export risk data (basic + advanced metrics)
            logger.info("\n[7/12] Exporting risk data...")
            
            # Calculate comprehensive risk profile using RiskAnalyzer (with metadata)
            risk_profile = None
            try:
                price_data = collected_data['processed_data']['Close']
                
                # Get market data (S&P 500) if available
                market_data = None
                if 'SP500' in collected_data['processed_data'].columns:
                    market_data = collected_data['processed_data']['SP500']
                
                # Calculate comprehensive risk profile (with metadata)
                risk_profile = self.risk_analyzer.comprehensive_risk_profile_with_metadata(
                    price_data=price_data,
                    market_data=market_data,
                    ticker=ticker,
                    info=collected_data['info'],
                    processed_data=collected_data['processed_data'],
                    balance_sheet=collected_data.get('balance_sheet'),
                    financials=collected_data.get('financials')
                )
                logger.info("  ✓ Calculated comprehensive risk profile (with metadata)")
            except Exception as e:
                logger.warning(f"  ⚠ Could not calculate comprehensive risk profile: {e}")
                risk_profile = None
            
            # Map and export basic risk data (includes CVaR and volatility)
            risk_record = self.mapper.map_risk_data(
                stock_id,
                collected_data['processed_data'],
                collected_data['info'],
                risk_profile=risk_profile,
                market_data=None,  # TODO: Add S&P 500 data if available
                ticker=ticker  # Pass ticker for liquidity/Altman metadata
            )
            
            self.db.client.table('risk_data').upsert(
                [risk_record],
                on_conflict='stock_id,date'
            ).execute()
            logger.info(f"  ✓ Exported enhanced risk metrics (basic)")
            result['records_exported']['risk_data'] = 1
            self.mapper.sync_stats['records_inserted'] += 1
            
            # Export advanced risk tables (if data available)
            if risk_profile and 'metrics' in risk_profile:
                metrics = risk_profile['metrics']
                # 2.7a: Export liquidity risk data
                liquidity_record = self.mapper.map_liquidity_risk_data(stock_id, metrics)
                if liquidity_record:
                    self.db.client.table('liquidity_risk_data').upsert(
                        [liquidity_record],
                        on_conflict='stock_id,date'
                    ).execute()
                    logger.info(f"  ✓ Exported liquidity risk metrics")
                    result['records_exported']['liquidity_risk_data'] = 1
                    self.mapper.sync_stats['records_inserted'] += 1
                else:
                    logger.info("  ℹ No liquidity risk data available")
                
                # 2.7b: Export Altman Z-Score data
                altman_record = self.mapper.map_altman_zscore_data(stock_id, metrics)
                if altman_record:
                    self.db.client.table('altman_zscore_data').upsert(
                        [altman_record],
                        on_conflict='stock_id,date'
                    ).execute()
                    logger.info(f"  ✓ Exported Altman Z-Score metrics")
                    result['records_exported']['altman_zscore_data'] = 1
                    self.mapper.sync_stats['records_inserted'] += 1
                else:
                    logger.info("  ℹ No Altman Z-Score data available")
                
                # 2.7c: Export regime risk data
                regime_record = self.mapper.map_regime_risk_data(stock_id, metrics)
                if regime_record:
                    self.db.client.table('regime_risk_data').upsert(
                        [regime_record],
                        on_conflict='stock_id,date'
                    ).execute()
                    logger.info(f"  ✓ Exported regime risk metrics")
                    result['records_exported']['regime_risk_data'] = 1
                    self.mapper.sync_stats['records_inserted'] += 1
                else:
                    logger.info("  ℹ No regime risk data available")
            else:
                logger.info("  ℹ Skipping advanced risk exports (no risk profile)")
            
            # 2.8: Export market snapshot
            logger.info("\n[8/12] Exporting market snapshot...")
            snapshot_record = self.mapper.map_market_snapshot(
                stock_id,
                collected_data['current_price'],
                collected_data['info'],
                collected_data['processed_data']
            )
            self._carry_forward_snapshot_size(stock_id, snapshot_record)

            self.db.client.table('market_price_snapshot').upsert(
                [snapshot_record],
                on_conflict='stock_id,date'
            ).execute()
            logger.info(f"  ✓ Exported market snapshot")
            result['records_exported']['market_price_snapshot'] = 1
            self.mapper.sync_stats['records_inserted'] += 1
            
            # 2.9: Export dividend data (FORCED UPDATE due to recent dividend bug fixes)
            logger.info("\n[9/12] Exporting dividend data...")
            logger.info("  🔄 Force updating dividend data (dividend formatting bug recently fixed)")
            
            dividend_record = self.mapper.map_dividend_data(
                stock_id,
                collected_data['info'],
                ticker=ticker
            )
            
            try:
                exist_check = self.db.client.table('dividend_data').select('id').eq('stock_id', stock_id).execute()
                if exist_check.data:
                    self.db.client.table('dividend_data').update(dividend_record).eq('stock_id', stock_id).execute()
                    logger.info(f"  ✓ Force updated dividend data (update)")
                else:
                    self.db.client.table('dividend_data').insert(dividend_record).execute()
                    logger.info(f"  ✓ Force inserted dividend data (insert)")
                result['records_exported']['dividend_data'] = 1
                self.mapper.sync_stats['records_inserted'] += 1
            except Exception as e:
                logger.warning(f"  ⚠️  Could not update dividend data: {e}")
                result['records_exported']['dividend_data'] = 0
            
            # 2.10: Export ownership data
            logger.info("\n[10/12] Exporting ownership data...")
            if self._is_ownership_data_updated_this_week(stock_id):
                logger.info("  ⏭️  Ownership data already updated this week - skipping")
                result['records_exported']['ownership_data'] = 0
            else:
                ownership_record = self.mapper.map_ownership_data(
                    stock_id,
                    collected_data['info']
                )
                
                self.db.client.table('ownership_data').upsert(
                    [ownership_record],
                    on_conflict='stock_id,report_date'
                ).execute()
                logger.info(f"  ✓ Exported ownership data")
                result['records_exported']['ownership_data'] = 1
                self.mapper.sync_stats['records_inserted'] += 1
            
            # 2.11: Export sentiment data
            logger.info("\n[11/12] Exporting sentiment data...")
            sentiment_record = self.mapper.map_sentiment_data(
                stock_id,
                collected_data.get('news', []),
                collected_data['info']
            )
            
            self.db.client.table('sentiment_data').upsert(
                [sentiment_record],
                on_conflict='stock_id,date'
            ).execute()
            logger.info(f"  ✓ Exported sentiment data")
            result['records_exported']['sentiment_data'] = 1
            self.mapper.sync_stats['records_inserted'] += 1
            
            # 2.11b: Export stock-specific news data (NEW - Force update on every run)
            logger.info("\n[11b/12] Exporting stock-specific news articles...")
            logger.info("  🔄 Force updating news table with latest 15-20 articles")
            
            news_articles = collected_data.get('news', [])
            logger.info(f"  🔍 News data type from collected_data: {type(news_articles).__name__}, size: {len(news_articles) if hasattr(news_articles, '__len__') else 'N/A'}")
            
            if news_articles and isinstance(news_articles, list) and len(news_articles) > 0:
                logger.info(f"  🔍 Passing {len(news_articles)} articles to mapper...")
                news_records = self.mapper.map_stock_news_data(
                    stock_id,
                    news_articles,
                    ticker,
                    company_name=collected_data.get('info', {}).get('shortName')
                    or collected_data.get('info', {}).get('longName'),
                )
                
                if news_records:
                    try:
                        # Delete old news for this stock (keep only recent ones)
                        # Keep last 60 days of news, delete older
                        from datetime import datetime as dt, timedelta
                        cutoff_date = (dt.now() - timedelta(days=60)).isoformat()
                        
                        self.db.client.table('stock_news_data')\
                            .delete()\
                            .eq('stock_id', stock_id)\
                            .lt('published_date', cutoff_date)\
                            .execute()
                        
                        # Insert new news records (upsert to avoid duplicates)
                        batch_size = 100
                        total_inserted = 0
                        
                        for i in range(0, len(news_records), batch_size):
                            batch = news_records[i:i+batch_size]
                            self.db.client.table('stock_news_data').upsert(
                                batch,
                                on_conflict='stock_id,url,published_date'
                            ).execute()
                            total_inserted += len(batch)
                        
                        logger.info(f"  ✓ Exported {total_inserted} news articles (15-20 most recent)")
                        result['records_exported']['stock_news_data'] = total_inserted
                        self.mapper.sync_stats['records_inserted'] += total_inserted
                        
                    except Exception as e:
                        logger.warning(f"  ⚠️  Could not export news articles: {e}")
                        result['records_exported']['stock_news_data'] = 0
                else:
                    logger.info("  ℹ No news articles to export")
                    result['records_exported']['stock_news_data'] = 0
            else:
                logger.info("  ℹ No news data available")
                result['records_exported']['stock_news_data'] = 0
            
            # 2.12: Export insider transactions (staggered 30-day refresh)
            logger.info("\n[12/12] Exporting insider transactions...")
            
            # Check if insider transactions are already current
            if self._are_insider_transactions_current(stock_id, ticker):
                logger.info("  ⏭️  Insider transactions already current - skipping")
                result['records_exported']['insider_transactions'] = 0  # Track as skipped
            elif collected_data.get('insider_transactions') is not None and not collected_data['insider_transactions'].empty:
                insider_records = self.mapper.map_insider_transactions(
                    stock_id,
                    collected_data['insider_transactions']
                )
                
                if insider_records:
                    # Sanitize records to remove NaN/Infinity before JSON serialization
                    insider_records = self._sanitize_records_for_json(insider_records)
                    
                    # Yahoo rows carry no shares-owned-after, so the unique indexes (which separate the holdings of
                    # one SEC filing by ownership_key) cannot recognise a trade already stored from the SEC feed:
                    # skip every (insider, date, shares) this stock already has, then insert the rest.
                    existing = self.db.client.table('insider_transactions')\
                        .select('insider_name,transaction_date,shares_change')\
                        .eq('stock_id', stock_id)\
                        .limit(50000)\
                        .execute()
                    stored = {
                        (str(r.get('insider_name')), str(r.get('transaction_date'))[:10], float(r.get('shares_change') or 0))
                        for r in (existing.data or [])
                    }
                    insider_records = [
                        r for r in insider_records
                        if (str(r.get('insider_name')), str(r.get('transaction_date'))[:10],
                            float(r.get('shares_change') or 0)) not in stored
                    ]

                    # Batch upsert insider transactions (ON CONFLICT DO NOTHING)
                    total_inserted = 0
                    batch_size = 1000
                    for i in range(0, len(insider_records), batch_size):
                        batch = insider_records[i:i+batch_size]
                        has_accessions = all(record.get('accession_number') for record in batch)
                        conflict_key = (
                            'stock_id,accession_number,transaction_date,shares_change,ownership_key'
                            if has_accessions else
                            'stock_id,insider_name,transaction_date,shares_change,ownership_key'
                        )
                        self.db.client.table('insider_transactions').upsert(
                            batch,
                            on_conflict=conflict_key,
                            ignore_duplicates=True
                        ).execute()
                        total_inserted += len(batch)
                    
                    logger.info(f"  ✓ Exported {total_inserted} insider transactions (duplicates ignored)")
                    result['records_exported']['insider_transactions'] = total_inserted
                    self.mapper.sync_stats['records_inserted'] += total_inserted
                else:
                    logger.info("  ℹ No insider transactions to export")
            else:
                logger.info("  ℹ Insider transactions data not available")
            
            # 2.13: Export peer comparison data
            logger.info("\n[13/13] Exporting peer comparison data...")
            if collected_data.get('peer_comparison'):
                peer_records = self.mapper.map_peer_comparison_data(
                    stock_id,
                    collected_data['peer_comparison'],
                    ticker
                )
                
                if peer_records:
                    replace_atomic = getattr(self.db.client, 'replace_rows_atomic', None)
                    if callable(replace_atomic):
                        replace_atomic('peer_comparison_data', {'stock_id': stock_id}, peer_records)
                    else:
                        # Do not delete first on legacy clients; stale extras are
                        # safer than exposing an empty peer section on failure.
                        self.db.client.table('peer_comparison_data').upsert(
                            peer_records,
                            on_conflict='stock_id,peer_ticker',
                        ).execute()
                    logger.info(f"  ✓ Exported {len(peer_records)} peer comparison records")
                    result['records_exported']['peer_comparison_data'] = len(peer_records)
                    self.mapper.sync_stats['records_inserted'] += len(peer_records)
                else:
                    logger.info("  ℹ No peer comparison data to export")
            else:
                logger.info("  ℹ Peer comparison data not available")

            # Publish a compact canonical read-model only after every required
            # table write above has completed. The previous current snapshot is
            # retained if this publication fails, preserving last-known-good
            # semantics for readers.
            market_price_records = self._sanitize_records_for_json(
                [snapshot_record] if isinstance(snapshot_record, dict) else []
            )
            peer_snapshot_records = self._snapshot_peer_records(
                peer_records if 'peer_records' in locals() else [],
                collected_data.get('peer_comparison') or {},
            )
            snapshot_payload = {
                'ticker': ticker,
                'stock': self._sanitize_records_for_json([self._snapshot_stock_metadata(
                    ticker,
                    collected_data.get('info', {}),
                )])[0],
                'market_price': market_price_records[0] if market_price_records else {},
                'forecast': self._sanitize_records_for_json(forecast_records if 'forecast_records' in locals() else []),
                # Include the validated point-in-time records needed by the
                # ticker adapter.  Historical series remain in their source
                # tables; this keeps the canonical read model bounded while
                # allowing consumers to load one internally consistent batch.
                'analyst': self._sanitize_records_for_json([analyst_record] if 'analyst_record' in locals() else []),
                'fundamental': self._sanitize_records_for_json([fundamental_record] if 'fundamental_record' in locals() else []),
                'risk': self._sanitize_records_for_json([risk_record] if 'risk_record' in locals() else []),
                'liquidity_risk': self._sanitize_records_for_json([liquidity_record] if 'liquidity_record' in locals() else []),
                'altman_zscore': self._sanitize_records_for_json([altman_record] if 'altman_record' in locals() else []),
                'regime_risk': self._sanitize_records_for_json([regime_record] if 'regime_record' in locals() else []),
                'sentiment': self._sanitize_records_for_json([sentiment_record] if 'sentiment_record' in locals() else []),
                'dividend': self._sanitize_records_for_json([dividend_record] if 'dividend_record' in locals() else []),
                'ownership': self._sanitize_records_for_json([ownership_record] if 'ownership_record' in locals() else []),
                'news': self._sanitize_records_for_json(news_records if 'news_records' in locals() else []),
                'peers': self._sanitize_records_for_json(peer_snapshot_records),
                'schema_version': SCHEMA_VERSION,
                'calculation_version': CALCULATION_VERSION,
            }
            snapshot_validation_errors = self._validate_snapshot_payload(snapshot_payload)
            if snapshot_validation_errors:
                logger.error("  ✗ Canonical snapshot rejected: %s", snapshot_validation_errors)
                result['errors'].extend(
                    f"canonical_snapshot: {error}" for error in snapshot_validation_errors
                )
                result['status'] = 'partial'
                result['records_exported']['stock_page_snapshot'] = 0
            else:
                snapshot_row = {
                    'stock_id': stock_id,
                    'ticker': ticker,
                    'schema_version': SCHEMA_VERSION,
                    'calculation_version': CALCULATION_VERSION,
                    'source_batch_id': f"{ticker}-{datetime.now().strftime('%Y%m%d%H%M%S')}",
                    'market_price_as_of': snapshot_payload['market_price'].get('date'),
                    'fundamentals_period_end': (
                        snapshot_payload['fundamental'][0].get('period_date')
                        if snapshot_payload.get('fundamental') else None
                    ),
                    'generated_at': datetime.now().isoformat(),
                    'quality_status': result['status'],
                    'quality_score': 100 if result['status'] == 'success' else 0,
                    'validation_errors': result['errors'],
                    'snapshot_json': snapshot_payload,
                    'content_hash': stable_content_hash(snapshot_payload),
                    'is_current': result['status'] == 'success',
                }
                try:
                    publish_snapshot = getattr(self.db.client, 'publish_stock_page_snapshot', None)
                    if callable(publish_snapshot):
                        publish_snapshot(snapshot_row)
                    else:
                        self.db.client.table('stock_page_snapshot').upsert(
                            [snapshot_row], on_conflict='stock_id,content_hash'
                        ).execute()
                    result['records_exported']['stock_page_snapshot'] = 1
                    self.mapper.sync_stats['records_inserted'] += 1
                except Exception as snapshot_error:
                    # Keep compatibility with pre-migration deployments while
                    # surfacing the missing publication capability in diagnostics.
                    logger.warning("  ⚠️ Canonical snapshot publication unavailable: %s", snapshot_error)
                    result['errors'].append(f'canonical_snapshot: {snapshot_error}')
                    result['status'] = 'partial'

            # Step 3: Create sync log
            logger.info("\nPHASE 3: SYNC LOGGING")
            logger.info("-" * 40)
            
            duration = int(time.time() - start_time)
            sync_log = self.mapper.create_sync_log(
                stock_id=stock_id,
                sync_type='full_history',
                sync_status=result['status'],
                error_message='; '.join(result['errors']) if result['errors'] else None,
                sync_duration=duration
            )
            
            self.db.client.table('data_sync_log').insert(sync_log).execute()
            logger.info(f"  ✓ Sync log created")
            
            # CRITICAL FIX: Final timestamp update to ensure updated_at and last_sync_date are always current
            logger.info("\nFINAL STEP: Updating sync timestamps")
            logger.info("-" * 40)

            self._write_final_sync_markers(stock_id, result['status'], collected_data.get('processed_data'))

            logger.info(f"  ✓ Final sync timestamps updated for stock ID {stock_id}")

            # Confirm the final marker can be read back from the database. This
            # catches false-positive success states where the write call returns
            # but the update is not actually visible/persisted.
            self._verify_stock_sync_persisted(stock_id, result['status'])
            logger.info(f"  ✓ Post-write verification passed for stock ID {stock_id}")
            
            # Calculate total records
            total_records = sum(result['records_exported'].values())
            result['duration_seconds'] = duration
            
            # Final summary
            logger.info(f"\n{'='*80}")
            logger.info(f"EXPORT COMPLETE: {ticker}")
            logger.info(f"{'='*80}")
            logger.info(f"Status: {result['status'].upper()}")
            logger.info(f"Duration: {duration} seconds")
            logger.info(f"Total Records: {total_records}")
            logger.info(f"Errors: {len(result['errors'])}")
            
            logger.info("\nRecords by Table:")
            for table, count in result['records_exported'].items():
                logger.info(f"  • {table}: {count}")
            
            if result['errors']:
                logger.info("\nErrors Encountered:")
                for error in result['errors']:
                    logger.info(f"  • {error}")
            
            logger.info(f"{'='*80}\n")
            
            return result
            
        except Exception as e:
            duration = int(time.time() - start_time)
            error_msg = f"Critical export error: {e}"
            result['status'] = 'failed'
            result['errors'].append(error_msg)
            result['duration_seconds'] = duration
            
            logger.error(f"\n✗ {error_msg}")
            logger.exception("Full traceback:")
            
            return result
    
    def export_multiple_stocks(self, tickers: List[str]) -> Dict:
        """
        Export data for multiple stocks
        
        Args:
            tickers: List of stock symbols
            
        Returns:
            Dict with aggregated results
        """
        results = {
            'total': len(tickers),
            'successful': 0,
            'partial': 0,
            'failed': 0,
            'details': {}
        }
        
        for ticker in tickers:
            result = self.export_stock_data(ticker)
            results['details'][ticker] = result
            
            if result['status'] == 'success':
                results['successful'] += 1
            elif result['status'] == 'partial':
                results['partial'] += 1
            else:
                results['failed'] += 1
        
        # Print summary
        logger.info(f"\n{'='*80}")
        logger.info("BATCH EXPORT SUMMARY")
        logger.info(f"{'='*80}")
        logger.info(f"Total Stocks: {results['total']}")
        logger.info(f"Successful: {results['successful']}")
        logger.info(f"Partial: {results['partial']}")
        logger.info(f"Failed: {results['failed']}")
        logger.info(f"{'='*80}\n")
        
        return results


def main():
    """Command line interface"""
    parser = argparse.ArgumentParser(
        description='Export stock data to Supabase',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Export single stock:
    python export_to_supabase.py AAPL
  
  Export multiple stocks:
    python export_to_supabase.py AAPL GOOGL MSFT TSLA
        """
    )
    
    parser.add_argument(
        'tickers',
        nargs='+',
        help='Stock ticker symbols to export (e.g., AAPL GOOGL MSFT)'
    )
    
    parser.add_argument(
        '--url',
        help='Supabase project URL (default: from SUPABASE_URL env var)'
    )
    
    parser.add_argument(
        '--key',
        help='Supabase API key (default: from SUPABASE_KEY env var)'
    )
    
    args = parser.parse_args()
    
    try:
        # Initialize exporter
        exporter = SupabaseDataExporter(
            supabase_url=args.url,
            supabase_key=args.key
        )
        
        # Export data
        if len(args.tickers) == 1:
            result = exporter.export_stock_data(args.tickers[0])
            sys.exit(0 if result['status'] in ['success', 'partial'] else 1)
        else:
            results = exporter.export_multiple_stocks(args.tickers)
            sys.exit(0 if results['failed'] == 0 else 1)
            
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.exception("Traceback:")
        sys.exit(1)


if __name__ == '__main__':
    main()
