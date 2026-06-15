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
from datetime import datetime
from typing import List, Dict

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))


from database.pipeline_data_collector import PipelineDataCollector
from database.data_mapper import DataMapper
from database.supabase_client import SupabaseClient
from analysis_scripts.risk_analysis import RiskAnalyzer

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

    def _upsert_with_schema_fallback(self, table_name: str, records: List[Dict], on_conflict: str):
        """
        Upsert records and gracefully handle schema drift by removing unknown columns
        reported by PostgREST (PGRST204).
        """
        if not records:
            return

        working_records = [dict(record) for record in records]
        removed_columns = set()

        for _ in range(10):
            try:
                self.db.client.table(table_name).upsert(
                    working_records,
                    on_conflict=on_conflict
                ).execute()
                return
            except Exception as error:
                unknown_column = self._extract_unknown_column(error, table_name)
                if not unknown_column:
                    raise

                if unknown_column in removed_columns:
                    raise

                removed_columns.add(unknown_column)
                for record in working_records:
                    record.pop(unknown_column, None)

                logger.warning(
                    f"  ⚠ Column '{unknown_column}' missing in '{table_name}' schema; retrying without it"
                )

        raise RuntimeError(
            f"Unable to upsert into {table_name}: too many schema fallback retries"
        )
    
    def _is_forecast_updated_this_month(self, stock_id: int) -> bool:
        """
        Check if forecast data has been updated for this stock in the current month
        
        Args:
            stock_id: Database ID of the stock
            
        Returns:
            True if forecast data exists for this month, False otherwise
        """
        try:
            from datetime import datetime
            current_month = datetime.now().strftime('%Y-%m')
            
            # Check if there's any forecast data for this stock created/updated this month
            result = self.db.client.table('forecast_data')\
                .select('forecast_date')\
                .eq('stock_id', stock_id)\
                .gte('created_at', f'{current_month}-01')\
                .limit(1)\
                .execute()
            
            has_data = bool(result.data)
            if has_data:
                logger.info(f"  ℹ Forecast data already updated this month ({current_month}) - skipping")
            
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
    
    def _is_company_profile_updated_this_month(self, stock_id: int) -> bool:
        """
        Check if company profile data has been updated for this stock in the current month
        
        Args:
            stock_id: Database ID of the stock
            
        Returns:
            True if company profile updated this month, False otherwise
        """
        try:
            from datetime import datetime
            current_month = datetime.now().strftime('%Y-%m')
            
            # Check if stock metadata was updated this month
            result = self.db.client.table('stocks')\
                .select('updated_at')\
                .eq('id', stock_id)\
                .gte('updated_at', f'{current_month}-01')\
                .limit(1)\
                .execute()
            
            has_data = bool(result.data)
            if has_data:
                logger.info(f"  ℹ Company profile already updated this month ({current_month}) - skipping")
            
            return has_data
            
        except Exception as e:
            logger.warning(f"Error checking company profile update status: {e}")
            return False
    
    def _is_insider_transactions_updated_this_month(self, stock_id: int) -> bool:
        """
        Check if insider transactions have been updated for this stock in the current month
        
        Args:
            stock_id: Database ID of the stock
            
        Returns:
            True if insider transactions updated this month, False otherwise
        """
        try:
            from datetime import datetime
            current_month = datetime.now().strftime('%Y-%m')
            
            # Check if there are any insider transactions for this stock created this month
            result = self.db.client.table('insider_transactions')\
                .select('transaction_date')\
                .eq('stock_id', stock_id)\
                .gte('created_at', f'{current_month}-01')\
                .limit(1)\
                .execute()
            
            has_data = bool(result.data)
            if has_data:
                logger.info(f"  ℹ Insider transactions already updated this month ({current_month}) - skipping")
            
            return has_data
            
        except Exception as e:
            logger.warning(f"Error checking insider transactions update status: {e}")
            return False
    

    def _filter_new_records_only(self, data, latest_db_date: str, date_column: str = 'Date'):
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
            
            # Step 2: Transform and export data
            logger.info("\nPHASE 2: DATA TRANSFORMATION & EXPORT")
            logger.info("-" * 40)
            
            # Reset mapper stats
            self.mapper.reset_sync_stats()
            
            # 2.1: Export stock metadata (company profile - monthly update)
            logger.info("\n[1/11] Exporting stock metadata...")
            
            # First, check if the stock already exists and has a valid profile updated this month
            existing_stock = self.db.client.table('stocks')\
                .select('id, updated_at, company_name')\
                .eq('symbol', ticker)\
                .execute()
            
            stock_id = None
            profile_needs_update = True
            
            if existing_stock.data:
                stock_id = existing_stock.data[0]['id']
                company_name = existing_stock.data[0].get('company_name')
                updated_at_val = existing_stock.data[0].get('updated_at')
                
                if company_name and updated_at_val:
                    try:
                        current_month = datetime.now().strftime('%Y-%m')
                        
                        if isinstance(updated_at_val, str):
                            is_updated_this_month = updated_at_val.startswith(current_month)
                        else:
                            is_updated_this_month = updated_at_val.strftime('%Y-%m') == current_month
                            
                        if is_updated_this_month:
                            profile_needs_update = False
                    except Exception as e:
                        logger.warning(f"Error checking company profile update status: {e}")
            
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
                logger.info("  ⏭️  Company profile already updated this month - skipping profile update")
                
                # Even though we skip the full profile update, we still need to update sync timestamps
                # Use UPDATE instead of UPSERT to only touch timestamp fields and avoid NULL violations
                self.db.client.table('stocks').update(
                    {
                        'updated_at': datetime.now().isoformat(),
                        'last_sync_date': datetime.now().isoformat(),
                        'last_sync_status': 'success'
                    }
                ).eq('id', stock_id).execute()
                
                logger.info("  ✓ Updated sync timestamps")
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
                'Date'
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
                'Date'
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
                    self._upsert_with_schema_fallback(
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
            if self._is_analyst_data_updated_this_week(stock_id):
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
            
            # 2.5: Export forecast data (12 monthly forecasts - updated monthly only)
            logger.info("\n[5/11] Exporting forecast data...")
            
            # Check if forecast data was already updated this month
            if self._is_forecast_updated_this_month(stock_id):
                logger.info("  ⏭️  Forecast data already updated this month - skipping")
                result['records_exported']['forecast_data'] = 0  # Track as skipped
            elif collected_data.get('forecast_data') is not None:
                forecast_records = self.mapper.map_forecast_data(
                    stock_id,
                    collected_data['forecast_data'],
                    collected_data['info']
                )
                
                if forecast_records:
                    # Delete existing forecasts for this stock first, then insert new
                    self.db.client.table('forecast_data').delete().eq('stock_id', stock_id).execute()
                    self.db.client.table('forecast_data').insert(forecast_records).execute()
                    logger.info(f"  ✓ Exported {len(forecast_records)} forecast records")
                    result['records_exported']['forecast_data'] = len(forecast_records)
                    self.mapper.sync_stats['records_inserted'] += len(forecast_records)
                else:
                    logger.info("  ℹ No forecast data to export")
            else:
                logger.info("  ℹ Forecast data not available")
            
            # 2.6: Export fundamental data
            logger.info("\n[6/12] Exporting fundamental data...")
            fundamental_record = self.mapper.map_fundamental_data(
                stock_id,
                collected_data['info'],
                collected_data.get('balance_sheet'),
                collected_data.get('financials'),
                ticker=ticker
            )
            
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
                    ticker
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
            
            # 2.12: Export insider transactions (monthly update)
            logger.info("\n[12/12] Exporting insider transactions...")
            
            # Check if insider transactions were already updated this month
            if self._is_insider_transactions_updated_this_month(stock_id):
                logger.info("  ⏭️  Insider transactions already updated this month - skipping")
                result['records_exported']['insider_transactions'] = 0  # Track as skipped
            elif collected_data.get('insider_transactions') is not None and not collected_data['insider_transactions'].empty:
                insider_records = self.mapper.map_insider_transactions(
                    stock_id,
                    collected_data['insider_transactions']
                )
                
                if insider_records:
                    # Sanitize records to remove NaN/Infinity before JSON serialization
                    insider_records = self._sanitize_records_for_json(insider_records)
                    
                    # Batch upsert insider transactions (ON CONFLICT DO NOTHING)
                    total_inserted = 0
                    batch_size = 1000
                    for i in range(0, len(insider_records), batch_size):
                        batch = insider_records[i:i+batch_size]
                        self.db.client.table('insider_transactions').upsert(
                            batch,
                            on_conflict='stock_id,insider_name,transaction_date,shares_change',
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
                    # Delete existing peer data for this stock
                    self.db.client.table('peer_comparison_data').delete().eq('stock_id', stock_id).execute()
                    
                    # Insert new peer data
                    self.db.client.table('peer_comparison_data').upsert(
                        peer_records,
                        on_conflict='stock_id,peer_ticker'
                    ).execute()
                    logger.info(f"  ✓ Exported {len(peer_records)} peer comparison records")
                    result['records_exported']['peer_comparison_data'] = len(peer_records)
                    self.mapper.sync_stats['records_inserted'] += len(peer_records)
                else:
                    logger.info("  ℹ No peer comparison data to export")
            else:
                logger.info("  ℹ Peer comparison data not available")
            
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
            
            # Use UPDATE instead of UPSERT to only touch timestamp fields and avoid NULL violations
            self.db.client.table('stocks').update(
                {
                    'updated_at': datetime.now().isoformat(),
                    'last_sync_date': datetime.now().isoformat(),
                    'last_sync_status': result['status']  # success, partial, or failed
                }
            ).eq('id', stock_id).execute()
            
            logger.info(f"  ✓ Final sync timestamps updated for stock ID {stock_id}")
            
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
