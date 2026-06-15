#!/usr/bin/env python3
"""
Pipeline Data Collector
========================

Data-only pipeline that collects comprehensive stock data without generating reports.
Uses existing pipeline functions to gather all necessary data for Supabase export.

Data Collection:
---------------
1. Historical price data (10+ years)
2. Real-time market data
3. Technical indicators (calculated)
4. Forecast predictions (Prophet)
5. Fundamental metrics
6. Risk analysis
7. Dividend information
8. Ownership data
9. Sentiment analysis
10. Insider transactions

This module focuses purely on data collection and does NOT generate any reports.
"""

import os
import sys
import logging
from datetime import datetime
from typing import Dict, Tuple, Optional, List
import pandas as pd
import yfinance as yf

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import data collection functions
from data_processing_scripts.data_collection import fetch_real_time_data, _yf_ticker
from data_processing_scripts.macro_data import fetch_macro_indicators
from data_processing_scripts.data_preprocessing import preprocess_data
from Models.prophet_model import train_prophet_model
from analysis_scripts.fundamental_analysis import extract_quarterly_earnings_data, extract_peer_comparison_data

logger = logging.getLogger(__name__)


class PipelineDataCollector:
    """
    Collects comprehensive stock data using existing pipeline functions.
    Does NOT generate reports - only collects and prepares data.
    """
    
    def __init__(self, app_root: str = None):
        """
        Initialize data collector
        
        Args:
            app_root: Root directory of the application
        """
        if app_root is None:
            app_root = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
        
        self.app_root = app_root
        self.ticker = None
        self.timestamp = None
        self.db = None

    @property
    def db_client(self):
        """Get database client shim dynamically."""
        if hasattr(self, 'db') and self.db:
            return getattr(self.db, 'client', self.db)
        try:
            from .supabase_client import get_supabase_client
            return get_supabase_client().client
        except Exception:
            return None
    
    def _is_forecast_calculated_this_month(self, ticker: str) -> bool:
        """
        Check if forecast has already been calculated for this ticker this month
        
        Args:
            ticker: Stock symbol
            
        Returns:
            True if forecast calculated this month, False otherwise
        """
        try:
            from datetime import datetime
            from pathlib import Path
            
            current_month = datetime.now().strftime('%Y-%m')
            
            # Check if cached forecast file exists for this month
            cache_dir = Path(self.app_root) / 'generated_data' / 'forecast_cache'
            cache_file = cache_dir / f'{ticker}_forecast_{current_month}.json'
            
            if cache_file.exists():
                logger.info(f"  ℹ Forecast already calculated for {ticker} this month ({current_month}) - using cached")
                return True
                
            return False
            
        except Exception as e:
            logger.warning(f"Error checking forecast cache: {e}")
            return False  # If we can't check, proceed with calculation to be safe
    
    def _save_forecast_cache(self, ticker: str, forecast_df: pd.DataFrame) -> None:
        """
        Save forecast data to monthly cache file and clean up old forecast files
        
        Args:
            ticker: Stock symbol
            forecast_df: Forecast dataframe to cache
        """
        try:
            from datetime import datetime
            from pathlib import Path
            import json
            import os
            
            current_month = datetime.now().strftime('%Y-%m')
            
            # Create cache directory
            cache_dir = Path(self.app_root) / 'generated_data' / 'forecast_cache'
            cache_dir.mkdir(parents=True, exist_ok=True)
            
            # Clean up old forecast files for this ticker (older than this month)
            try:
                for filepath in cache_dir.glob(f'{ticker}_forecast_*.json'):
                    if current_month not in filepath.name:
                        try:
                            os.remove(filepath)
                            logger.info(f"  Removed stale forecast cache file: {filepath.name}")
                        except Exception as fe:
                            logger.debug(f"Failed to clean old forecast file {filepath.name}: {fe}")
            except Exception as ce:
                logger.debug(f"Failed to run forecast cache cleanup: {ce}")

            # Save forecast data as JSON
            cache_file = cache_dir / f'{ticker}_forecast_{current_month}.json'
            forecast_data = {
                'ticker': ticker,
                'date': current_month,
                'created_at': datetime.now().isoformat(),
                'forecast': forecast_df.to_dict('records') if forecast_df is not None else None
            }
            
            with open(cache_file, 'w') as f:
                json.dump(forecast_data, f, default=str)
                
            logger.info(f"  ✓ Cached forecast data for {ticker} ({current_month})")
            
        except Exception as e:
            logger.warning(f"Failed to cache forecast data: {e}")
    
    def _load_forecast_cache(self, ticker: str) -> Optional[pd.DataFrame]:
        """
        Load cached forecast data for this month
        
        Args:
            ticker: Stock symbol
            
        Returns:
            Cached forecast DataFrame or None
        """
        try:
            from datetime import datetime
            from pathlib import Path
            import json
            
            current_month = datetime.now().strftime('%Y-%m')
            
            cache_dir = Path(self.app_root) / 'generated_data' / 'forecast_cache'
            cache_file = cache_dir / f'{ticker}_forecast_{current_month}.json'
            
            if not cache_file.exists():
                return None
                
            with open(cache_file, 'r') as f:
                cache_data = json.load(f)
            
            if cache_data.get('forecast'):
                return pd.DataFrame(cache_data['forecast'])
                
            return None
            
        except Exception as e:
            logger.warning(f"Failed to load forecast cache: {e}")
            return None
    
    def _is_company_profile_cached_this_month(self, ticker: str) -> bool:
        """
        Check if company profile has been cached for this ticker this month
        
        Args:
            ticker: Stock symbol
            
        Returns:
            True if company profile cached this month, False otherwise
        """
        try:
            from datetime import datetime
            from pathlib import Path
            
            current_month = datetime.now().strftime('%Y-%m')
            
            # Check if cached company profile exists for this month
            cache_dir = Path(self.app_root) / 'generated_data' / 'profile_cache'
            cache_file = cache_dir / f'{ticker}_profile_{current_month}.json'
            
            if cache_file.exists():
                logger.info(f"  ℹ Company profile already cached for {ticker} this month ({current_month}) - using cached")
                return True
                
            return False
            
        except Exception as e:
            logger.warning(f"Error checking company profile cache: {e}")
            return False
    
    def _is_insider_transactions_cached_this_month(self, ticker: str) -> bool:
        """
        Check if insider transactions have been cached for this ticker this month
        
        Args:
            ticker: Stock symbol
            
        Returns:
            True if insider transactions cached this month, False otherwise
        """
        try:
            from datetime import datetime
            from pathlib import Path
            
            current_month = datetime.now().strftime('%Y-%m')
            
            # Check if cached insider transactions exist for this month
            cache_dir = Path(self.app_root) / 'generated_data' / 'insider_cache'
            cache_file = cache_dir / f'{ticker}_insider_{current_month}.json'
            
            if cache_file.exists():
                logger.info(f"  ℹ Insider transactions already cached for {ticker} this month ({current_month}) - using cached")
                return True
                
            return False
            
        except Exception as e:
            logger.warning(f"Error checking insider transactions cache: {e}")
            return False
    
    def _save_company_profile_cache(self, ticker: str, profile_data: dict) -> None:
        """
        Save company profile data to monthly cache file
        
        Args:
            ticker: Stock symbol
            profile_data: Company profile dictionary to cache
        """
        try:
            from datetime import datetime
            from pathlib import Path
            import json
            
            current_month = datetime.now().strftime('%Y-%m')
            
            # Create cache directory
            cache_dir = Path(self.app_root) / 'generated_data' / 'profile_cache'
            cache_dir.mkdir(parents=True, exist_ok=True)
            
            # Save profile data as JSON
            cache_file = cache_dir / f'{ticker}_profile_{current_month}.json'
            cache_data = {
                'ticker': ticker,
                'month': current_month,
                'created_at': datetime.now().isoformat(),
                'profile': profile_data
            }
            
            with open(cache_file, 'w') as f:
                json.dump(cache_data, f, default=str)
                
            logger.info(f"  ✓ Cached company profile for {ticker} ({current_month})")
            
        except Exception as e:
            logger.warning(f"Failed to cache company profile: {e}")
    
    def _save_balance_sheet_cache(self, ticker: str, bs_df: pd.DataFrame) -> None:
        """Save balance sheet DataFrame to monthly cache file"""
        try:
            from datetime import datetime
            from pathlib import Path
            import pandas as pd
            
            if bs_df is None or bs_df.empty:
                return
                
            current_month = datetime.now().strftime('%Y-%m')
            cache_dir = Path(self.app_root) / 'generated_data' / 'profile_cache'
            cache_dir.mkdir(parents=True, exist_ok=True)
            cache_file = cache_dir / f'{ticker}_balance_sheet_{current_month}.json'
            
            bs_df.to_json(cache_file, date_format='iso')
            logger.info(f"  ✓ Cached balance sheet for {ticker} ({current_month})")
        except Exception as e:
            logger.warning(f"Failed to save balance sheet cache: {e}")

    def _load_balance_sheet_cache(self, ticker: str) -> Optional[pd.DataFrame]:
        """Load cached balance sheet DataFrame for current month"""
        try:
            from datetime import datetime
            from pathlib import Path
            import pandas as pd
            
            current_month = datetime.now().strftime('%Y-%m')
            cache_dir = Path(self.app_root) / 'generated_data' / 'profile_cache'
            cache_file = cache_dir / f'{ticker}_balance_sheet_{current_month}.json'
            
            if not cache_file.exists():
                return None
                
            return pd.read_json(cache_file)
        except Exception as e:
            logger.warning(f"Failed to load balance sheet cache: {e}")
            return None

    def _save_financials_cache(self, ticker: str, fin_df: pd.DataFrame) -> None:
        """Save financials DataFrame to monthly cache file"""
        try:
            from datetime import datetime
            from pathlib import Path
            import pandas as pd
            
            if fin_df is None or fin_df.empty:
                return
                
            current_month = datetime.now().strftime('%Y-%m')
            cache_dir = Path(self.app_root) / 'generated_data' / 'profile_cache'
            cache_dir.mkdir(parents=True, exist_ok=True)
            cache_file = cache_dir / f'{ticker}_financials_{current_month}.json'
            
            fin_df.to_json(cache_file, date_format='iso')
            logger.info(f"  ✓ Cached financials for {ticker} ({current_month})")
        except Exception as e:
            logger.warning(f"Failed to save financials cache: {e}")

    def _load_financials_cache(self, ticker: str) -> Optional[pd.DataFrame]:
        """Load cached financials DataFrame for current month"""
        try:
            from datetime import datetime
            from pathlib import Path
            import pandas as pd
            
            current_month = datetime.now().strftime('%Y-%m')
            cache_dir = Path(self.app_root) / 'generated_data' / 'profile_cache'
            cache_file = cache_dir / f'{ticker}_financials_{current_month}.json'
            
            if not cache_file.exists():
                return None
                
            return pd.read_json(cache_file)
        except Exception as e:
            logger.warning(f"Failed to load financials cache: {e}")
            return None

    def _save_quarterly_earnings_cache(self, ticker: str, qe_data: dict) -> None:
        """Save quarterly earnings dictionary to monthly cache file"""
        try:
            from datetime import datetime
            from pathlib import Path
            import json
            
            if not qe_data:
                return
                
            current_month = datetime.now().strftime('%Y-%m')
            cache_dir = Path(self.app_root) / 'generated_data' / 'profile_cache'
            cache_dir.mkdir(parents=True, exist_ok=True)
            cache_file = cache_dir / f'{ticker}_quarterly_earnings_{current_month}.json'
            
            with open(cache_file, 'w') as f:
                json.dump(qe_data, f, default=str)
            logger.info(f"  ✓ Cached quarterly earnings for {ticker} ({current_month})")
        except Exception as e:
            logger.warning(f"Failed to save quarterly earnings cache: {e}")

    def _load_quarterly_earnings_cache(self, ticker: str) -> Optional[dict]:
        """Load cached quarterly earnings dictionary for current month"""
        try:
            from datetime import datetime
            from pathlib import Path
            import json
            
            current_month = datetime.now().strftime('%Y-%m')
            cache_dir = Path(self.app_root) / 'generated_data' / 'profile_cache'
            cache_file = cache_dir / f'{ticker}_quarterly_earnings_{current_month}.json'
            
            if not cache_file.exists():
                return None
                
            with open(cache_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load quarterly earnings cache: {e}")
            return None

    def _save_insider_transactions_cache(self, ticker: str, insider_data: pd.DataFrame) -> None:
        """
        Save insider transactions data to monthly cache file
        
        Args:
            ticker: Stock symbol
            insider_data: Insider transactions dataframe to cache
        """
        try:
            from datetime import datetime
            from pathlib import Path
            import json
            
            current_month = datetime.now().strftime('%Y-%m')
            
            # Create cache directory
            cache_dir = Path(self.app_root) / 'generated_data' / 'insider_cache'
            cache_dir.mkdir(parents=True, exist_ok=True)
            
            # Save insider data as JSON
            cache_file = cache_dir / f'{ticker}_insider_{current_month}.json'
            cache_data = {
                'ticker': ticker,
                'month': current_month,
                'created_at': datetime.now().isoformat(),
                'insider_transactions': insider_data.to_dict('records') if not insider_data.empty else []
            }
            
            with open(cache_file, 'w') as f:
                json.dump(cache_data, f, default=str)
                
            logger.info(f"  ✓ Cached insider transactions for {ticker} ({current_month})")
            
        except Exception as e:
            logger.warning(f"Failed to cache insider transactions: {e}")
    
    def _load_company_profile_cache(self, ticker: str) -> Optional[dict]:
        """
        Load cached company profile data for current month
        
        Args:
            ticker: Stock symbol
            
        Returns:
            Cached company profile dict or None
        """
        try:
            from datetime import datetime
            from pathlib import Path
            import json
            
            current_month = datetime.now().strftime('%Y-%m')
            
            cache_dir = Path(self.app_root) / 'generated_data' / 'profile_cache'
            cache_file = cache_dir / f'{ticker}_profile_{current_month}.json'
            
            if not cache_file.exists():
                return None
                
            with open(cache_file, 'r') as f:
                cache_data = json.load(f)
            
            return cache_data.get('profile', {})
            
        except Exception as e:
            logger.warning(f"Failed to load company profile cache: {e}")
            return None
    
    def _load_insider_transactions_cache(self, ticker: str) -> Optional[pd.DataFrame]:
        """
        Load cached insider transactions data for current month
        
        Args:
            ticker: Stock symbol
            
        Returns:
            Cached insider transactions DataFrame or None
        """
        try:
            from datetime import datetime
            from pathlib import Path
            import json
            
            current_month = datetime.now().strftime('%Y-%m')
            
            cache_dir = Path(self.app_root) / 'generated_data' / 'insider_cache'
            cache_file = cache_dir / f'{ticker}_insider_{current_month}.json'
            
            if not cache_file.exists():
                return None
                
            with open(cache_file, 'r') as f:
                cache_data = json.load(f)
            
            insider_records = cache_data.get('insider_transactions', [])
            return pd.DataFrame(insider_records) if insider_records else pd.DataFrame()
            
        except Exception as e:
            logger.warning(f"Failed to load insider transactions cache: {e}")
            return None

    def _is_peer_comparison_cached_this_month(self, ticker: str) -> bool:
        """
        Check if peer comparison has been cached for this ticker this month
        """
        try:
            from datetime import datetime
            from pathlib import Path
            
            current_month = datetime.now().strftime('%Y-%m')
            cache_dir = Path(self.app_root) / 'generated_data' / 'peer_cache'
            cache_file = cache_dir / f'{ticker}_peer_{current_month}.json'
            
            return cache_file.exists()
        except Exception as e:
            logger.warning(f"Error checking peer comparison cache: {e}")
            return False

    def _save_peer_comparison_cache(self, ticker: str, peer_data: dict) -> None:
        """
        Save peer comparison data to monthly cache file
        """
        try:
            from datetime import datetime
            from pathlib import Path
            import json
            
            current_month = datetime.now().strftime('%Y-%m')
            cache_dir = Path(self.app_root) / 'generated_data' / 'peer_cache'
            cache_dir.mkdir(parents=True, exist_ok=True)
            
            cache_file = cache_dir / f'{ticker}_peer_{current_month}.json'
            cache_data = {
                'ticker': ticker,
                'month': current_month,
                'created_at': datetime.now().isoformat(),
                'peer_comparison': peer_data
            }
            
            with open(cache_file, 'w') as f:
                json.dump(cache_data, f, default=str)
                
            logger.info(f"  ✓ Cached peer comparison data for {ticker} ({current_month})")
        except Exception as e:
            logger.warning(f"Failed to cache peer comparison data: {e}")

    def _load_peer_comparison_cache(self, ticker: str) -> Optional[dict]:
        """
        Load cached peer comparison data for current month
        """
        try:
            from datetime import datetime
            from pathlib import Path
            import json
            
            current_month = datetime.now().strftime('%Y-%m')
            cache_dir = Path(self.app_root) / 'generated_data' / 'peer_cache'
            cache_file = cache_dir / f'{ticker}_peer_{current_month}.json'
            
            if not cache_file.exists():
                return None
                
            with open(cache_file, 'r') as f:
                cache_data = json.load(f)
            
            return cache_data.get('peer_comparison', {})
        except Exception as e:
            logger.warning(f"Failed to load peer comparison cache: {e}")
            return None
    
    def _check_forecast_exists_in_database(self, ticker: str) -> bool:
        """
        Check if forecast data already exists in database for this month
        
        Args:
            ticker: Stock symbol
            
        Returns:
            True if forecast data exists in DB for this month, False otherwise
        """
        try:
            from datetime import datetime
            
            # Get current month start date
            current_month_start = datetime.now().strftime('%Y-%m-01')
            
            # Get db client (check self.db or fall back to supabase client)
            if hasattr(self, 'db') and self.db:
                db_client = self.db.client
            else:
                from .supabase_client import get_supabase_client
                db_client = get_supabase_client().client
            
            # Get stock_id for this ticker
            stock_response = db_client.table('stocks').select('id').eq('symbol', ticker).execute()
            
            if not stock_response.data:
                return False
                
            stock_id = stock_response.data[0]['id']
            
            # Check if forecast data exists for this month
            forecast_response = db_client.table('forecast_data')\
                .select('forecast_date')\
                .eq('stock_id', stock_id)\
                .gte('created_at', current_month_start)\
                .limit(1)\
                .execute()
            
            exists = len(forecast_response.data) > 0
            
            if exists:
                logger.info(f"  ℹ️ Database already contains forecast data for {ticker} this month ({current_month_start})")
            
            return exists
            
        except Exception as e:
            logger.warning(f"Error checking database forecast: {e}")
            return False
    
    def collect_all_data(self, ticker: str) -> Dict:
        """
        Collect all stock data using pipeline functions
        
        Args:
            ticker: Stock symbol to collect data for
            
        Returns:
            Dict containing all collected data:
            {
                'ticker': str,
                'timestamp': str,
                'stock_data': pd.DataFrame,  # Historical OHLCV
                'processed_data': pd.DataFrame,  # With technical indicators
                'current_price': Dict,  # Real-time price info
                'forecast_data': pd.DataFrame,  # Prophet predictions
                'info': Dict,  # Company fundamentals
                'news': List[Dict],  # News articles
                'recommendations': pd.DataFrame,  # Analyst recommendations
                'balance_sheet': pd.DataFrame,  # Balance sheet data
                'financials': pd.DataFrame,  # Financial statements
                'collection_status': str,  # success/partial/failed
                'errors': List[str]  # Any errors encountered
            }
        """
        self.ticker = ticker
        self.timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        
        result = {
            'ticker': ticker,
            'timestamp': self.timestamp,
            'collection_status': 'success',
            'errors': []
        }
        
        try:
            logger.info(f"\n{'='*80}")
            logger.info(f"COLLECTING DATA FOR: {ticker}")
            logger.info(f"{'='*80}\n")
            
            # Step 1: Fetch real-time and historical data
            logger.info("Step 1/6: Fetching real-time and historical price data...")
            try:
                real_time_data = fetch_real_time_data(
                    ticker, 
                    self.app_root, 
                    include_price=True, 
                    include_intraday=True
                )
                result['stock_data'] = real_time_data.get('daily_data')
                result['current_price'] = real_time_data.get('current_price_data')
                
                if result['stock_data'] is None or result['stock_data'].empty:
                    raise ValueError(f"No historical data found for {ticker}")
                
                logger.info(f"  ✓ Collected {len(result['stock_data'])} days of price data")
                logger.info(f"  ✓ Current price: ${result['current_price']['current_price']}")
                
            except Exception as e:
                error_msg = f"Failed to fetch price data: {e}"
                result['errors'].append(error_msg)
                logger.error(f"  ✗ {error_msg}")
                result['collection_status'] = 'failed'
                return result
            
            # Step 2: Fetch macroeconomic indicators
            logger.info("\nStep 2/6: Fetching macroeconomic indicators...")
            try:
                macro_data = fetch_macro_indicators(
                    app_root=self.app_root,
                    stock_data=result['stock_data']
                )
                logger.info(f"  ✓ Macro indicators loaded")
            except Exception as e:
                error_msg = f"Failed to fetch macro data: {e}"
                result['errors'].append(error_msg)
                logger.warning(f"  ! {error_msg} - Continuing without macro data")
                macro_data = None
            
            # Step 3: Calculate technical indicators
            logger.info("\nStep 3/6: Calculating technical indicators...")
            try:
                processed_data = preprocess_data(
                    result['stock_data'], 
                    macro_data
                )
                
                if processed_data is None or processed_data.empty:
                    raise ValueError("Preprocessing returned empty data")
                
                result['processed_data'] = processed_data
                
                # Count available indicators
                indicator_cols = [col for col in processed_data.columns if col not in ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close']]
                logger.info(f"  ✓ Calculated {len(indicator_cols)} technical indicators")
                logger.info(f"  ✓ Total rows: {len(processed_data)}")
                
            except Exception as e:
                error_msg = f"Failed to calculate indicators: {e}"
                result['errors'].append(error_msg)
                logger.error(f"  ✗ {error_msg}")
                result['collection_status'] = 'partial'
                result['processed_data'] = result['stock_data']  # Use unprocessed data
            
            # Step 4: Generate forecast using Prophet (with monthly caching)
            logger.info("\nStep 4/6: Generating price forecast...")
            
            forecast_skipped = False
            
            # First check if forecast already exists in database for this month
            forecast_exists_in_db = self._check_forecast_exists_in_database(ticker)
            
            if forecast_exists_in_db:
                # Skip expensive model training if database already has current month's data
                result['forecast_data'] = pd.DataFrame()  # Empty - will be loaded from DB during export
                result['forecast_model'] = None
                result['actual_data'] = None
                forecast_skipped = True
                logger.info(f"  ⏭️  Forecast already exists in database for this month - skipping Prophet training")
            
            # If no database record, check file cache
            elif self._is_forecast_calculated_this_month(ticker):
                # Load cached forecast
                cached_forecast = self._load_forecast_cache(ticker)
                if cached_forecast is not None:
                    result['forecast_data'] = cached_forecast
                    result['forecast_model'] = None  # Model not needed for cached data
                    result['actual_data'] = None
                    forecast_skipped = True
                    logger.info(f"  ⏭️  Using cached forecast data ({len(cached_forecast)} records)")
                else:
                    logger.info("  ⚠️  Cache file found but data invalid - recalculating...")
                    # Proceed to calculation below
            
            # Calculate forecast if not cached or cache invalid
            if not forecast_skipped:
                try:
                    model, forecast, actual_df, forecast_df = train_prophet_model(
                        result['processed_data'].copy(),
                        ticker,
                        forecast_horizon='1y',
                        timestamp=self.timestamp
                    )
                    
                    if forecast_df is None or forecast_df.empty:
                        raise ValueError("Prophet returned empty forecast")
                    
                    result['forecast_data'] = forecast_df
                    result['forecast_model'] = model
                    result['actual_data'] = actual_df
                    
                    # Cache the forecast for future use this month
                    self._save_forecast_cache(ticker, forecast_df)
                    
                    logger.info(f"  ✓ Generated {len(forecast_df)} days of forecast")
                    
                except Exception as e:
                    error_msg = f"Failed to generate forecast: {e}"
                    result['errors'].append(error_msg)
                    logger.error(f"  ✗ {error_msg}")
                    result['collection_status'] = 'partial'
                    result['forecast_data'] = None
            
            # Step 5: Fetch fundamental data in parallel
            logger.info("\nStep 5/6: Fetching fundamental data in parallel...")
            try:
                yf_ticker = _yf_ticker(ticker)
                
                # Check cache statuses first
                profile_cached = self._is_company_profile_cached_this_month(ticker)
                insider_cached = self._is_insider_transactions_cached_this_month(ticker)
                peer_cached = self._is_peer_comparison_cached_this_month(ticker)
                
                # Define fetching functions
                def fetch_info():
                    if profile_cached:
                        logger.info("  ⚡ Skipping info fetch (cached)")
                        return None
                    logger.info("  ⚡ Starting info fetch...")
                    info = yf_ticker.info
                    logger.info("  ⚡ Finished info fetch.")
                    return info or {}

                def fetch_news():
                    logger.info("  ⚡ Starting news fetch...")
                    news = yf_ticker.news if hasattr(yf_ticker, 'news') else []
                    
                    # Incremental filter
                    try:
                        db_client = self.db_client
                        if db_client:
                            stock_res = db_client.table('stocks').select('id').eq('symbol', ticker).execute()
                            if stock_res.data:
                                stock_id = stock_res.data[0]['id']
                                news_res = db_client.table('stock_news_data')\
                                    .select('published_date')\
                                    .eq('stock_id', stock_id)\
                                    .order('published_date', desc=True)\
                                    .limit(1)\
                                    .execute()
                                if news_res.data and news_res.data[0].get('published_date'):
                                    # parse last published date
                                    last_published = pd.to_datetime(news_res.data[0]['published_date'])
                                    if last_published.tzinfo is not None:
                                        last_published = last_published.tz_convert(None)
                                    
                                    logger.info(f"  🔍 Database contains news up to {last_published}. Filtering incrementally.")
                                    
                                    filtered_news = []
                                    for article in news:
                                        content_obj = article.get('content', {})
                                        ts = (article.get('datetime') or 
                                              article.get('providerPublishTime') or
                                              content_obj.get('publishTime'))
                                        
                                        article_time = None
                                        if ts:
                                            try:
                                                if isinstance(ts, (int, float)) and ts > 1000000000:
                                                    article_time = datetime.fromtimestamp(ts)
                                                else:
                                                    article_time = pd.to_datetime(ts)
                                                    if article_time.tzinfo is not None:
                                                        article_time = article_time.tz_convert(None)
                                            except Exception:
                                                pass
                                                
                                        if article_time is None or article_time > last_published:
                                            filtered_news.append(article)
                                    
                                    logger.info(f"  ✓ Filtered news: {len(news)} total → {len(filtered_news)} new incremental articles")
                                    news = filtered_news
                    except Exception as ne:
                        logger.warning(f"Error doing incremental news filtering: {ne}")
                        
                    logger.info("  ⚡ Finished news fetch.")
                    return news

                def fetch_recommendations():
                    if profile_cached:
                        return pd.DataFrame()
                    logger.info("  ⚡ Starting recommendations fetch...")
                    recs = yf_ticker.recommendations if hasattr(yf_ticker, 'recommendations') and yf_ticker.recommendations is not None else pd.DataFrame()
                    logger.info("  ⚡ Finished recommendations fetch.")
                    return recs

                def fetch_balance_sheet():
                    if profile_cached:
                        from pathlib import Path
                        cache_file = Path(self.app_root) / 'generated_data' / 'profile_cache' / f'{ticker}_balance_sheet_{datetime.now().strftime("%Y-%m")}.json'
                        if cache_file.exists():
                            return None
                    logger.info("  ⚡ Starting balance sheet fetch...")
                    bs = yf_ticker.balance_sheet
                    logger.info("  ⚡ Finished balance sheet fetch.")
                    return bs

                def fetch_financials():
                    if profile_cached:
                        from pathlib import Path
                        cache_file = Path(self.app_root) / 'generated_data' / 'profile_cache' / f'{ticker}_financials_{datetime.now().strftime("%Y-%m")}.json'
                        if cache_file.exists():
                            return None
                    logger.info("  ⚡ Starting financials fetch...")
                    fin = yf_ticker.financials
                    logger.info("  ⚡ Finished financials fetch.")
                    return fin

                def fetch_insider():
                    if insider_cached:
                        logger.info("  ⚡ Skipping insider transactions fetch (cached)")
                        return None
                    logger.info("  ⚡ Starting insider transactions fetch...")
                    insider = yf_ticker.insider_transactions if hasattr(yf_ticker, 'insider_transactions') else pd.DataFrame()
                    logger.info("  ⚡ Finished insider transactions fetch.")
                    return insider

                def fetch_dividends():
                    if profile_cached:
                        return pd.DataFrame()
                    logger.info("  ⚡ Starting dividends fetch...")
                    div = yf_ticker.dividends
                    logger.info("  ⚡ Finished dividends fetch.")
                    return div

                def fetch_quarterly():
                    if profile_cached:
                        from pathlib import Path
                        cache_file = Path(self.app_root) / 'generated_data' / 'profile_cache' / f'{ticker}_quarterly_earnings_{datetime.now().strftime("%Y-%m")}.json'
                        if cache_file.exists():
                            return None
                    logger.info("  ⚡ Starting quarterly earnings fetch...")
                    qe = extract_quarterly_earnings_data(result, ticker)
                    logger.info("  ⚡ Finished quarterly earnings fetch.")
                    return qe

                def fetch_peers():
                    if peer_cached:
                        logger.info("  ⚡ Skipping peer comparison fetch (cached)")
                        return None
                    logger.info("  ⚡ Starting peer comparison fetch...")
                    peers = extract_peer_comparison_data(ticker, db_client=self.db_client)
                    logger.info("  ⚡ Finished peer comparison fetch.")
                    return peers

                # Submit to ThreadPoolExecutor
                from concurrent.futures import ThreadPoolExecutor, as_completed
                tasks = {
                    'info': fetch_info,
                    'news': fetch_news,
                    'recommendations': fetch_recommendations,
                    'balance_sheet': fetch_balance_sheet,
                    'financials': fetch_financials,
                    'insider': fetch_insider,
                    'dividends': fetch_dividends,
                    'quarterly': fetch_quarterly,
                    'peers': fetch_peers
                }

                fetched_data = {}
                with ThreadPoolExecutor(max_workers=len(tasks)) as executor:
                    future_to_key = {executor.submit(func): key for key, func in tasks.items()}
                    for future in as_completed(future_to_key):
                        key = future_to_key[future]
                        try:
                            fetched_data[key] = future.result()
                        except Exception as exc:
                            logger.warning(f"  ⚠️  Parallel fetch for '{key}' generated an exception: {exc}")
                            fetched_data[key] = None

                # Process results: info (handle cache & fresh dividends)
                if profile_cached:
                    cached_profile = self._load_company_profile_cache(ticker)
                    result['info'] = cached_profile if cached_profile is not None else {}
                    # Update with fresh dividend fields from fetched info if available
                    fresh_info = fetched_data.get('info')
                    fresh_info = fresh_info if fresh_info is not None else {}
                    dividend_fields = [
                        'dividendYield', 'dividendRate', 'forwardDividendYield', 'forwardDividendRate',
                        'trailingAnnualDividendYield', 'trailingAnnualDividendRate',
                        'fiveYearAvgDividendYield', 'payoutRatio', 'exDividendDate',
                        'lastSplitDate', 'lastSplitFactor'
                    ]
                    for field in dividend_fields:
                        if field in fresh_info:
                            result['info'][field] = fresh_info[field]
                    logger.info(f"  ✓ Processed cached profile with updated dividend fields")
                else:
                    info_val = fetched_data.get('info')
                    result['info'] = info_val if info_val is not None else {}
                    self._save_company_profile_cache(ticker, result['info'])
                    logger.info(f"  ✓ Processed and cached fresh company info")

                # news
                news_data = fetched_data.get('news')
                news_data = news_data if news_data is not None else []
                if isinstance(news_data, dict):
                    if 'result' in news_data:
                        result['news'] = news_data['result'] if isinstance(news_data['result'], list) else []
                    else:
                        result['news'] = list(news_data.values()) if news_data else []
                elif isinstance(news_data, list):
                    result['news'] = news_data
                else:
                    result['news'] = []
                logger.info(f"  ✓ Processed news articles ({len(result['news'])})")

                # recommendations
                result['recommendations'] = fetched_data.get('recommendations') if fetched_data.get('recommendations') is not None else pd.DataFrame()
                
                # balance_sheet
                cached_bs = self._load_balance_sheet_cache(ticker) if profile_cached else None
                if cached_bs is not None:
                    result['balance_sheet'] = cached_bs
                else:
                    result['balance_sheet'] = fetched_data.get('balance_sheet')
                    if result['balance_sheet'] is not None and not result['balance_sheet'].empty:
                        self._save_balance_sheet_cache(ticker, result['balance_sheet'])
                
                # financials
                cached_fin = self._load_financials_cache(ticker) if profile_cached else None
                if cached_fin is not None:
                    result['financials'] = cached_fin
                else:
                    result['financials'] = fetched_data.get('financials')
                    if result['financials'] is not None and not result['financials'].empty:
                        self._save_financials_cache(ticker, result['financials'])

                # insider transactions
                if insider_cached:
                    cached_insider = self._load_insider_transactions_cache(ticker)
                    result['insider_transactions'] = cached_insider if cached_insider is not None else pd.DataFrame()
                else:
                    result['insider_transactions'] = fetched_data.get('insider') if fetched_data.get('insider') is not None else pd.DataFrame()
                    self._save_insider_transactions_cache(ticker, result['insider_transactions'])
                logger.info(f"  ✓ Processed insider transactions ({len(result['insider_transactions'])})")

                # quarterly earnings
                cached_qe = self._load_quarterly_earnings_cache(ticker) if profile_cached else None
                if cached_qe is not None:
                    result['quarterly_earnings'] = cached_qe
                else:
                    result['quarterly_earnings'] = fetched_data.get('quarterly')
                    if result['quarterly_earnings']:
                        self._save_quarterly_earnings_cache(ticker, result['quarterly_earnings'])

                # peer comparison
                if peer_cached:
                    peer_val = self._load_peer_comparison_cache(ticker)
                    result['peer_comparison'] = peer_val if peer_val is not None else {}
                else:
                    peer_val = fetched_data.get('peers')
                    result['peer_comparison'] = peer_val if peer_val is not None else {}
                    self._save_peer_comparison_cache(ticker, result['peer_comparison'])
                peer_count = max(0, len(result['peer_comparison']) - 1)
                logger.info(f"  ✓ Processed peer comparison data ({peer_count} peers)")

                # dividends
                result['dividends'] = fetched_data.get('dividends') if fetched_data.get('dividends') is not None else pd.DataFrame()
                logger.info(f"  ✓ Processed dividends ({len(result['dividends'])} records)")

            except Exception as e:
                error_msg = f"Failed to fetch fundamentals: {e}"
                result['errors'].append(error_msg)
                logger.exception(f"  ✗ {error_msg}")
                result['collection_status'] = 'partial'
                result['info'] = {}
                result['news'] = []
                result['recommendations'] = pd.DataFrame()
            
            # Step 6: Collection summary
            logger.info("\nStep 6/6: Data collection summary...")
            logger.info(f"  • Historical Data: {len(result.get('stock_data', []))} rows")
            logger.info(f"  • Technical Indicators: {len(result.get('processed_data', []))} rows")
            logger.info(f"  • Forecast: {len(result.get('forecast_data', []))} rows")
            logger.info(f"  • Fundamental Fields: {len(result.get('info', {}))}")
            logger.info(f"  • News Articles: {len(result.get('news', []))}")
            logger.info(f"  • Errors: {len(result['errors'])}")
            logger.info(f"  • Status: {result['collection_status'].upper()}")
            
            logger.info(f"\n{'='*80}")
            logger.info(f"DATA COLLECTION COMPLETE: {ticker}")
            logger.info(f"{'='*80}\n")
            
            return result
            
        except Exception as e:
            error_msg = f"Critical error during data collection: {e}"
            result['errors'].append(error_msg)
            logger.error(f"\n✗ {error_msg}")
            result['collection_status'] = 'failed'
            return result
    
    def validate_data(self, data: Dict) -> Tuple[bool, List[str]]:
        """
        Validate collected data for completeness and quality
        
        Args:
            data: Collected data dictionary
            
        Returns:
            Tuple of (is_valid, list_of_issues)
        """
        issues = []
        
        # Check required fields
        required_fields = ['stock_data', 'processed_data', 'current_price', 'info']
        for field in required_fields:
            if field not in data or data[field] is None:
                issues.append(f"Missing required field: {field}")
        
        # Check data quality
        if 'stock_data' in data and data['stock_data'] is not None:
            if len(data['stock_data']) < 252:  # Less than 1 year
                issues.append(f"Insufficient historical data: only {len(data['stock_data'])} days")
        
        if 'processed_data' in data and data['processed_data'] is not None:
            required_cols = ['Date', 'Close', 'Volume']
            missing_cols = [col for col in required_cols if col not in data['processed_data'].columns]
            if missing_cols:
                issues.append(f"Missing required columns: {missing_cols}")
        
        is_valid = len(issues) == 0
        return is_valid, issues
    
    def get_data_summary(self, data: Dict) -> Dict:
        """
        Generate summary statistics for collected data
        
        Args:
            data: Collected data dictionary
            
        Returns:
            Dict with summary statistics
        """
        summary = {
            'ticker': data.get('ticker'),
            'timestamp': data.get('timestamp'),
            'status': data.get('collection_status'),
            'total_errors': len(data.get('errors', [])),
            'data_counts': {}
        }
        
        # Count records in each dataset
        if 'stock_data' in data and data['stock_data'] is not None:
            summary['data_counts']['historical_prices'] = len(data['stock_data'])
        
        if 'processed_data' in data and data['processed_data'] is not None:
            summary['data_counts']['technical_indicators'] = len(data['processed_data'])
        
        if 'forecast_data' in data and data['forecast_data'] is not None:
            summary['data_counts']['forecast_periods'] = len(data['forecast_data'])
        
        if 'info' in data:
            summary['data_counts']['fundamental_fields'] = len(data['info'])
        
        if 'news' in data:
            summary['data_counts']['news_articles'] = len(data['news'])
        
        return summary


def main():
    """Test the data collector"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Test with AAPL
    collector = PipelineDataCollector()
    data = collector.collect_all_data('AAPL')
    
    # Validate data
    is_valid, issues = collector.validate_data(data)
    
    print("\n" + "="*80)
    print("VALIDATION RESULTS")
    print("="*80)
    print(f"Valid: {is_valid}")
    if issues:
        print("Issues:")
        for issue in issues:
            print(f"  - {issue}")
    
    # Print summary
    summary = collector.get_data_summary(data)
    print("\n" + "="*80)
    print("DATA SUMMARY")
    print("="*80)
    print(f"Ticker: {summary['ticker']}")
    print(f"Status: {summary['status']}")
    print(f"Errors: {summary['total_errors']}")
    print("\nData Counts:")
    for key, value in summary['data_counts'].items():
        print(f"  {key}: {value}")


if __name__ == '__main__':
    main()
