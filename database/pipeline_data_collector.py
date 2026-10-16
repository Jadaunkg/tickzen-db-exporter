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
import re
import glob
import sys
import hashlib
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Tuple, Optional, List
import pandas as pd
import yfinance as yf

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import data collection functions
from data_processing_scripts.data_collection import NEWS_COUNT, fetch_real_time_data, fetch_ticker_news, _yf_ticker
from data_processing_scripts.macro_data import fetch_macro_indicators
from data_processing_scripts.data_preprocessing import preprocess_data
from Models import forecast_core
from Models.prophet_model import TICKZEN_PROPHET_PIPELINE_VERSION, train_prophet_model
from analysis_scripts.fundamental_analysis import extract_quarterly_earnings_data, extract_peer_comparison_data

logger = logging.getLogger(__name__)


def news_time_utc(article: dict) -> Optional[datetime]:
    """Publish time of a news article as a naive UTC datetime (None when it has none)."""
    content = article.get('content') if isinstance(article.get('content'), dict) else {}
    ts = article.get('datetime') or article.get('providerPublishTime') or content.get('publishTime') or content.get('pubDate')
    if not ts:
        return None
    try:
        if isinstance(ts, (int, float)) and ts > 1000000000:
            return datetime.fromtimestamp(ts, timezone.utc).replace(tzinfo=None)
        stamp = pd.to_datetime(ts)
        return stamp.tz_convert(None) if stamp.tzinfo is not None else stamp
    except Exception:
        return None


# Slow-moving data (company profile, balance sheet, financials, quarterly earnings, insider
# transactions, peers, Prophet forecast) is refreshed every REFRESH_PERIOD_DAYS on a slot day
# derived from a stable hash of the symbol, so about 1/30 of the universe refreshes on any night.
# A calendar-month cache made every stock cold on the first night of each month (~45 Yahoo
# requests per stock instead of ~15, and every forecast retrained together).
REFRESH_PERIOD_DAYS = 30
_REFRESH_EPOCH = date(2026, 1, 1)
# Data fetched before this date came from the calendar-month schedule. It stays valid until the
# symbol's first slot day after the end of its month, so the old caches expire spread over the
# following month instead of together. Must be a first-of-month date.
ROLLING_REFRESH_START = date(2026, 11, 1)

_CACHE_DATE_RE = r'(\d{4}-\d{2}(?:-\d{2})?)'


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


def refresh_slot(symbol: str) -> int:
    """Stable day-of-cycle (0..REFRESH_PERIOD_DAYS-1) on which this symbol's slow data refreshes."""
    digest = hashlib.sha1(symbol.strip().upper().encode('utf-8')).digest()
    return int.from_bytes(digest[:4], 'big') % REFRESH_PERIOD_DAYS


def last_refresh_day(symbol: str, today: Optional[date] = None) -> date:
    """The symbol's most recent slot day on or before ``today`` (UTC)."""
    today = today or _utc_today()
    days_since_slot = ((today - _REFRESH_EPOCH).days - refresh_slot(symbol)) % REFRESH_PERIOD_DAYS
    return today - timedelta(days=days_since_slot)


def refresh_cutoff(symbol: str, today: Optional[date] = None) -> date:
    """Data for ``symbol`` fetched on or after this UTC date is current; anything older is refetched."""
    last_slot = last_refresh_day(symbol, today)
    if last_slot < ROLLING_REFRESH_START:
        return last_slot.replace(day=1)
    return last_slot


def as_utc_date(value) -> Optional[date]:
    """UTC calendar date of a DB timestamp (ISO string or datetime); naive values are taken as UTC."""
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value.strip().replace('Z', '+00:00'))
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).date() if value.tzinfo else value.date()
    if isinstance(value, date):
        return value
    return None


def _cache_file_date(stamp: str) -> date:
    """Date from a cache file stamp: ``YYYY-MM-DD``, or legacy ``YYYY-MM`` (first of the month)."""
    return date.fromisoformat(stamp if len(stamp) == 10 else f'{stamp}-01')


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
        self._loaded_forecast_metadata = None

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
    
    def _cache_dir(self, subdir: str) -> Path:
        return Path(self.app_root) / 'generated_data' / subdir

    @staticmethod
    def _cache_name_pattern(ticker: str, kind: str):
        return re.compile(re.escape(f'{ticker}_{kind}_') + _CACHE_DATE_RE + r'\.json')

    def _current_cache_file(self, subdir: str, ticker: str, kind: str) -> Optional[Path]:
        """
        Newest ``{ticker}_{kind}_<date>.json`` cache file, if it was fetched on or after the
        symbol's refresh cutoff; None when there is none or it is due for a refresh.

        Reads the date-stamped names written now and the legacy ``YYYY-MM`` monthly names, so
        caches written before the switch to the rolling schedule stay usable.
        """
        cache_dir = self._cache_dir(subdir)
        if not cache_dir.is_dir():
            return None
        pattern = self._cache_name_pattern(ticker, kind)
        newest = None
        for path in cache_dir.glob(f'{glob.escape(ticker)}_{kind}_*.json'):
            match = pattern.fullmatch(path.name)
            if not match:
                continue
            try:
                key = (_cache_file_date(match.group(1)), len(match.group(1)))
            except ValueError:
                continue
            if newest is None or key > newest[0]:
                newest = (key, path)
        if newest is None or newest[0][0] < refresh_cutoff(ticker):
            return None
        return newest[1]

    def _new_cache_file(self, subdir: str, ticker: str, kind: str) -> Path:
        """Path for a cache fetched today (UTC); creates the cache directory."""
        cache_dir = self._cache_dir(subdir)
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir / f'{ticker}_{kind}_{_utc_today().isoformat()}.json'

    def _remove_older_cache_files(self, cache_file: Path, ticker: str, kind: str) -> None:
        """Delete this ticker's other cache files of the same kind once ``cache_file`` is written."""
        pattern = self._cache_name_pattern(ticker, kind)
        for path in cache_file.parent.glob(f'{glob.escape(ticker)}_{kind}_*.json'):
            if path != cache_file and pattern.fullmatch(path.name):
                try:
                    path.unlink()
                    logger.info(f"  Removed stale cache file: {path.name}")
                except OSError as e:
                    logger.debug(f"Failed to remove old cache file {path.name}: {e}")

    def _is_forecast_cached(self, ticker: str) -> bool:
        """
        Check if a forecast for this ticker was calculated since its refresh cutoff

        Args:
            ticker: Stock symbol

        Returns:
            True if a current forecast is cached, False otherwise
        """
        try:
            cache_file = self._current_cache_file('forecast_cache', ticker, 'forecast')

            if cache_file is not None:
                # A cache written by an older model version must not be served as current.
                import json
                try:
                    with open(cache_file, 'r') as f:
                        cached_meta = (json.load(f) or {}).get('forecast_metadata') or {}
                except Exception:
                    cached_meta = {}
                if cached_meta.get('model_version') != TICKZEN_PROPHET_PIPELINE_VERSION:
                    logger.info(f"  ℹ Cached forecast for {ticker} is from another model version - recalculating")
                    return False
                logger.info(f"  ℹ Forecast already calculated for {ticker} ({cache_file.name}) - using cached")
                return True

            return False

        except Exception as e:
            logger.warning(f"Error checking forecast cache: {e}")
            return False  # If we can't check, proceed with calculation to be safe

    def _save_forecast_cache(
        self,
        ticker: str,
        forecast_df: pd.DataFrame,
        forecast_metadata: Optional[Dict] = None,
    ) -> None:
        """
        Save forecast data to a dated cache file and clean up old forecast files

        Args:
            ticker: Stock symbol
            forecast_df: Forecast dataframe to cache
        """
        try:
            import json

            cache_file = self._new_cache_file('forecast_cache', ticker, 'forecast')
            forecast_data = {
                'ticker': ticker,
                'date': _utc_today().isoformat(),
                'created_at': datetime.now().isoformat(),
                'forecast': forecast_df.to_dict('records') if forecast_df is not None else None,
                'forecast_metadata': forecast_metadata,
            }

            with open(cache_file, 'w') as f:
                json.dump(forecast_data, f, default=str)
            self._remove_older_cache_files(cache_file, ticker, 'forecast')

            logger.info(f"  ✓ Cached forecast data for {ticker} ({cache_file.name})")

        except Exception as e:
            logger.warning(f"Failed to cache forecast data: {e}")

    def _load_forecast_cache(self, ticker: str) -> Optional[pd.DataFrame]:
        """
        Load the current cached forecast data

        Args:
            ticker: Stock symbol

        Returns:
            Cached forecast DataFrame or None
        """
        try:
            import json

            self._loaded_forecast_metadata = None

            cache_file = self._current_cache_file('forecast_cache', ticker, 'forecast')
            if cache_file is None:
                return None

            with open(cache_file, 'r') as f:
                cache_data = json.load(f)

            if cache_data.get('forecast'):
                metadata = cache_data.get('forecast_metadata')
                self._loaded_forecast_metadata = metadata if isinstance(metadata, dict) else None
                return pd.DataFrame(cache_data['forecast'])

            return None

        except Exception as e:
            logger.warning(f"Failed to load forecast cache: {e}")
            return None

    def _is_company_profile_cached(self, ticker: str) -> bool:
        """
        Check if the company profile for this ticker was cached since its refresh cutoff

        Args:
            ticker: Stock symbol

        Returns:
            True if a current company profile is cached, False otherwise
        """
        try:
            cache_file = self._current_cache_file('profile_cache', ticker, 'profile')
            if cache_file is not None:
                if not self._load_company_profile_cache(ticker):
                    logger.info(f"  ℹ Cached company profile for {ticker} is empty - refetching")
                    return False
                logger.info(f"  ℹ Company profile already cached for {ticker} ({cache_file.name}) - using cached")
                return True

            return False

        except Exception as e:
            logger.warning(f"Error checking company profile cache: {e}")
            return False

    def _is_insider_transactions_cached(self, ticker: str) -> bool:
        """
        Check if insider transactions for this ticker were cached since its refresh cutoff

        Args:
            ticker: Stock symbol

        Returns:
            True if current insider transactions are cached, False otherwise
        """
        try:
            cache_file = self._current_cache_file('insider_cache', ticker, 'insider')
            if cache_file is not None:
                logger.info(f"  ℹ Insider transactions already cached for {ticker} ({cache_file.name}) - using cached")
                return True

            return False

        except Exception as e:
            logger.warning(f"Error checking insider transactions cache: {e}")
            return False

    def _save_company_profile_cache(self, ticker: str, profile_data: dict) -> None:
        """
        Save company profile data to a dated cache file

        Args:
            ticker: Stock symbol
            profile_data: Company profile dictionary to cache
        """
        try:
            import json

            cache_file = self._new_cache_file('profile_cache', ticker, 'profile')
            cache_data = {
                'ticker': ticker,
                'date': _utc_today().isoformat(),
                'created_at': datetime.now().isoformat(),
                'profile': profile_data
            }

            with open(cache_file, 'w') as f:
                json.dump(cache_data, f, default=str)
            self._remove_older_cache_files(cache_file, ticker, 'profile')

            logger.info(f"  ✓ Cached company profile for {ticker} ({cache_file.name})")

        except Exception as e:
            logger.warning(f"Failed to cache company profile: {e}")

    def _save_balance_sheet_cache(self, ticker: str, bs_df: pd.DataFrame) -> None:
        """Save balance sheet DataFrame to a dated cache file"""
        try:
            if bs_df is None or bs_df.empty:
                return

            cache_file = self._new_cache_file('profile_cache', ticker, 'balance_sheet')
            bs_df.to_json(cache_file, date_format='iso')
            self._remove_older_cache_files(cache_file, ticker, 'balance_sheet')
            logger.info(f"  ✓ Cached balance sheet for {ticker} ({cache_file.name})")
        except Exception as e:
            logger.warning(f"Failed to save balance sheet cache: {e}")

    def _load_balance_sheet_cache(self, ticker: str) -> Optional[pd.DataFrame]:
        """Load the current cached balance sheet DataFrame"""
        try:
            cache_file = self._current_cache_file('profile_cache', ticker, 'balance_sheet')
            if cache_file is None:
                return None

            return pd.read_json(cache_file)
        except Exception as e:
            logger.warning(f"Failed to load balance sheet cache: {e}")
            return None

    def _save_financials_cache(self, ticker: str, fin_df: pd.DataFrame) -> None:
        """Save financials DataFrame to a dated cache file"""
        try:
            if fin_df is None or fin_df.empty:
                return

            cache_file = self._new_cache_file('profile_cache', ticker, 'financials')
            fin_df.to_json(cache_file, date_format='iso')
            self._remove_older_cache_files(cache_file, ticker, 'financials')
            logger.info(f"  ✓ Cached financials for {ticker} ({cache_file.name})")
        except Exception as e:
            logger.warning(f"Failed to save financials cache: {e}")

    def _load_financials_cache(self, ticker: str) -> Optional[pd.DataFrame]:
        """Load the current cached financials DataFrame"""
        try:
            cache_file = self._current_cache_file('profile_cache', ticker, 'financials')
            if cache_file is None:
                return None

            return pd.read_json(cache_file)
        except Exception as e:
            logger.warning(f"Failed to load financials cache: {e}")
            return None

    def _save_quarterly_earnings_cache(self, ticker: str, qe_data: dict) -> None:
        """Save quarterly earnings dictionary to a dated cache file"""
        try:
            import json

            if not qe_data:
                return

            cache_file = self._new_cache_file('profile_cache', ticker, 'quarterly_earnings')
            with open(cache_file, 'w') as f:
                json.dump(qe_data, f, default=str)
            self._remove_older_cache_files(cache_file, ticker, 'quarterly_earnings')
            logger.info(f"  ✓ Cached quarterly earnings for {ticker} ({cache_file.name})")
        except Exception as e:
            logger.warning(f"Failed to save quarterly earnings cache: {e}")

    def _load_quarterly_earnings_cache(self, ticker: str) -> Optional[dict]:
        """Load the current cached quarterly earnings dictionary"""
        try:
            import json

            cache_file = self._current_cache_file('profile_cache', ticker, 'quarterly_earnings')
            if cache_file is None:
                return None

            with open(cache_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load quarterly earnings cache: {e}")
            return None

    def _save_insider_transactions_cache(self, ticker: str, insider_data: pd.DataFrame) -> None:
        """
        Save insider transactions data to a dated cache file

        Args:
            ticker: Stock symbol
            insider_data: Insider transactions dataframe to cache
        """
        try:
            import json

            cache_file = self._new_cache_file('insider_cache', ticker, 'insider')
            cache_data = {
                'ticker': ticker,
                'date': _utc_today().isoformat(),
                'created_at': datetime.now().isoformat(),
                'insider_transactions': insider_data.to_dict('records') if not insider_data.empty else []
            }

            with open(cache_file, 'w') as f:
                json.dump(cache_data, f, default=str)
            self._remove_older_cache_files(cache_file, ticker, 'insider')

            logger.info(f"  ✓ Cached insider transactions for {ticker} ({cache_file.name})")

        except Exception as e:
            logger.warning(f"Failed to cache insider transactions: {e}")

    def _load_company_profile_cache(self, ticker: str) -> Optional[dict]:
        """
        Load the current cached company profile data

        Args:
            ticker: Stock symbol

        Returns:
            Cached company profile dict or None
        """
        try:
            import json

            cache_file = self._current_cache_file('profile_cache', ticker, 'profile')
            if cache_file is None:
                return None

            with open(cache_file, 'r') as f:
                cache_data = json.load(f)

            return cache_data.get('profile', {})

        except Exception as e:
            logger.warning(f"Failed to load company profile cache: {e}")
            return None

    def _load_insider_transactions_cache(self, ticker: str) -> Optional[pd.DataFrame]:
        """
        Load the current cached insider transactions data

        Args:
            ticker: Stock symbol

        Returns:
            Cached insider transactions DataFrame or None
        """
        try:
            import json

            cache_file = self._current_cache_file('insider_cache', ticker, 'insider')
            if cache_file is None:
                return None

            with open(cache_file, 'r') as f:
                cache_data = json.load(f)

            insider_records = cache_data.get('insider_transactions', [])
            return pd.DataFrame(insider_records) if insider_records else pd.DataFrame()

        except Exception as e:
            logger.warning(f"Failed to load insider transactions cache: {e}")
            return None

    def _is_peer_comparison_cached(self, ticker: str) -> bool:
        """
        Check if peer comparison for this ticker was cached since its refresh cutoff
        """
        try:
            return self._current_cache_file('peer_cache', ticker, 'peer') is not None
        except Exception as e:
            logger.warning(f"Error checking peer comparison cache: {e}")
            return False

    def _save_peer_comparison_cache(self, ticker: str, peer_data: dict) -> None:
        """
        Save peer comparison data to a dated cache file
        """
        try:
            import json

            cache_file = self._new_cache_file('peer_cache', ticker, 'peer')
            cache_data = {
                'ticker': ticker,
                'date': _utc_today().isoformat(),
                'created_at': datetime.now().isoformat(),
                'peer_comparison': peer_data
            }

            with open(cache_file, 'w') as f:
                json.dump(cache_data, f, default=str)
            self._remove_older_cache_files(cache_file, ticker, 'peer')

            logger.info(f"  ✓ Cached peer comparison data for {ticker} ({cache_file.name})")
        except Exception as e:
            logger.warning(f"Failed to cache peer comparison data: {e}")

    def _load_peer_comparison_cache(self, ticker: str) -> Optional[dict]:
        """
        Load the current cached peer comparison data
        """
        try:
            import json

            cache_file = self._current_cache_file('peer_cache', ticker, 'peer')
            if cache_file is None:
                return None

            with open(cache_file, 'r') as f:
                cache_data = json.load(f)

            return cache_data.get('peer_comparison', {})
        except Exception as e:
            logger.warning(f"Failed to load peer comparison cache: {e}")
            return None

    def _check_forecast_exists_in_database(self, ticker: str) -> bool:
        """
        Check if the database already has a forecast for this ticker created since its refresh cutoff

        Args:
            ticker: Stock symbol

        Returns:
            True if a current forecast exists in the DB, False otherwise
        """
        try:
            cutoff = refresh_cutoff(ticker).isoformat()

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
            
            # Check if forecast data was created since the refresh cutoff
            forecast_response = db_client.table('forecast_data')\
                .select('forecast_date')\
                .eq('stock_id', stock_id)\
                .eq('model_version', TICKZEN_PROPHET_PIPELINE_VERSION)\
                .gte('created_at', cutoff)\
                .limit(1)\
                .execute()

            exists = len(forecast_response.data) > 0

            if exists:
                logger.info(f"  ℹ️ Database already contains forecast data for {ticker} since {cutoff}")
            
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
        self._loaded_forecast_metadata = None
        
        result = {
            'ticker': ticker,
            'timestamp': self.timestamp,
            'collection_status': 'success',
            'errors': []
        }
        # Only a model trained in this run receives complete provenance.
        # Monthly cached forecasts predate a durable provenance sidecar and
        # therefore remain explicitly unversioned until regenerated.
        result['forecast_metadata'] = None
        
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
            
            # Step 4: Generate forecast using Prophet (retrained on the symbol's 30-day refresh slot)
            logger.info("\nStep 4/6: Generating price forecast...")

            forecast_skipped = False

            # First check if a current forecast already exists in the database
            forecast_exists_in_db = self._check_forecast_exists_in_database(ticker)

            if forecast_exists_in_db:
                # Skip expensive model training if the database already has a current forecast
                result['forecast_data'] = pd.DataFrame()  # Empty - will be loaded from DB during export
                result['forecast_model'] = None
                result['actual_data'] = None
                forecast_skipped = True
                logger.info(f"  ⏭️  Current forecast already exists in database - skipping Prophet training")
            
            # If no database record, check file cache
            elif self._is_forecast_cached(ticker):
                # Load cached forecast
                cached_forecast = self._load_forecast_cache(ticker)
                if cached_forecast is not None:
                    result['forecast_data'] = cached_forecast
                    result['forecast_model'] = None  # Model not needed for cached data
                    result['actual_data'] = None
                    result['forecast_metadata'] = self._loaded_forecast_metadata
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

                    training_data = result['processed_data'].copy()
                    clean_rows = pd.DataFrame()
                    if {'Date', 'Close'}.issubset(training_data.columns):
                        training_dates = pd.to_datetime(training_data['Date'], errors='coerce')
                        training_close = pd.to_numeric(training_data['Close'], errors='coerce')
                        valid_training = training_dates.notna() & training_close.notna() & (training_close > 0)
                        clean_rows = training_data.loc[valid_training].copy()
                        clean_rows['_forecast_date'] = training_dates[valid_training]
                        clean_rows['_forecast_close'] = training_close[valid_training]
                        clean_rows = clean_rows.drop_duplicates('_forecast_date', keep='last').sort_values('_forecast_date')
                    if not clean_rows.empty:
                        last_training_row = clean_rows.iloc[-1]
                        result['forecast_metadata'] = {
                            'model_name': 'Prophet',
                            'method': forecast_core.METHOD_DESCRIPTION,
                            'model_version': TICKZEN_PROPHET_PIPELINE_VERSION,
                            'generated_at': datetime.now().astimezone().isoformat(),
                            'source_class': 'tickzen_statistical_model',
                            'training_start_date': clean_rows['_forecast_date'].iloc[0].date().isoformat(),
                            'training_cutoff_date': clean_rows['_forecast_date'].iloc[-1].date().isoformat(),
                            'input_series': 'Daily yfinance adjusted Close price',
                            'adjustment_policy': 'yfinance auto_adjust=true; dividend/split-adjusted OHLC series',
                            'band_definition': forecast_core.BAND_DEFINITION,
                            'anchor_price': float(last_training_row['_forecast_close']),
                            'anchor_as_of': last_training_row['_forecast_date'].date().isoformat(),
                        }
                    
                    # Persist exact provenance with cached predictions so later
                    # exports do not manufacture a new model generation date.
                    self._save_forecast_cache(ticker, forecast_df, result['forecast_metadata'])
                    
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
                profile_cached = self._is_company_profile_cached(ticker)
                insider_cached = self._is_insider_transactions_cached(ticker)
                peer_cached = self._is_peer_comparison_cached(ticker)
                
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
                    news = fetch_ticker_news(ticker)
                    if not news:
                        logger.warning(f"  ⚠️  Yahoo returned no headlines tagged {ticker.upper()}")

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
                                        article_time = news_time_utc(article)
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
                    if profile_cached and self._current_cache_file('profile_cache', ticker, 'balance_sheet'):
                        return None
                    logger.info("  ⚡ Starting balance sheet fetch...")
                    bs = yf_ticker.balance_sheet
                    logger.info("  ⚡ Finished balance sheet fetch.")
                    return bs

                def fetch_financials():
                    if profile_cached and self._current_cache_file('profile_cache', ticker, 'financials'):
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
                    if profile_cached and self._current_cache_file('profile_cache', ticker, 'quarterly_earnings'):
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
                    # Tells the exporter to write the stocks profile even if this worker had no cache.
                    result['profile_refreshed'] = bool(result['info'])
                    if result['info']:
                        self._save_company_profile_cache(ticker, result['info'])
                        logger.info(f"  ✓ Processed and cached fresh company info")
                    else:
                        # An empty profile cached for the month would block a refetch and blank the stocks row.
                        logger.warning(f"  ⚠️  Yahoo returned an empty company profile for {ticker}; not caching it")

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
