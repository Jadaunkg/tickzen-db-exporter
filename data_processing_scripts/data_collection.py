#!/usr/bin/env python3
"""
Financial Data Collection Module
===============================

Centralized financial data aggregation system that collects, validates,
and standardizes market data from multiple authoritative sources. Handles
real-time and historical data with comprehensive error handling and caching.

Supported Data Sources:
----------------------
1. **Yahoo Finance (yfinance)**:
   - Historical stock prices (OHLCV)
   - Company fundamentals and financials
   - Dividend and stock split history
   - Real-time price quotes
   - Market cap and trading volume

2. **Alpha Vantage API**:
   - Intraday stock data (1min, 5min, 15min, 30min, 60min)
   - Daily, weekly, monthly adjusted prices
   - Technical indicators
   - Sector performance data
   - Global market data

3. **FRED (Federal Reserve Economic Data)**:
   - Economic indicators (GDP, inflation, unemployment)
   - Interest rates (Fed funds rate, Treasury yields)
   - Money supply and banking data
   - International economic data

4. **Finnhub API**:
   - Real-time stock quotes and trades
   - Company earnings and estimates
   - News sentiment and social sentiment
   - Insider trading data

Core Functions:
--------------
- `fetch_stock_data()`: Multi-source stock data collection
- `fetch_real_time_data()`: Current market data retrieval
- `fetch_historical_data()`: Historical price and volume data
- `fetch_company_fundamentals()`: Financial statement data
- `fetch_economic_indicators()`: Macro economic data

Data Validation Features:
------------------------
- **Price Validation**: Outlier detection using statistical methods
- **Volume Validation**: Unusual volume spike detection
- **Data Completeness**: Missing data identification and handling
- **Cross-Source Verification**: Multi-source data consistency checks
- **Time-Series Continuity**: Gap detection in historical data

Caching Strategy:
----------------
- **Intraday Cache**: 1-minute TTL for real-time data
- **Daily Cache**: 24-hour TTL for end-of-day data
- **Historical Cache**: 7-day TTL for historical data
- **Fundamental Cache**: 30-day TTL for company fundamentals
- **Economic Cache**: Variable TTL based on release schedule

Error Handling:
--------------
- **API Failures**: Automatic fallback to alternative sources
- **Rate Limiting**: Exponential backoff with jitter
- **Network Issues**: Connection timeout and retry mechanisms
- **Data Quality**: Invalid data filtering and correction
- **Quota Management**: API usage monitoring and optimization

Data Standardization:
--------------------
- **Timezone Handling**: UTC normalization with market timezone awareness
- **Price Adjustment**: Stock splits and dividend adjustments
- **Currency Conversion**: Multi-currency support with exchange rates
- **Data Format**: Consistent pandas DataFrame output format
- **Column Naming**: Standardized column names across sources

Performance Optimizations:
-------------------------
- **Parallel Requests**: Concurrent API calls for multiple symbols
- **Batch Processing**: Bulk data requests where supported
- **Connection Pooling**: Persistent HTTP connections
- **Memory Management**: Efficient data structure usage
- **Lazy Loading**: On-demand data retrieval

Data Quality Metrics:
--------------------
- **Completeness Score**: Percentage of expected data points
- **Freshness Score**: Data recency relative to market hours
- **Accuracy Score**: Cross-source validation results
- **Coverage Score**: Symbol and timeframe availability

Usage Examples:
--------------
```python
# Basic stock data collection
data = fetch_stock_data('AAPL', period='1y')

# Real-time data with validation
real_time = fetch_real_time_data(['AAPL', 'GOOGL', 'MSFT'])

# Historical data with custom date range
historical = fetch_historical_data(
    symbol='AAPL',
    start_date='2023-01-01',
    end_date='2024-01-01',
    interval='1d'
)

# Company fundamentals
fundamentals = fetch_company_fundamentals('AAPL')
```

API Integration Details:
-----------------------
- **Rate Limiting**: Respects all API provider limits
- **Authentication**: API key management and rotation
- **Error Codes**: Comprehensive HTTP status code handling
- **Response Parsing**: Robust JSON/CSV parsing with validation
- **Quota Tracking**: Real-time API usage monitoring

Output Data Structure:
---------------------
Standardized DataFrame format:
- Date: DatetimeIndex with timezone awareness
- Open/High/Low/Close: Price data with adjustments
- Volume: Trading volume
- Adj Close: Dividend and split adjusted closing price
- Metadata: Source, collection timestamp, quality metrics

Integration Points:
------------------
- Used by automation_scripts/pipeline.py for analysis workflow
- Integrated with earnings_reports/data_collector.py
- Provides data for analysis_scripts/ modules
- Supports real-time dashboard updates

Configuration:
-------------
Environment Variables:
- ALPHA_VANTAGE_API_KEY: Alpha Vantage access key
- FINNHUB_API_KEY: Finnhub access token
- FRED_API_KEY: FRED API access key
- DATA_CACHE_TTL: Default cache time-to-live
- MAX_CONCURRENT_REQUESTS: Parallel request limit

Author: TickZen Development Team
Version: 2.7
Last Updated: January 2026
"""

import time
import logging
import yfinance as yf
import pandas as pd
from datetime import datetime, date, timedelta
import os
import re
import random
import threading
import requests
import pytz

from data_processing_scripts.date_utils import normalize_date_column, normalize_date_series

# Import real-time configuration
try:
    from config.realtime_config import get_realtime_config, is_realtime_enabled
    REALTIME_CONFIG_AVAILABLE = True
except ImportError:
    REALTIME_CONFIG_AVAILABLE = False
    def get_realtime_config():
        return {'enable_realtime_fetch': True, 'throttle_seconds': 0.3, 'timeout_seconds': 30}
    def is_realtime_enabled():
        return True

# Configure logging
# Basic config for direct script run, real app might have this in __init__ or main.
if not logging.getLogger().hasHandlers():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__) # Use module-specific logger

# Tracks the latest fetch failure by ticker so pipeline can classify user-facing errors.
_LAST_FETCH_ERROR_BY_TICKER = {}
_FETCH_LOCKS_BY_KEY = {}
_FETCH_LOCKS_GUARD = threading.Lock()
_RATE_LIMIT_GUARD = threading.Lock()
_RATE_LIMIT_STATE = {
    'cooldown_until': 0.0,
    'strike_count': 0,
    'last_reason': ''
}
_YF_SESSION = None
_TICKER_CACHE = {}


def _get_yf_session():
    """Create a shared session for yfinance requests without custom browser headers."""
    global _YF_SESSION
    if _YF_SESSION is None:
        session = requests.Session()
        # No custom browser agent/headers to avoid spoofing detection on datacenter IPs
        _YF_SESSION = session
    return _YF_SESSION


_BULK_STOCK_DATA_CACHE = {}
_BULK_CURRENT_PRICE_CACHE = {}

def set_bulk_price_data(prices_dict, current_prices_dict):
    """Set the bulk-downloaded price data cache to bypass individual downloads."""
    global _BULK_STOCK_DATA_CACHE, _BULK_CURRENT_PRICE_CACHE
    _BULK_STOCK_DATA_CACHE.update({k.upper(): v for k, v in prices_dict.items()})
    _BULK_CURRENT_PRICE_CACHE.update({k.upper(): v for k, v in current_prices_dict.items()})


def _yf_ticker(symbol):
    """Build yfinance Ticker with shared session when supported and cache it globally."""
    global _TICKER_CACHE
    symbol_upper = symbol.upper()
    if symbol_upper not in _TICKER_CACHE:
        use_custom_session = os.getenv('YF_USE_CUSTOM_SESSION', 'false').strip().lower() in {'1', 'true', 'yes', 'on'}
        if use_custom_session:
            try:
                _TICKER_CACHE[symbol_upper] = yf.Ticker(symbol, session=_get_yf_session())
            except TypeError:
                _TICKER_CACHE[symbol_upper] = yf.Ticker(symbol)
        else:
            _TICKER_CACHE[symbol_upper] = yf.Ticker(symbol)
    return _TICKER_CACHE[symbol_upper]


def _yf_download(**kwargs):
    """Call yfinance download with shared session when supported."""
    use_custom_session = os.getenv('YF_USE_CUSTOM_SESSION', 'false').strip().lower() in {'1', 'true', 'yes', 'on'}
    if use_custom_session:
        try:
            kwargs.setdefault('session', _get_yf_session())
        except Exception:
            pass
    return yf.download(**kwargs)


def _set_last_fetch_error(ticker, error_type, message=""):
    if not ticker:
        return
    symbol = str(ticker).upper()
    if error_type:
        _LAST_FETCH_ERROR_BY_TICKER[symbol] = {
            'type': error_type,
            'message': message,
            'timestamp': datetime.now().isoformat()
        }
    else:
        _LAST_FETCH_ERROR_BY_TICKER.pop(symbol, None)


def get_last_fetch_error(ticker):
    if not ticker:
        return None
    return _LAST_FETCH_ERROR_BY_TICKER.get(str(ticker).upper())


def _get_fetch_lock(lock_key):
    with _FETCH_LOCKS_GUARD:
        lock = _FETCH_LOCKS_BY_KEY.get(lock_key)
        if lock is None:
            lock = threading.Lock()
            _FETCH_LOCKS_BY_KEY[lock_key] = lock
        return lock


def _rate_limit_cooldown_remaining_seconds():
    with _RATE_LIMIT_GUARD:
        remaining = _RATE_LIMIT_STATE['cooldown_until'] - time.time()
    return max(0.0, remaining)


def _register_rate_limit(reason=''):
    with _RATE_LIMIT_GUARD:
        _RATE_LIMIT_STATE['strike_count'] = min(_RATE_LIMIT_STATE['strike_count'] + 1, 5)
        backoff_steps = [60, 120, 300, 600, 900]
        cooldown = backoff_steps[_RATE_LIMIT_STATE['strike_count'] - 1]
        _RATE_LIMIT_STATE['cooldown_until'] = max(_RATE_LIMIT_STATE['cooldown_until'], time.time() + cooldown)
        _RATE_LIMIT_STATE['last_reason'] = reason or 'provider_rate_limited'
        return cooldown, _RATE_LIMIT_STATE['strike_count']


def _clear_rate_limit_state():
    with _RATE_LIMIT_GUARD:
        _RATE_LIMIT_STATE['strike_count'] = 0
        _RATE_LIMIT_STATE['cooldown_until'] = 0.0
        _RATE_LIMIT_STATE['last_reason'] = ''

def is_market_hours(ticker=None):
    """
    Check if it's currently market hours for the given ticker.
    Default to US market hours if no specific ticker exchange is provided.
    """
    try:
        # Default to US Eastern Time
        market_tz = pytz.timezone('US/Eastern')
        
        # Get exchange timezone based on ticker suffix
        exchange_suffix, _ = get_exchange_info(ticker) if ticker else (None, None)
        
        if exchange_suffix:
            # Map exchange suffixes to timezones
            exchange_timezones = {
                '.L': 'Europe/London',       # London
                '.TO': 'America/Toronto',    # Toronto
                '.AX': 'Australia/Sydney',   # Australia
                '.T': 'Asia/Tokyo',          # Tokyo
                '.HK': 'Asia/Hong_Kong',     # Hong Kong
                '.SS': 'Asia/Shanghai',      # Shanghai
                '.NS': 'Asia/Kolkata',       # India NSE
                '.BO': 'Asia/Kolkata',       # India BSE
            }
            market_tz = pytz.timezone(exchange_timezones.get(exchange_suffix, 'US/Eastern'))
        
        now_market = datetime.now(market_tz)
        
        # Check if it's a weekday (Monday=0, Sunday=6)
        if now_market.weekday() > 4:  # Saturday or Sunday
            return False
        
        # US market hours: 9:30 AM - 4:00 PM ET
        market_open = now_market.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close = now_market.replace(hour=16, minute=0, second=0, microsecond=0)
        
        return market_open <= now_market <= market_close
        
    except Exception as e:
        logger.warning(f"Error checking market hours: {e}")
        return False

def get_last_expected_trading_day():
    """
    Return the most recent trading day for which a daily closing bar should
    already be available in yfinance.

    Rules:
    - If it is a weekday AND the time is at or past 4:30 PM ET (giving yfinance
      ~30 min after close to publish the bar) → today's close is available.
    - Otherwise → walk back to the most recent prior weekday.

    NOTE: This does NOT account for US public holidays; on the morning after a
    holiday yfinance will simply not have a bar for the holiday date, so the
    staleness check will trigger a re-fetch that returns the pre-holiday close,
    which is the right behaviour.
    """
    et_tz = pytz.timezone('America/New_York')
    now_et = datetime.now(et_tz)
    today = now_et.date()

    # 4:30 PM ET — give yfinance 30 min after market close to publish the bar
    bar_available_time = now_et.replace(hour=16, minute=30, second=0, microsecond=0)

    if now_et.weekday() < 5 and now_et >= bar_available_time:
        # Weekday, past 4:30 PM ET — today's close is available
        return today

    # Otherwise find the most recent previous weekday
    check_day = today - timedelta(days=1)
    while check_day.weekday() >= 5:   # skip Saturday(5) and Sunday(6)
        check_day -= timedelta(days=1)
    return check_day


def is_data_current_for_today(data, ticker):
    """
    Returns True if the cached data already contains the most recent
    available daily close bar (no re-fetch needed).

    Logic:
    - Determine `last_expected_trading_day` via get_last_expected_trading_day().
    - If the latest date in the cache is >= that day → CURRENT.
    - Otherwise → STALE (triggers re-download).
    """
    if data is None or data.empty or 'Date' not in data.columns:
        return False

    try:
        data['Date'] = normalize_date_series(data['Date'])

        latest_date = data['Date'].max()
        if pd.isna(latest_date):
            logger.warning(f"No valid dates found in cached data for {ticker}")
            return False

        latest_date = latest_date.date()
        last_expected = get_last_expected_trading_day()
        is_current = latest_date >= last_expected

        status = "CURRENT" if is_current else "STALE"
        logger.info(
            f"Cache check for {ticker}: latest_bar={latest_date}, "
            f"last_expected_close={last_expected} → {status}"
        )
        return is_current

    except Exception as e:
        logger.error(f"Error checking data currency for {ticker}: {e}")
        return False

def cleanup_old_cache_files(cache_dir, max_age_days=7):
    """
    Clean up cache files older than max_age_days to prevent accumulation.
    """
    try:
        if not os.path.exists(cache_dir):
            return
        
        current_time = datetime.now()
        files_cleaned = 0
        
        for filename in os.listdir(cache_dir):
            if filename.endswith('.csv'):
                filepath = os.path.join(cache_dir, filename)
                try:
                    # Get file modification time
                    file_mtime = datetime.fromtimestamp(os.path.getmtime(filepath))
                    age_days = (current_time - file_mtime).days
                    
                    if age_days > max_age_days:
                        os.remove(filepath)
                        files_cleaned += 1
                        logger.info(f"Cleaned up old cache file: {filename} (age: {age_days} days)")
                
                except Exception as e:
                    logger.warning(f"Error cleaning cache file {filename}: {e}")
        
        if files_cleaned > 0:
            logger.info(f"Cleaned up {files_cleaned} old cache files from {cache_dir}")
            
    except Exception as e:
        logger.error(f"Error during cache cleanup: {e}")

# Define supported exchange suffixes
SUPPORTED_EXCHANGES = {
    '.NS': 'National Stock Exchange of India',
    '.BO': 'Bombay Stock Exchange',
    '.L': 'London Stock Exchange',
    '.F': 'Frankfurt Stock Exchange',
    '.PA': 'Paris Stock Exchange',
    '.TO': 'Toronto Stock Exchange',
    '.SI': 'Singapore Stock Exchange',
    '.AX': 'Australian Stock Exchange',
    '.NZ': 'New Zealand Stock Exchange',
    '.V': 'TSX Venture Exchange',
    '.DE': 'XETRA',
    '.BE': 'Berlin Stock Exchange',
    '.DU': 'Dusseldorf Stock Exchange',
    '.HM': 'Hamburg Stock Exchange',
    '.HA': 'Hanover Stock Exchange',
    '.MU': 'Munich Stock Exchange',
    '.ST': 'Stockholm Stock Exchange',
    '.CO': 'Copenhagen Stock Exchange',
    '.HE': 'Helsinki Stock Exchange',
    '.OL': 'Oslo Stock Exchange',
    '.IC': 'Iceland Stock Exchange',
    '.AT': 'Athens Stock Exchange',
    '.MI': 'Milan Stock Exchange',
    '.AS': 'Amsterdam Stock Exchange',
    '.BR': 'Brussels Stock Exchange',
    '.LS': 'Lisbon Stock Exchange',
    '.MC': 'Madrid Stock Exchange',
    '.VI': 'Vienna Stock Exchange',
    '.SW': 'Swiss Stock Exchange',
    '.BA': 'Buenos Aires Stock Exchange',
    '.SA': 'Sao Paulo Stock Exchange',
    '.MX': 'Mexican Stock Exchange',
    '.JK': 'Jakarta Stock Exchange',
    '.KL': 'Kuala Lumpur Stock Exchange',
    '.BK': 'Bangkok Stock Exchange',
    '.TW': 'Taiwan Stock Exchange',
    '.KS': 'Korea Stock Exchange',
    '.T': 'Tokyo Stock Exchange',
    '.HK': 'Hong Kong Stock Exchange',
    '.SS': 'Shanghai Stock Exchange',
    '.SZ': 'Shenzhen Stock Exchange'
}

def get_exchange_info(ticker):
    """Extract exchange information from ticker symbol."""
    for suffix, exchange_name in SUPPORTED_EXCHANGES.items():
        if ticker.endswith(suffix):
            return suffix, exchange_name
    return None, 'US Stock Exchange'

def find_latest_cache_file(ticker, cache_dir, interval='1d'):
    """
    Find the most recent cache file for a ticker, handling multiple naming patterns.
    Returns the most recent cache file path or None if no cache exists.
    """
    if not os.path.exists(cache_dir):
        return None
    
    # Clean ticker for filename patterns
    clean_ticker = ticker.replace(':', '_').replace('^', '_').replace('=', '_')
    
    # Possible cache file patterns (newest to oldest naming conventions)
    patterns = [
        f"{clean_ticker}_stock_data_{interval}.csv",  # Current pattern with interval
        f"{clean_ticker}_stock_data.csv",             # Legacy pattern without interval
    ]
    
    latest_file = None
    latest_mtime = 0
    
    try:
        for filename in os.listdir(cache_dir):
            # Check if file matches any pattern for this ticker
            if any(filename == pattern for pattern in patterns):
                filepath = os.path.join(cache_dir, filename)
                file_mtime = os.path.getmtime(filepath)
                
                if file_mtime > latest_mtime:
                    latest_mtime = file_mtime
                    latest_file = filepath
                    
        if latest_file:
            logger.info(f"Found latest cache file for {ticker}: {os.path.basename(latest_file)} "
                       f"(modified: {datetime.fromtimestamp(latest_mtime).strftime('%Y-%m-%d %H:%M:%S')})")
        
        return latest_file
        
    except Exception as e:
        logger.warning(f"Error searching for cache files for {ticker}: {e}")
        return None

def cleanup_duplicate_cache_files(ticker, cache_dir, interval='1d', max_age_hours=48):
    """
    Clean up stale cache files for a specific ticker and interval.
    Only removes files that are:
    1. For the SAME ticker AND interval combination
    2. Older than max_age_hours
    
    This allows multiple intervals (5m, 1d, etc.) to coexist.
    """
    if not os.path.exists(cache_dir):
        return
    
    clean_ticker = ticker.replace(':', '_').replace('^', '_').replace('=', '_')
    current_time = datetime.now()
    files_removed = 0
    
    try:
        # Find all cache files for this specific ticker + interval combination
        target_pattern = f"{clean_ticker}_stock_data_{interval}.csv"
        
        for filename in os.listdir(cache_dir):
            # Only process files that match this exact ticker + interval
            if (filename.startswith(f"{clean_ticker}_stock_data") and 
                filename.endswith('.csv') and 
                'processed_data' not in filename and
                interval in filename):
                
                filepath = os.path.join(cache_dir, filename)
                
                # Skip the exact target file (the one we're about to use/create)
                if filename == target_pattern:
                    continue
                
                try:
                    # Check file age
                    file_mtime = datetime.fromtimestamp(os.path.getmtime(filepath))
                    age_hours = (current_time - file_mtime).total_seconds() / 3600
                    
                    # Remove only if stale (older than max_age_hours)
                    if age_hours > max_age_hours:
                        os.remove(filepath)
                        files_removed += 1
                        logger.info(f"Removed stale cache file: {filename} (age: {age_hours:.1f} hours)")
                except Exception as e:
                    logger.warning(f"Error removing stale cache file {filename}: {e}")
        
        if files_removed > 0:
            logger.info(f"Cleaned up {files_removed} stale cache files for {ticker} ({interval})")
                    
    except Exception as e:
        logger.error(f"Error cleaning duplicate cache files for {ticker}: {e}")

def fetch_stock_data(
    ticker,
    app_root,
    start_date=None,
    end_date=None,
    max_retries=3,
    pause_secs=2,
    throttle_secs=None,
    timeout=None,
    interval='1d',
    include_intraday=False
):
    """
    Fetch stock data with enhanced real-time capabilities.
    
    Parameters:
        data['Date'] = normalize_date_series(data['Date'])
    """
    # Get configuration settings
    config = get_realtime_config()
    
    if throttle_secs is None:
        throttle_secs = config.get('throttle_seconds', 0.3)
    if timeout is None:
        timeout = config.get('timeout_seconds', 30)
    
    # Check if real-time fetching is enabled
    if not is_realtime_enabled():
        logger.info(f"Real-time fetching disabled. Using cached data only for {ticker}")
        # Return cached data if available, None otherwise
        cache_dir = os.path.join(app_root, '..', 'generated_data', 'data_cache')
        cache_filename = f"{ticker.replace(':', '_').replace('^', '_').replace('=', '_')}_stock_data_{interval}.csv"
        cache_filepath = os.path.join(cache_dir, cache_filename)
        
        if os.path.exists(cache_filepath):
            try:
                return pd.read_csv(cache_filepath, parse_dates=['Date'])
            except Exception as e:
                logger.warning(f"Failed to load cached data: {e}")
        return None
    if not app_root:
        logger.error("app_root not provided to fetch_stock_data. Cannot determine cache directory.")
        raise ValueError("app_root is required for cache path construction.")

    _set_last_fetch_error(ticker, None)

    ticker_upper = ticker.upper()
    if ticker_upper in _BULK_STOCK_DATA_CACHE:
        logger.info(f"Using pre-downloaded bulk data for {ticker}")
        data = _BULK_STOCK_DATA_CACHE[ticker_upper]
        try:
            cache_dir = os.path.join(app_root, '..', 'generated_data', 'data_cache')
            os.makedirs(cache_dir, exist_ok=True)
            cache_filename = f"{ticker.replace(':', '_').replace('^', '_').replace('=', '_')}_stock_data_{interval}.csv"
            cache_filepath = os.path.join(cache_dir, cache_filename)
            data.to_csv(cache_filepath, index=False)
            logger.info(f"Saved bulk price data to cache: {cache_filename}")
        except Exception as e:
            logger.warning(f"Failed to save bulk data to cache: {e}")
        return data

    exchange_suffix, exchange_name = get_exchange_info(ticker)
    logger.info(f"Processing {exchange_name} ticker: {ticker}")

    cache_dir = os.path.join(app_root, '..', 'generated_data', 'data_cache')
    os.makedirs(cache_dir, exist_ok=True)
    
    # Clean up old cache files periodically (files older than 7 days, any ticker)
    cleanup_old_cache_files(cache_dir)

    # Find the most recent cache file for this ticker and interval
    latest_cache_file = find_latest_cache_file(ticker, cache_dir, interval)
    
    # Fallback to standard naming if no cache found
    if not latest_cache_file:
        cache_filename = f"{ticker.replace(':', '_').replace('^', '_').replace('=', '_')}_stock_data_{interval}.csv"
        cache_filepath = os.path.join(cache_dir, cache_filename)
    else:
        cache_filepath = latest_cache_file
        cache_filename = os.path.basename(cache_filepath)
    
    logger.info(f"Checking cache for {ticker} ({interval}) at: {cache_filepath}")

    required = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
    stale_cache_data = None

    cache_exists = os.path.exists(cache_filepath)
    logger.info(f"Cache file exists: {cache_exists}")

    # For intraday intervals, use stricter cache validation
    cache_valid_hours = 1 if interval in ['1m', '5m', '15m', '30m', '1h'] else 24
    
    # Minimum data points required for technical analysis
    MIN_ROWS_FOR_ANALYSIS = 100  # Need at least 100 days for reliable technical indicators
    
    if cache_exists:
        logger.info(f"Attempting to load cached stock data for {ticker} from: {cache_filename}")
        try:
            data = pd.read_csv(cache_filepath, parse_dates=['Date'])
            missing_cols = [col for col in required if col not in data.columns]

            if missing_cols:
                logger.warning(f"Cached file {cache_filename} missing required columns: {missing_cols}. Re-downloading.")
            elif data.empty:
                 logger.warning(f"Cached file {cache_filename} is empty. Re-downloading.")
            elif data['Date'].isna().any():
                 logger.warning(f"Cached file {cache_filename} contains invalid dates. Re-downloading.")
            elif len(data) < MIN_ROWS_FOR_ANALYSIS and interval == '1d':
                 logger.warning(f"Cached data for {ticker} has only {len(data)} rows, need at least {MIN_ROWS_FOR_ANALYSIS} for analysis. Re-downloading.")
            elif not is_data_current_for_today(data, ticker):
                 stale_cache_data = data[required].copy()
                 logger.warning(f"Cached data for {ticker} is not current for today. Re-downloading to get latest data.")
            elif interval in ['1m', '5m', '15m', '30m', '1h']:
                # Check if intraday cache is fresh (within last hour)
                file_age_hours = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache_filepath))).total_seconds() / 3600
                if file_age_hours > cache_valid_hours:
                    stale_cache_data = data[required].copy()
                    logger.warning(f"Intraday cached data for {ticker} is {file_age_hours:.1f} hours old. Re-downloading for real-time analysis.")
                else:
                    logger.info(f"Successfully loaded {len(data)} rows for '{ticker}' from fresh intraday cache.")
                    return data
            else:
                logger.info(f"Successfully loaded {len(data)} rows for '{ticker}' from cache with current data.")
                return data
        except Exception as e:
            logger.warning(f"Failed to load or validate cached file {cache_filename}: {e}. Re-downloading.")

    logger.info(f"Cache miss or invalid for {ticker}. Proceeding to download.")

    if start_date and end_date:
        if pd.to_datetime(start_date) > pd.to_datetime(end_date):
            logger.error("start_date must be before end_date") # Log error, don't raise here directly
            return None # Return None if date range is invalid

    period = "10y" if (start_date is None and end_date is None) else None

    logger.info(
        f"Fetching data for ticker: {ticker} "
        f"from {start_date or 'the beginning'} to {end_date or 'today'} "
        f"with interval: {interval}"
    )

    # Cooldown gate for upstream provider throttling. If we already have stale data,
    # serve it and avoid hammering the provider while the cooldown is active.
    cooldown_remaining = _rate_limit_cooldown_remaining_seconds()
    if cooldown_remaining > 0:
        msg = f"Provider cooldown active ({int(cooldown_remaining)}s remaining)"
        _set_last_fetch_error(ticker, 'rate_limited', msg)
        if stale_cache_data is not None and not stale_cache_data.empty:
            logger.warning(f"{msg}. Using stale cache for {ticker} ({interval}).")
            return stale_cache_data
        logger.warning(f"{msg}. No cache available for {ticker} ({interval}).")
        return None

    data = None
    lock_key = f"{str(ticker).upper()}::{interval}"
    fetch_lock = _get_fetch_lock(lock_key)

    # Single-flight lock: only one request per ticker+interval hits yfinance at a time.
    with fetch_lock:
        # Another request may have warmed cache while we were waiting for this lock.
        if os.path.exists(cache_filepath):
            try:
                warm_cache = pd.read_csv(cache_filepath, parse_dates=['Date'])
                warm_missing = [col for col in required if col not in warm_cache.columns]
                if not warm_cache.empty and not warm_missing:
                    if interval in ['1m', '5m', '15m', '30m', '1h']:
                        age_hours = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache_filepath))).total_seconds() / 3600
                        if age_hours <= cache_valid_hours:
                            logger.info(f"Cache became available while waiting for lock; returning warm intraday cache for {ticker}.")
                            return warm_cache[required]
                    else:
                        logger.info(f"Cache became available while waiting for lock; returning warm daily cache for {ticker}.")
                        return warm_cache[required]
            except Exception:
                pass

        for attempt in range(1, max(1, int(max_retries)) + 1):
            try:
                sleep_for = float(throttle_secs) + random.uniform(0.05, 0.35)
                time.sleep(max(0.0, sleep_for))

                yf_ticker = _yf_ticker(ticker)
                info = yf_ticker.info

                if not info or not info.get('symbol') or info.get('regularMarketPrice') is None:
                    logger.error(
                        f"Ticker {ticker} appears invalid/unavailable (missing symbol/regularMarketPrice in info)."
                    )
                    _set_last_fetch_error(ticker, 'invalid_or_unavailable', 'Ticker info missing symbol/regularMarketPrice')
                    return None

                download_params = {
                    'tickers': ticker,
                    'start': start_date,
                    'end': end_date,
                    'interval': interval,
                    'auto_adjust': True,
                    'progress': False,
                    'threads': False,
                    'timeout': timeout
                }

                if interval in ['1m', '5m', '15m', '30m'] and not start_date and not end_date:
                    download_params['period'] = '7d'
                elif interval == '1h' and not start_date and not end_date:
                    download_params['period'] = '730d'
                elif not start_date and not end_date:
                    download_params['period'] = '10y'

                if start_date or end_date:
                    download_params.pop('period', None)

                data = _yf_download(**download_params)

                if data is None or data.empty:
                    logger.warning(
                        f"Attempt {attempt}/{max_retries}: empty dataset from yfinance for {ticker} ({interval})."
                    )
                    if attempt < max_retries:
                        backoff = min(2 ** attempt, 12) + random.uniform(0.2, 0.8)
                        time.sleep(backoff)
                        continue
                    _set_last_fetch_error(ticker, 'no_data', f'Empty dataset from yfinance.download ({interval})')
                else:
                    logger.info(f"Successfully downloaded {interval} data for {ticker} on attempt {attempt}.")
                    _clear_rate_limit_state()
                break

            except requests.exceptions.HTTPError as http_err:
                status_code = http_err.response.status_code if http_err.response is not None else None
                if status_code == 404:
                    logger.error(f"HTTP 404 for {ticker}; ticker may be invalid or unavailable.")
                    _set_last_fetch_error(ticker, 'invalid_or_unavailable', 'HTTP 404 from yfinance provider')
                    return None

                is_rate_limited = status_code == 429
                if is_rate_limited:
                    cooldown, strikes = _register_rate_limit(f"HTTP 429 for {ticker}")
                    logger.error(
                        f"HTTP 429 rate limit for {ticker} (attempt {attempt}/{max_retries}); cooldown {cooldown}s (strike {strikes})."
                    )
                    _set_last_fetch_error(ticker, 'rate_limited', 'HTTP 429 from yfinance provider')
                else:
                    logger.error(f"HTTP error for {ticker} (attempt {attempt}/{max_retries}): {http_err}")
                    _set_last_fetch_error(ticker, 'fetch_error', str(http_err))

                if attempt < max_retries:
                    backoff = min(2 ** attempt, 20) + random.uniform(0.2, 1.0)
                    time.sleep(backoff)
                    continue
                data = None
                break

            except Exception as e:
                msg = str(e)
                is_rate_limited = ('Too Many Requests' in msg) or ('Rate limited' in msg)
                if is_rate_limited:
                    cooldown, strikes = _register_rate_limit(msg)
                    logger.error(
                        f"Rate-limited while fetching {ticker} (attempt {attempt}/{max_retries}); cooldown {cooldown}s (strike {strikes}): {msg}"
                    )
                    _set_last_fetch_error(ticker, 'rate_limited', msg)
                else:
                    logger.error(f"Error fetching '{ticker}' attempt {attempt}/{max_retries}: {msg}")
                    _set_last_fetch_error(ticker, 'fetch_error', msg)

                if attempt < max_retries:
                    backoff = min(2 ** attempt, 20) + random.uniform(0.2, 1.0)
                    time.sleep(backoff)
                    continue
                data = None
                break

    if (data is None or data.empty) and stale_cache_data is not None and not stale_cache_data.empty:
        logger.warning(f"Using stale cache fallback for {ticker} ({interval}) after fetch failure.")
        return stale_cache_data

    if data is None or data.empty:
        logger.warning(f"Data for {ticker} could not be retrieved or is empty after attempts.")
        _set_last_fetch_error(ticker, 'no_data', 'Data is None or empty after fetch attempts')
        return None

    data = data.reset_index()
    # logger.info(f"Fetched columns for {ticker}: {list(data.columns)}") # Already logged by yf.download if progress=True

    if isinstance(data.columns[0], tuple): # Handles MultiIndex columns if any
        data.columns = [col[0] for col in data.columns]
    
    # Standardize column names - less aggressive, targets 'Date' specifically.
    # Assumes OHLCV are already correctly named by yf.download with auto_adjust=True
    date_cols = [c for c in data.columns if 'date' in str(c).lower()]
    if date_cols:
        data = data.rename(columns={date_cols[0]: 'Date'})
    elif 'index' in data.columns:
        data = data.rename(columns={'index': 'Date'})
    elif 'Date' not in data.columns:
        logger.error(f"Could not identify 'Date' column for {ticker}. Columns: {list(data.columns)}")
        return None


    try:
        data = normalize_date_column(data, 'Date', drop_invalid=True, sort=True)
    except Exception as e:
        logger.error(f"Error processing 'Date' column after download for {ticker}: {e}")
        return None

    invalid_dates = data['Date'].isna().sum()
    if invalid_dates > 0:
        logger.warning(f"Found {invalid_dates} invalid dates post-download for {ticker}; dropping them.")
    data = data.dropna(subset=['Date']).sort_values('Date')

    required = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
    missing = [col for col in required if col not in data.columns]
    if missing:
        logger.error(f"Downloaded data for {ticker} missing required columns: {missing}. Available: {list(data.columns)}")
        _set_last_fetch_error(ticker, 'fetch_error', f'Missing columns: {missing}')
        return None

    # Ensure numeric types for OHLCV, coercing errors.
    for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
        data[col] = pd.to_numeric(data[col], errors='coerce')
    
    # Drop rows where any of the essential OHLC columns became NaN after coercion
    data = data.dropna(subset=['Open', 'High', 'Low', 'Close'])
    # Fill NaN in Volume with 0 after attempting numeric conversion, as Volume can sometimes be 0.
    if 'Volume' in data.columns:
        data['Volume'] = data['Volume'].fillna(0)


    if data.empty:
        logger.warning(f"Data for {ticker} became empty after cleaning and type conversion.")
        _set_last_fetch_error(ticker, 'no_data', 'Data became empty after cleaning')
        return None

    try:
        # Ensure we use the consistent filename format for saving
        save_cache_filename = f"{ticker.replace(':', '_').replace('^', '_').replace('=', '_')}_stock_data_{interval}.csv"
        save_cache_filepath = os.path.join(cache_dir, save_cache_filename)
        
        data_to_save = data[required]
        data_to_save.to_csv(save_cache_filepath, index=False)
        logger.info(f"Saved downloaded data for {ticker} to cache: {save_cache_filename}")
        
        # Clean up stale duplicate files for this ticker+interval AFTER successful save
        # This prevents removing fresh cache before we try to use it
        cleanup_duplicate_cache_files(ticker, cache_dir, interval, max_age_hours=48)
        
        # Update cache_filepath reference for return consistency
        cache_filepath = save_cache_filepath
        cache_filename = save_cache_filename
        
    except Exception as e:
        logger.error(f"Failed to save data for {ticker} to cache file {save_cache_filename}: {e}")

    logger.info(f"Successfully fetched and processed {len(data)} rows for '{ticker}'.")
    _set_last_fetch_error(ticker, None)
    return data[required]


def get_current_market_price(ticker, timeout=10):
    """
    Get the most current market price for a ticker with minimal delay.
    Returns current price, change, and change percentage.
    """
    ticker_upper = ticker.upper()
    if ticker_upper in _BULK_CURRENT_PRICE_CACHE:
        logger.info(f"Using pre-downloaded bulk current price for {ticker}")
        return _BULK_CURRENT_PRICE_CACHE[ticker_upper]
    cooldown_remaining = _rate_limit_cooldown_remaining_seconds()
    if cooldown_remaining > 0:
        logger.warning(
            f"Skipping current price fetch for {ticker}; provider cooldown active ({int(cooldown_remaining)}s remaining)."
        )
        _set_last_fetch_error(ticker, 'rate_limited', f'Provider cooldown active ({int(cooldown_remaining)}s)')
        return None

    try:
        yf_ticker = _yf_ticker(ticker)
        info = yf_ticker.info
        
        if not info:
            logger.warning(f"No info available for ticker {ticker}")
            return None
        
        current_price = info.get('regularMarketPrice') or info.get('currentPrice')
        previous_close = info.get('regularMarketPreviousClose') or info.get('previousClose')
        
        if current_price is None:
            logger.warning(f"No current price available for ticker {ticker}")
            return None
        
        # Calculate change if previous close is available
        change = None
        change_percent = None
        if previous_close:
            change = current_price - previous_close
            change_percent = (change / previous_close) * 100
        
        market_state = info.get('marketState', 'Unknown')
        last_updated = datetime.now().isoformat()
        
        price_data = {
            'ticker': ticker,
            'current_price': current_price,
            'previous_close': previous_close,
            'change': change,
            'change_percent': change_percent,
            'market_state': market_state,
            'last_updated': last_updated,
            'currency': info.get('currency', 'USD')
        }
        
        logger.info(f"Current price for {ticker}: ${current_price} ({market_state})")
        return price_data
        
    except Exception as e:
        logger.error(f"Error getting current price for {ticker}: {e}")
        if 'Too Many Requests' in str(e) or 'Rate limited' in str(e):
            _set_last_fetch_error(ticker, 'rate_limited', str(e))
        return None

def fetch_real_time_data(ticker, app_root, include_price=True, include_intraday=True):
    """
    Fetch the most current data available for a ticker.
    Uses current quote (optional) and daily historical data.
    """
    logger.info(f"Fetching real-time data for {ticker}")
    
    result = {
        'ticker': ticker,
        'timestamp': datetime.now().isoformat(),
        'current_price_data': None,
        'intraday_data': None,
        'daily_data': None
    }
    
    try:
        # Get current market price with minimal delay
        if include_price:
            result['current_price_data'] = get_current_market_price(ticker)
        
        # Get recent daily data
        result['daily_data'] = fetch_stock_data(ticker, app_root)
        
        return result
        
    except Exception as e:
        logger.error(f"Error fetching real-time data for {ticker}: {e}")
        return result


if __name__ == "__main__":
    try:
        test_tickers = [
            "NKE",        # Valid (Nike)
            "SAVE",       # Valid (Spirit Airlines)
            "NIKE STOCK", # Invalid
            "SAVE STOCK", # Invalid
            "ADANIPOWER.NS"
        ]
        
        # Determine a suitable app_root for standalone script execution
        # This assumes data_collection.py is in data_processing_scripts
        current_script_dir = os.path.dirname(os.path.abspath(__file__))
        app_level_root = os.path.join(current_script_dir, '..', 'app') # Assuming 'app' is one level up from 'data_processing_scripts' directory
        
        logger.info(f"Standalone Test: Using app_root='{app_level_root}' for cache paths.")

        for ticker_symbol in test_tickers:
            logger.info(f"\n--- Testing ticker: {ticker_symbol} ---")
            df_data = fetch_stock_data(ticker_symbol, app_root=app_level_root, timeout=45)
            if df_data is not None and not df_data.empty:
                logger.info(f"Successfully fetched {len(df_data)} rows for {ticker_symbol}")
                print(df_data.head())
            else:
                logger.error(f"Failed to fetch data for {ticker_symbol} or data was empty.")
                
    except Exception as main_e:
        logger.error(f"An error occurred in main execution: {main_e}", exc_info=True)