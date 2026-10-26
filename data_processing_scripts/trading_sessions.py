"""
Real trading sessions out of the calendar-daily frame the pipeline builds.

data_preprocessing.preprocess_data re-indexes prices to EVERY calendar day and forward-fills, so the models get a
continuous series. Everything a stock page shows must instead be counted in trading sessions: on the calendar frame a
"200-day average" spans about 138 sessions (AAPL 23 Oct 2026: $309.14 stored vs $294.29 true), and the 30-day
volatility mixes in weekend "days" with zero change (10.9 % stored vs 16.4 % true).

A filled day is an exact copy of the previous row's Open/High/Low/Close/Volume; real consecutive sessions never repeat
all five. The Next.js pages use the same rule (lib/data/price-facts.ts tradingSessions).
"""
from __future__ import annotations

from typing import Optional

import pandas as pd

OHLCV = ['Open', 'High', 'Low', 'Close', 'Volume']
TRADING_DAYS_PER_YEAR = 252
# A frame with at least this many sessions covers (nearly) a year: its own 52-week range is used.
MIN_SESSIONS_FOR_OWN_RANGE = 240
# With less history, the vendor range is widened by the most recent sessions.
RANGE_GUARD_SESSIONS = 30
MARKET_TZ = 'America/New_York'


def market_date(sessions: Optional[pd.DataFrame], quote: Optional[dict] = None) -> str:
    """
    The trading day a price belongs to, as YYYY-MM-DD. The quote's own market time when the provider gives one, else
    the newest session in the frame, else today in New York. Never the machine's clock: the Azure workers run on
    Asia/Kolkata time, so a Friday close written at 21:50 UTC was stored as Saturday's row.
    """
    ts = (quote or {}).get('market_time')
    if ts:
        try:
            return pd.Timestamp(int(ts), unit='s', tz='UTC').tz_convert(MARKET_TZ).strftime('%Y-%m-%d')
        except (TypeError, ValueError, OverflowError):
            pass
    if sessions is not None and not getattr(sessions, 'empty', True) and 'Date' in sessions.columns:
        last = pd.to_datetime(sessions['Date'], errors='coerce').max()
        if pd.notna(last):
            return last.strftime('%Y-%m-%d')
    return pd.Timestamp.now(tz=MARKET_TZ).strftime('%Y-%m-%d')


def trading_rows(frame: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """Rows that are real sessions (oldest first), dropping forward-filled calendar days. Same columns, new index."""
    if frame is None or getattr(frame, 'empty', True) or not set(OHLCV).issubset(frame.columns):
        return frame
    df = frame.sort_values('Date') if 'Date' in frame.columns else frame
    values = df[OHLCV]
    repeat = values.eq(values.shift(1)).all(axis=1)
    return df[~repeat].reset_index(drop=True)


def session_indicator_frame(processed: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """
    The processed frame restricted to trading sessions, with every technical indicator recomputed on those sessions
    (same add_technical_indicators as the models use, so the column names and formulas do not change). Columns the
    indicator step does not produce are kept from the processed frame.
    """
    from data_processing_scripts.feature_engineering import add_technical_indicators

    sessions = trading_rows(processed)
    if sessions is None or sessions.empty or 'Date' not in sessions.columns:
        return sessions
    # The indicator step reads the prices and the S&P 500 level (Market_Cap_Relative); everything else it adds.
    inputs = ['Date'] + OHLCV + [c for c in ('SP500',) if c in sessions.columns]
    recomputed = add_technical_indicators(sessions[inputs].copy())
    indicator_cols = [c for c in recomputed.columns if c not in inputs]
    kept = sessions.drop(columns=[c for c in indicator_cols if c in sessions.columns])
    return kept.merge(recomputed[['Date'] + indicator_cols], on='Date', how='inner')


def range_52w(sessions: Optional[pd.DataFrame], vendor_high, vendor_low):
    """
    52-week high/low from the daily highs and lows of the last 252 sessions: the same price series the charts draw.
    The vendor value lags (refreshed about monthly): NVDA kept 236.54 while it traded up to 243.37, and kept an expired
    low of 138.83 when the real 52-week low was 151.29. With less than a year of sessions (a recent listing, a short
    frame) the vendor value is used, widened by the sessions we have so a new high is never missing.
    """
    def _num(v):
        try:
            f = float(v)
        except (TypeError, ValueError):
            return None
        return f if f > 0 else None

    high, low = _num(vendor_high), _num(vendor_low)
    if sessions is None or getattr(sessions, 'empty', True):
        return high, low
    if len(sessions) >= MIN_SESSIONS_FOR_OWN_RANGE:
        year = sessions.tail(TRADING_DAYS_PER_YEAR)
        own_high = pd.to_numeric(year.get('High'), errors='coerce').max() if 'High' in year else None
        own_low = pd.to_numeric(year.get('Low'), errors='coerce').min() if 'Low' in year else None
        if own_high is not None and pd.notna(own_high) and own_high > 0 and own_low is not None and pd.notna(own_low) and own_low > 0:
            return float(own_high), float(own_low)
    recent = sessions.tail(RANGE_GUARD_SESSIONS)
    recent_high = pd.to_numeric(recent.get('High'), errors='coerce').max() if 'High' in recent else None
    recent_low = pd.to_numeric(recent.get('Low'), errors='coerce').min() if 'Low' in recent else None
    if recent_high is not None and pd.notna(recent_high) and recent_high > 0:
        high = max(high, float(recent_high)) if high is not None else None
    if recent_low is not None and pd.notna(recent_low) and recent_low > 0:
        low = min(low, float(recent_low)) if low is not None else None
    return high, low
