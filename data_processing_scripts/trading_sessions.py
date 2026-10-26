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
# Recent sessions used to widen the vendor's 52-week range (older rows are dividend-adjusted, so lower than traded).
RANGE_GUARD_SESSIONS = 30


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


def widened_52w_range(sessions: Optional[pd.DataFrame], vendor_high, vendor_low):
    """
    Vendor 52-week high/low (refreshed about monthly) widened by the last RANGE_GUARD_SESSIONS sessions, so a new high
    made since the vendor refresh is never missing (AAPL: vendor 348.15 while it traded at 351.73).
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
    recent = sessions.tail(RANGE_GUARD_SESSIONS)
    recent_high = pd.to_numeric(recent.get('High'), errors='coerce').max() if 'High' in recent else None
    recent_low = pd.to_numeric(recent.get('Low'), errors='coerce').min() if 'Low' in recent else None
    if recent_high is not None and pd.notna(recent_high) and recent_high > 0:
        high = max(high, float(recent_high)) if high is not None else None
    if recent_low is not None and pd.notna(recent_low) and recent_low > 0:
        low = min(low, float(recent_low)) if low is not None else None
    return high, low
