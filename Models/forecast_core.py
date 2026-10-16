"""Forecast core: positive-by-construction price paths with calibrated prediction bands.

Why this exists
---------------
Walk-forward experiments (see docs/PROPHET_V2_RESEARCH.md; 300 stocks x 10 origins, 2-year training windows
like production) showed that the previous model - Prophet on raw price with logistic growth, floor 0 and
multiplicative seasonality - is significantly *worse than the last close* at every horizon (+0.21 log error at
12 months, 95% CI [+0.18, +0.24]), produced 80% intervals that contained the outcome only 23% / 31% / 44% of the
time at 3 / 6 / 12 months, predicted a collapse to under half the current price for 13-15% of forecasts (8% happened)
and clipped paths to exactly 0.  No Prophet configuration (log vs raw, linear vs logistic, with/without
seasonality or regressors, any changepoint scale) beat persistence, because the extrapolated trend of a price
series carries essentially no out-of-sample information.

So the forecast is built from the two things that *are* measurable:

* a long-run drift (``DRIFT_ANNUAL``) plus a heavily shrunk Prophet log-trend (``TREND_WEIGHT``) for the centre line;
* the stock's own realised volatility, which grows with the square root of time, for the bands.

Everything is computed in log space, so a price can never be zero or negative, ``low <= base <= high`` holds by
construction, and the interval width reflects how volatile the stock really is.

This module is pure numpy/pandas (no Prophet import) so it can be unit-tested and reused anywhere.
"""

from __future__ import annotations

import math
from typing import Optional

import numpy as np
import pandas as pd

PIPELINE_VERSION = 'tickzen-prophet-v2'

TRADING_DAYS = 252
# Long-run equity drift in log-price terms.  Backtests: 7%/yr beat 0% and the GBM-adjusted median.
DRIFT_ANNUAL = 0.07
# Weight on Prophet's extrapolated log-trend.  Walk-forward optimum is ~0.1; 0.5 is already significantly worse
# than 0 (see research doc).  Set to 0.0 to drop Prophet's contribution entirely.
TREND_WEIGHT = 0.10
# Hard cap on what the trend component may move the centre line, in log terms (~ +/-28%).
TREND_MAX_ABS_LOG = 0.25

INTERVAL = 0.80
Z_INTERVAL = 1.2815515655446004  # standard-normal quantile for a central 80% interval

MIN_OBSERVATIONS = 60  # fewer valid closes than this cannot support a volatility estimate
VOL_SHORT_WINDOW = 126
VOL_LONG_WINDOW = 504
VOL_FLOOR_DAILY = 0.003  # ~4.8% annualised
VOL_CEIL_DAILY = 0.12    # ~190% annualised

BAND_DEFINITION = (
    'Central 80% prediction interval at the end of each period from a lognormal model scaled by the stock\'s own '
    'realised volatility (sqrt-time growth); base is the median path (long-run drift plus a shrunk Prophet trend).'
)
METHOD_DESCRIPTION = (
    'Log-price Prophet trend (linear, no seasonality, weight %.2f) + %.0f%% annual drift, '
    'volatility-calibrated lognormal bands' % (TREND_WEIGHT, DRIFT_ANNUAL * 100)
)


def daily_log_returns(close: pd.Series) -> np.ndarray:
    values = pd.to_numeric(close, errors='coerce').to_numpy(dtype=float)
    values = values[np.isfinite(values) & (values > 0)]
    if len(values) < 2:
        return np.array([])
    return np.diff(np.log(values))


def estimate_daily_volatility(close: pd.Series) -> float:
    """Daily log-return volatility: average variance of the last 126 and last 504 sessions (clamped).

    The short window reacts to regime changes, the long one stops a quiet quarter from producing bands that are
    too narrow for a 12-month horizon; the mix gave the best interval score in the walk-forward tests.
    """
    returns = daily_log_returns(close)
    if len(returns) < 20:
        raise ValueError('Not enough price history to estimate volatility.')
    var_short = float(np.var(returns[-VOL_SHORT_WINDOW:], ddof=1))
    var_long = float(np.var(returns[-VOL_LONG_WINDOW:], ddof=1))
    sigma = math.sqrt(0.5 * var_short + 0.5 * var_long)
    return float(min(max(sigma, VOL_FLOOR_DAILY), VOL_CEIL_DAILY))


def project_paths(
    close: pd.Series,
    future_dates: pd.DatetimeIndex,
    trend_log_deviation: Optional[np.ndarray] = None,
) -> pd.DataFrame:
    """Daily median path and 80% band for ``future_dates``.

    ``trend_log_deviation`` is Prophet's extrapolated log-price move relative to its fitted level at the last
    training date (one value per future date); ``None`` means no trend component (e.g. Prophet unavailable).
    """
    clean = pd.to_numeric(close, errors='coerce').dropna()
    clean = clean[clean > 0]
    if len(clean) < MIN_OBSERVATIONS:
        raise ValueError(f'Need at least {MIN_OBSERVATIONS} valid closes to forecast; got {len(clean)}.')
    n = len(future_dates)
    if n == 0:
        raise ValueError('No future dates to forecast.')

    sigma = estimate_daily_volatility(clean)
    t = np.arange(1, n + 1, dtype=float)

    applied_trend = np.zeros(n)
    trend_used = False
    if trend_log_deviation is not None:
        deviation = np.asarray(trend_log_deviation, dtype=float)
        if deviation.shape == (n,) and np.isfinite(deviation).all():
            applied_trend = np.clip(TREND_WEIGHT * deviation, -TREND_MAX_ABS_LOG, TREND_MAX_ABS_LOG)
            trend_used = TREND_WEIGHT > 0

    center = math.log(float(clean.iloc[-1])) + (DRIFT_ANNUAL / TRADING_DAYS) * t + applied_trend
    half_width = Z_INTERVAL * sigma * np.sqrt(t)

    frame = pd.DataFrame(
        {
            'ds': pd.DatetimeIndex(future_dates),
            'yhat': np.exp(center),
            'yhat_lower': np.exp(center - half_width),
            'yhat_upper': np.exp(center + half_width),
            'trend': np.exp(center),
        }
    )
    frame.attrs['daily_volatility'] = sigma
    frame.attrs['trend_component_used'] = bool(trend_used)
    check_forecast_invariants(frame, float(clean.iloc[-1]))
    return frame


def check_forecast_invariants(frame: pd.DataFrame, anchor_price: float) -> None:
    """Raise ValueError if a forecast frame could ever show a zero/negative/unordered price."""
    for column in ('yhat', 'yhat_lower', 'yhat_upper'):
        values = frame[column].to_numpy(dtype=float)
        if not np.isfinite(values).all():
            raise ValueError(f'Forecast column {column} contains non-finite values.')
        if (values <= 0).any():
            raise ValueError(f'Forecast column {column} contains non-positive prices.')
    if not ((frame['yhat_lower'] <= frame['yhat']) & (frame['yhat'] <= frame['yhat_upper'])).all():
        raise ValueError('Forecast bounds are not ordered low <= base <= high.')
    if not (anchor_price > 0 and math.isfinite(anchor_price)):
        raise ValueError('Forecast anchor price must be positive and finite.')


def aggregate_period_end(frame: pd.DataFrame, cadence: str) -> pd.DataFrame:
    """Low / Average(base) / High at the LAST forecast session of each daily, weekly or monthly period.

    The backtests measured coverage at the horizon date, so the reported band is the band at the period end
    (not the min/max over the period, which mixes a narrow early band with a wide late one).
    """
    columns = ['Period', 'Low', 'Average', 'High']
    if frame is None or frame.empty:
        return pd.DataFrame(columns=columns)
    data = frame[['ds', 'yhat_lower', 'yhat', 'yhat_upper']].copy()
    data['ds'] = pd.to_datetime(data['ds'], errors='coerce')
    for column in ['yhat_lower', 'yhat', 'yhat_upper']:
        data[column] = pd.to_numeric(data[column], errors='coerce')
    data = data.dropna(subset=['ds', 'yhat_lower', 'yhat', 'yhat_upper']).sort_values('ds')
    if data.empty:
        return pd.DataFrame(columns=columns)

    if cadence == 'daily':
        data['Period'] = data['ds'].dt.strftime('%Y-%m-%d')
    elif cadence == 'weekly':
        data['Period'] = data['ds'].dt.to_period('W-SUN').dt.start_time.dt.strftime('%Y-%m-%d')
    elif cadence == 'monthly':
        data['Period'] = data['ds'].dt.to_period('M').dt.strftime('%Y-%m')
    else:
        raise ValueError(f'Unsupported forecast cadence: {cadence}')

    last = data.groupby('Period', sort=True).tail(1)
    result = last.rename(columns={'yhat_lower': 'Low', 'yhat': 'Average', 'yhat_upper': 'High'})[columns]
    return result.reset_index(drop=True)
