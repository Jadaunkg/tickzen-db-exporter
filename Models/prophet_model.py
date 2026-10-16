#!/usr/bin/env python3
"""
Prophet forecasting model (v2)
==============================

Produces a daily median path with an 80% prediction band for a stock's adjusted close, plus the weekly/monthly
period-end table the rest of the platform consumes.

What changed from v1 and why
----------------------------
v1 fitted Prophet to the raw price with logistic growth (floor 0, cap 2x the historical max), multiplicative
seasonality and RSI/MACD regressors held at their last 7-day mean.  Walk-forward tests (docs/PROPHET_V2_RESEARCH.md)
showed that this configuration is significantly worse than simply carrying the last close forward, that its
"80%" band contained the outcome only 23-44% of the time, that it forecast a collapse to less than half the price
for 13-15% of cases and that clipping produced exact zero prices (which the exporter then dropped, leaving
stocks with a truncated forecast).  No Prophet variant fixed this because a price trend carries almost no
out-of-sample information.

v2 therefore works in log space (a price can never be <= 0), keeps Prophet only as a heavily shrunk,
bounded trend component (``forecast_core.TREND_WEIGHT``) and derives the bands from the stock's own realised
volatility, which the backtests show to be calibrated (79-84% coverage at 3/6/12 months).  The maths lives in
``Models/forecast_core.py`` (pure numpy/pandas); this module handles input cleaning, the optional Prophet fit
and the report-friendly aggregation.

Contract (unchanged for callers)
--------------------------------
``train_prophet_model(data, ticker, forecast_horizon, timestamp, macro_data)`` returns
``(model_or_None, forecast_df, agg_actual, agg_forecast)``:

* ``forecast_df``: future business days only - ``ds, yhat, yhat_lower, yhat_upper, trend``
* ``agg_actual``: recent actual prices, ``Period, Average``
* ``agg_forecast``: ``Period, Low, Average, High`` at the end of each period (``Average`` is the median/base)

The model never raises on a Prophet/CmdStan failure: it falls back to the no-trend forecast and records that in
the returned frame's ``attrs['trend_component_used']``.  Forecasts are statistical scenarios, not advice.
"""

import logging

import numpy as np
import pandas as pd
import re

from Models import forecast_core

logger = logging.getLogger(__name__)
TICKZEN_PROPHET_PIPELINE_VERSION = forecast_core.PIPELINE_VERSION
FORECAST_BAND_DEFINITION = forecast_core.BAND_DEFINITION

try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except (ImportError, AttributeError) as e:  # pragma: no cover - environment dependent
    logger.warning("Prophet is not importable (%s); forecasts will use the no-trend path.", e)
    Prophet = None
    PROPHET_AVAILABLE = False


# ------------------ Helper Function ------------------
def parse_time_period(time_period: str) -> int:
    """
    Converts a custom time-period string into a number of days.
    See docstring above.
    """
    time_period = time_period.lower().strip()
    pattern = r"^(\d+)([dwmy])$"
    match = re.match(pattern, time_period)
    if not match:
        raise ValueError("Invalid time period format. Please use e.g., '3m', '6m', '1y'.")
    value = int(match.group(1))
    unit = match.group(2)
    if unit == 'd':
        return value
    elif unit == 'w':
        return value * 7
    elif unit == 'm':
        return value * 30
    elif unit == 'y':
        return value * 365
    else:
        raise ValueError("Unsupported time unit. Supported: d, w, m, y.")


def _forecast_cadence(forecast_days: int) -> str:
    """Return the reporting cadence for a calendar-day forecast horizon."""
    if forecast_days <= 30:
        return 'daily'
    if forecast_days <= 90:
        return 'weekly'
    return 'monthly'


def calendar_horizon_end(last_date, forecast_horizon: str):
    """Resolve a selected horizon to its actual calendar endpoint."""
    match = re.fullmatch(r'(\d+)([dwmy])', str(forecast_horizon).lower().strip())
    if not match:
        raise ValueError('Invalid forecast horizon.')
    amount = int(match.group(1))
    unit = match.group(2)
    anchor = pd.Timestamp(last_date).normalize()
    if unit == 'd':
        return anchor + pd.Timedelta(days=amount)
    if unit == 'w':
        return anchor + pd.Timedelta(weeks=amount)
    if unit == 'm':
        return anchor + pd.DateOffset(months=amount)
    return anchor + pd.DateOffset(years=amount)


def _business_day_forecast_dates(last_date, forecast_horizon: str):
    """Weekdays through the selected calendar horizon; exchange holidays are not modeled."""
    anchor = pd.Timestamp(last_date).normalize()
    horizon_end = calendar_horizon_end(anchor, forecast_horizon)
    return pd.bdate_range(start=anchor + pd.Timedelta(days=1), end=horizon_end)


def _aggregate_forecast_bands(forecast, cadence: str):
    """Low / Average / High at the end of each period (see forecast_core.aggregate_period_end)."""
    return forecast_core.aggregate_period_end(forecast, cadence)


# ------------------ Main Training Function ------------------
def _clean_history(data):
    if not isinstance(data, pd.DataFrame) or not {'Date', 'Close'}.issubset(data.columns):
        raise ValueError("Forecast input must contain 'Date' and 'Close' columns.")
    data = data.copy()
    data['Date'] = pd.to_datetime(data['Date'], errors='coerce')
    data['Close'] = pd.to_numeric(data['Close'], errors='coerce')
    data = data.dropna(subset=['Date', 'Close'])
    data = data[data['Close'] > 0].drop_duplicates(subset=['Date'], keep='last').sort_values('Date')
    if data.empty:
        raise ValueError('Forecast input has no valid positive closing prices.')
    return data


def train_prophet_model(data, ticker='STOCK', forecast_horizon='1y', timestamp=None, macro_data=None):
    """
    Forecast a stock price path for ``forecast_horizon`` (e.g. '3m', '6m', '1y').

    Args:
        data (pd.DataFrame): at least ``Date`` and ``Close`` (split/dividend-adjusted closes).
        ticker (str): informational only; v2 uses no per-ticker tuning.
        forecast_horizon (str): calendar horizon such as '3m', '1y'.
        timestamp, macro_data: accepted for backward compatibility; unused.

    Returns:
        (model or None, forecast, agg_actual, agg_forecast) - see the module docstring.
    """
    cleaned = _clean_history(data)
    return _train_prophet_model_native(cleaned, ticker, forecast_horizon, timestamp, macro_data)


def _fit_prophet_trend(history, future_dates):
    """Prophet log-linear trend -> (fitted model, log move vs today per future date), or (None, None) on any failure."""
    if not PROPHET_AVAILABLE or forecast_core.TREND_WEIGHT <= 0:
        return None, None
    try:
        logging.getLogger('cmdstanpy').setLevel(logging.WARNING)
        logging.getLogger('prophet').setLevel(logging.WARNING)
        frame = pd.DataFrame({'ds': history['Date'].values, 'y': np.log(history['Close'].astype(float).values)})
        model = Prophet(
            growth='linear',
            yearly_seasonality=False,
            weekly_seasonality=False,
            daily_seasonality=False,
            changepoint_prior_scale=0.01,
            uncertainty_samples=0,
        )
        model.fit(frame)
        predicted = model.predict(pd.DataFrame({'ds': list(frame['ds']) + list(future_dates)}))
        fitted_today = float(predicted['yhat'].iloc[len(frame) - 1])
        deviation = predicted['yhat'].iloc[len(frame):].to_numpy(dtype=float) - fitted_today
        if not np.isfinite(deviation).all():
            return None, None
        return model, deviation
    except Exception as exc:  # CmdStan missing/crashing must never take the forecast down
        logger.warning("Prophet trend component unavailable (%s); using the no-trend forecast.", exc)
        return None, None


def _aggregate_actuals(df, last_date, cadence):
    """Recent actual prices on the same period grid as the forecast table."""
    if cadence == 'daily':
        recent = df[df['ds'] >= last_date - pd.Timedelta(days=15)].copy()
        recent['Period'] = recent['ds'].dt.strftime('%Y-%m-%d')
        out = recent.groupby('Period').agg({'y': 'mean'}).reset_index()
        return out.rename(columns={'y': 'Average'})
    if cadence == 'weekly':
        recent = df[df['ds'] >= last_date - pd.DateOffset(months=3)].copy()
        recent['Period'] = recent['ds'].dt.to_period('W-SUN').dt.start_time.dt.strftime('%Y-%m-%d')
        out = recent.groupby('Period', as_index=False)['y'].mean()
        return out.rename(columns={'y': 'Average'})
    recent = df[df['ds'] >= last_date - pd.DateOffset(months=8)].copy()
    recent['Period'] = recent['ds'].dt.to_period('M').dt.strftime('%Y-%m')
    current_month = last_date.to_period('M').strftime('%Y-%m')
    rows = []
    for period, group in recent.groupby('Period'):
        # the current month is partial: show the latest price, not a month-to-date average
        rows.append({'Period': period, 'Average': group['y'].iloc[-1] if period == current_month else group['y'].mean()})
    return pd.DataFrame(rows, columns=['Period', 'Average'])


def _train_prophet_model_native(data, ticker='STOCK', forecast_horizon='1y', timestamp=None, macro_data=None):
    """Build the v2 forecast from already-cleaned history."""
    cleaned = _clean_history(data)
    df = cleaned.rename(columns={'Date': 'ds', 'Close': 'y'})[['ds', 'y']].copy()
    df['ds'] = pd.to_datetime(df['ds'])
    last_date = df['ds'].max()

    forecast_days = parse_time_period(forecast_horizon)
    future_dates = _business_day_forecast_dates(last_date, forecast_horizon)
    if len(future_dates) == 0:
        raise ValueError('Forecast horizon contains no business days.')

    model, deviation = _fit_prophet_trend(cleaned, future_dates)
    forecast = forecast_core.project_paths(cleaned.set_index('Date')['Close'], future_dates, deviation)

    cadence = _forecast_cadence(forecast_days)
    agg_actual = _aggregate_actuals(df, last_date, cadence)
    agg_forecast = _aggregate_forecast_bands(forecast, cadence)
    return model, forecast, agg_actual, agg_forecast
