"""Stock-page numbers are counted in trading sessions, not in the calendar days the model frame is filled to."""
import numpy as np
import pandas as pd
import pytest

from data_processing_scripts.trading_sessions import range_52w, session_indicator_frame, trading_rows
from database.data_mapper import DataMapper


def _sessions(n=320, seed=7):
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2025-06-02", periods=n)
    close = 100 * np.exp(np.cumsum(rng.normal(0, 0.01, n)))
    return pd.DataFrame({
        "Date": dates, "Open": close * 0.999, "High": close * 1.01, "Low": close * 0.99, "Close": close,
        "Volume": rng.integers(1_000_000, 2_000_000, n).astype(float), "SP500": 5000 + np.arange(n, dtype=float),
    })


def _calendar_filled(sessions):
    # Exactly what data_preprocessing.preprocess_data does: every calendar day, forward-filled.
    days = pd.date_range(sessions["Date"].min(), sessions["Date"].max(), freq="D")
    return sessions.set_index("Date").reindex(days).rename_axis("Date").ffill().reset_index()


def test_filled_weekend_days_are_dropped_and_real_sessions_kept():
    s = _sessions()
    filled = _calendar_filled(s)
    assert len(filled) > len(s)  # weekends were added
    back = trading_rows(filled)
    assert list(back["Date"]) == list(s["Date"])


def test_indicators_are_computed_over_sessions_not_calendar_days():
    s = _sessions()
    frame = session_indicator_frame(_calendar_filled(s))
    last = frame.iloc[-1]
    assert last["Date"] == s["Date"].iloc[-1]
    assert last["MA_200"] == pytest.approx(s["Close"].tail(200).mean(), rel=1e-9)
    assert last["MA_50"] == pytest.approx(s["Close"].tail(50).mean(), rel=1e-9)
    expected_vol = np.log(s["Close"] / s["Close"].shift(1)).tail(20).std() * np.sqrt(252) * 100
    assert last["Volatility_30d"] == pytest.approx(expected_vol, rel=1e-9)
    assert frame["Date"].dt.dayofweek.max() <= 4  # no weekend rows are written


def test_the_calendar_frame_understates_volatility_this_is_the_bug_fixed():
    s = _sessions()
    filled = _calendar_filled(s)
    calendar_vol = np.log(filled["Close"] / filled["Close"].shift(1)).tail(20).std() * np.sqrt(252) * 100
    session_vol = session_indicator_frame(filled).iloc[-1]["Volatility_30d"]
    assert calendar_vol < session_vol * 0.9


def test_52_week_range_short_history_widens_the_vendor_value_with_recent_sessions():
    s = _sessions(120)  # less than a year of sessions
    recent_high = float(s["High"].tail(30).max())
    high, low = range_52w(s, recent_high - 5, 1.0)
    assert high == pytest.approx(recent_high)  # a new high since the vendor refresh is not missed
    assert low == 1.0  # the vendor's traded low is kept
    assert range_52w(s, None, None) == (None, None)  # no vendor range and no year of data: nothing is invented
    assert range_52w(None, 348.15, 243.42) == (348.15, 243.42)


def test_market_snapshot_uses_a_year_of_sessions_and_session_counts():
    s = _sessions(400)
    filled = _calendar_filled(s)
    info = {"fiftyTwoWeekHigh": 1.0, "fiftyTwoWeekLow": 0.5}  # stale vendor values are not used
    record = DataMapper().map_market_snapshot(1, {"current_price": float(s["Close"].iloc[-1])}, info, filled)
    assert record["high_52w"] == pytest.approx(float(s["High"].tail(252).max()))
    assert record["low_52w"] == pytest.approx(float(s["Low"].tail(252).min()))
    year_ago = float(s["Close"].iloc[-252])
    assert record["change_52w_pct"] == pytest.approx((float(s["Close"].iloc[-1]) - year_ago) / year_ago * 100, abs=0.01)
