"""Indicators recomputed from the stored prices over the whole window (database/history_sync.py), the older-history
check, the stale-cache guard and the backfill script. No network, no database."""

from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from data_processing_scripts.feature_engineering import add_technical_indicators
from data_processing_scripts.trading_sessions import stored_indicator_frame
from database import price_history as ph
from database.data_mapper import DataMapper
from database.history_sync import (
    _day, check_history_before_download, history_before_download_is_stale, plan_indicator_rewrite, plan_price_rows,
)
from database.pipeline_data_collector import stale_price_download

TODAY = date(2026, 11, 3)


def price_rows(closes, start='2024-01-02', volume=1_000_000):
    days = pd.bdate_range(start, periods=len(closes))
    return [{'date': d.date(), 'open_price': c, 'high_price': c * 1.01, 'low_price': c * 0.99, 'close_price': c,
             'volume': volume} for d, c in zip(days, closes)]


class FakeDB:
    """execute_raw_sql stand-in: daily_price_data and technical_indicators rows in memory."""

    def __init__(self, prices, indicators=None):
        self.prices, self.indicators, self.calls = prices, indicators or [], []

    def __call__(self, sql, params=None):
        self.calls.append((sql, params))
        if 'FROM technical_indicators' in sql:
            return [r for r in self.indicators if r['stock_id'] == params[0]]
        if 'FROM daily_price_data' in sql:
            rows = [r for r in self.prices if _day(r['date']) >= _day(params[1])]
            if len(params) > 2:
                rows = [r for r in rows if _day(r['date']) <= _day(params[2])]
            return rows
        return []


def closes_series(n, seed=1):
    rng = np.random.default_rng(seed)
    return list(np.round(100 * np.exp(np.cumsum(rng.normal(0, 0.02, n))), 4))


# ---------------------------------------------------------------- A: whole window, full warm-up

def test_old_wrong_rows_before_the_last_months_are_rewritten():
    mapper = DataMapper()
    prices = price_rows(closes_series(900))
    db = FakeDB(prices)
    good, deletes, why = plan_indicator_rewrite(db, mapper, 1, today=TODAY)
    assert why == 'ok' and not deletes
    # store the right values, then spoil a row from 2025 (the old calendar-day values)
    stored = [dict(r) for r in good]
    old_day = next(r for r in stored if r['date'].startswith('2025-03'))
    old_day['sma_200'] = old_day['sma_200'] * 1.3
    db.indicators = stored
    rows, deletes, why = plan_indicator_rewrite(db, mapper, 1, today=TODAY)
    assert [r['date'] for r in rows] == [old_day['date']]
    assert not deletes


def test_the_window_is_two_years_and_the_200_day_average_is_200_sessions():
    mapper = DataMapper()
    closes = closes_series(900)
    prices = price_rows(closes)
    rows, _, _ = plan_indicator_rewrite(FakeDB(prices), mapper, 1, today=TODAY)
    assert rows[0]['date'] >= (TODAY - timedelta(days=730)).isoformat()
    last = rows[-1]
    assert last['sma_200'] == pytest.approx(np.mean(closes[-200:]))
    assert last['sma_50'] == pytest.approx(np.mean(closes[-50:]))


def test_short_history_uses_every_session_and_true_windows_alh():
    """ALH: 268 sessions. The 50-session average is the mean of the last 50 closes; the 200 one is empty until 200."""
    mapper = DataMapper()
    closes = closes_series(268, seed=7)
    prices = price_rows(closes, start='2025-10-09')
    rows, _, _ = plan_indicator_rewrite(FakeDB(prices), mapper, 1, today=TODAY)
    assert len(rows) == 268
    assert rows[-1]['sma_50'] == pytest.approx(np.mean(closes[-50:]))
    assert rows[-1]['sma_200'] == pytest.approx(np.mean(closes[-200:]))
    assert rows[198]['sma_200'] is None and rows[199]['sma_200'] is not None
    assert rows[48]['sma_50'] is None


def test_split_in_stored_prices_gives_split_consistent_indicators():
    """After the price step made the stored prices split-adjusted, the averages have no 10x jump (KLAC)."""
    mapper = DataMapper()
    closes = [c / 10 for c in closes_series(600, seed=3)]   # adjusted series, ~$10
    rows, _, _ = plan_indicator_rewrite(FakeDB(price_rows(closes)), mapper, 1, today=TODAY)
    sma = [r['sma_200'] for r in rows if r['sma_200'] is not None]
    assert max(sma) / min(sma) < 3
    stored_old = [dict(r, sma_200=r['sma_200'] * 10 if r['sma_200'] else None) for r in rows]
    fixed, _, _ = plan_indicator_rewrite(FakeDB(price_rows(closes), stored_old), mapper, 1, today=TODAY)
    assert len(fixed) == sum(1 for r in rows if r['sma_200'] is not None)


def test_indicator_days_without_a_session_are_deleted():
    mapper = DataMapper()
    prices = price_rows(closes_series(600))
    db = FakeDB(prices)
    good, _, _ = plan_indicator_rewrite(db, mapper, 1, today=TODAY)
    holiday = dict(good[-5], date='2026-07-04')
    db.indicators = [dict(r) for r in good] + [holiday]
    rows, deletes, _ = plan_indicator_rewrite(db, mapper, 1, today=TODAY)
    assert rows == [] and deletes == ['2026-07-04']


def test_unchanged_indicators_write_nothing():
    mapper = DataMapper()
    db = FakeDB(price_rows(closes_series(600)))
    good, _, _ = plan_indicator_rewrite(db, mapper, 1, today=TODAY)
    # stored values are rounded to 4 decimals: still "unchanged"
    db.indicators = [{k: (round(v, 4) if isinstance(v, float) else v) for k, v in r.items()} for r in good]
    rows, deletes, _ = plan_indicator_rewrite(db, mapper, 1, today=TODAY)
    assert rows == [] and deletes == []


def test_no_stored_prices_falls_back():
    assert plan_indicator_rewrite(FakeDB([]), DataMapper(), 1, today=TODAY)[0] is None


def test_standard_windows_do_not_change_the_model_path():
    frame = pd.DataFrame({'Date': pd.bdate_range('2026-01-01', periods=120)})
    closes = closes_series(120)
    for c in ('Open', 'High', 'Low', 'Close'):
        frame[c] = closes
    frame['Volume'] = 1000.0
    frame['SP500'] = 1.0
    model = add_technical_indicators(frame)              # adaptive windows, small dataset back-filled
    stored = add_technical_indicators(frame, standard_windows=True)
    assert len(model) == 120 and model['MA_200'].notna().all()
    assert stored['MA_200'].isna().all() and stored['MA_50'].notna().sum() == 71


def test_stored_indicator_frame_accepts_decimal_like_strings():
    rows = price_rows(closes_series(60))
    frame = pd.DataFrame([{'Date': r['date'], 'Open': str(r['open_price']), 'High': r['high_price'],
                           'Low': r['low_price'], 'Close': r['close_price'], 'Volume': r['volume']} for r in rows])
    out = stored_indicator_frame(frame)
    assert len(out) == 60 and out['MA_20'].iloc[-1] == pytest.approx(np.mean([r['close_price'] for r in rows[-20:]]))


# ---------------------------------------------------------------- B: last days and older history

def _frame(rows):
    return pd.DataFrame([{'Date': pd.Timestamp(r['date']), 'Open': r['open_price'], 'High': r['high_price'],
                          'Low': r['low_price'], 'Close': r['close_price'], 'Volume': r['volume']} for r in rows])


def test_mid_day_last_session_is_corrected_by_the_next_sync_aumn():
    mapper = DataMapper()
    final = price_rows([0.18, 0.18, 0.185, 0.19], start='2026-10-28', volume=14_000)
    stored = mapper.map_daily_prices(1, _frame(final))
    stored[-1]['close_price'], stored[-1]['volume'] = 0.1744, 1_000   # saved mid-day
    for r in stored:
        r['date'] = date.fromisoformat(r['date'])
    db = FakeDB(stored)
    rows, why = plan_price_rows(db, mapper, 1, _frame(final + price_rows([0.2], start='2026-11-03', volume=9_000)))
    assert why == 'ok'
    assert [r['date'] for r in rows] == ['2026-11-02', '2026-11-03']
    assert rows[0]['close_price'] == 0.19 and rows[0]['volume'] == 14_000


def test_stale_rows_before_the_download_are_detected_klac():
    mapper = DataMapper()
    download = price_rows([66.0, 65.97, 67.59], start='2024-11-04')
    stored = [dict(r) for r in download]
    stored[0]['close_price'] = 654.24                        # unadjusted, never rewritten
    stale, why = check_history_before_download(FakeDB(stored), mapper, 1, _frame(download))
    assert stale and '2024-11-04' in why
    assert not check_history_before_download(FakeDB(download), mapper, 1, _frame(download))[0]
    assert history_before_download_is_stale([], stored) == (False, 'no download')


def test_full_history_job_rewrites_rows_older_than_the_download(monkeypatch):
    class Ticker:
        calls = []

        def __init__(self, symbol):
            pass

        def history(self, **kw):
            Ticker.calls.append(kw)
            idx = pd.DatetimeIndex(pd.bdate_range('2024-06-10', '2024-11-20').tz_localize('America/New_York'))
            return pd.DataFrame({'Open': 70.0, 'High': 71.0, 'Low': 69.0, 'Close': 70.0, 'Volume': 1000}, index=idx)

    YF = type('YF', (), {'Ticker': Ticker})

    upserts, sql = [], []

    class DB:
        def table(self, name):
            class T:
                def upsert(self_, rows, on_conflict=None):
                    upserts.extend(rows)
                    return self_

                def execute(self_):
                    return None
            return T()

        def execute_raw_sql(self, q, p=None):
            sql.append(q)
            return []

    monkeypatch.setattr(ph, '_yf', lambda: YF)
    # KLAC: stored from 2024-06-10 (complete before), download window starts ~2024-11-04
    n = ph.backfill_price_history(DB(), DataMapper(), 9, 'KLAC', date(2024, 6, 10), today=date(2026, 11, 3))
    assert Ticker.calls[0]['auto_adjust'] is True
    days = [r['date'] for r in upserts]
    assert '2024-06-10' in days and '2024-11-04' in days and max(days) <= '2024-11-13'
    assert n == len(upserts) - 1
    assert any('price_history_complete_at = now()' in q for q in sql)


# ---------------------------------------------------------------- C: stale cached prices are not "success"

def test_stale_cache_after_failed_fetch_is_flagged_azo():
    frame = pd.DataFrame({'Date': pd.to_datetime(['2026-10-29', '2026-10-30'])})
    why = stale_price_download(frame, {'type': 'rate_limited'}, date(2026, 11, 2))
    assert why and '2026-10-30' in why and 'rate_limited' in why
    assert stale_price_download(frame, None, date(2026, 11, 2)) is None          # halted stock, no fetch error
    fresh = pd.DataFrame({'Date': pd.to_datetime(['2026-10-30', '2026-11-02'])})
    assert stale_price_download(fresh, {'type': 'rate_limited'}, date(2026, 11, 2)) is None


# ---------------------------------------------------------------- D: backfill script helpers

def test_backfill_script_cross_and_merge():
    from scripts.indicator_history_backfill import last_cross, merged, upsert_sql
    rows = [{'date': '2026-01-0%d' % i, 'sma_50': a, 'sma_200': 10} for i, a in enumerate([9, 9, 11, 11, 9], 1)]
    assert last_cross(rows) == ('2026-01-05', 'death')
    after = merged(rows, [{'date': '2026-01-05', 'sma_50': 12, 'sma_200': 10}], ['2026-01-01'])
    assert last_cross(after) == ('2026-01-03', 'golden') and len(after) == 4
    sql, cols = upsert_sql([{'stock_id': 1, 'date': '2026-01-01', 'sma_50': 1.0}])
    assert 'ON CONFLICT (stock_id, date)' in sql and 'sma_50 = EXCLUDED.sma_50' in sql and cols[0] == 'stock_id'
