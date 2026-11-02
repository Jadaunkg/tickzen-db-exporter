"""Stored history follows today's download: database/history_sync.py (no network, no database)."""

from datetime import date
from decimal import Decimal

import pandas as pd

from database.data_mapper import DataMapper
from database.history_sync import (
    PRICE_COMPARE,
    changed_records,
    coverage_ok,
    is_new,
    plan_indicator_rows,
    plan_price_rows,
    values_differ,
)


def test_values_differ_ignores_4_decimal_rounding_but_not_real_changes():
    assert not values_differ(204.09221, Decimal('204.0922'))
    assert values_differ(204.1, Decimal('204.0922'))
    assert values_differ(None, Decimal('1.0'))
    assert values_differ(1.0, None)
    assert not values_differ(None, None)
    assert not values_differ(float('nan'), None)
    assert values_differ('Increasing', 'Mixed')
    assert not values_differ('', None)


def test_changed_records_keeps_new_and_changed_rows_only():
    new = [
        {'date': '2026-10-28', 'close_price': 10.0},
        {'date': '2026-10-29', 'close_price': 11.0},
        {'date': '2026-10-30', 'close_price': 12.0},
    ]
    stored = [
        {'date': date(2026, 10, 28), 'close_price': Decimal('10.0000')},
        {'date': date(2026, 10, 29), 'close_price': Decimal('10.5000')},
    ]
    out = changed_records(new, stored, ['close_price'])
    assert [r['date'] for r in out] == ['2026-10-29', '2026-10-30']


def test_cumulative_and_bookkeeping_columns_never_force_a_rewrite():
    new = [{'stock_id': 1, 'date': '2026-10-30', 'sma_200': 204.0922, 'obv': 999.0, 'vpt': 5.0}]
    stored = [{'id': 7, 'stock_id': 1, 'date': date(2026, 10, 30), 'sma_200': Decimal('204.0922'), 'obv': 1.0, 'vpt': 2.0,
               'created_at': 'x', 'updated_at': 'y'}]
    assert changed_records(new, stored) == []
    stored[0]['sma_200'] = Decimal('210.9478')  # the old calendar-day value
    assert len(changed_records(new, stored)) == 1


def test_coverage_guard():
    days = [d.strftime('%Y-%m-%d') for d in pd.bdate_range('2026-01-02', '2026-10-30')]
    ok, _ = coverage_ok(days, days, days[0])
    assert ok
    # a download that starts months late must not rewrite the window
    ok, why = coverage_ok(days[60:], days, days[0])
    assert not ok and 'starts' in why
    # a download missing many stored sessions is not trusted either
    ok, why = coverage_ok(days[::2], days, days[0])
    assert not ok and 'only' in why
    assert coverage_ok([], days, days[0])[0] is False


def _frame(closes, start='2026-06-01'):
    dates = pd.bdate_range(start, periods=len(closes))
    return pd.DataFrame({'Date': dates, 'Open': closes, 'High': [c * 1.01 for c in closes], 'Low': [c * 0.99 for c in closes],
                         'Close': closes, 'Volume': [1_000_000] * len(closes)})


class FakeSQL:
    """Stands in for execute_raw_sql: returns the stored rows dated on or after the given day."""

    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def __call__(self, sql, params):
        self.calls.append((sql, params))
        return [r for r in self.rows if str(r['date']) >= str(params[1])]


def test_split_adjusted_download_rewrites_the_old_unadjusted_rows():
    mapper = DataMapper()
    # stored: 10 days at ~$2,000 (before a 10-for-1 split), then 5 days at ~$210
    stored_closes = [2000 + i for i in range(10)] + [210 + i for i in range(5)]
    stored = mapper.map_daily_prices(1, _frame(stored_closes))
    for r in stored:
        r['date'] = date.fromisoformat(r['date'])
    # today's download: the same days split-adjusted, plus one new day
    adjusted = [c / 10 for c in stored_closes[:10]] + stored_closes[10:] + [216]
    rows, why = plan_price_rows(FakeSQL(stored), mapper, 1, _frame(adjusted))
    assert why == 'ok'
    dates = [r['date'] for r in rows]
    # the oldest downloaded day is never rewritten (it has no previous close), the other 9 split days are, plus the new day
    assert len(rows) == 10
    assert dates[-1] == pd.bdate_range('2026-06-01', periods=16)[-1].strftime('%Y-%m-%d')
    assert rows[0]['close_price'] == stored_closes[1] / 10
    assert sum(1 for r in rows if is_new(r, stored[-1]['date'])) == 1


def test_unchanged_history_writes_only_the_new_day():
    mapper = DataMapper()
    closes = [100 + i for i in range(30)]
    stored = mapper.map_daily_prices(1, _frame(closes))
    rows, why = plan_price_rows(FakeSQL(stored), mapper, 1, _frame(closes + [131]))
    assert why == 'ok'
    assert len(rows) == 1 and rows[0]['close_price'] == 131


def test_price_rewrite_does_not_fill_years_before_the_first_stored_day():
    mapper = DataMapper()
    closes = [100 + i for i in range(40)]
    stored = mapper.map_daily_prices(1, _frame(closes))[20:]  # only the newest 20 days are stored
    rows, why = plan_price_rows(FakeSQL(stored), mapper, 1, _frame(closes + [141]))
    assert why == 'ok'
    assert len(rows) == 1  # the new day only, not the 20 older days


def test_first_load_and_untrusted_downloads_fall_back():
    mapper = DataMapper()
    rows, why = plan_price_rows(FakeSQL([]), mapper, 1, _frame([1, 2, 3]))
    assert rows is None and 'nothing stored' in why
    stored = mapper.map_daily_prices(1, _frame(list(range(100, 160))))
    rows, why = plan_price_rows(FakeSQL(stored), mapper, 1, _frame(list(range(100, 160))).iloc[::3])
    assert rows is None


def test_indicator_rows_differing_from_stored_are_rewritten():
    mapper = DataMapper()
    frame = pd.DataFrame({'Date': pd.bdate_range('2026-10-26', periods=5), 'MA_200': [203.5, 203.7, 203.8, 204.0, 204.1]})
    stored = mapper.map_technical_indicators(1, frame)
    stored[0]['sma_200'] = 210.9  # an old calendar-day row
    rows, why = plan_indicator_rows(FakeSQL(stored), mapper, 1, frame)
    assert why == 'ok'
    assert [r['date'] for r in rows] == ['2026-10-26']
    assert PRICE_COMPARE[3] == 'close_price'
