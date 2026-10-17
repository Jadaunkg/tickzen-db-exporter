"""Every sync moves stocks.data_end_date and stamps last_sync_date in real UTC (not only the 30-day profile refresh)."""
from datetime import datetime, timedelta, timezone

import pandas as pd

from database.data_mapper import DataMapper
from database.export_to_supabase import SupabaseDataExporter


class _Query:
    def __init__(self, log):
        self.log = log

    def __getattr__(self, name):
        def call(*args, **kwargs):
            self.log.append((name, args))
            return self
        return call

    def execute(self):
        return type('Result', (), {'data': []})()


class _Client:
    def __init__(self):
        self.log = []

    def table(self, name):
        self.log.append(('table', (name,)))
        return _Query(self.log)


def _exporter():
    exporter = SupabaseDataExporter.__new__(SupabaseDataExporter)
    exporter.db = type('DB', (), {'client': _Client()})()
    exporter.mapper = DataMapper()
    return exporter


def _update_payload(exporter):
    (payload,) = [args[0] for name, args in exporter.db.client.log if name == 'update']
    return payload


def _prices(*days):
    return pd.DataFrame({'Date': pd.to_datetime(list(days)), 'Close': [1.0] * len(days)})


def test_coverage_comes_from_the_price_history():
    assert DataMapper().map_data_coverage(_prices('2025-04-19', '2026-10-01', '2026-10-16')) == {
        'data_start_date': '2025-04-19', 'data_end_date': '2026-10-16', 'total_records': 3,
    }


def test_no_history_writes_no_coverage():
    mapper = DataMapper()
    assert mapper.map_data_coverage(None) == {}
    assert mapper.map_data_coverage(pd.DataFrame()) == {}
    assert mapper.map_data_coverage(pd.DataFrame({'Date': [None]})) == {}


def test_final_markers_move_data_end_date_when_the_profile_was_skipped():
    # AAPL: profile served from the cache, so the profile upsert (the only old writer of data_end_date) did not run.
    exporter = _exporter()
    exporter._write_final_sync_markers(7, 'success', _prices('2025-04-19', '2026-10-16'))
    payload = _update_payload(exporter)
    assert payload['data_end_date'] == '2026-10-16'
    assert payload['data_start_date'] == '2025-04-19'
    assert payload['total_records'] == 2
    assert payload['last_sync_status'] == 'success'
    assert ('eq', ('id', 7)) in exporter.db.client.log


def test_final_markers_keep_stored_dates_without_history():
    exporter = _exporter()
    exporter._write_final_sync_markers(7, 'partial', pd.DataFrame())
    payload = _update_payload(exporter)
    assert not {'data_start_date', 'data_end_date', 'total_records'} & payload.keys()
    assert payload['last_sync_status'] == 'partial'


def test_sync_timestamps_are_real_utc():
    # A naive now() (IST on the Mac and in the containers) has no offset and was stored 5.5 h ahead.
    exporter = _exporter()
    exporter._write_final_sync_markers(7, 'success', None)
    payload = _update_payload(exporter)
    for column in ('last_sync_date', 'updated_at'):
        stamp = datetime.fromisoformat(payload[column])
        assert stamp.utcoffset() == timedelta(0)
        assert abs(stamp - datetime.now(timezone.utc)) < timedelta(minutes=1)

    record = DataMapper().map_stock_metadata('AAPL', {'longName': 'Apple Inc.'}, _prices('2026-10-16'))
    assert datetime.fromisoformat(record['last_sync_date']).utcoffset() == timedelta(0)
    assert record['data_end_date'] == '2026-10-16'
