"""database/price_history.py without network or database: Yahoo and the DB client are fakes."""
from datetime import date, datetime, timezone

import pandas as pd
import pytest

from database import price_history as ph
from database.data_mapper import DataMapper


def yahoo_frame(days, start=100.0):
    idx = pd.DatetimeIndex([pd.Timestamp(d).tz_localize("America/New_York") for d in days], name="Date")
    closes = [start + i for i in range(len(days))]
    return pd.DataFrame({"Open": closes, "High": closes, "Low": closes, "Close": closes,
                         "Adj Close": [c * 0.9 for c in closes], "Volume": [1000] * len(days)}, index=idx)


class FakeTable:
    def __init__(self, db, name):
        self.db, self.name = db, name

    def upsert(self, rows, on_conflict=None):
        self.db.upserts.setdefault(self.name, []).extend(rows)
        return self

    def execute(self):
        return None


class FakeDB:
    def __init__(self, answers=None):
        self.answers = answers or {}
        self.sql, self.upserts = [], {}

    def table(self, name):
        return FakeTable(self, name)

    def execute_raw_sql(self, query, params=None):
        self.sql.append((query, params))
        for key, value in self.answers.items():
            if key in query:
                return value(params) if callable(value) else value
        return []


class FakeTicker:
    calls = []

    def __init__(self, symbol, frame):
        self.symbol, self.frame = symbol, frame

    def history(self, **kw):
        FakeTicker.calls.append((self.symbol, kw))
        return self.frame


@pytest.fixture
def yahoo(monkeypatch):
    frames = {}

    class FakeYF:
        @staticmethod
        def Ticker(symbol):
            return FakeTicker(symbol, frames.get(symbol, pd.DataFrame()))

    FakeTicker.calls = []
    monkeypatch.setattr(ph, "_yf", lambda: FakeYF)
    return frames


def test_bars_uses_new_york_trading_date():
    bars = ph._bars(yahoo_frame(["2026-10-23", "2026-10-26"]))
    assert [d.strftime("%Y-%m-%d") for d in bars["Date"]] == ["2026-10-23", "2026-10-26"]


def test_market_index_first_load_and_skip_when_fresh(yahoo):
    yahoo["^GSPC"] = yahoo_frame(["2026-10-22", "2026-10-23"], start=7700)
    yahoo["SPY"] = yahoo_frame(["2026-10-22", "2026-10-23"], start=700)
    db = FakeDB({"max(date)": [{"d": None}]})
    written = ph.update_market_index(db)
    assert written == {"^GSPC": 2, "SPY": 2}
    assert FakeTicker.calls[0][1]["start"] == ph.PRICE_HISTORY_START
    row = db.upserts["market_index_daily"][1]
    assert (row["symbol"], row["date"], row["close_price"]) == ("^GSPC", "2026-10-23", 7701.0)

    cutoff = datetime(2026, 10, 23, 20, 30, tzinfo=timezone.utc)
    fresh = FakeDB({"min(u)": [{"u": datetime(2026, 10, 23, 21, 0, tzinfo=timezone.utc)}]})
    FakeTicker.calls = []
    assert ph.update_market_index(fresh, fresh_after=cutoff) == {}
    assert FakeTicker.calls == []   # no Yahoo request once tonight's bars are stored


def test_market_index_incremental_rereads_ten_days(yahoo):
    yahoo["^GSPC"] = yahoo_frame(["2026-10-30"])
    db = FakeDB({"max(date)": [{"d": date(2026, 10, 29)}]})
    ph.update_market_index(db)
    assert FakeTicker.calls[0][1]["start"] == "2026-10-19"


def test_align_benchmark_same_date_or_none():
    sessions = pd.DataFrame({"Date": pd.to_datetime(["2026-10-22", "2026-10-23"]), "Close": [1.0, 2.0]})
    db = FakeDB({"FROM market_index_daily": [{"date": date(2026, 10, 22), "close_price": 7712.57},
                                             {"date": date(2026, 10, 23), "close_price": 7757.17}]})
    assert list(ph.align_benchmark(db, sessions)) == [7712.57, 7757.17]
    partial = FakeDB({"FROM market_index_daily": [{"date": date(2026, 10, 22), "close_price": 7712.57}]})
    assert ph.align_benchmark(partial, sessions) is None   # never a value carried over from another day


def test_backfill_adds_older_sessions_and_marks_complete(yahoo):
    yahoo["ALH"] = yahoo_frame(["2024-06-05", "2024-06-06", "2024-06-07", "2024-06-10"])
    db = FakeDB()
    n = ph.backfill_price_history(db, DataMapper(), 7, "ALH", date(2024, 6, 10), today=date(2025, 1, 1))
    assert n == 3
    rows = db.upserts["daily_price_data"]
    assert [r["date"] for r in rows] == ["2024-06-05", "2024-06-06", "2024-06-07", "2024-06-10"]
    assert rows[-1]["daily_return_pct"] is not None   # first stored day gets its change from the new previous close
    assert any("price_history_complete_at = now()" in q for q, _ in db.sql)
    assert FakeTicker.calls[0][1]["end"] == "2024-06-11"


def test_backfill_recent_listing_with_nothing_older_is_marked(yahoo):
    yahoo["ALH"] = yahoo_frame(["2025-10-09"])
    db = FakeDB()
    assert ph.backfill_price_history(db, DataMapper(), 7, "ALH", date(2025, 10, 9)) == 0
    assert "daily_price_data" not in db.upserts
    assert any("price_history_complete_at" in q for q, _ in db.sql)


def test_backfill_yahoo_error_leaves_stock_pending(monkeypatch):
    class Boom:
        @staticmethod
        def Ticker(symbol):
            raise RuntimeError("Too Many Requests")
    monkeypatch.setattr(ph, "_yf", lambda: Boom)
    db = FakeDB()
    assert ph.backfill_price_history(db, DataMapper(), 7, "AAPL", date(2024, 6, 7)) is None
    assert not any("price_history_complete_at" in q for q, _ in db.sql)


def test_run_backfill_stops_when_told(yahoo):
    yahoo["A"] = yahoo["B"] = yahoo_frame(["2024-06-06", "2024-06-07"])
    db = FakeDB({"GROUP BY s.id": [{"stock_id": 1, "symbol": "A", "first_date": date(2024, 6, 7)},
                                   {"stock_id": 2, "symbol": "B", "first_date": date(2024, 6, 7)}]})
    calls = iter([False, True])
    assert list(ph.run_backfill(db, DataMapper(), ["A", "B"], 5, stop=lambda: next(calls))) == ["A"]


def test_price_checks_replace_table_and_describe():
    issue = {"symbol": "BNY", "issue": "missing_days", "first_date": date(2026, 7, 6),
             "last_date": date(2026, 7, 9), "days": 4}
    db = FakeDB({"WITH m AS": [issue], "max(date)": [{"d": date(2026, 10, 30)}]})
    assert ph.run_price_checks(db) == 1
    assert any(q.startswith("DELETE FROM price_data_issues") for q, _ in db.sql)
    stored = db.upserts["price_data_issues"][0]
    assert stored["detail"] == "4 trading days missing between 2026-07-06 and 2026-07-09"


def test_price_checks_need_calendar():
    assert ph.run_price_checks(FakeDB({"max(date)": [{"d": None}]})) is None


# ---------------------------------------------------------------- full-history queue
def test_pending_backfill_redoes_old_histories_after_the_new_ones():
    db = FakeDB({"GROUP BY s.id": [{"stock_id": 127, "symbol": "KLAC", "first_date": date(2024, 6, 10)}]})
    assert ph.pending_backfill(db, ["klac"], 100)[0]["symbol"] == "KLAC"
    query, params = db.sql[0]
    assert "price_history_complete_at IS NULL OR s.price_history_complete_at < now() - make_interval" in query
    assert "ORDER BY s.price_history_complete_at NULLS FIRST, s.created_at" in query
    assert params == [["KLAC"], ph.FULL_HISTORY_REFRESH_DAYS, 100]


def test_backfill_rewrites_unadjusted_rows_before_the_daily_window(yahoo):
    # KLAC: rows before 2024-11-05 were stored before its 10-for-1 split (2024-11-04 $654.24). The full-history job
    # downloads up to REWRITE_UPTO_DAYS ago, so those rows are rewritten with the split-adjusted prices.
    yahoo["KLAC"] = yahoo_frame(["2024-11-01", "2024-11-04", "2024-11-05"], start=65.0)
    db = FakeDB()
    ph.backfill_price_history(db, DataMapper(), 127, "KLAC", date(2024, 6, 10), today=date(2026, 10, 27))
    rows = {r["date"]: r["close_price"] for r in db.upserts["daily_price_data"]}
    assert rows["2024-11-04"] == 66.0
    assert FakeTicker.calls[0][1]["end"] == "2024-11-07"   # 720 days before 2026-10-27, plus one


def test_nightly_backfill_default_is_100_stocks_a_night(monkeypatch):
    monkeypatch.delenv("PRICE_BACKFILL_PER_NIGHT", raising=False)
    seen = {}
    monkeypatch.setattr(ph, "confirm_final_bars", lambda *a, **k: None)
    monkeypatch.setattr(ph, "backfilled_since", lambda db, since: 30)
    monkeypatch.setattr(ph, "run_backfill", lambda db, mapper, symbols, limit, stop=None: seen.setdefault("limit", limit) and {})
    monkeypatch.setattr(ph, "checks_ran_since", lambda db, since: True)
    ph.nightly_step(FakeDB(), DataMapper(), ["KLAC"], datetime(2026, 11, 4, 21, 30, tzinfo=timezone.utc))
    assert seen["limit"] == 70


# ---------------------------------------------------------------- final bars of the last session
CUTOFF = datetime(2026, 11, 4, 21, 30, tzinfo=timezone.utc)   # Wed 4 Nov 16:30 New York


def bars(rows):
    """Yahoo history(auto_adjust=True) frame: (day, close, volume) rows, no Adj Close column."""
    idx = pd.DatetimeIndex([pd.Timestamp(d).tz_localize("America/New_York") for d, _, _ in rows], name="Date")
    closes = [c for _, c, _ in rows]
    return pd.DataFrame({"Open": closes, "High": closes, "Low": closes, "Close": closes,
                         "Volume": [v for _, _, v in rows]}, index=idx)


AUMN_FINAL = [("2026-10-30", 0.17, 6600), ("2026-11-02", 0.19, 14100), ("2026-11-03", 0.20, 5300),
              ("2026-11-04", 0.20, 17500)]


def stored(rows):
    return [{"date": date.fromisoformat(d), "open_price": c, "high_price": c, "low_price": c, "close_price": c,
             "adjusted_close": c, "volume": v} for d, c, v in rows]


def test_final_bar_time_is_2am_new_york_the_next_day():
    assert ph.final_bar_time(CUTOFF) == datetime(2026, 11, 5, 7, 0, tzinfo=timezone.utc)
    friday = datetime(2026, 10, 30, 20, 30, tzinfo=timezone.utc)   # before the clocks changed: EDT
    assert ph.final_bar_time(friday) == datetime(2026, 10, 31, 6, 0, tzinfo=timezone.utc)


def test_final_bars_wait_for_the_final_time(yahoo):
    db = FakeDB()
    assert ph.confirm_final_bars(db, DataMapper(), ["AUMN"], CUTOFF,
                                 now=datetime(2026, 11, 5, 6, 59, tzinfo=timezone.utc)) is None
    assert db.sql == [] and FakeTicker.calls == []


def test_unfinished_evening_bar_is_rewritten_with_the_final_one(yahoo, monkeypatch):
    # AUMN 4 Nov was stored at 19:25 New York as $0.1998 / 7,520 shares; Yahoo's final bar is $0.20 / 17,500.
    yahoo["AUMN"] = bars(AUMN_FINAL)
    evening = stored(AUMN_FINAL[:-1] + [("2026-11-04", 0.1998, 7520)])
    db = FakeDB({"FROM stocks s JOIN daily_price_data": [{"stock_id": 1260, "symbol": "AUMN"}],
                 "FROM daily_price_data WHERE stock_id": evening})
    recomputed = []
    monkeypatch.setattr(ph, "rewrite_indicators", lambda db, mapper, stock_id: recomputed.append(stock_id))
    done = ph.confirm_final_bars(db, DataMapper(), ["aumn"], CUTOFF, now=datetime(2026, 11, 5, 8, tzinfo=timezone.utc))
    assert done == {"AUMN": 1}
    written = db.upserts["daily_price_data"]
    assert [(r["date"], r["close_price"], r["volume"]) for r in written] == [("2026-11-04", 0.20, 17500)]
    assert recomputed == [1260]
    assert db.sql[0][1] == [date(2026, 11, 4), ["AUMN"], datetime(2026, 11, 5, 7, 0, tzinfo=timezone.utc)]
    assert FakeTicker.calls[0][1]["period"] == "1mo" and FakeTicker.calls[0][1]["auto_adjust"] is True


def test_final_bar_equal_to_stored_only_marks_the_row(monkeypatch):
    db = FakeDB({"FROM daily_price_data WHERE stock_id": stored(AUMN_FINAL)})
    monkeypatch.setattr(ph, "rewrite_indicators", lambda *a: pytest.fail("nothing changed"))
    n = ph.apply_final_bars(db, DataMapper(), 1260, ph._bars(bars(AUMN_FINAL)), date(2026, 11, 4))
    assert n == 0
    assert [r["date"] for r in db.upserts["daily_price_data"]] == ["2026-11-04"]   # updated_at marks it final


def test_final_bars_without_the_session_or_with_gaps_are_not_trusted():
    db = FakeDB({"FROM daily_price_data WHERE stock_id": stored(AUMN_FINAL)})
    no_session = ph._bars(bars(AUMN_FINAL[:-1]))
    assert ph.apply_final_bars(db, DataMapper(), 1260, no_session, date(2026, 11, 4)) is None
    gappy = ph._bars(bars([AUMN_FINAL[0], AUMN_FINAL[1], AUMN_FINAL[3]]))   # misses 3 Nov: 2 of 3 sessions
    assert ph.apply_final_bars(db, DataMapper(), 1260, gappy, date(2026, 11, 4)) is None
    assert "daily_price_data" not in db.upserts


def test_a_later_day_from_yahoo_is_never_written_early():
    db = FakeDB({"FROM daily_price_data WHERE stock_id": stored(AUMN_FINAL)})
    with_today = ph._bars(bars(AUMN_FINAL + [("2026-11-05", 0.21, 100)]))
    ph.apply_final_bars(db, DataMapper(), 1260, with_today, date(2026, 11, 4))
    assert [r["date"] for r in db.upserts["daily_price_data"]] == ["2026-11-04"]


def test_final_bar_failure_is_retried_next_run(monkeypatch):
    class Boom:
        @staticmethod
        def Ticker(symbol):
            raise RuntimeError("Too Many Requests")
    monkeypatch.setattr(ph, "_yf", lambda: Boom)
    db = FakeDB({"FROM stocks s JOIN daily_price_data": [{"stock_id": 1260, "symbol": "AUMN"}]})
    assert ph.confirm_final_bars(db, DataMapper(), ["AUMN"], CUTOFF,
                                 now=datetime(2026, 11, 5, 8, tzinfo=timezone.utc)) == {}
    assert "daily_price_data" not in db.upserts


def test_rewrite_indicators_upserts_and_removes_days_without_a_session(monkeypatch):
    import database.history_sync as hs
    monkeypatch.setattr(hs, "plan_indicator_rewrite",
                        lambda run_sql, mapper, stock_id: ([{"stock_id": 1, "date": "2026-11-04"}], ["2026-11-01"], "ok"))
    db = FakeDB()
    assert ph.rewrite_indicators(db, DataMapper(), 1) == 1
    assert db.upserts["technical_indicators"] == [{"stock_id": 1, "date": "2026-11-04"}]
    assert db.sql[-1] == (hs.DELETE_INDICATOR_SQL, [1, ["2026-11-01"]])


# ---------------------------------------------------------------- stocks that stopped updating
def test_price_checks_flag_a_stock_with_no_new_prices():
    issue = {"symbol": "KVUE", "issue": "no_new_prices", "first_date": date(2026, 10, 30),
             "last_date": date(2026, 11, 4), "days": 3}
    db = FakeDB({"WITH m AS": [issue], "max(date)": [{"d": date(2026, 11, 4)}]})
    assert ph.run_price_checks(db) == 1
    query, params = next((q, p) for q, p in db.sql if q.lstrip().startswith("WITH m AS"))
    assert "'no_new_prices'" in query and params["behind_min"] == 2
    assert db.upserts["price_data_issues"][0]["detail"].startswith("no new prices since 2026-10-30 (3 trading days")


# ---------------------------------------------------------------- unadjusted rows from the first backfill version
def test_unadjusted_history_goes_back_into_the_queue():
    db = FakeDB({"UPDATE stocks s SET price_history_complete_at = NULL": [{"symbol": "NVDA"}, {"symbol": "MSFT"}]})
    assert ph.requeue_unadjusted(db) == ["MSFT", "NVDA"]
    query, params = db.sql[0]
    assert "abs(d.close_price / d.adjusted_close - 1) > %s" in query and "t.is_active" in query
    assert params == [0.002]


@pytest.mark.parametrize("worker, checked, requeued", [(0, False, True), (0, True, False), (1, False, False)])
def test_requeue_runs_once_per_session_on_worker_0_before_the_backfill(monkeypatch, worker, checked, requeued):
    order = []
    monkeypatch.setattr(ph, "confirm_final_bars", lambda *a, **k: None)
    monkeypatch.setattr(ph, "checks_ran_since", lambda db, since: checked)
    monkeypatch.setattr(ph, "requeue_unadjusted", lambda db: order.append("requeue"))
    monkeypatch.setattr(ph, "backfilled_since", lambda db, since: 0)
    monkeypatch.setattr(ph, "run_backfill", lambda *a, **k: order.append("backfill") and {})
    monkeypatch.setattr(ph, "run_price_checks", lambda db: None)
    ph.nightly_step(FakeDB(), DataMapper(), ["MSFT"], CUTOFF, worker_index=worker)
    assert order == (["requeue", "backfill"] if requeued else ["backfill"])
