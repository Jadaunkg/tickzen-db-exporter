"""Shared updater rules: export window, freshness cutoff, sync-time parsing, worker parts."""

from __future__ import annotations

import collections
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from database import updater_schedule as us

ET = us.MARKET_TZ


def et(*args):
    return datetime(*args, tzinfo=ET)


def test_window_is_closed_market_only():
    assert not us.in_export_window(et(2026, 10, 15, 9, 0))      # 30 min before the open
    assert not us.in_export_window(et(2026, 10, 15, 13, 0))     # market open
    assert not us.in_export_window(et(2026, 10, 15, 16, 29))    # 30 min after the close not reached
    assert us.in_export_window(et(2026, 10, 15, 16, 30))
    assert us.in_export_window(et(2026, 10, 16, 8, 59))
    assert us.in_export_window(et(2026, 10, 17, 12, 0))         # Saturday
    assert us.in_export_window(et(2026, 11, 26, 12, 0))         # Thanksgiving: NYSE closed all day


def test_cutoff_is_latest_trading_day_close_plus_buffer():
    assert us.latest_session_cutoff(et(2026, 10, 15, 13, 0)) == et(2026, 10, 14, 16, 30)
    assert us.latest_session_cutoff(et(2026, 10, 15, 17, 0)) == et(2026, 10, 15, 16, 30)
    assert us.latest_session_cutoff(et(2026, 10, 17, 12, 0)) == et(2026, 10, 16, 16, 30)   # Saturday -> Friday
    assert us.latest_session_cutoff(et(2026, 10, 19, 5, 0)) == et(2026, 10, 16, 16, 30)    # Monday pre-open
    assert us.latest_session_cutoff(et(2026, 11, 27, 5, 0)) == et(2026, 11, 25, 16, 30)    # day after Thanksgiving


def test_cutoff_follows_daylight_saving_in_ist():
    ist = ZoneInfo("Asia/Kolkata")
    assert us.latest_session_cutoff(et(2026, 10, 15, 17, 0)).astimezone(ist).strftime("%H:%M") == "02:00"
    assert us.latest_session_cutoff(et(2026, 11, 3, 17, 0)).astimezone(ist).strftime("%H:%M") == "03:00"


def test_deadline_and_window_edges():
    cutoff = et(2026, 10, 16, 16, 30)                            # Friday
    assert us.session_deadline(cutoff) == et(2026, 10, 19, 9, 0)  # Monday before the open
    assert us.next_window_open(et(2026, 10, 15, 13, 0)) == et(2026, 10, 15, 16, 30)
    now = et(2026, 10, 15, 20, 0)
    assert us.next_window_open(now) == now
    assert us.window_close(now) == et(2026, 10, 16, 9, 0)
    assert us.window_close(et(2026, 10, 16, 5, 0)) == et(2026, 10, 16, 9, 0)


def test_sync_instant_reads_utc():
    stored = datetime(2026, 10, 14, 12, 22, tzinfo=timezone.utc)
    assert us.sync_instant(stored).astimezone(ET) == et(2026, 10, 14, 8, 22)
    assert us.sync_instant("2026-10-14T12:22:00+00:00") == stored
    assert us.sync_instant("2026-10-14T12:22:00Z") == stored
    assert us.sync_instant("2026-10-14T12:22:00") == stored          # naive is UTC
    assert us.sync_instant("2026-10-14T17:52:00+05:30") == stored    # an aware IST value keeps its instant
    assert us.sync_instant(None) is None


def test_is_current_needs_success_after_cutoff():
    stored = datetime(2026, 10, 14, 12, 22, tzinfo=timezone.utc)
    assert us.is_current(stored, "success", et(2026, 10, 13, 16, 30))
    assert not us.is_current(stored, "success", et(2026, 10, 14, 16, 30))
    assert not us.is_current(stored, "partial", et(2026, 10, 13, 16, 30))
    assert not us.is_current(None, "success", et(2026, 10, 13, 16, 30))


def test_old_ist_labelled_values_never_look_current_too_early():
    # Before the fix a sync stored IST wall time labelled UTC (5.5 h ahead). Scheduled exports stop at 09:00 ET,
    # so even the latest one read 5.5 h late lands before that day's 16:30 ET cutoff.
    for day in (datetime(2026, 10, 14), datetime(2026, 11, 4)):           # EDT and EST
        last_export = datetime.combine(day.date(), us.DEFAULT_STOP, tzinfo=ET) - timedelta(minutes=1)
        stored_ist_label = last_export.astimezone(timezone.utc) + timedelta(hours=5, minutes=30)
        cutoff = datetime.combine(day.date(), us.DEFAULT_START, tzinfo=ET)
        assert not us.is_current(stored_ist_label, "success", cutoff)


def test_parts_are_even_stable_and_order_independent():
    symbols = [f"S{i:04d}" for i in range(4000)]
    for n in (1, 2, 6, 10):
        sizes = collections.Counter(us.assign_parts(symbols, n).values())
        assert len(sizes) == n and max(sizes.values()) - min(sizes.values()) <= 1
    assert us.assign_parts(symbols, 6) == us.assign_parts(list(reversed(symbols)), 6)


def test_parse_hhmm():
    assert us.parse_hhmm("17:15", us.DEFAULT_START) == time(17, 15)
    assert us.parse_hhmm("", us.DEFAULT_START) == us.DEFAULT_START
    assert us.parse_hhmm(None, us.DEFAULT_STOP) == us.DEFAULT_STOP


def test_options_session_is_the_regular_session_minus_its_edges():
    assert us.in_options_session(et(2026, 10, 16, 9, 45))
    assert us.in_options_session(et(2026, 10, 16, 15, 44))
    assert not us.in_options_session(et(2026, 10, 16, 9, 40))
    assert not us.in_options_session(et(2026, 10, 16, 15, 45))
    assert not us.in_options_session(et(2026, 10, 17, 12, 0))     # Saturday
    assert not us.in_options_session(et(2026, 11, 26, 12, 0))     # Thanksgiving


def test_implied_move_job_cron_slots_fall_inside_the_session_in_summer_and_winter():
    # tickzen-db-exporter deploy/azure/deploy_implied_moves_job.sh: "15 15,17,19 * * 1-5" (UTC)
    for day in (datetime(2026, 10, 16), datetime(2026, 12, 4)):           # EDT and EST
        for hour in (15, 17, 19):
            slot = day.replace(hour=hour, minute=15, tzinfo=timezone.utc)
            assert us.in_options_session(slot), slot
