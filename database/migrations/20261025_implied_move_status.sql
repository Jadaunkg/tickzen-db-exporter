-- Why an upcoming report has (or lacks) an options-implied move, so the job can retry on a schedule that fits the
-- reason and the pages can say why instead of showing a bare dash.
--
-- earnings_calendar.implied_move_status      last attempt's outcome: ok | no_options | no_usable_expiry |
--                                            wide_spread | thin_open_interest | no_two_sided_quote | no_strike_near_spot |
--                                            no_common_strike | flat_term_structure | no_event_premium |
--                                            small_event_premium | implausible_move | no_daily_vol | no_reliable_quote
-- earnings_calendar.implied_move_method      how the stored move was made: straddle | term_structure | single_expiry
--                                            (single_expiry = one monthly straddle minus the stock's normal daily
--                                            movement; shown as "estimated")
-- earnings_calendar.implied_move_checked_at  last attempt, whatever its outcome (implied_move_updated_at stays the
--                                            time the stored value was written)
-- earnings_history_status.daily_vol_pct      standard deviation of the daily close-to-close change over the last 60
--                                            sessions, report reactions left out, percent points; computed from the
--                                            price history the earnings refresh already downloads (no extra request)
-- earnings_history_status.daily_vol_at       when daily_vol_pct was computed
--
-- Written by database/export_earnings_history.py. Additive and idempotent: safe to run more than once, touches no
-- existing data.
BEGIN;

ALTER TABLE public.earnings_calendar ADD COLUMN IF NOT EXISTS implied_move_status TEXT;
ALTER TABLE public.earnings_calendar ADD COLUMN IF NOT EXISTS implied_move_method TEXT;
ALTER TABLE public.earnings_calendar ADD COLUMN IF NOT EXISTS implied_move_checked_at TIMESTAMPTZ;

ALTER TABLE public.earnings_history_status ADD COLUMN IF NOT EXISTS daily_vol_pct NUMERIC;
ALTER TABLE public.earnings_history_status ADD COLUMN IF NOT EXISTS daily_vol_at TIMESTAMPTZ;

COMMIT;
