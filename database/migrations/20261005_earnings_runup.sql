-- Pre-report run-up for each reported quarter: how far the stock moved in the sessions BEFORE the report.
-- Powers the "priced in?" view on /stocks/<ticker>/earnings (run-up vs the reaction that followed).
-- Additive and idempotent: safe to run more than once, touches no existing data.
BEGIN;

ALTER TABLE public.earnings_history ADD COLUMN IF NOT EXISTS runup_5d_pct  NUMERIC(12, 4);  -- percent_points: move over the 5 sessions up to price_before
ALTER TABLE public.earnings_history ADD COLUMN IF NOT EXISTS runup_20d_pct NUMERIC(12, 4);  -- percent_points: move over the 20 sessions up to price_before

COMMIT;
