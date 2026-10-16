-- Earnings history + implied-move columns that power the earnings calendar pages
-- (Flask /stock-analysis/earnings-calendar/*) and the Next.js /stocks/<ticker>/earnings tab.
-- Additive and idempotent: safe to run more than once, touches no existing data.
BEGIN;

-- One row per (stock, report date) for REPORTED results only (eps_actual is known), with the measured
-- price reaction. Upcoming reports live in earnings_calendar.
CREATE TABLE IF NOT EXISTS public.earnings_history (
    id               BIGSERIAL PRIMARY KEY,
    stock_id         BIGINT NOT NULL REFERENCES public.stocks(id) ON DELETE CASCADE,
    earnings_date    DATE NOT NULL,
    time_of_day      VARCHAR(10),            -- BMO (before open) | AMC (after close) | DMH (during hours) | NULL unknown
    eps_estimate     NUMERIC(12, 4),         -- usd_per_share
    eps_actual       NUMERIC(12, 4),         -- usd_per_share
    eps_surprise_pct NUMERIC(12, 4),         -- percent_points: (actual - estimate) / |estimate| * 100
    price_before     NUMERIC(14, 4),         -- close the session before the market could react
    price_after      NUMERIC(14, 4),         -- close of the first session that reflects the report
    reaction_date    DATE,                   -- that first reacting session
    revenue_estimate NUMERIC(20, 2),         -- usd (only when the Finnhub calendar captured it)
    revenue_actual   NUMERIC(20, 2),         -- usd
    revenue_surprise_pct NUMERIC(12, 4),     -- percent_points
    eps_source_conflict BOOLEAN NOT NULL DEFAULT FALSE,  -- Yahoo and Finnhub disagree on beat vs miss (different EPS basis)
    reaction_1d_pct  NUMERIC(12, 4),         -- percent_points: close-to-close move over the reaction session
    reaction_3d_pct  NUMERIC(12, 4),         -- percent_points: move to the close two sessions after the reaction session
    gap_pct          NUMERIC(12, 4),         -- percent_points: open of reaction session vs price_before
    source           VARCHAR(24) NOT NULL DEFAULT 'yfinance',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_earnings_history_stock_date UNIQUE (stock_id, earnings_date)
);

CREATE INDEX IF NOT EXISTS idx_earnings_history_date ON public.earnings_history (earnings_date DESC);
CREATE INDEX IF NOT EXISTS idx_earnings_history_stock_date ON public.earnings_history (stock_id, earnings_date DESC);

-- When each stock's history was last (re)built, including stocks with nothing reported yet, so the
-- batch refresher can pick the stalest stocks first instead of retrying empty ones forever.
CREATE TABLE IF NOT EXISTS public.earnings_history_status (
    stock_id   BIGINT PRIMARY KEY REFERENCES public.stocks(id) ON DELETE CASCADE,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    rows       INTEGER NOT NULL DEFAULT 0,
    note       TEXT
);

-- Shared fixed-window counters so every process (web cron job, CLI backfill, exporter) stays inside the
-- free-tier limits of Finnhub and Yahoo together. See earnings_reports/rate_limit.py.
CREATE TABLE IF NOT EXISTS public.api_rate_limits (
    key          TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    calls        INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (key, window_start)
);

-- Options-implied move for the upcoming report (nearest at-the-money straddle that expires after it).
-- Actuals captured from Finnhub's bulk daily calendar (same-day results before Yahoo catches up).
ALTER TABLE public.earnings_calendar ADD COLUMN IF NOT EXISTS eps_actual NUMERIC(12, 4);            -- usd_per_share
ALTER TABLE public.earnings_calendar ADD COLUMN IF NOT EXISTS revenue_actual NUMERIC(20, 2);        -- usd
ALTER TABLE public.earnings_calendar ADD COLUMN IF NOT EXISTS implied_move_pct NUMERIC(10, 4);        -- percent_points
ALTER TABLE public.earnings_calendar ADD COLUMN IF NOT EXISTS implied_move_expiry DATE;
ALTER TABLE public.earnings_calendar ADD COLUMN IF NOT EXISTS implied_move_updated_at TIMESTAMPTZ;

COMMIT;
