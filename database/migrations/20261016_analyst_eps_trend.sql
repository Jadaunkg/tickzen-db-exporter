-- How the consensus EPS estimate moved: the estimate now and 7 / 30 / 60 / 90 days ago, plus how many analysts
-- raised or cut it in the last 7 / 30 days. One row per stock and period, refreshed with analyst_detail_data
-- (weekly). Source: Yahoo (yfinance eps_trend + eps_revisions, the same quoteSummary 'earningsTrend' answer the
-- EPS estimates come from, so it costs no extra Yahoo request).
-- Additive and idempotent: safe to run more than once, touches no existing data.
BEGIN;

CREATE TABLE IF NOT EXISTS public.analyst_eps_trend (
    stock_id      BIGINT NOT NULL REFERENCES public.stocks(id) ON DELETE CASCADE,
    period        VARCHAR(8) NOT NULL,       -- '0q' current quarter | '+1q' next quarter | '0y' current year | '+1y' next year
    eps_current   NUMERIC,                   -- unit: usd_per_share (quote currency in `currency`)
    eps_7d_ago    NUMERIC,
    eps_30d_ago   NUMERIC,
    eps_60d_ago   NUMERIC,
    eps_90d_ago   NUMERIC,
    up_last_7d    INTEGER,                   -- unit: count (analysts who raised the estimate)
    up_last_30d   INTEGER,
    down_last_7d  INTEGER,                   -- unit: count (analysts who cut the estimate)
    down_last_30d INTEGER,
    currency      VARCHAR(8),
    source        VARCHAR(24) NOT NULL DEFAULT 'yfinance',
    fetched_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (stock_id, period)
);

CREATE INDEX IF NOT EXISTS idx_analyst_eps_trend_fetched ON public.analyst_eps_trend (fetched_at);

COMMIT;
