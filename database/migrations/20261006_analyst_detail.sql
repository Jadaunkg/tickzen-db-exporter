-- Analyst detail that powers the Next.js /stocks/<ticker>/forecast tab (analyst-based section):
-- how many analysts say buy/hold/sell and how that moved over 3 months, the latest rating and price-target
-- changes by firm, and the consensus EPS / revenue estimates. Source: Yahoo (yfinance), refreshed weekly.
-- Additive and idempotent: safe to run more than once, touches no existing data.
BEGIN;

CREATE TABLE IF NOT EXISTS public.analyst_detail_data (
    stock_id          BIGINT PRIMARY KEY REFERENCES public.stocks(id) ON DELETE CASCADE,
    -- [{period: '0m' | '-1m' | '-2m' | '-3m', strong_buy, buy, hold, sell, strong_sell}]   unit: analyst_count (integers)
    rating_trend      JSONB,
    -- newest first: [{date: 'YYYY-MM-DD', firm, action, from_grade, to_grade, price_target_action,
    --                 target_price, prior_target_price}]   action: up | down | init | main | reit | target;
    --                 prices unit: usd_per_share (null when the firm gave none)
    recent_actions    JSONB,
    -- [{period: '0q' | '+1q' | '0y' | '+1y', avg, low, high, year_ago, analysts, growth_pct, currency}]
    --  avg/low/high/year_ago unit: usd_per_share; growth_pct unit: percent_points (vendor fraction x 100)
    eps_estimates     JSONB,
    -- same shape; avg/low/high/year_ago unit: usd (total revenue); growth_pct unit: percent_points
    revenue_estimates JSONB,
    source            VARCHAR(24) NOT NULL DEFAULT 'yfinance',
    fetched_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),   -- when Yahoo was last asked (even if it returned nothing)
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_analyst_detail_fetched ON public.analyst_detail_data (fetched_at);

COMMIT;
