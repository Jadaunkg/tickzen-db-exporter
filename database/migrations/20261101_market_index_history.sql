-- Clean daily market benchmark, full price history for monitored stocks, and nightly price-data checks.
--
-- market_index_daily                 one row per index/ETF per trading day, from Yahoo (^GSPC = S&P 500 price index,
--                                    SPY = S&P 500 ETF; adjusted_close includes dividends). The single source for
--                                    every "vs market" comparison. Its own table rather than SPY rows in
--                                    daily_price_data: daily_price_data rows hang off ``stocks``, and an SPY stock row
--                                    would show up on stock pages, sitemaps, counts and the monitored queue.
--                                    market_price_snapshot.sp500_index (from FRED, which lags several days) is kept but
--                                    is now written from this table for the snapshot's own date.
-- stocks.price_history_complete_at   when daily_price_data was backfilled back to PRICE_HISTORY_START (or the
--                                    listing date). NULL = older years still to load. Written by
--                                    database/price_history.py.
-- price_data_issues                  result of the nightly price check (flat zero-volume runs, missing trading days)
--                                    for monitored stocks; replaced every night, read by /admin/db-updater.
--
-- Additive and idempotent: safe to run more than once, touches no existing data.
BEGIN;

CREATE TABLE IF NOT EXISTS public.market_index_daily (
    symbol          TEXT NOT NULL,
    date            DATE NOT NULL,
    open_price      NUMERIC,
    high_price      NUMERIC,
    low_price       NUMERIC,
    close_price     NUMERIC NOT NULL,
    adjusted_close  NUMERIC,
    volume          BIGINT,
    source          TEXT NOT NULL DEFAULT 'yahoo',
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, date)
);

ALTER TABLE public.stocks ADD COLUMN IF NOT EXISTS price_history_complete_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS public.price_data_issues (
    symbol          TEXT NOT NULL,
    issue           TEXT NOT NULL,          -- flat_zero_volume | missing_days
    first_date      DATE,
    last_date       DATE,
    days            INTEGER,
    detail          TEXT,
    checked_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, issue)
);

COMMIT;
