-- Run log of the stock database updater (Azure workers in tickzen-db-exporter and the Mac runner).
-- Read by the admin dashboard (/admin/db-updater). Additive and idempotent.
--
-- updater_workers     one row per worker: last time it woke up and what it found (liveness, even when idle)
-- updater_runs        one row per run that exported at least one stock
-- updater_run_stocks  one row per stock attempt in a run (data_sync_log has no row when an export fails early)

CREATE TABLE IF NOT EXISTS public.updater_workers (
    worker_name     TEXT PRIMARY KEY,
    worker_index    INTEGER,
    worker_count    INTEGER,
    region          TEXT,
    egress_ip       TEXT,
    image_tag       TEXT,
    pace_per_hour   INTEGER,
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_outcome    TEXT,
    current_run_id  BIGINT
);

CREATE TABLE IF NOT EXISTS public.updater_runs (
    id              BIGSERIAL PRIMARY KEY,
    worker_name     TEXT NOT NULL,
    worker_index    INTEGER,
    worker_count    INTEGER,
    region          TEXT,
    egress_ip       TEXT,
    image_tag       TEXT,
    session_cutoff  TIMESTAMPTZ,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    heartbeat_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'running',
    stocks_success  INTEGER NOT NULL DEFAULT 0,
    stocks_partial  INTEGER NOT NULL DEFAULT 0,
    stocks_failed   INTEGER NOT NULL DEFAULT 0,
    stocks_helped   INTEGER NOT NULL DEFAULT 0,
    yahoo_requests  INTEGER,
    yahoo_429       INTEGER,
    pace_per_hour   INTEGER,
    note            TEXT
);
CREATE INDEX IF NOT EXISTS updater_runs_started_idx ON public.updater_runs (started_at DESC);
CREATE INDEX IF NOT EXISTS updater_runs_worker_idx ON public.updater_runs (worker_name, started_at DESC);

CREATE TABLE IF NOT EXISTS public.updater_run_stocks (
    id              BIGSERIAL PRIMARY KEY,
    run_id          BIGINT NOT NULL REFERENCES public.updater_runs (id) ON DELETE CASCADE,
    symbol          TEXT NOT NULL,
    status          TEXT NOT NULL,
    own_part        BOOLEAN,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    yahoo_requests  INTEGER,
    errors          TEXT
);
CREATE INDEX IF NOT EXISTS updater_run_stocks_finished_idx ON public.updater_run_stocks (finished_at DESC);
CREATE INDEX IF NOT EXISTS updater_run_stocks_symbol_idx ON public.updater_run_stocks (symbol, finished_at DESC);
CREATE INDEX IF NOT EXISTS updater_run_stocks_run_idx ON public.updater_run_stocks (run_id);

-- The dashboard reads each stock's latest export result and the per-night history from data_sync_log.
CREATE INDEX IF NOT EXISTS data_sync_log_stock_created_idx ON public.data_sync_log (stock_id, created_at DESC);
CREATE INDEX IF NOT EXISTS data_sync_log_created_idx ON public.data_sync_log (created_at DESC);
