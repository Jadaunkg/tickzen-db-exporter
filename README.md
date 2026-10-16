# TickZen Database Exporter

Standalone nightly updater for the TickZen production stock database (Azure PostgreSQL, the database behind
the `/stocks/<ticker>` pages). Per stock it runs the **same export** as
`tickzen-github/database/cron_update_runner.py` (the macOS launchd job `com.tickzen.dbupdater`), split across
several workers that each run on Azure in a different region.

## How it stays identical to tickzen-github

Every pipeline module is a byte-for-byte copy of tickzen-github, listed in `scripts/sync_manifest.txt`
(the full import closure of the runner plus the runtime data it reads: `column_units.json`, the
earnings/analyst migrations). After changing the pipeline in tickzen-github:

```bash
scripts/sync_from_tickzen.sh --check     # list files that differ
scripts/sync_from_tickzen.sh             # copy them
```

The scheduling rules are shared too, so the workers, the Mac runner and the admin dashboard always agree:
`database/updater_schedule.py` (export window, what counts as current, worker parts), `database/updater_locks.py`
(per-stock claims) and `database/updater_run_log.py` (the run log behind the dashboard).

## Dashboard

TickZen admin panel -> **Database Updater** (`/admin/db-updater`): tonight's progress and estimated finish, each
worker's state, region, IP, Yahoo requests per stock and 429s, stocks that need attention with the error, data
freshness per dataset, 30 nights of history, and the stock list where stocks are added, paused or removed.

Exporter-only files (never overwritten by the sync):

| File | Why it differs |
| :--- | :--- |
| `database/cron_update_runner.py` | Same per-stock export; scheduling for many workers: stock list from `monitored_tickers`, US-market window, parts per worker, per-stock claims, Azure-only |
| `database/yahoo_pacer.py` | Spaces every Yahoo request of a worker evenly; backs off on HTTP 429; counts requests |
| `app/html_components.py` | Only `get_currency_symbol` (identical) - the real file is the Flask UI |
| `Dockerfile`, `.dockerignore`, `requirements.txt` | Container build; package versions pinned to tickzen-github's `.venv` |
| `deploy/azure/` | Azure Container Apps deployment, one job per worker |
| `database/generate_dashboard.py`, `database/create_monitored_tickers.py`, `dashboard.html` | Legacy Render-era tools, not used by the runner |

## How it works

* **Stock list:** every active row of `monitored_tickers`, read fresh on every run. Add a stock there and the
  next run does its full first load.
* **When:** only while the US market is closed: trading days 16:30-09:00 New York time (30 min after the close to
  30 min before the open; `EXPORT_START_ET` / `EXPORT_STOP_ET`), weekends and NYSE holidays all day. Daylight saving is automatic.
* **What is stale:** a stock is current when its last *successful* sync is after the latest trading-day 16:30 ET.
  So each stock is exported once per trading session; weekends find everything current; partial/failed stocks
  stay stale and are retried.
* **Parts:** with `WORKER_COUNT` workers the list is split into equal parts (stocks ordered by md5 of the symbol
  and dealt out in turn, so every worker computes the same split). Worker `WORKER_INDEX` exports its own part
  first, oldest first; new stocks go to the front.
* **Failover:** when a worker has finished its part it helps with stale stocks of the other parts. Each stock
  is claimed (Postgres advisory lock) only right before it is exported, so an idle worker can take any stock
  another worker has not reached yet, and two workers never export the same stock at once. If one region's IP
  is blocked, the others finish its part.
* **Hourly batches:** each worker's job starts every hour at :30. A run exports at most `STOCKS_PER_RUN` (70)
  stocks and stops starting new ones after `RUN_TIME_BUDGET_MINUTES` (55), then waits for the next hour. It exits
  at once if the market is open, nothing is stale, or the worker is already running.
* **Rate limit:** inside a run every request to Yahoo goes through `database/yahoo_pacer.py`: at most
  `YAHOO_REQUESTS_PER_HOUR` (default 1,200 = one every 3 s, so never more than 20 in a minute), evenly spaced.
  A real HTTP 429 pauses the worker 60 s and halves its pace until the run ends (the next hourly run starts at
  full pace again); `STOP_AFTER_429` (3) of them end the run. The connectivity check deliberately does not call
  Yahoo: a plain request to Yahoo is answered with 429, which on 2026-10-15 halved both workers' pace all night.
* **Earnings step** (recent-reporter history) runs once per run on whichever worker gets to it first, after that
  worker's own part is done.
* **Implied moves** run in their own job, `tickzen-implied-moves` (`database/implied_moves_job.py`,
  `deploy/azure/deploy_implied_moves_job.sh`), at 15:15, 17:15 and 19:15 UTC on weekdays, because they need live
  option quotes: after the close Yahoo returns bid = ask = open interest = 0 on every contract, which is why the
  nightly runs stored NULL for 224 of 232 reports. The job exits at once outside 09:45-15:45 New York on trading
  days, only reads option chains (2 paced requests per report in the next 10 days) and only writes
  `earnings_calendar.implied_move_*`.

### Adding stocks

Use **Manage stocks** on the dashboard. The SQL equivalent:

```sql
INSERT INTO monitored_tickers (symbol, is_active) VALUES ('ABCD', true), ('EFGH', true)
ON CONFLICT (symbol) DO UPDATE SET is_active = true, updated_at = now();
```

To stop updating a stock: `UPDATE monitored_tickers SET is_active = false WHERE symbol = 'ABCD';`

## Capacity

Measured Yahoo requests per stock (MSFT, data collection, 2026-10-15): **38 with an empty cache, 6 with this
month's cache**. Adding the export's own calls (TTM cash flow, earnings history, analyst detail, live price),
plan with **~15 per stock on a normal night and ~45 when the cache is cold** (first export of each calendar
month, or a worker without its cache share).

| | Per worker per weekday night |
| :--- | :--- |
| Window | 16.5 h |
| Yahoo budget at 1,200/h | 19,800 requests |
| Stocks, cached (~15 each) | ~1,300 |
| Stocks, cold (~45 each) | ~440 |

For 4,000 stocks:

* Normal night: 4,000 x 15 = 60,000 requests -> 3.1 workers' budget.
* First night of a month (the profile/financials/peer caches are keyed by calendar month, so all of them
  expire together): 4,000 x 45 = 180,000 requests -> 9.1 workers' budget. Prophet forecasts also retrain monthly
  (~15 s CPU per stock) on that night.

## Deploy to Azure

```bash
brew install azure-cli && az login && az extension add -n containerapp
export AZURE_POSTGRES_PASSWORD=... FINNHUB_API_KEY=... FRED_API_KEY=...
WORKER_INDEX=0 WORKER_COUNT=6 LOCATION=centralindia deploy/azure/deploy_container_apps_job.sh
WORKER_INDEX=1 WORKER_COUNT=6 LOCATION=southindia   SKIP_BUILD=1 deploy/azure/deploy_container_apps_job.sh
# ... one call per worker, each in a different region
```

Each call creates (or updates) one worker: a Container Apps environment in that region (its own outbound IP), a
file share for its cache, and the job `tickzen-db-updater-<index>` (hourly at :30). The registry and image are shared;
`SKIP_BUILD=1` reuses the last image. `WORKER_COUNT` must be the same everywhere: when you add workers, redeploy
all of them with the new count (`SKIP_BUILD=1`). The script also warns if the Postgres firewall does not admit
Azure services.

After the Azure workers have completed a night, stop the Mac job so both do not compete:

```bash
launchctl unload ~/Library/LaunchAgents/com.tickzen.dbupdater.plist
```

## Run locally

```bash
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in the Azure password and API keys
python database/cron_update_runner.py --preview --force    # show this worker's queue
```

`.github/workflows/daily_sync.yml` is a manual-only fallback that runs one batch from a GitHub runner.
