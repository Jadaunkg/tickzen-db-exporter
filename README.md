# TickZen Standalone Database Exporter

This is a completely standalone backend pipeline project for extracting stock data from free APIs (`yfinance`, `finnhub`, `FRED`) and exporting/syncing it to an external database (Azure PostgreSQL or Supabase).

It is isolated from the main TickZen Flask application portal to allow lightweight, multi-instance background daemon deployments on free cloud platforms (like Render).

---

## Why Is This Project Isolated?
Free market data APIs place strict limits on how many requests can be made from a single IP address. Making thousands of requests concurrently will result in temporary or permanent IP bans. 

To resolve this:
1. The 1,600+ ticker stock list is split into **batches of 300–400 stocks**.
2. **5 different Render services** are deployed (which are free under Render's free tier).
3. Each Render deployment is configured with a specific **limit and offset** (e.g. Service 1 processes tickers 0-400, Service 2 processes 400-800, etc.).
4. This distributes the API request load across 5 different egress IPs, successfully bypassing API rate limits.

---

## Directory Structure

```
tickzen-db-exporter/
├── .env.example
├── .gitignore
├── Dockerfile
├── README.md
├── requirements.txt
├── app/
│   └── html_components.py        # Mocked dependencies (currency helper)
├── database/
│   ├── azure_postgres_client.py  # Azure client
│   ├── supabase_client.py        # Supabase client
│   ├── cron_update_runner.py     # Main Cron/batch script
│   ├── smart_updater.py          # Smart update logic (daily/weekly/monthly)
│   ├── pipeline_data_collector.py# Data ingestion pipeline
│   ├── data_mapper.py            # DB schema mapping
│   ├── export_to_azure_postgres.py # Direct Azure entrypoint
│   ├── export_to_supabase.py     # Direct Supabase entrypoint
│   └── stock_tickers_list.json   # Full list of stock tickers
├── data_processing_scripts/      # Preprocessing, indicators, macro data
├── Models/                       # Prophet forecasting models
└── analysis_scripts/             # Core analytical engines
```

---

## Local Setup & Installation

1. **Create and activate a virtual environment**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Environment Variables**:
   Copy `.env.example` to `.env` and fill in your actual credentials:
   ```bash
   cp .env.example .env
   ```

---

## How it Works: Smart Update Strategy

The export pipeline (`smart_updater.py`) does not upload all data every single time. It categorizes database tables to optimize performance:

* **DAILY Update (Fast Incremental, 3–5 seconds per stock)**:
  - Updates: `daily_price_data`, `technical_indicators`, `market_price_snapshot`, `sentiment_data`.
  - Only fetches prices since the last recorded date in the database.
* **WEEKLY Update**:
  - Updates: `forecast_data` (Prophet), `risk_data` (Value-at-Risk, Sharpe, maximum drawdown), `analyst_data`.
* **MONTHLY Update**:
  - Updates: `ownership_data`, `peer_comparison_data`.
* **QUARTERLY / EVENT Update**:
  - Updates: `fundamental_data`, `dividend_data`, `insider_transactions`.
* **NEW STOCKS (Full Load, 30–60 seconds)**:
  - Automatically detects if a stock symbol does not exist in the database and performs a full 10-year historical load of all tables.

---

## Execution Guide

### 1. Test Database Connectivity
You can test your connection to Azure Postgres or Supabase using:
```bash
python database/export_to_azure_postgres.py --test-connection
```

### 2. Run Single Stock Sync
To sync/update a single ticker (e.g. Apple):
```bash
python database/export_to_azure_postgres.py AAPL
```

### 3. Run Automated Cron/Batch Updates
To run the automated scheduler runner:
```bash
python database/cron_update_runner.py --limit 70
```
* **Note**: The cron runner is designed to skip runs during active market hours to prevent rate limit conflicts. You can force execution at any time by adding the `--force` flag:
```bash
python database/cron_update_runner.py --limit 70 --force
```

---

## Database Ticker List Setup (Option A)

Instead of editing local JSON files, the exporter queries the database table `monitored_tickers` for active tickers. 

### 1. Create the `monitored_tickers` Table
Run this SQL query on your database:
```sql
CREATE TABLE IF NOT EXISTS monitored_tickers (
    symbol VARCHAR(10) PRIMARY KEY,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 2. Seed/Insert Tickers
Insert the stock symbols you want the exporter to sync:
```sql
INSERT INTO monitored_tickers (symbol, is_active)
VALUES 
('AAPL', true),
('MSFT', true),
('NVDA', true),
('TSLA', true)
ON CONFLICT (symbol) DO UPDATE 
SET is_active = true;
```

If the `monitored_tickers` table does not exist or is empty, the exporter will automatically fall back to reading symbols from `database/stock_tickers_list.json`.

## Cloud Automation (Deployment & Scheduling)

You can deploy and automate this sync system in two ways: using **GitHub Actions (Recommended & Free)** or **Render Services**.

---

### Option A: GitHub Actions (Recommended & Free)
Using GitHub Actions, you can run the 5 instances in parallel directly inside GitHub's infrastructure. It is completely free for public repositories and private repositories (up to monthly limits, extended by the GitHub Student Developer Pack).

#### How to Set Up:
1. **Push this repository to GitHub**.
2. **Add Secrets**: In your GitHub Repository, navigate to **Settings > Secrets and variables > Actions** and click **New repository secret** to add:
   - `AZURE_POSTGRES_URL` = `postgresql://Jadaunkg:<password>@<host>:5432/<database>?sslmode=require`
   - `FRED_API_KEY` = `your_fred_key` (Shared key)
   - `FINNHUB_API_KEY_0` = `first_finnhub_key` (Used by Instance 0)
   - `FINNHUB_API_KEY_1` = `second_finnhub_key` (Used by Instance 1)
   - `FINNHUB_API_KEY_2` = `third_finnhub_key` (Used by Instance 2)
   - `FINNHUB_API_KEY_3` = `fourth_finnhub_key` (Used by Instance 3)
   - `FINNHUB_API_KEY_4` = `fifth_finnhub_key` (Used by Instance 4)
3. **Execution**:
   - **Automatic**: The workflow file `.github/workflows/daily_sync.yml` is configured to run automatically everyday at 21:00 UTC (2:30 AM IST).
   - **Manual**: Go to the **Actions** tab in your repository, select **Daily Stock Data Sync**, and click **Run workflow**.

This spins up 5 parallel runners, each working on its alphabetical slice of tickers with sub-batch throttling (70 stocks at a time, sleeping 70 minutes between batches), completing the sync overnight.

---

### Option B: Render Service Setup
If you prefer to deploy on Render, create **5 Background Workers** (or Web Services) pointing to this GitHub repository.

#### Method 1: Partitioning via Instance Index
You configure each Render service with the same start command:
```bash
python database/cron_update_runner.py --force
```
Then, you set a different `INSTANCE_INDEX` environment variable on each Render service:

| Service Name | Environment Variables | Target Tickers |
| :--- | :--- | :--- |
| **Instance 1** | `INSTANCE_INDEX=0`, `TOTAL_INSTANCES=5` | First 20% of tickers sorted alphabetically |
| **Instance 2** | `INSTANCE_INDEX=1`, `TOTAL_INSTANCES=5` | Second 20% of tickers sorted alphabetically |
| **Instance 3** | `INSTANCE_INDEX=2`, `TOTAL_INSTANCES=5` | Third 20% of tickers sorted alphabetically |
| **Instance 4** | `INSTANCE_INDEX=3`, `TOTAL_INSTANCES=5` | Fourth 20% of tickers sorted alphabetically |
| **Instance 5** | `INSTANCE_INDEX=4`, `TOTAL_INSTANCES=5` | Fifth 20% of tickers sorted alphabetically |

#### Method 2: Direct Limits and Offsets
Alternatively, configure unique start commands for each service (using either CLI flags or environment variables):

| Instance ID | Start Command | Target Tickers | Description |
| :--- | :--- | :--- | :--- |
| **Instance 1** | `python database/cron_update_runner.py --limit 400 --offset 0` | Tickers 1 - 400 | First batch of stocks |
| **Instance 2** | `python database/cron_update_runner.py --limit 400 --offset 400` | Tickers 401 - 800 | Second batch of stocks |
| **Instance 3** | `python database/cron_update_runner.py --limit 400 --offset 800` | Tickers 801 - 1200 | Third batch of stocks |
| **Instance 4** | `python database/cron_update_runner.py --limit 400 --offset 1200` | Tickers 1201 - 1600 | Fourth batch of stocks |
| **Instance 5** | `python database/cron_update_runner.py --limit 400 --offset 1600` | Tickers 1601 - End | Fifth batch of stocks |

#### Environment Variables for Render
Make sure to define these variables in the Render dashboard for each service:
- `DB_TYPE`: `azure`
- `AZURE_POSTGRES_URL`: `postgresql://Jadaunkg:<password>@<host>:5432/<database>?sslmode=require`
- `FINNHUB_API_KEY`: `<your key>`
- `FRED_API_KEY`: `<your key>`
- `SUB_BATCH_SIZE`: `70`
- `BATCH_SLEEP_SECONDS`: `4200`
- `PYTHONUNBUFFERED`: `1`


