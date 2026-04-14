# Stock Batch Analytics Pipeline

## Project Overview
Daily batch stock market data pipeline: Polygon API -> Databricks (Delta Lake + Spark) -> Snowflake -> Streamlit Dashboard.

Tracked stocks: AAPL, GOOGL, MSFT, TSLA, AMZN.

## Current Status
- **Phases 1-2 (Kafka + Python Producer):** Built and tested, but **deprecated** — pipeline pivoted from streaming to daily batch.
- **Phase 1 (Databricks Setup):** Complete
  - Cluster created (Personal Compute, LTS runtime, Unity Catalog)
  - Databricks Secrets configured via OAuth CLI (scope: `stock-pipeline`, key: `polygon-api-key`)
  - Schemas created: `bootcamp_students.bd_bronze`, `bootcamp_students.bd_silver`, `bootcamp_students.bd_gold` (prefixed with `bd_` — `bootcamp_students` is a shared community catalog)
  - `databricks/config.py` written — reads secret on cluster, falls back to `.env` locally
  - `databricks/notebooks/00_setup.py` written and verified — packages, schemas, secret, and Polygon API all confirmed
- **Phase 2 (Bronze Layer - Ingestion):** Complete
  - `fetch_daily_ohlcv()` implemented using Polygon `/v1/open-close/{symbol}/{date}` endpoint
  - Loops over all 5 symbols, collects records, writes to `bootcamp_students.bd_bronze.stock_prices` (Delta)
  - Supports `run_date` widget for backfills; defaults to yesterday
  - Gracefully skips 404s (holidays/weekends)
  - Verified: 5 rows written and confirmed via `spark.table().show()` (2026-03-06 data)
  - Note: `vwap` is `0.0` for all rows — Polygon's `/v1/open-close` endpoint does not return vwap; field is retained for schema compatibility
  - **Idempotency fix:** writes now use Delta `merge` on `(symbol, date)` instead of append — re-running for the same date is a no-op
- **Phase 3 (Silver Layer - Cleaning):** Complete
  - `02_silver_transform.py` built and tested — 5 rows in, 5 rows out, no drops
  - Casts `date` to `DateType`, drops null/zero closes, deduplicates on `(symbol, date)` by most recent `ingested_at`, adds `transformed_at` timestamp
  - Uses Write-Audit-Publish: transforms staged to `bd_silver.stock_prices_staging`, audited (row count, no nulls, symbol completeness, close range), then published to `bd_silver.stock_prices` — live table not updated if any check fails
- **Phase 4 (Gold Layer - Indicators):** Complete
  - `databricks/indicators.py` — pure functions: `calculate_sma`, `calculate_ema`, `calculate_rsi`, `calculate_macd`, `calculate_bollinger_bands`, `add_indicators`
  - `03_gold_indicators.py` — reads Silver, applies `add_indicators` per symbol via `groupBy("symbol").applyInPandas()`, writes to `bootcamp_students.bd_gold.stock_indicators` (Delta, overwrite mode)
  - Repo path resolved dynamically at runtime so import works regardless of Databricks user email
  - Verified: 5 rows in, 5 rows out, 22 columns (symbol + date + 20 indicator fields)
  - Uses Write-Audit-Publish: staged to `bd_gold.stock_indicators_staging`, audited (row count matches Silver, symbol completeness, RSI in [0,100], SMA-20 positive), then published
- **Phase 5 (Snowflake Integration):** Complete
  - Snowflake account: `LYWBBPJ-ODB66944`, database: `DATAEXPERT_STUDENT`, schema: `BRYAN`
  - Auth: RSA key pair — public key registered in Snowflake UI, private key stored in Databricks Secrets
  - Secrets stored in scope `stock-pipeline`: `snowflake-account`, `snowflake-user`, `snowflake-private-key`, `snowflake-database`, `snowflake-schema`
  - Rewrote from Spark connector (Maven) to `snowflake-connector-python` + `cryptography` — Spark connector caused JWT auth failures on the shared DataExpert cluster
  - `04_snowflake_load.py` uses `%pip install snowflake-connector-python cryptography`, parses PEM key via `load_pem_private_key` → DER bytes, writes via pandas (5 rows)
  - Verified: `FCT_STOCK_PRICES` and `FCT_TECHNICAL_INDICATORS` written and read back successfully
  - Key lessons: shared cluster is serverless (blocks Maven connectors); RSA public key in Snowflake must match private key in Databricks Secrets (fingerprint mismatch was root cause of JWT errors)
- **Phase 6 (Streamlit Dashboard):** Complete
  - `dashboard/app.py` — stock dropdown, interval selector (1M/3M/6M/1Y/YTD), 3-panel Plotly chart (price+BB, RSI, MACD), metrics row
  - `dashboard/requirements.txt` — streamlit, snowflake-connector-python, cryptography, pandas, plotly
  - Deployed to **Streamlit Community Cloud** (not Streamlit in Snowflake — SiS requires Snowflake login, not publicly accessible)
  - Auto-deploys on every `git push` to `main`
  - Secrets added via Streamlit Cloud UI settings — paste full `secrets.toml` contents there
  - Local testing: run `streamlit run app.py` from `dashboard/` directory; secrets must be at `dashboard/.streamlit/secrets.toml`
  - **RSA key sync issue (resolved):** local `.pem` files and Databricks Secrets were out of sync with Snowflake registered key; fixed by registering the public key (with PEM line breaks) via DataExpert org UI — Snowflake UI breaks single-line keys, must paste with 64-char line breaks
  - Active public key fingerprint: `SHA256:PUw88mqTNMUdknWEr/g09N7A3zyyriszORxv2pgoKes=`
- **Phase 7 (Scheduling & Monitoring):** Complete
  - Databricks Job created via **Workflows → Jobs and Pipelines** UI
  - Job name: `stock-pipeline-daily`, 4 tasks chained: `01_bronze_ingestion` → `02_silver_transform` → `03_gold_indicators` → `04_snowflake_load`
  - Each task: Git source, repo `bkdonnel/stock-streaming-pipeline`, branch `main`, DataExpert cluster
  - **Important:** notebook paths must NOT include `.py` extension (e.g. `databricks/notebooks/01_bronze_ingestion`)
  - Schedule: daily at 21:30 UTC (4:30pm ET, after US market close)
  - Email notifications configured for on-failure
  - Verified: all 4 tasks completed successfully in manual test run
  - Key fixes made during setup:
    - `01_bronze_ingestion.py`: handle 403 (weekend/holiday) same as 404, exit cleanly via `dbutils.notebook.exit()` instead of raising `RuntimeError`
    - `03_gold_indicators.py`: path resolution for `indicators.py` — Job Git source sets cwd to `databricks/notebooks/`, so `indicators.py` is found at `cwd/../indicators.py`
  - **Backfill:** `05_backfill.py` written to load historical data
    - Initial attempt used `/v1/open-close` per date — failed (free tier only serves yesterday's data)
    - Rewrote to use `/v2/aggs/ticker/{ticker}/range/1/day/{from}/{to}` — fetches full date range per symbol in one API call, works on free tier
    - Idempotency fix applied: deduplicates existing Bronze table in place before writing, then merges new records on `(symbol, date)`
  - **Dashboard updates post-launch:**
    - Replaced date picker with interval radio buttons: 1M / 3M / 6M (default) / 1Y / YTD
    - Increased chart vertical spacing and height for better readability
- **Phase 8 (Signal Generation):** Complete
  - `06_signals.py` — reads Gold table, computes 4 rule-based signals per symbol per day via `applyInPandas`
  - Signals: RSI (oversold <30 / overbought >70), MACD crossover (line crosses signal), Bollinger Band (close outside bands), SMA crossover (golden/death cross)
  - Composite signal derived from net bullish/bearish count: `strong_buy / buy / neutral / sell / strong_sell`
  - `signal_strength` float in [-1.0, 1.0] (net score / 4)
  - Uses Write-Audit-Publish: staged to `bd_gold.stock_signals_staging`, audited (row count, symbol completeness, signal_strength in range), then published to `bd_gold.stock_signals`
  - Writes `FCT_SIGNALS` to Snowflake (same RSA auth pattern as `04_snowflake_load.py`)
  - Verified: ran successfully, 5 symbols × full history, FCT_SIGNALS confirmed in Snowflake
  - Dashboard updated: Trading Signals section added below chart — composite signal badge (color-coded), per-component breakdown, signal strength time series chart, expandable 10-day history table
  - **Job update needed:** add `databricks/notebooks/06_signals` as 5th task after `04_snowflake_load` in `stock-pipeline-daily`

See `IMPLEMENTATION.md` for the full phase-by-phase plan and `ARCHITECTURE.md` for system design.

## Why Batch Instead of Streaming
The technical indicators (SMA, EMA, RSI, MACD, Bollinger Bands) are all designed around daily closing prices. A 60-second streaming interval adds infrastructure complexity without improving indicator quality. A daily batch job after market close produces identical, valid results with far less overhead.

## Infrastructure

### Databricks Workspace (free tier - 1 year)
- **Notebooks** — development and transformation logic
- **Jobs** — daily scheduled pipeline runs (built-in cron, no Airflow needed)
- **Delta Lake** — medallion architecture storage (Bronze / Silver / Gold)
- **Compute** — managed Spark clusters (no EC2 needed)
- **Unity Catalog** — workspace uses Unity Catalog; catalog is `bootcamp_students`
- **Auth** — Personal Access Tokens are disabled; CLI authenticated via OAuth (`databricks auth login`)
- **Secrets** — API key stored in Databricks Secrets (scope: `stock-pipeline`, key: `polygon-api-key`)
- **Compute** — shared DataExpert cluster (serverless); Maven packages not supported — use `%pip install` instead
- **Git Integration** — Databricks Repos (Git Folders) connected to GitHub (`bkdonnel/stock-streaming-pipeline`); workflow is `git push` locally then Pull in Databricks UI — no manual notebook uploads needed

### Delta Lake Schemas (Unity Catalog)
- `bootcamp_students.bd_bronze` — raw ingestion layer
- `bootcamp_students.bd_silver` — cleaned and validated
- `bootcamp_students.bd_gold` — indicators computed, ready for serving

### Data Source
- **Polygon.io** — primary API for daily OHLCV data
- API key retrieved via `dbutils.secrets.get(scope="stock-pipeline", key="polygon-api-key")` on cluster
- Falls back to `POLYGON_API_KEY` in `.env` for local development

### Snowflake (Serving Layer)
- Account: `LYWBBPJ-ODB66944` (shared DataExpert community account)
- Database: `DATAEXPERT_STUDENT`, Schema: `BRYAN`
- Warehouse: `COMPUTE_WH`
- Auth: RSA key pair (public key registered in Snowflake UI, private key in Databricks Secrets)
- Tables: `FCT_STOCK_PRICES`, `FCT_TECHNICAL_INDICATORS`, `FCT_SIGNALS`
- Queried by Streamlit dashboard

## Deprecated (Kafka / Streaming)
The `kafka/` directory and `docker-compose.yml` are kept for reference. The Polygon API utility functions in `kafka/producer/utils.py` (`fetch_stock_data_polygon`, `validate_stock_data`) can be reused in the Databricks ingestion notebook.

## Medallion Architecture (Delta Lake)
- **Bronze** — raw OHLCV data from Polygon, written as-is, immutable
- **Silver** — cleaned, validated, deduplicated
- **Gold** — technical indicators computed and joined with price data

## Technical Indicators
All computed in the Gold layer using PySpark + pandas UDFs:
- **SMA** — 20-period, 50-period
- **EMA** — 12-period, 26-period
- **RSI** — 14-period (oversold <30, overbought >70)
- **MACD** — line, signal line, histogram (12/26/9)
- **Bollinger Bands** — 20-period, 2 std dev

## Expansion Roadmap

The pipeline has a clear path toward a trading bot. Each phase builds on the previous one — do not skip backtesting before live execution.

### Phase 9 — Backtesting Framework (next up)
Validate signals against historical data before trusting them with real money.
- Input: `FCT_SIGNALS` + `FCT_STOCK_PRICES` (already in Snowflake)
- Logic: for each signal day, simulate buy at next-day open, sell at exit signal or fixed hold period
- Metrics to track: win rate, average return per trade, max drawdown, Sharpe ratio
- Can be a standalone Python script or a Databricks notebook (`07_backtest.py`)
- No new infrastructure needed — pure pandas on top of existing data

### Phase 10 — ML Prediction Layer
Use existing Gold indicator columns (~20 features) to train a directional model.
- Target: next-day return > 0% (binary classification) or return magnitude (regression)
- Model: XGBoost or LightGBM — interpretable, fast to train on small datasets
- More history = better model; use Polygon `/v2/aggs` to backfill 2+ years if needed
- Output: `predicted_direction` + `confidence` score per symbol per day → `FCT_PREDICTIONS` in Snowflake
- Optional: Claude API reasoning layer — feed signals + prediction to Claude for a plain-English daily summary on the dashboard

### Phase 11 — Paper Trading Bot
Simulate trades using live signal/prediction output before touching real money.
- Reads `FCT_SIGNALS` (and optionally `FCT_PREDICTIONS`) from Snowflake each morning
- Maintains a `portfolio` table: open positions, entry price, date, size
- Evaluates exit conditions on open positions; logs all trades with P&L
- Runs as a Databricks Job task or standalone cron script
- Run for 3-6 months to validate signals on live data

### Phase 12 — Live Execution (high risk, do last)
Connect to a brokerage API once paper trading is consistently profitable.
- **Alpaca** is the recommended starting point — free paper + live trading API, REST-based Python SDK, no pattern-day-trader restrictions on paper accounts
- Requires: position sizing logic, stop-losses, circuit breakers before going live
- Interactive Brokers is an alternative for more realistic execution but higher complexity

## Code Style
- Python: use functional/declarative style, type hints, Pydantic models
- Use lowercase with underscores for files/directories
- No emojis in Python files
- See `AGENTS.md` for full coding conventions

## Project Structure
```
databricks/
  notebooks/
    00_setup.py               # One-time setup: verify env, create schemas, smoke test API
    01_bronze_ingestion.py    # Fetch from Polygon, merge raw to Delta (idempotent on symbol+date)
    02_silver_transform.py    # Clean, validate, deduplicate — Write-Audit-Publish to Silver
    03_gold_indicators.py     # Compute SMA, EMA, RSI, MACD, BB — Write-Audit-Publish to Gold
    04_snowflake_load.py      # Write Gold data to Snowflake FCT tables
    05_backfill.py            # Historical backfill via Polygon /v2/aggs — idempotent merge
    06_signals.py             # Rule-based signals (RSI, MACD cross, BB, SMA cross) — WAP to bd_gold.stock_signals + FCT_SIGNALS
  config.py                  # Shared Spark + API config (secrets + .env fallback)
  indicators.py              # Pure indicator functions (calculate_sma, ema, rsi, macd, bb, add_indicators)
  audit.py                   # WAP utilities: AuditResult, check_* functions, write_audit_publish
dashboard/
  app.py                     # Streamlit dashboard (stock dropdown, date picker, price/RSI/MACD charts)
  requirements.txt           # streamlit, snowflake-connector-python, cryptography, pandas, plotly
  .streamlit/
    secrets.toml             # Snowflake credentials (gitignored — never commit)
.env                         # API keys (gitignored)
requirements.txt             # Python dependencies
ARCHITECTURE.md              # System design and data model
IMPLEMENTATION.md            # Phase-by-phase plan
AGENTS.md                    # Coding style and conventions
kafka/                       # Deprecated — kept for reference
  producer/
    utils.py                 # fetch_stock_data_polygon() reusable here
```
