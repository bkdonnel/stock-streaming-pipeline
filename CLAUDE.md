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
  - Loops over all 5 symbols, collects records, writes to `bootcamp_students.bd_bronze.stock_prices` (Delta, append mode)
  - Supports `run_date` widget for backfills; defaults to yesterday
  - Gracefully skips 404s (holidays/weekends)
  - Verified: 5 rows written and confirmed via `spark.table().show()` (2026-03-06 data)
  - Note: `vwap` is `0.0` for all rows — Polygon's `/v1/open-close` endpoint does not return vwap; field is retained for schema compatibility
- **Phase 3 (Silver Layer - Cleaning):** Complete
  - `02_silver_transform.py` built and tested — 5 rows in, 5 rows out, no drops
  - Casts `date` to `DateType`, drops null/zero closes, deduplicates on `(symbol, date)` by most recent `ingested_at`, adds `transformed_at` timestamp
  - Writes to `bootcamp_students.bd_silver.stock_prices` (Delta, overwrite mode)
- **Phase 4 (Gold Layer - Indicators):** Complete
  - `databricks/indicators.py` — pure functions: `calculate_sma`, `calculate_ema`, `calculate_rsi`, `calculate_macd`, `calculate_bollinger_bands`, `add_indicators`
  - `03_gold_indicators.py` — reads Silver, applies `add_indicators` per symbol via `groupBy("symbol").applyInPandas()`, writes to `bootcamp_students.bd_gold.stock_indicators` (Delta, overwrite mode)
  - Repo path resolved dynamically at runtime so import works regardless of Databricks user email
  - Verified: 5 rows in, 5 rows out, 22 columns (symbol + date + 20 indicator fields)
- **Phase 5 (Snowflake Integration):** Complete
  - Snowflake account: `LYWBBPJ-ODB66944`, database: `DATAEXPERT_STUDENT`, schema: `BRYAN`
  - Auth: RSA key pair — public key registered in Snowflake UI, private key stored in Databricks Secrets
  - Secrets stored in scope `stock-pipeline`: `snowflake-account`, `snowflake-user`, `snowflake-private-key`, `snowflake-database`, `snowflake-schema`
  - Rewrote from Spark connector (Maven) to `snowflake-connector-python` + `cryptography` — Spark connector caused JWT auth failures on the shared DataExpert cluster
  - `04_snowflake_load.py` uses `%pip install snowflake-connector-python cryptography`, parses PEM key via `load_pem_private_key` → DER bytes, writes via pandas (5 rows)
  - Verified: `FCT_STOCK_PRICES` and `FCT_TECHNICAL_INDICATORS` written and read back successfully
  - Key lessons: shared cluster is serverless (blocks Maven connectors); RSA public key in Snowflake must match private key in Databricks Secrets (fingerprint mismatch was root cause of JWT errors)
- **Phase 6 (Streamlit Dashboard):** In progress — app built, local testing blocked by RSA key mismatch
  - `dashboard/app.py` written — stock dropdown, date picker, 3-panel Plotly chart (price+BB, RSI, MACD), metrics row
  - `dashboard/requirements.txt` — streamlit, snowflake-connector-python, cryptography, pandas, plotly
  - Decided on **Streamlit Community Cloud** (not Streamlit in Snowflake) — SiS requires Snowflake login, not publicly accessible
  - Local secrets: `.streamlit/secrets.toml` (gitignored) — must be placed at `dashboard/.streamlit/secrets.toml` when running from `dashboard/` subdirectory
  - **Blocker:** JWT token invalid on local test — `~/rsa_key.pem` fingerprint (`SHA256:PUw88...`) does not match the key registered in Snowflake (`SHA256:fXaWO...`)
  - **Fix:** Extract the correct private key from Databricks Secrets: run `dbutils.secrets.get(scope="stock-pipeline", key="snowflake-private-key")` in a notebook cell, copy output into `secrets.toml`
  - The authoritative private key lives in Databricks Secrets — local `.pem` files are copies that may be out of sync
- **Phase 7 (Scheduling & Monitoring):** Not started

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
- Tables: `FCT_STOCK_PRICES`, `FCT_TECHNICAL_INDICATORS`
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
    01_bronze_ingestion.py    # Fetch from Polygon, write raw to Delta
    02_silver_transform.py    # Clean, validate, deduplicate
    03_gold_indicators.py     # Compute SMA, EMA, RSI, MACD, BB
    04_snowflake_load.py      # Write Gold data to Snowflake FCT tables
  config.py                  # Shared Spark + API config (secrets + .env fallback)
  indicators.py              # Pure indicator functions (calculate_sma, ema, rsi, macd, bb, add_indicators)
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
