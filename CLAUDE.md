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
- **Phase 2 (Bronze Layer - Ingestion):** Built (`databricks/notebooks/01_bronze_ingestion.py`) — pending test run in Databricks
  - `fetch_daily_ohlcv()` implemented using Polygon `/v1/open-close/{symbol}/{date}` endpoint
  - Loops over all 5 symbols, collects records, writes to `bootcamp_students.bd_bronze.stock_prices` (Delta, append mode)
  - Supports `run_date` widget for backfills; defaults to yesterday
  - Gracefully skips 404s (holidays/weekends)
- **Phase 3 (Silver Layer - Cleaning):** Not started
- **Phase 4 (Gold Layer - Indicators):** Not started
- **Phase 5 (Snowflake Integration):** Not started
- **Phase 6 (Streamlit Dashboard):** Not started
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

### Delta Lake Schemas (Unity Catalog)
- `bootcamp_students.bd_bronze` — raw ingestion layer
- `bootcamp_students.bd_silver` — cleaned and validated
- `bootcamp_students.bd_gold` — indicators computed, ready for serving

### Data Source
- **Polygon.io** — primary API for daily OHLCV data
- API key retrieved via `dbutils.secrets.get(scope="stock-pipeline", key="polygon-api-key")` on cluster
- Falls back to `POLYGON_API_KEY` in `.env` for local development

### Snowflake (Serving Layer)
- Receives Gold layer data from Databricks
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
  config.py                  # Shared Spark + API config (secrets + .env fallback)
dashboard/
  app.py                     # Streamlit dashboard
  requirements.txt           # Streamlit dependencies
.env                         # API keys (gitignored)
requirements.txt             # Python dependencies
ARCHITECTURE.md              # System design and data model
IMPLEMENTATION.md            # Phase-by-phase plan
AGENTS.md                    # Coding style and conventions
kafka/                       # Deprecated — kept for reference
  producer/
    utils.py                 # fetch_stock_data_polygon() reusable here
```
