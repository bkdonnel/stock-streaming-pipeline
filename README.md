# Stock Market Analytics Pipeline

A fully automated, end-to-end batch data pipeline that ingests daily stock market data, computes technical indicators using Apache Spark, stores results in Snowflake, and serves them through a live interactive dashboard.

**[View Live Dashboard](https://your-app.streamlit.app)** <!-- replace with your Streamlit URL -->

---

## Architecture

```
Polygon.io API
     |
     | (daily OHLCV after market close)
     v
Databricks Job (scheduled 4:30pm ET)
     |
     |-- Bronze Layer  (raw ingestion, Delta Lake)
     |-- Silver Layer  (cleaning + deduplication, Delta Lake)
     |-- Gold Layer    (technical indicators, Delta Lake)
     |
     v
Snowflake (serving layer)
     |
     v
Streamlit Dashboard (public, Streamlit Community Cloud)
```

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python, Polygon.io API |
| Processing | Apache Spark (Databricks) |
| Storage | Delta Lake (medallion architecture) |
| Warehouse | Snowflake |
| Dashboard | Streamlit, Plotly |
| Scheduling | Databricks Jobs (daily cron) |
| Version Control | GitHub (Databricks Git integration) |

## Features

- **Medallion architecture** — Bronze (raw) → Silver (cleaned) → Gold (enriched) Delta Lake tables
- **5 technical indicators** computed per symbol on daily closing prices:
  - Simple Moving Average (SMA 20, SMA 50)
  - Exponential Moving Average (EMA 12, EMA 26)
  - Relative Strength Index (RSI 14)
  - MACD (12/26/9) with signal line and histogram
  - Bollinger Bands (20-period, 2 std dev)
- **Fully automated** — Databricks Job runs daily, no manual intervention required
- **Live dashboard** — publicly hosted on Streamlit Community Cloud, auto-deploys on every GitHub push
- **Secure credential management** — API keys and RSA private keys stored in Databricks Secrets and Streamlit Cloud secrets (never in code)

## Tracked Stocks

AAPL · GOOGL · MSFT · TSLA · AMZN

## Dashboard

The dashboard connects directly to Snowflake and displays:
- Price chart with SMA-20, SMA-50, and Bollinger Bands overlay
- RSI panel with overbought (70) / oversold (30) reference lines
- MACD panel with signal line and histogram
- Key metrics: latest close price, RSI, SMA-20, SMA-50, MACD
- Interval selector: 1M / 3M / 6M / 1Y / YTD

## Project Structure

```
databricks/
  notebooks/
    01_bronze_ingestion.py   # Fetch from Polygon, write raw to Delta
    02_silver_transform.py   # Clean, validate, deduplicate
    03_gold_indicators.py    # Compute SMA, EMA, RSI, MACD, Bollinger Bands
    04_snowflake_load.py     # Write Gold data to Snowflake
    05_backfill.py           # Load historical data via Polygon aggregates API
  indicators.py              # Pure indicator functions (pandas)
  config.py                  # Shared config (Databricks Secrets + .env fallback)
dashboard/
  app.py                     # Streamlit dashboard
  requirements.txt
```

## Data Pipeline

1. **Bronze** — Raw OHLCV records fetched from Polygon.io and written to Delta Lake as-is. Append-only, immutable.
2. **Silver** — Data cleaned: date cast to `DateType`, null/zero closes dropped, duplicates removed by keeping the most recently ingested record per `(symbol, date)`.
3. **Gold** — Technical indicators computed per symbol using `groupBy().applyInPandas()`. All 5 indicators joined with price data into a single wide table.
4. **Snowflake** — Gold table written to `FCT_STOCK_PRICES` and `FCT_TECHNICAL_INDICATORS` via `snowflake-connector-python` with RSA key pair authentication.
5. **Dashboard** — Streamlit app queries Snowflake directly, renders interactive Plotly charts.

## Cost

| Service | Cost |
|---|---|
| Databricks | Free (1-year access) |
| Snowflake | Free (community account) |
| Streamlit Community Cloud | Free |
| Polygon.io | Free tier |
| **Total** | **$0/month** |
