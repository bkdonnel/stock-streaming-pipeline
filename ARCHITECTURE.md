# Stock Batch Analytics Pipeline - Architecture

## Overview
Daily batch pipeline that fetches stock market data after market close, computes technical indicators using Spark on Databricks, stores results in Snowflake, and visualizes them in a Streamlit dashboard.

## Architecture Diagram
```
┌─────────────┐
│ Polygon API │ (daily OHLCV after market close)
└──────┬──────┘
       │
       ▼
┌──────────────────────────────────────┐
│         Databricks Job               │ (scheduled daily @ 4:30pm ET)
│                                      │
│  ┌──────────┐                        │
│  │  Bronze  │ Raw OHLCV (Delta Lake) │
│  └────┬─────┘                        │
│       ▼                              │
│  ┌──────────┐                        │
│  │  Silver  │ Cleaned + validated    │
│  └────┬─────┘                        │
│       ▼                              │
│  ┌──────────┐                        │
│  │   Gold   │ + Technical Indicators │
│  └────┬─────┘                        │
└───────┼──────────────────────────────┘
        │
        ▼
   ┌───────────┐
   │ Snowflake │ (serving layer)
   │  MARTS    │ DIM_STOCKS, FCT_STOCK_PRICES, FCT_TECHNICAL_INDICATORS
   └─────┬─────┘
         │
         ▼
  ┌──────────────┐
  │   Streamlit  │ (interactive dashboard, cloud hosted)
  └──────────────┘
```

## Components

### Databricks Workspace (free tier - 1 year)
- **Notebooks**: Three-stage medallion pipeline (Bronze → Silver → Gold)
- **Jobs**: Built-in scheduler triggers daily after market close
- **Delta Lake**: Versioned, ACID-compliant storage with time travel
- **Compute**: Managed Spark clusters — no EC2 or local infrastructure needed
- **Unity Catalog**: Workspace uses Unity Catalog; all tables live under the `bootcamp_students` catalog
- **Auth**: OAuth-based CLI authentication (Personal Access Tokens disabled at workspace level)
- **Secrets**: Polygon API key stored in Databricks Secrets (scope: `stock-pipeline`)

### Data Flow
1. **Ingest**: Databricks job calls Polygon API, fetches daily OHLCV for AAPL, GOOGL, MSFT, TSLA, AMZN
2. **Bronze**: Raw data written to Delta Lake as-is — immutable, append-only
3. **Silver**: Data cleaned, validated, and deduplicated
4. **Gold**: Technical indicators computed and joined with price data
5. **Store**: Gold layer written to Snowflake MARTS schema
6. **Visualize**: Streamlit queries Snowflake, renders charts

### Medallion Architecture (Delta Lake)

**Bronze Layer** — raw, immutable:
- `bootcamp_students.bd_bronze.stock_prices` — raw OHLCV from Polygon (symbol, date, open, high, low, close, volume, vwap, source)

**Silver Layer** — cleaned:
- `bootcamp_students.bd_silver.stock_prices` — validated types, deduplicated by (symbol, date), nulls handled

**Gold Layer** — enriched for serving:
- `bootcamp_students.bd_gold.stock_indicators` — OHLCV + all computed technical indicators

### Snowflake (Serving Layer)

**MARTS Schema:**
- `DIM_STOCKS` — stock dimension (symbol, company name, sector)
- `FCT_STOCK_PRICES` — daily price facts (OHLC, volume, vwap)
- `FCT_TECHNICAL_INDICATORS` — indicator facts (SMA, EMA, RSI, MACD, BB)

### Technical Indicators
All computed in the Gold notebook on daily closing prices:

| Indicator | Parameters | Description |
|-----------|-----------|-------------|
| **SMA** | 20-period, 50-period | Simple Moving Average |
| **EMA** | 12-period, 26-period | Exponential Moving Average |
| **RSI** | 14-period | Momentum oscillator; <30 oversold, >70 overbought |
| **MACD** | 12/26/9 | Line, signal line, histogram |
| **Bollinger Bands** | 20-period, 2 std dev | Upper, middle (SMA), lower bands |

## Technologies

| Layer | Technology | Notes |
|-------|------------|-------|
| Data Source | Polygon.io | Free tier, daily OHLCV |
| Processing | Apache Spark (Databricks) | Managed, no setup |
| Storage | Delta Lake | Built into Databricks |
| Warehouse | Snowflake | Serving layer |
| Visualization | Streamlit | Free cloud hosting |
| Language | Python | 3.9+ |

## Key Design Decisions
- **Batch over streaming**: Technical indicators (SMA, RSI, MACD, BB) are designed for daily closing prices — streaming adds complexity with no accuracy benefit
- **Databricks over local Spark**: Managed compute, built-in scheduling, Delta Lake, no infrastructure cost
- **Medallion architecture**: Standard Databricks pattern; Bronze/Silver/Gold separation makes the pipeline auditable and rerunnable
- **Snowflake as serving layer**: Decouples compute (Databricks) from serving (Snowflake); Streamlit gets fast SQL query performance

## Cost Breakdown
- **Databricks**: FREE (1-year access)
- **Snowflake**: Existing account
- **Streamlit Cloud**: FREE
- **Polygon API**: FREE tier
- **Total**: ~$0/month
