# Stock Streaming Analytics Pipeline - Architecture

## Overview
Real-time stock market data pipeline processing streaming data from APIs to interactive dashboard.

## Architecture Diagram
```
┌─────────────┐
│ Stock APIs  │ (Polygon/Alpha Vantage/Yahoo Finance)
└──────┬──────┘
       │
       ▼
┌─────────────────┐
│ Python Producer │ (Fetches every 60s, runs as systemd service)
└────────┬────────┘
         │
         ▼
    ┌────────┐
    │ Kafka  │ (3 partitions, Docker container)
    └────┬───┘
         │
         ▼
┌────────────────────┐
│ Spark Streaming    │ (Processes batches every 1 min, systemd service)
│ - Read from Kafka  │
│ - Calculate SMA    │
│ - Calculate RSI    │
│ - Calculate MACD   │
│ - Bollinger Bands  │
└─────────┬──────────┘
          │
          ▼
    ┌───────────┐
    │ Snowflake │ (Data warehouse)
    │ RAW        │ (Landing)
    │ STAGING    │ (Cleaned)
    │ MARTS      │ (Star schema)
    └─────┬─────┘
          │
          ▼
   ┌──────────────┐
   │  Streamlit   │ (Interactive dashboard, cloud hosted)
   │  Dashboard   │
   └──────────────┘
```

## Components

### EC2 Instance (t3.medium - $35/month)
- **OS**: Ubuntu 22.04
- **Docker**: Kafka + Zookeeper containers
- **Spark**: Standalone installation
- **Python**: Producer script
- **Systemd**: Manages producer + Spark services

### Data Flow
1. **Ingest**: Producer fetches stock prices (AAPL, GOOGL, MSFT, TSLA, AMZN)
2. **Stream**: Kafka receives and stores messages
3. **Process**: Spark reads stream, calculates indicators
4. **Store**: Snowflake receives processed data
5. **Visualize**: Streamlit queries Snowflake, displays charts

### Data Model (Snowflake)

**RAW Schema:**
- `STOCK_PRICES_STREAMING` - Raw data from Spark

**MARTS Schema:**
- `DIM_STOCKS` - Stock dimension (symbol, company, sector)
- `FCT_STOCK_PRICES` - Price facts (OHLC, volume)
- `FCT_TECHNICAL_INDICATORS` - Indicator facts (SMA, RSI, MACD, BB)

### Technical Indicators
- **SMA**: 20-period, 50-period
- **EMA**: 12-period, 26-period
- **MACD**: MACD line, Signal line, Histogram
- **RSI**: 14-period (oversold <30, overbought >70)
- **Bollinger Bands**: 20-period, 2 std dev

## Technologies

| Layer | Technology | Version |
|-------|------------|---------|
| Messaging | Apache Kafka | 7.5.0 |
| Processing | Apache Spark | 3.5.0 |
| Storage | Snowflake | Cloud |
| Visualization | Streamlit | 1.31.0 |
| Infrastructure | AWS EC2 | t3.medium |
| Containerization | Docker | Latest |
| Language | Python | 3.9+ |

## Key Features
- ✅ Real-time streaming (60-second intervals)
- ✅ Fault-tolerant (systemd auto-restart)
- ✅ Scalable (Kafka partitions, Spark parallelism)
- ✅ Production-ready (logging, monitoring, health checks)
- ✅ Always-on (24/7 data pipeline)
- ✅ Cloud dashboard (accessible anywhere)

## Cost Breakdown
- **EC2 t3.medium**: ~$35/month
- **Snowflake**: Your existing account
- **Streamlit Cloud**: FREE
- **APIs**: FREE tier
- **Total**: ~$35/month