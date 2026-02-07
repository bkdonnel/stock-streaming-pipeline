# Stock Streaming Analytics Pipeline - Implementation Plan

## Phase 1: Local Kafka Setup
**Goal:** Get Kafka running locally with Docker and verify message flow.

- [ ] Remove deprecated `version` field from `docker-compose.yml`
- [ ] Start Kafka and Zookeeper containers with `docker compose up -d`
- [ ] Verify Kafka is running and accessible on `localhost:9092`
- [ ] Verify Kafka UI is accessible on `localhost:8080`
- [ ] Create a `stock-prices` topic with 3 partitions
- [ ] Test producing and consuming messages manually

## Phase 2: Python Producer
**Goal:** Fetch real-time stock data from an API and publish to Kafka.

- [ ] Set up Python project structure (`producer/`)
- [ ] Create `requirements.txt` (kafka-python, requests, python-dotenv)
- [ ] Choose a stock API (Polygon, Alpha Vantage, or Yahoo Finance) and get API key
- [ ] Create `.env` file for API keys and configuration (add to `.gitignore`)
- [ ] Write producer script that fetches AAPL, GOOGL, MSFT, TSLA, AMZN prices
- [ ] Publish price messages to `stock-prices` Kafka topic as JSON
- [ ] Add logging and error handling
- [ ] Test producer locally — verify messages appear in Kafka UI

## Phase 3: Spark Streaming Processor
**Goal:** Consume Kafka messages, compute technical indicators, and output results.

- [ ] Set up Spark project structure (`spark/`)
- [ ] Create `requirements.txt` (pyspark, kafka dependencies)
- [ ] Write Spark Structured Streaming job to read from `stock-prices` topic
- [ ] Implement technical indicator calculations:
  - [ ] SMA (20-period, 50-period)
  - [ ] EMA (12-period, 26-period)
  - [ ] RSI (14-period)
  - [ ] MACD (MACD line, Signal line, Histogram)
  - [ ] Bollinger Bands (20-period, 2 std dev)
- [ ] Test Spark job locally — verify indicator calculations are correct
- [ ] Output processed data to console for validation

## Phase 4: Snowflake Integration
**Goal:** Store raw and processed data in Snowflake with a star schema.

- [ ] Create Snowflake database and schemas (RAW, STAGING, MARTS)
- [ ] Create RAW table: `STOCK_PRICES_STREAMING`
- [ ] Create MARTS tables: `DIM_STOCKS`, `FCT_STOCK_PRICES`, `FCT_TECHNICAL_INDICATORS`
- [ ] Add Snowflake connector to Spark dependencies (snowflake-connector-python, snowflake-spark-connector)
- [ ] Update Spark job to write raw data to `RAW.STOCK_PRICES_STREAMING`
- [ ] Create SQL transformations from RAW to STAGING to MARTS
- [ ] Test end-to-end: Producer → Kafka → Spark → Snowflake
- [ ] Verify data in Snowflake tables

## Phase 5: Streamlit Dashboard
**Goal:** Build an interactive dashboard that visualizes stock data and indicators.

- [ ] Set up Streamlit project structure (`dashboard/`)
- [ ] Create `requirements.txt` (streamlit, snowflake-connector-python, plotly, pandas)
- [ ] Connect Streamlit to Snowflake
- [ ] Build dashboard pages:
  - [ ] Stock price charts (candlestick/line) with SMA and Bollinger Bands overlay
  - [ ] RSI chart with overbought/oversold zones
  - [ ] MACD chart with signal line and histogram
  - [ ] Stock selector dropdown (AAPL, GOOGL, MSFT, TSLA, AMZN)
  - [ ] Time range selector
- [ ] Test dashboard locally with Snowflake data
- [ ] Deploy to Streamlit Cloud

## Phase 6: AWS Deployment
**Goal:** Deploy the pipeline to EC2 for 24/7 operation.

- [ ] Launch EC2 t3.medium instance (Ubuntu 22.04)
- [ ] Install Docker, Python, and Spark on EC2
- [ ] Copy project files to EC2
- [ ] Start Kafka/Zookeeper with Docker Compose
- [ ] Create systemd service for the Python producer (auto-restart on failure)
- [ ] Create systemd service for the Spark streaming job (auto-restart on failure)
- [ ] Configure security groups (restrict ports, allow SSH)
- [ ] Add logging and health check monitoring
- [ ] Verify full pipeline runs end-to-end on EC2

## Phase 7: Polish & Monitoring
**Goal:** Harden the pipeline for production reliability.

- [ ] Add `.gitignore` for `.env`, `__pycache__`, `.venv`, etc.
- [ ] Add error alerting (email or Slack on failures)
- [ ] Add Kafka consumer lag monitoring
- [ ] Add data freshness checks in Snowflake
- [ ] Update `README.md` with setup instructions and architecture diagram
- [ ] Final end-to-end validation
