# Stock Streaming Analytics Pipeline

## Project Overview
Real-time stock market data pipeline: Stock APIs -> Python Producer -> Kafka -> Spark Streaming -> Snowflake -> Streamlit Dashboard.

Tracked stocks: AAPL, GOOGL, MSFT, TSLA, AMZN.

## Current Status
- **Phase 1 (Local Kafka Setup):** Complete
- **Phase 2-7:** Not started

See `IMPLEMENTATION.md` for the full phase-by-phase plan and `ARCHITECTURE.md` for system design.

## Infrastructure

### Docker Services (`docker-compose.yml`)
- **Zookeeper** - `confluentinc/cp-zookeeper:7.5.0` on port 2181
- **Kafka** - `confluentinc/cp-kafka:7.5.0` on port 9092 (host), 29092 (internal)
- **Kafka UI** - `provectuslabs/kafka-ui:latest` on port 8080
- All on a shared `kafka-network` bridge network

### Starting the stack
```bash
docker compose up -d
```

### Kafka Topics
- `stock-prices` - 3 partitions, replication factor 1
  - Created manually: `docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic stock-prices --partitions 3 --replication-factor 1`

### Key URLs
- Kafka UI: http://localhost:8080
- Kafka broker: localhost:9092 (binary protocol, not accessible via browser)

## Issues Resolved
- **Kafka crash on startup:** The original `docker-compose.yml` referenced `ConfluentMetricsReporter` (`KAFKA_METRIC_REPORTERS`, `KAFKA_CONFLUENT_METRICS_*` env vars) which is not included in the base `cp-kafka` image. Removed those settings to fix.

## Code Style
- Python: use functional/declarative style, type hints, Pydantic models
- Use lowercase with underscores for files/directories
- No emojis in Python files
- See `AGENTS.md` for full coding conventions

## Project Structure
```
docker-compose.yml      # Kafka + Zookeeper + Kafka UI
kafka/producer/         # Producer code (WIP)
  config.py
  utils.py
requirements.txt        # All Python dependencies
ARCHITECTURE.md         # System design and data model
IMPLEMENTATION.md       # Phase-by-phase implementation plan
AGENTS.md               # Coding style and conventions
```
