# Phase 3: Spark Streaming Processor — Build Guide

## Goal
Consume stock price messages from the `stock-prices` Kafka topic using Spark Structured Streaming, compute technical indicators, and output results to the console for validation.

## Prerequisites
- Docker stack running (`docker compose up -d`)
- Producer has published messages to `stock-prices` (run `python kafka/producer/producer.py`)
- Java 11+ installed (required by Spark)
- Python dependencies installed (`pip install pyspark==3.5.0`)

### Verify Java
```bash
java -version
```
If not installed, use `brew install openjdk@11` (macOS) or `sudo apt install openjdk-11-jdk` (Ubuntu).

## Project Structure
```
spark/
  __init__.py           # Module init
  config.py             # Spark and Kafka config
  indicators.py         # Technical indicator calculations
  processor.py          # Main Spark Structured Streaming job
```

## Input Message Schema
Messages on the `stock-prices` topic have this JSON structure (produced by `kafka/producer/producer.py`):
```json
{
  "symbol": "AAPL",
  "timestamp": "2026-02-13T13:00:00",
  "open": 255.50,
  "high": 256.80,
  "low": 254.20,
  "close": 255.78,
  "volume": 56291457,
  "vwap": 255.60,
  "source": "polygon"
}
```

**Key:** The message key is the stock symbol encoded as UTF-8 (e.g. `AAPL`).

---

## Step 1: Create `spark/config.py`

Define configuration for Spark and Kafka connection settings.

```python
import os
from dotenv import load_dotenv

load_dotenv()

class SparkConfig:
    """Configuration for Spark Streaming processor."""

    # Kafka settings
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-prices")

    # Spark settings
    APP_NAME = "StockStreamProcessor"
    PROCESSING_INTERVAL = "60 seconds"

    # Indicator periods
    SMA_SHORT_PERIOD = 20
    SMA_LONG_PERIOD = 50
    EMA_SHORT_PERIOD = 12
    EMA_LONG_PERIOD = 26
    RSI_PERIOD = 14
    MACD_SIGNAL_PERIOD = 9
    BOLLINGER_PERIOD = 20
    BOLLINGER_STD_DEV = 2
```

## Step 2: Create `spark/indicators.py`

Implement technical indicator calculations. These functions operate on pandas DataFrames (one per stock symbol) since windowed calculations are easier in pandas than in Spark SQL.

### SMA (Simple Moving Average)
Average of the last N closing prices.
```python
def calculate_sma(df, period, column="close"):
    return df[column].rolling(window=period, min_periods=1).mean()
```

### EMA (Exponential Moving Average)
Weighted average giving more importance to recent prices.
```python
def calculate_ema(df, period, column="close"):
    return df[column].ewm(span=period, adjust=False).mean()
```

### RSI (Relative Strength Index)
Momentum oscillator measuring speed and magnitude of price changes. Range 0-100; below 30 = oversold, above 70 = overbought.
```python
def calculate_rsi(df, period=14, column="close"):
    delta = df[column].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period, min_periods=1).mean()
    avg_loss = loss.rolling(window=period, min_periods=1).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))
```

### MACD (Moving Average Convergence Divergence)
Trend-following momentum indicator. Returns MACD line, signal line, and histogram.
```python
def calculate_macd(df, fast=12, slow=26, signal=9, column="close"):
    ema_fast = df[column].ewm(span=fast, adjust=False).mean()
    ema_slow = df[column].ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram
```

### Bollinger Bands
Volatility bands placed above and below a moving average. Default: 20-period SMA with 2 standard deviations.
```python
def calculate_bollinger_bands(df, period=20, std_dev=2, column="close"):
    sma = df[column].rolling(window=period, min_periods=1).mean()
    rolling_std = df[column].rolling(window=period, min_periods=1).std()
    upper_band = sma + (rolling_std * std_dev)
    lower_band = sma - (rolling_std * std_dev)
    return upper_band, sma, lower_band
```

### Putting It Together
Write an `add_indicators(df)` function that takes a pandas DataFrame sorted by timestamp (for a single symbol) and adds all indicator columns:
- `sma_20`, `sma_50`
- `ema_12`, `ema_26`
- `rsi_14`
- `macd_line`, `macd_signal`, `macd_histogram`
- `bb_upper`, `bb_middle`, `bb_lower`

## Step 3: Create `spark/processor.py`

This is the main Spark Structured Streaming job.

### Part 1: Create SparkSession
```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName(SparkConfig.APP_NAME)
    .master("local[*]")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
```

### Part 2: Read from Kafka
```python
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", SparkConfig.KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", SparkConfig.KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)
```

### Part 3: Parse the JSON Messages
Define a schema matching the producer's output, then parse:
```python
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)
from pyspark.sql.functions import from_json, col

schema = StructType([
    StructField("symbol", StringType()),
    StructField("timestamp", StringType()),
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("volume", LongType()),
    StructField("vwap", DoubleType()),
    StructField("source", StringType()),
])

parsed = (
    raw_stream
    .selectExpr("CAST(key AS STRING) AS symbol_key",
                "CAST(value AS STRING) AS json_value")
    .select(from_json(col("json_value"), schema).alias("data"))
    .select("data.*")
)
```

### Part 4: Apply Indicators with `foreachBatch`
Use `foreachBatch` to process each micro-batch. Convert to pandas, group by symbol, apply indicators, then print or write results.

```python
def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    pdf = batch_df.toPandas()

    results = []
    for symbol, group in pdf.groupby("symbol"):
        group = group.sort_values("timestamp")
        group = add_indicators(group)
        results.append(group)

    if results:
        import pandas as pd
        final = pd.concat(results)
        print(f"\n--- Batch {batch_id} ---")
        print(final.to_string(index=False))
```

### Part 5: Start the Stream
```python
query = (
    parsed.writeStream
    .foreachBatch(process_batch)
    .trigger(processingTime=SparkConfig.PROCESSING_INTERVAL)
    .option("checkpointLocation", "/tmp/spark-checkpoints/stock-processor")
    .start()
)

query.awaitTermination()
```

### Part 6: Entry Point
```python
if __name__ == "__main__":
    # Build SparkSession, read stream, start query
    # Wrap in try/finally to call spark.stop() on exit
```

## Step 4: Add `spark/__init__.py`
Create an empty `__init__.py` in the `spark/` directory.

---

## Testing

### 1. Install dependencies
```bash
pip install pyspark==3.5.0 pandas numpy
```

### 2. Make sure Kafka has messages
Either run the producer (`python kafka/producer/producer.py`) or check Kafka UI at http://localhost:8080 to confirm messages exist on `stock-prices`.

### 3. Run the Spark job
```bash
python spark/processor.py
```

### 4. What to expect
- Spark will read all existing messages from `stock-prices` (since `startingOffsets=earliest`)
- The first batch will print a table with OHLCV data plus all indicator columns
- With only a few data points, indicators like SMA-50 and RSI will have limited accuracy — this is expected
- New batches will appear every 60 seconds if the producer is running simultaneously

### 5. Troubleshooting
- **`ClassNotFoundException` for Kafka**: Make sure `spark.jars.packages` includes the correct Kafka connector JAR (`spark-sql-kafka-0-10_2.12:3.5.0`)
- **Java not found**: Spark requires Java 11+. Set `JAVA_HOME` if needed.
- **No output**: Check that `stock-prices` has messages in Kafka UI. Also make sure `startingOffsets` is set to `earliest`.
- **Checkpoint errors on re-run**: Delete `/tmp/spark-checkpoints/stock-processor` and restart.

---

## What's Next
Once the Spark processor is working and outputting correct indicators to the console, Phase 4 will add Snowflake integration to persist the results.
