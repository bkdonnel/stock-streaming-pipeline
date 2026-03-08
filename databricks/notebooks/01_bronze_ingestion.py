# Databricks notebook source
# Fetches daily OHLCV data from Polygon.io for each tracked stock symbol
# and writes raw records to the Bronze Delta table.
# Run after market close (4:30pm ET).

# COMMAND ----------

import sys
import requests
import logging
from datetime import date, timedelta
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DateType, DoubleType, LongType,
)
from pyspark.sql.functions import current_timestamp

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# COMMAND ----------

# Widget: override date for backfills; defaults to yesterday
dbutils.widgets.text("run_date", "", "Run Date (YYYY-MM-DD, blank = yesterday)")

_widget_date = dbutils.widgets.get("run_date").strip()
if _widget_date:
    RUN_DATE = _widget_date
else:
    RUN_DATE = (date.today() - timedelta(days=1)).isoformat()

print(f"Ingesting data for: {RUN_DATE}")

# COMMAND ----------

# Load config — reads Polygon API key from Databricks Secrets on cluster
sys.path.insert(0, "/Workspace/Repos")

api_key = dbutils.secrets.get(scope="stock-pipeline", key="polygon-api-key")
if not api_key:
    raise ValueError("Polygon API key is empty. Check Databricks Secrets (scope: stock-pipeline, key: polygon-api-key).")

STOCK_SYMBOLS = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"]
BRONZE_TABLE = "bootcamp_students.bd_bronze.stock_prices"

# COMMAND ----------

# Bronze schema — raw, as returned by Polygon; no transformations
BRONZE_SCHEMA = StructType([
    StructField("symbol", StringType(), nullable=False),
    StructField("date", StringType(), nullable=False),
    StructField("open", DoubleType(), nullable=True),
    StructField("high", DoubleType(), nullable=True),
    StructField("low", DoubleType(), nullable=True),
    StructField("close", DoubleType(), nullable=True),
    StructField("volume", LongType(), nullable=True),
    StructField("vwap", DoubleType(), nullable=True),
    StructField("source", StringType(), nullable=False),
])

# COMMAND ----------

def fetch_daily_ohlcv(symbol: str, api_key: str, run_date: str) -> Optional[dict]:
    """
    Fetch daily OHLCV data from Polygon /v1/open-close/{symbol}/{date}.

    Returns a dict on success, None on failure.
    """
    url = f"https://api.polygon.io/v1/open-close/{symbol}/{run_date}"
    params = {"adjusted": "true", "apiKey": api_key}

    try:
        response = requests.get(url, params=params, timeout=10)

        if response.status_code in (403, 404):
            logger.warning("No data for %s on %s (market closed or date unavailable)", symbol, run_date)
            return None

        response.raise_for_status()
        data = response.json()

        return {
            "symbol": symbol,
            "date": run_date,
            "open": float(data.get("open") or 0),
            "high": float(data.get("high") or 0),
            "low": float(data.get("low") or 0),
            "close": float(data.get("close") or 0),
            "volume": int(data.get("volume") or 0),
            "vwap": float(data.get("vwap") or 0),
            "source": "polygon",
        }

    except requests.exceptions.RequestException as e:
        logger.error("Request failed for %s: %s", symbol, e)
        return None
    except (KeyError, ValueError, TypeError) as e:
        logger.error("Failed to parse response for %s: %s", symbol, e)
        return None

# COMMAND ----------

# Fetch all symbols
records = []
failed = []

for symbol in STOCK_SYMBOLS:
    record = fetch_daily_ohlcv(symbol, api_key, RUN_DATE)
    if record:
        records.append(record)
        logger.info("Fetched %s: close=%.2f volume=%d", symbol, record["close"], record["volume"])
    else:
        failed.append(symbol)
        logger.warning("Skipped %s — no data returned", symbol)

print(f"\nFetched: {len(records)} symbols | Failed/skipped: {len(failed)}")
if failed:
    print(f"Skipped symbols: {failed}")

if not records:
    logger.warning("No data fetched for any symbol on %s — market likely closed. Exiting.", RUN_DATE)
    dbutils.notebook.exit("No data — market closed")

# COMMAND ----------

# Convert to Spark DataFrame and write to Bronze (append — never overwrite raw data)
df = (
    spark.createDataFrame(records, schema=BRONZE_SCHEMA)
    .withColumn("ingested_at", current_timestamp())
)

df.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)

print(f"Written {df.count()} rows to {BRONZE_TABLE}")

# COMMAND ----------

# Verify — show today's records
display(
    spark.table(BRONZE_TABLE)
    .filter(f"date = '{RUN_DATE}'")
    .orderBy("symbol")
)
