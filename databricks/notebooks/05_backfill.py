# Databricks notebook source
# Backfill notebook — fetches historical OHLCV data for a date range using
# Polygon's aggregates endpoint (/v2/aggs), which supports historical data
# on the free tier. Writes directly to Bronze, then runs Silver/Gold/Snowflake.

# COMMAND ----------

import requests
import logging
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType,
)
from pyspark.sql.functions import current_timestamp, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# COMMAND ----------

dbutils.widgets.text("start_date", "2025-12-01", "Start Date (YYYY-MM-DD)")
dbutils.widgets.text("end_date",   "2026-03-14", "End Date (YYYY-MM-DD)")

START_DATE = dbutils.widgets.get("start_date").strip()
END_DATE   = dbutils.widgets.get("end_date").strip()
SYMBOLS    = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"]
BRONZE_TABLE = "bootcamp_students.bd_bronze.stock_prices"

api_key = dbutils.secrets.get(scope="stock-pipeline", key="polygon-api-key")
print(f"Backfill range: {START_DATE} to {END_DATE}")

# COMMAND ----------

BRONZE_SCHEMA = StructType([
    StructField("symbol",  StringType(),  nullable=False),
    StructField("date",    StringType(),  nullable=False),
    StructField("open",    DoubleType(),  nullable=True),
    StructField("high",    DoubleType(),  nullable=True),
    StructField("low",     DoubleType(),  nullable=True),
    StructField("close",   DoubleType(),  nullable=True),
    StructField("volume",  LongType(),    nullable=True),
    StructField("vwap",    DoubleType(),  nullable=True),
    StructField("source",  StringType(),  nullable=False),
])

# COMMAND ----------

def fetch_aggs(symbol: str, api_key: str, start: str, end: str) -> list[dict]:
    """
    Fetch daily OHLCV bars using Polygon /v2/aggs (supports historical data on free tier).
    Returns a list of daily records for the symbol.
    """
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start}/{end}"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 500,
        "apiKey": api_key,
    }

    try:
        response = requests.get(url, params=params, timeout=15)
        if response.status_code in (403, 404):
            logger.warning("No data for %s (%s to %s)", symbol, start, end)
            return []
        response.raise_for_status()
        data = response.json()

        records = []
        for bar in data.get("results", []):
            # Polygon returns timestamps in milliseconds UTC — convert to date string
            ts_ms = bar.get("t", 0)
            bar_date = date.fromtimestamp(ts_ms / 1000).isoformat()
            records.append({
                "symbol": symbol,
                "date":   bar_date,
                "open":   float(bar.get("o") or 0),
                "high":   float(bar.get("h") or 0),
                "low":    float(bar.get("l") or 0),
                "close":  float(bar.get("c") or 0),
                "volume": int(bar.get("v") or 0),
                "vwap":   float(bar.get("vw") or 0),
                "source": "polygon",
            })
        logger.info("Fetched %d bars for %s", len(records), symbol)
        return records

    except requests.exceptions.RequestException as e:
        logger.error("Request failed for %s: %s", symbol, e)
        return []

# COMMAND ----------

# Fetch all symbols across the full date range in one pass
all_records = []
for symbol in SYMBOLS:
    records = fetch_aggs(symbol, api_key, START_DATE, END_DATE)
    all_records.extend(records)

print(f"Total records fetched: {len(all_records)}")

if not all_records:
    raise RuntimeError("No data fetched. Check API key and date range.")

# COMMAND ----------

# One-time deduplication of existing Bronze rows before writing new data
if spark.catalog.tableExists(BRONZE_TABLE):
    existing = spark.table(BRONZE_TABLE)
    window = Window.partitionBy("symbol", "date").orderBy(existing["ingested_at"].desc())
    deduped = (
        existing
        .withColumn("_rn", row_number().over(window))
        .filter("_rn = 1")
        .drop("_rn")
    )
    deduped.write.format("delta").mode("overwrite").option("overwriteSchema", "false").saveAsTable(BRONZE_TABLE)
    print("Bronze deduplicated.")

# Merge new records into Bronze (insert-only on new symbol+date pairs)
df = (
    spark.createDataFrame(all_records, schema=BRONZE_SCHEMA)
    .withColumn("ingested_at", current_timestamp())
)

if spark.catalog.tableExists(BRONZE_TABLE):
    DeltaTable.forName(spark, BRONZE_TABLE).alias("t").merge(
        df.alias("s"),
        "t.symbol = s.symbol AND t.date = s.date"
    ).whenNotMatchedInsertAll().execute()
else:
    df.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)

print(f"Written {df.count()} rows to {BRONZE_TABLE}")

# COMMAND ----------

# Run Silver, Gold, and Snowflake load once to process the full backfilled history
dbutils.notebook.run("02_silver_transform", timeout_seconds=300)
print("Silver transform complete.")

# COMMAND ----------

dbutils.notebook.run("03_gold_indicators", timeout_seconds=300)
print("Gold indicators complete.")

# COMMAND ----------

dbutils.notebook.run("04_snowflake_load", timeout_seconds=600)
print("Snowflake load complete.")

# COMMAND ----------

print(f"Backfill complete: {len(all_records)} records loaded for {len(SYMBOLS)} symbols.")
