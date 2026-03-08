# Databricks notebook source
# Reads cleaned records from the Silver Delta table, computes technical indicators
# per symbol using applyInPandas, and writes enriched data to the Gold Delta table.

# COMMAND ----------

import sys
import logging

from pyspark.sql.types import (
    StructType, StructField,
    StringType, DateType, DoubleType, LongType, TimestampType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# COMMAND ----------

SILVER_TABLE = "bootcamp_students.bd_silver.stock_prices"
GOLD_TABLE = "bootcamp_students.bd_gold.stock_indicators"

# COMMAND ----------

# Resolve path to indicators.py — tries multiple strategies to handle both
# interactive Repos execution and Job runs with Git source.
import os

def _find_indicators_dir() -> str:
    """Return the directory containing indicators.py."""
    candidates = []

    # Strategy 1: cwd is repo root when running as a Job with Git source
    cwd = os.getcwd()
    candidates.append(os.path.join(cwd, "databricks"))
    candidates.append(cwd)

    # Strategy 2: dynamic resolution via notebook context (interactive Repos)
    try:
        notebook_path = (
            dbutils.notebook.entry_point
            .getDbutils().notebook().getContext()
            .notebookPath().get()
        )
        parts = notebook_path.lstrip("/").split("/")
        repo_root = "/Workspace/" + "/".join(parts[:3])
        candidates.append(repo_root + "/databricks")
    except Exception:
        pass

    for path in candidates:
        if os.path.isfile(os.path.join(path, "indicators.py")):
            return path

    raise RuntimeError(f"Could not find indicators.py. Searched: {candidates}")

indicators_dir = _find_indicators_dir()
if indicators_dir not in sys.path:
    sys.path.insert(0, indicators_dir)

from indicators import add_indicators

# COMMAND ----------

# Output schema: all Silver columns plus every indicator column.
# applyInPandas requires an explicit schema that matches the pandas return value exactly.
GOLD_SCHEMA = StructType([
    StructField("symbol", StringType(), nullable=False),
    StructField("date", DateType(), nullable=False),
    StructField("open", DoubleType(), nullable=True),
    StructField("high", DoubleType(), nullable=True),
    StructField("low", DoubleType(), nullable=True),
    StructField("close", DoubleType(), nullable=True),
    StructField("volume", LongType(), nullable=True),
    StructField("vwap", DoubleType(), nullable=True),
    StructField("source", StringType(), nullable=True),
    StructField("ingested_at", TimestampType(), nullable=True),
    StructField("transformed_at", TimestampType(), nullable=True),
    StructField("sma_20", DoubleType(), nullable=True),
    StructField("sma_50", DoubleType(), nullable=True),
    StructField("ema_12", DoubleType(), nullable=True),
    StructField("ema_26", DoubleType(), nullable=True),
    StructField("rsi_14", DoubleType(), nullable=True),
    StructField("macd_line", DoubleType(), nullable=True),
    StructField("macd_signal", DoubleType(), nullable=True),
    StructField("macd_histogram", DoubleType(), nullable=True),
    StructField("bb_upper", DoubleType(), nullable=True),
    StructField("bb_middle", DoubleType(), nullable=True),
    StructField("bb_lower", DoubleType(), nullable=True),
])

# COMMAND ----------

silver_df = spark.table(SILVER_TABLE)
silver_count = silver_df.count()
logger.info("Silver row count: %d", silver_count)

# COMMAND ----------

# Apply indicators per symbol.
# applyInPandas groups by symbol, passes each group as a pandas DataFrame,
# and reassembles the results into a Spark DataFrame using GOLD_SCHEMA.
gold_df = (
    silver_df
    .groupBy("symbol")
    .applyInPandas(add_indicators, schema=GOLD_SCHEMA)
)

gold_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE)

gold_count = gold_df.count()
logger.info("Gold row count: %d", gold_count)
print(f"Silver rows: {silver_count} | Gold rows: {gold_count}")

# COMMAND ----------

# Verify -- show a sample of indicator values
display(
    spark.table(GOLD_TABLE)
    .select("symbol", "date", "close", "sma_20", "ema_12", "rsi_14", "macd_line", "bb_upper", "bb_lower")
    .orderBy("symbol", "date")
)
