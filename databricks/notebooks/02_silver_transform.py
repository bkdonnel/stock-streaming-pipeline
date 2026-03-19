# Databricks notebook source
# Reads raw OHLCV records from the Bronze Delta table, cleans and validates them,
# deduplicates by (symbol, date), and writes to the Silver Delta table.
# Uses Write-Audit-Publish: transforms are staged, audited, then published to the live table.

# COMMAND ----------

import os
import sys
import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, current_timestamp, row_number
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# COMMAND ----------

BRONZE_TABLE  = "bootcamp_students.bd_bronze.stock_prices"
SILVER_TABLE  = "bootcamp_students.bd_silver.stock_prices"
STAGING_TABLE = "bootcamp_students.bd_silver.stock_prices_staging"
EXPECTED_SYMBOLS = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"]

# COMMAND ----------

# Resolve path to audit.py in databricks/ — handles both interactive Repos and Job runs.
def _find_databricks_dir() -> str:
    candidates = []
    cwd = os.getcwd()
    candidates.append(os.path.normpath(os.path.join(cwd, "..")))
    candidates.append(os.path.join(cwd, "databricks"))
    candidates.append(cwd)
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
        if os.path.isfile(os.path.join(path, "audit.py")):
            return path
    raise RuntimeError(f"Could not find audit.py. Searched: {candidates}")

_databricks_dir = _find_databricks_dir()
if _databricks_dir not in sys.path:
    sys.path.insert(0, _databricks_dir)

from audit import (
    write_audit_publish, check_row_count, check_no_nulls,
    check_symbols_complete, check_column_range, AuditResult,
)

# COMMAND ----------

def cast_date_column(df: DataFrame) -> DataFrame:
    return df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))


def drop_invalid_rows(df: DataFrame) -> DataFrame:
    is_valid_close = col("close").isNotNull() & (col("close") > 0)
    is_valid_symbol = col("symbol").isNotNull()
    is_valid_date = col("date").isNotNull()
    return df.filter(is_valid_close & is_valid_symbol & is_valid_date)


def deduplicate(df: DataFrame) -> DataFrame:
    window = Window.partitionBy("symbol", "date").orderBy(col("ingested_at").desc())
    return (
        df.withColumn("_row_num", row_number().over(window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )


def add_silver_metadata(df: DataFrame) -> DataFrame:
    return df.withColumn("transformed_at", current_timestamp())


def transform_bronze_to_silver(df: DataFrame) -> DataFrame:
    df = cast_date_column(df)
    df = drop_invalid_rows(df)
    df = deduplicate(df)
    df = add_silver_metadata(df)
    return df

# COMMAND ----------

bronze_df = spark.table(BRONZE_TABLE)
bronze_count = bronze_df.count()
logger.info("Bronze row count: %d", bronze_count)

# COMMAND ----------

silver_df = transform_bronze_to_silver(bronze_df)

skipped = bronze_count - silver_df.count()
if skipped > 0:
    print(f"Dropped {skipped} rows (nulls, zero closes, or duplicates)")

# COMMAND ----------

def silver_audits(df: DataFrame) -> list[AuditResult]:
    return [
        check_row_count(df, min_rows=1),
        check_symbols_complete(df, EXPECTED_SYMBOLS),
        *check_no_nulls(df, ["symbol", "date", "close"]),
        check_column_range(df, "close", min_val=0.01, max_val=1_000_000.0),
    ]

write_audit_publish(spark, silver_df, SILVER_TABLE, STAGING_TABLE, silver_audits)

silver_count = spark.table(SILVER_TABLE).count()
logger.info("Silver row count: %d", silver_count)
print(f"Bronze rows: {bronze_count} | Silver rows: {silver_count}")

# COMMAND ----------

# Verify — show Silver table
display(
    spark.table(SILVER_TABLE)
    .orderBy("symbol", "date")
)
