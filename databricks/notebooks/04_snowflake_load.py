# Databricks notebook source
# Reads Gold Delta table and writes stock prices and technical indicators
# to Snowflake DATAEXPERT_STUDENT.BRYAN for serving to the dashboard.

# COMMAND ----------

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# COMMAND ----------

GOLD_TABLE = "bootcamp_students.bd_gold.stock_indicators"

SF_ACCOUNT   = dbutils.secrets.get(scope="stock-pipeline", key="snowflake-account")
SF_USER      = dbutils.secrets.get(scope="stock-pipeline", key="snowflake-user")
SF_PRIVATE_KEY = dbutils.secrets.get(scope="stock-pipeline", key="snowflake-private-key")
SF_DATABASE  = dbutils.secrets.get(scope="stock-pipeline", key="snowflake-database")
SF_SCHEMA    = dbutils.secrets.get(scope="stock-pipeline", key="snowflake-schema")
SF_WAREHOUSE = "COMPUTE_WH"

# COMMAND ----------

# Strip PEM headers and whitespace — Snowflake connector expects raw base64 key content
import re

sf_private_key_clean = re.sub(
    r"-----BEGIN [A-Z ]+-----|-----END [A-Z ]+-----|[\r\n\s]",
    "",
    SF_PRIVATE_KEY,
)

# COMMAND ----------

snowflake_options = {
    "sfURL":        f"{SF_ACCOUNT}.snowflakecomputing.com",
    "sfUser":       SF_USER,
    "pem_private_key": sf_private_key_clean,
    "sfDatabase":   SF_DATABASE,
    "sfSchema":     SF_SCHEMA,
    "sfWarehouse":  SF_WAREHOUSE,
}

# COMMAND ----------

gold_df = spark.table(GOLD_TABLE)
logger.info("Gold row count: %d", gold_df.count())

# COMMAND ----------

# FCT_STOCK_PRICES — OHLCV price data
price_cols = ["symbol", "date", "open", "high", "low", "close", "volume", "vwap", "source", "ingested_at", "transformed_at"]
price_df = gold_df.select(price_cols)

price_df.write \
    .format("net.snowflake.spark.snowflake") \
    .options(**snowflake_options) \
    .option("dbtable", "FCT_STOCK_PRICES") \
    .mode("overwrite") \
    .save()

logger.info("FCT_STOCK_PRICES written: %d rows", price_df.count())

# COMMAND ----------

# FCT_TECHNICAL_INDICATORS — all indicator columns
indicator_cols = [
    "symbol", "date",
    "sma_20", "sma_50",
    "ema_12", "ema_26",
    "rsi_14",
    "macd_line", "macd_signal", "macd_histogram",
    "bb_upper", "bb_middle", "bb_lower",
]
indicators_df = gold_df.select(indicator_cols)

indicators_df.write \
    .format("net.snowflake.spark.snowflake") \
    .options(**snowflake_options) \
    .option("dbtable", "FCT_TECHNICAL_INDICATORS") \
    .mode("overwrite") \
    .save()

logger.info("FCT_TECHNICAL_INDICATORS written: %d rows", indicators_df.count())

# COMMAND ----------

# Verify -- read back from Snowflake
verify_df = spark.read \
    .format("net.snowflake.spark.snowflake") \
    .options(**snowflake_options) \
    .option("dbtable", "FCT_TECHNICAL_INDICATORS") \
    .load()

display(verify_df.orderBy("symbol", "date"))
