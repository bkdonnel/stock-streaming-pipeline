# Databricks notebook source
# Reads Gold Delta table and writes stock prices and technical indicators
# to Snowflake DATAEXPERT_STUDENT.BRYAN for serving to the dashboard.
# Uses snowflake-connector-python + cryptography for RSA key auth (no Maven connector needed).

# COMMAND ----------

# MAGIC %pip install snowflake-connector-python cryptography

# COMMAND ----------

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# COMMAND ----------

GOLD_TABLE = "bootcamp_students.bd_gold.stock_indicators"

SF_ACCOUNT     = dbutils.secrets.get(scope="stock-pipeline", key="snowflake-account")
SF_USER        = dbutils.secrets.get(scope="stock-pipeline", key="snowflake-user")
SF_PRIVATE_KEY = dbutils.secrets.get(scope="stock-pipeline", key="snowflake-private-key")
SF_DATABASE    = dbutils.secrets.get(scope="stock-pipeline", key="snowflake-database")
SF_SCHEMA      = dbutils.secrets.get(scope="stock-pipeline", key="snowflake-schema")
SF_WAREHOUSE   = "COMPUTE_WH"

# COMMAND ----------

# Parse the PEM private key into DER bytes that snowflake-connector-python expects
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key,
    Encoding,
    PrivateFormat,
    NoEncryption,
)

# Wrap with PEM headers if the secret was stored as raw base64 (no headers)
pem_key = SF_PRIVATE_KEY.strip()
if not pem_key.startswith("-----"):
    pem_key = f"-----BEGIN PRIVATE KEY-----\n{pem_key}\n-----END PRIVATE KEY-----"

private_key_obj = load_pem_private_key(
    pem_key.encode("utf-8"),
    password=None,
    backend=default_backend(),
)

private_key_der = private_key_obj.private_bytes(
    encoding=Encoding.DER,
    format=PrivateFormat.PKCS8,
    encryption_algorithm=NoEncryption(),
)

# COMMAND ----------

import snowflake.connector

conn = snowflake.connector.connect(
    account=SF_ACCOUNT,
    user=SF_USER,
    private_key=private_key_der,
    database=SF_DATABASE,
    schema=SF_SCHEMA,
    warehouse=SF_WAREHOUSE,
)
logger.info("Snowflake connection established")

# COMMAND ----------

gold_df = spark.table(GOLD_TABLE)
logger.info("Gold row count: %d", gold_df.count())

# COMMAND ----------

def write_to_snowflake(conn, df, table_name: str, cols: list[str]) -> None:
    """Write a Spark DataFrame subset to a Snowflake table via pandas + connector."""
    pdf = df.select(cols).toPandas()
    # Convert date column to string so Snowflake accepts it without type issues
    if "date" in pdf.columns:
        pdf["date"] = pdf["date"].astype(str)

    cursor = conn.cursor()
    try:
        col_defs = ", ".join(f"{c.upper()} VARCHAR" if c in ("symbol", "source") else f"{c.upper()} VARIANT" for c in cols)
        cursor.execute(f"CREATE OR REPLACE TABLE {table_name} ({col_defs})")

        placeholders = ", ".join(["%s"] * len(cols))
        insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
        rows = [tuple(str(v) if v is not None else None for v in row) for row in pdf.itertuples(index=False)]
        cursor.executemany(insert_sql, rows)
        logger.info("%s written: %d rows", table_name, len(rows))
    finally:
        cursor.close()

# COMMAND ----------

# FCT_STOCK_PRICES — OHLCV price data
price_cols = ["symbol", "date", "open", "high", "low", "close", "volume", "vwap", "source", "ingested_at", "transformed_at"]
write_to_snowflake(conn, gold_df, "FCT_STOCK_PRICES", price_cols)

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
write_to_snowflake(conn, gold_df, "FCT_TECHNICAL_INDICATORS", indicator_cols)

# COMMAND ----------

# Verify -- read back from Snowflake
verify_cursor = conn.cursor()
try:
    verify_cursor.execute("SELECT * FROM FCT_TECHNICAL_INDICATORS ORDER BY SYMBOL, DATE")
    rows = verify_cursor.fetchall()
    cols = [desc[0] for desc in verify_cursor.description]
    import pandas as pd
    verify_pdf = pd.DataFrame(rows, columns=cols)
    display(verify_pdf)
    logger.info("Verification: %d rows read back from FCT_TECHNICAL_INDICATORS", len(verify_pdf))
finally:
    verify_cursor.close()

# COMMAND ----------

conn.close()
logger.info("Snowflake connection closed")
