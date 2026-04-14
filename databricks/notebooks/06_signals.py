# Databricks notebook source
# Reads enriched records from the Gold Delta table, computes rule-based trade signals
# per symbol, writes to bd_gold.stock_signals via Write-Audit-Publish, and loads
# FCT_SIGNALS to Snowflake for dashboard consumption.

# COMMAND ----------

# MAGIC %pip install snowflake-connector-python cryptography

# COMMAND ----------

import logging
import sys
import os

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DateType, DoubleType, IntegerType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# COMMAND ----------

GOLD_TABLE      = "bootcamp_students.bd_gold.stock_indicators"
SIGNALS_TABLE   = "bootcamp_students.bd_gold.stock_signals"
STAGING_TABLE   = "bootcamp_students.bd_gold.stock_signals_staging"
EXPECTED_SYMBOLS = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"]

SF_ACCOUNT     = dbutils.secrets.get(scope="stock-pipeline", key="snowflake-account")
SF_USER        = dbutils.secrets.get(scope="stock-pipeline", key="snowflake-user")
SF_PRIVATE_KEY = dbutils.secrets.get(scope="stock-pipeline", key="snowflake-private-key")
SF_DATABASE    = dbutils.secrets.get(scope="stock-pipeline", key="snowflake-database")
SF_SCHEMA      = dbutils.secrets.get(scope="stock-pipeline", key="snowflake-schema")
SF_WAREHOUSE   = "COMPUTE_WH"

# COMMAND ----------

# Resolve path to audit.py — same multi-strategy approach used by 03_gold_indicators.py
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

databricks_dir = _find_databricks_dir()
if databricks_dir not in sys.path:
    sys.path.insert(0, databricks_dir)

from audit import write_audit_publish, check_row_count, check_symbols_complete, check_column_range, AuditResult

# COMMAND ----------

SIGNALS_SCHEMA = StructType([
    StructField("symbol",           StringType(),  nullable=False),
    StructField("date",             DateType(),    nullable=False),
    StructField("close",            DoubleType(),  nullable=True),
    StructField("rsi_14",           DoubleType(),  nullable=True),
    StructField("macd_line",        DoubleType(),  nullable=True),
    StructField("macd_signal_line", DoubleType(),  nullable=True),
    StructField("bb_upper",         DoubleType(),  nullable=True),
    StructField("bb_lower",         DoubleType(),  nullable=True),
    StructField("sma_20",           DoubleType(),  nullable=True),
    StructField("sma_50",           DoubleType(),  nullable=True),
    # Individual signal components
    StructField("signal_rsi",       StringType(),  nullable=False),
    StructField("signal_macd",      StringType(),  nullable=False),
    StructField("signal_bb",        StringType(),  nullable=False),
    StructField("signal_sma_cross", StringType(),  nullable=False),
    # Composite
    StructField("bullish_count",    IntegerType(), nullable=False),
    StructField("bearish_count",    IntegerType(), nullable=False),
    StructField("composite_signal", StringType(),  nullable=False),
    StructField("signal_strength",  DoubleType(),  nullable=False),
])

# COMMAND ----------

def _composite(net: int) -> str:
    if net >= 3:
        return "strong_buy"
    if net >= 1:
        return "buy"
    if net <= -3:
        return "strong_sell"
    if net <= -1:
        return "sell"
    return "neutral"


def compute_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute rule-based trade signals for a single symbol group.

    Signals:
      - RSI: oversold (<30) -> bullish, overbought (>70) -> bearish
      - MACD crossover: macd_line crosses above signal -> bullish, below -> bearish
      - Bollinger Band: close < lower -> bullish, close > upper -> bearish
      - SMA crossover: sma_20 crosses above sma_50 (golden cross) -> bullish,
                       sma_20 crosses below sma_50 (death cross) -> bearish

    Composite: net = bullish_count - bearish_count (range -4 to 4).
    signal_strength = net / 4.0 (normalized to [-1.0, 1.0]).
    """
    df = df.sort_values("date").copy()
    n = len(df)

    # --- RSI signal ---
    rsi = df["rsi_14"]
    signal_rsi = ["neutral"] * n
    for i in range(n):
        if pd.notna(rsi.iloc[i]):
            if rsi.iloc[i] < 30:
                signal_rsi[i] = "bullish"
            elif rsi.iloc[i] > 70:
                signal_rsi[i] = "bearish"
    df["signal_rsi"] = signal_rsi

    # --- MACD crossover signal ---
    # Crossover detected when the sign of (macd_line - macd_signal) flips
    macd_diff = df["macd_line"] - df["macd_signal"]
    signal_macd = ["neutral"] * n
    for i in range(1, n):
        if pd.notna(macd_diff.iloc[i]) and pd.notna(macd_diff.iloc[i - 1]):
            if macd_diff.iloc[i - 1] < 0 and macd_diff.iloc[i] > 0:
                signal_macd[i] = "bullish"
            elif macd_diff.iloc[i - 1] > 0 and macd_diff.iloc[i] < 0:
                signal_macd[i] = "bearish"
    df["signal_macd"] = signal_macd

    # --- Bollinger Band signal ---
    signal_bb = ["neutral"] * n
    for i in range(n):
        c, lo, hi = df["close"].iloc[i], df["bb_lower"].iloc[i], df["bb_upper"].iloc[i]
        if pd.notna(c) and pd.notna(lo) and pd.notna(hi):
            if c < lo:
                signal_bb[i] = "bullish"
            elif c > hi:
                signal_bb[i] = "bearish"
    df["signal_bb"] = signal_bb

    # --- SMA crossover signal (golden / death cross) ---
    sma_diff = df["sma_20"] - df["sma_50"]
    signal_sma = ["neutral"] * n
    for i in range(1, n):
        if pd.notna(sma_diff.iloc[i]) and pd.notna(sma_diff.iloc[i - 1]):
            if sma_diff.iloc[i - 1] < 0 and sma_diff.iloc[i] > 0:
                signal_sma[i] = "bullish"   # golden cross
            elif sma_diff.iloc[i - 1] > 0 and sma_diff.iloc[i] < 0:
                signal_sma[i] = "bearish"   # death cross
    df["signal_sma_cross"] = signal_sma

    # --- Composite ---
    component_cols = ["signal_rsi", "signal_macd", "signal_bb", "signal_sma_cross"]
    df["bullish_count"] = df[component_cols].apply(lambda row: (row == "bullish").sum(), axis=1).astype(int)
    df["bearish_count"] = df[component_cols].apply(lambda row: (row == "bearish").sum(), axis=1).astype(int)
    net = df["bullish_count"] - df["bearish_count"]
    df["composite_signal"] = net.apply(_composite)
    df["signal_strength"] = (net / 4.0).round(4)

    return df[[
        "symbol", "date", "close",
        "rsi_14", "macd_line", "macd_signal",
        "bb_upper", "bb_lower", "sma_20", "sma_50",
        "signal_rsi", "signal_macd", "signal_bb", "signal_sma_cross",
        "bullish_count", "bearish_count", "composite_signal", "signal_strength",
    ]].rename(columns={"macd_signal": "macd_signal_line"})

# COMMAND ----------

gold_df = spark.table(GOLD_TABLE)
gold_count = gold_df.count()
logger.info("Gold row count: %d", gold_count)

# COMMAND ----------

signals_df = (
    gold_df
    .groupBy("symbol")
    .applyInPandas(compute_signals, schema=SIGNALS_SCHEMA)
)

def signals_audits(df: DataFrame) -> list[AuditResult]:
    return [
        check_row_count(df, min_rows=gold_count),
        check_symbols_complete(df, EXPECTED_SYMBOLS),
        check_column_range(df, "signal_strength", min_val=-1.0, max_val=1.0),
    ]

write_audit_publish(spark, signals_df, SIGNALS_TABLE, STAGING_TABLE, signals_audits)

signals_count = spark.table(SIGNALS_TABLE).count()
logger.info("Signals table row count: %d", signals_count)
print(f"Gold rows: {gold_count} | Signals rows: {signals_count}")

# COMMAND ----------

# Parse RSA private key for Snowflake auth
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key, Encoding, PrivateFormat, NoEncryption,
)

pem_key = SF_PRIVATE_KEY.strip()
if not pem_key.startswith("-----"):
    pem_key = f"-----BEGIN PRIVATE KEY-----\n{pem_key}\n-----END PRIVATE KEY-----"

private_key_der = load_pem_private_key(
    pem_key.encode("utf-8"),
    password=None,
    backend=default_backend(),
).private_bytes(encoding=Encoding.DER, format=PrivateFormat.PKCS8, encryption_algorithm=NoEncryption())

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

FCT_SIGNALS_COLS = [
    "symbol", "date",
    "close", "rsi_14", "macd_line", "macd_signal_line",
    "bb_upper", "bb_lower", "sma_20", "sma_50",
    "signal_rsi", "signal_macd", "signal_bb", "signal_sma_cross",
    "bullish_count", "bearish_count", "composite_signal", "signal_strength",
]

VARCHAR_COLS = {"symbol", "date", "signal_rsi", "signal_macd", "signal_bb", "signal_sma_cross", "composite_signal"}
INT_COLS     = {"bullish_count", "bearish_count"}

pdf = spark.table(SIGNALS_TABLE).select(FCT_SIGNALS_COLS).toPandas()
pdf["date"] = pdf["date"].astype(str)

cursor = conn.cursor()
try:
    col_defs = ", ".join(
        f"{c.upper()} VARCHAR"  if c in VARCHAR_COLS else
        f"{c.upper()} INTEGER"  if c in INT_COLS     else
        f"{c.upper()} FLOAT"
        for c in FCT_SIGNALS_COLS
    )
    cursor.execute(f"CREATE OR REPLACE TABLE FCT_SIGNALS ({col_defs})")

    placeholders = ", ".join(["%s"] * len(FCT_SIGNALS_COLS))
    rows = [tuple(str(v) if v is not None else None for v in row) for row in pdf.itertuples(index=False)]
    cursor.executemany(f"INSERT INTO FCT_SIGNALS VALUES ({placeholders})", rows)
    logger.info("FCT_SIGNALS written: %d rows", len(rows))
finally:
    cursor.close()

# COMMAND ----------

# Verify -- read back and display latest signal per symbol
verify_cursor = conn.cursor()
try:
    verify_cursor.execute("""
        SELECT SYMBOL, DATE, CLOSE, RSI_14, COMPOSITE_SIGNAL, SIGNAL_STRENGTH,
               SIGNAL_RSI, SIGNAL_MACD, SIGNAL_BB, SIGNAL_SMA_CROSS
        FROM FCT_SIGNALS
        WHERE DATE = (SELECT MAX(DATE) FROM FCT_SIGNALS)
        ORDER BY SYMBOL
    """)
    rows = verify_cursor.fetchall()
    cols = [desc[0] for desc in verify_cursor.description]
    import pandas as _pd
    display(_pd.DataFrame(rows, columns=cols))
    logger.info("Verification: %d rows for latest date", len(rows))
finally:
    verify_cursor.close()

# COMMAND ----------

conn.close()
logger.info("Snowflake connection closed")
