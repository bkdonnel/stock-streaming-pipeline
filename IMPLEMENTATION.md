# Stock Batch Analytics Pipeline - Implementation Plan

## Phase 1: Databricks Workspace Setup
**Goal:** Configure the Databricks workspace, connect to Delta Lake storage, and verify the environment.

- [x] Log into Databricks workspace and create a cluster (single-node is fine for this workload)
- [x] Create the Delta Lake schema structure (workspace uses Unity Catalog — `bootcamp_students` catalog, prefixed with `bd_` since it is a shared community catalog):
  ```sql
  CREATE SCHEMA IF NOT EXISTS bootcamp_students.bd_bronze;
  CREATE SCHEMA IF NOT EXISTS bootcamp_students.bd_silver;
  CREATE SCHEMA IF NOT EXISTS bootcamp_students.bd_gold;
  ```
  Note: `CREATE DATABASE` is blocked by Unity Catalog permissions. Use `CREATE SCHEMA` with the fully qualified catalog name instead.
- [x] Store the Polygon API key in Databricks Secrets via OAuth CLI (PATs are disabled at workspace level):
  ```bash
  databricks auth login --host https://dbc-7b106152-caf3.cloud.databricks.com
  databricks secrets create-scope stock-pipeline
  databricks secrets put-secret stock-pipeline polygon-api-key --string-value "..."
  ```
- [x] Create `databricks/config.py` with Spark + API settings (see config spec below)
- [x] Verify cluster can import `requests`, `pandas`, `pyspark`
- [x] Run `databricks/notebooks/00_setup.py` — all cells pass

### Config Spec (`databricks/config.py`)
Uses Databricks Secrets on cluster, falls back to `.env` locally:
```python
import os
from dotenv import load_dotenv

load_dotenv()

def _get_polygon_api_key() -> str:
    try:
        return dbutils.secrets.get(scope="stock-pipeline", key="polygon-api-key")
    except NameError:
        return os.getenv("POLYGON_API_KEY", "")

class Config:
    POLYGON_API_KEY: str = _get_polygon_api_key()
    STOCK_SYMBOLS: list[str] = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"]

    # Fully qualified Unity Catalog table names (catalog: bootcamp_students)
    BRONZE_TABLE: str = "bootcamp_students.bd_bronze.stock_prices"
    SILVER_TABLE: str = "bootcamp_students.bd_silver.stock_prices"
    GOLD_TABLE:   str = "bootcamp_students.bd_gold.stock_indicators"

    # Indicator periods
    SMA_SHORT:    int = 20
    SMA_LONG:     int = 50
    EMA_SHORT:    int = 12
    EMA_LONG:     int = 26
    RSI_PERIOD:   int = 14
    MACD_FAST:    int = 12
    MACD_SLOW:    int = 26
    MACD_SIGNAL:  int = 9
    BB_PERIOD:    int = 20
    BB_STD_DEV:   int = 2
```

---

## Phase 2: Bronze Layer — Data Ingestion
**Goal:** Fetch daily OHLCV data from Polygon and write raw records to Delta Lake.

- [x] Create `databricks/notebooks/01_bronze_ingestion.py`
- [x] Implement `fetch_daily_ohlcv(symbol, api_key, date)` using Polygon's `/v1/open-close/{symbol}/{date}` endpoint
  - Adapted from `kafka/producer/utils.py`; handles 404s (holidays/weekends) gracefully
- [x] Loop over `Config.STOCK_SYMBOLS`, fetch each symbol, collect into a list of dicts
- [x] Convert to a Spark DataFrame with an explicit schema:
  ```
  symbol (string), date (string), open (double), high (double),
  low (double), close (double), volume (long), vwap (double), source (string)
  ```
- [x] Write to `bootcamp_students.bd_bronze.stock_prices` in Delta format, append mode (never overwrite raw data)
- [x] Add basic logging — log each symbol fetched and any failures
- [x] Supports `run_date` widget (defaults to yesterday) to enable backfills
- [x] Test: verify rows appear via `spark.table("bootcamp_students.bd_bronze.stock_prices").show()`
  - Confirmed: 5 rows written for 2026-03-06 (AAPL, GOOGL, MSFT, TSLA, AMZN)
  - Note: `vwap` is `0.0` — not returned by the `/v1/open-close` endpoint; retained for schema compatibility

### Polygon endpoint
```
GET https://api.polygon.io/v1/open-close/{symbol}/{date}?adjusted=true&apiKey={key}
```
Returns: `open`, `high`, `low`, `close`, `volume`, `vwap`, `afterHours`, `preMarket`

---

## Phase 3: Silver Layer — Cleaning & Validation
**Goal:** Read Bronze, clean and validate the data, write deduplicated records to Silver.

- [x] Create `databricks/notebooks/02_silver_transform.py`
- [x] Read from `bootcamp_students.bd_bronze.stock_prices`
- [x] Apply transformations:
  - Cast `date` column to `DateType` if stored as string
  - Drop rows where `close` is null or zero
  - Deduplicate on `(symbol, date)` — keep the most recently ingested record
  - Add `transformed_at` timestamp column
- [x] Write to `bootcamp_students.bd_silver.stock_prices` in Delta format, overwrite mode (Silver is always rebuilt from Bronze)
- [x] Test: row counts Bronze vs Silver, verify no nulls in key columns
  - Confirmed: Bronze 5 rows → Silver 5 rows, no drops

---

## Phase 4: Gold Layer — Technical Indicators
**Goal:** Compute all technical indicators on Silver data and write to Gold.

- [x] Create `databricks/notebooks/03_gold_indicators.py`
- [x] Create `databricks/indicators.py` with pure functions for each indicator (see specs below)
- [x] Read `bootcamp_students.bd_silver.stock_prices`, sort by `(symbol, date)`
- [x] Use `groupBy("symbol")` + `applyInPandas` to apply `add_indicators(df)` per symbol
- [x] Write to `bootcamp_students.bd_gold.stock_indicators` in Delta format, overwrite mode
- [x] Test: spot-check SMA-20 values manually against known prices — verified 5 rows in/out, 22 columns

### Indicator function specs (`databricks/indicators.py`)

```python
import pandas as pd

def calculate_sma(df: pd.DataFrame, period: int, column: str = "close") -> pd.Series:
    return df[column].rolling(window=period, min_periods=1).mean()

def calculate_ema(df: pd.DataFrame, period: int, column: str = "close") -> pd.Series:
    return df[column].ewm(span=period, adjust=False).mean()

def calculate_rsi(df: pd.DataFrame, period: int = 14, column: str = "close") -> pd.Series:
    delta = df[column].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period, min_periods=1).mean()
    avg_loss = loss.rolling(window=period, min_periods=1).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def calculate_macd(
    df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9, column: str = "close"
) -> tuple[pd.Series, pd.Series, pd.Series]:
    ema_fast = df[column].ewm(span=fast, adjust=False).mean()
    ema_slow = df[column].ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram

def calculate_bollinger_bands(
    df: pd.DataFrame, period: int = 20, std_dev: int = 2, column: str = "close"
) -> tuple[pd.Series, pd.Series, pd.Series]:
    sma = df[column].rolling(window=period, min_periods=1).mean()
    rolling_std = df[column].rolling(window=period, min_periods=1).std()
    upper = sma + (rolling_std * std_dev)
    lower = sma - (rolling_std * std_dev)
    return upper, sma, lower

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all indicators to a single-symbol DataFrame sorted by date."""
    df = df.sort_values("date").copy()
    df["sma_20"] = calculate_sma(df, 20)
    df["sma_50"] = calculate_sma(df, 50)
    df["ema_12"] = calculate_ema(df, 12)
    df["ema_26"] = calculate_ema(df, 26)
    df["rsi_14"] = calculate_rsi(df, 14)
    df["macd_line"], df["macd_signal"], df["macd_histogram"] = calculate_macd(df)
    df["bb_upper"], df["bb_middle"], df["bb_lower"] = calculate_bollinger_bands(df)
    return df
```

---

## Phase 5: Snowflake Integration
**Goal:** Write Gold layer data to Snowflake for serving to the dashboard.

Note: Shared Snowflake account — write access limited to single schema `DATAEXPERT_STUDENT.BRYAN`.
Multi-schema design (RAW/STAGING/MARTS) replaced with two FCT tables in `BRYAN`.

- [x] Snowflake account identified: `LYWBBPJ-ODB66944`, database `DATAEXPERT_STUDENT`, schema `BRYAN`
- [x] RSA key pair generated locally; public key registered in Snowflake UI; private key stored in Databricks Secrets
- [x] Databricks Secrets stored (scope: `stock-pipeline`): `snowflake-account`, `snowflake-user`, `snowflake-private-key`, `snowflake-database`, `snowflake-schema`
- [x] Rewrote `04_snowflake_load.py` to use `snowflake-connector-python` + `cryptography` instead of Maven Spark connector (shared cluster is serverless — Maven packages blocked)
  - Uses `%pip install snowflake-connector-python cryptography`
  - Parses PEM private key via `load_pem_private_key` → DER bytes for `snowflake.connector.connect`
  - Writes via pandas `toPandas()` + `executemany` (5 rows — no need for Spark parallelism)
  - Handles PEM headers missing from secret with defensive wrapping
- [x] Verified: `FCT_STOCK_PRICES` and `FCT_TECHNICAL_INDICATORS` written and read back successfully
- [x] Root cause of JWT errors: RSA public key fingerprint in Snowflake did not match private key in Databricks Secrets — resolved by re-registering correct public key via `ALTER USER`

---

## Phase 6: Streamlit Dashboard
**Goal:** Build an interactive dashboard that visualizes stock prices and indicators, deployed publicly on Streamlit Community Cloud for portfolio use.

- [x] Decided on Streamlit Community Cloud over Streamlit in Snowflake — SiS is not publicly accessible without a Snowflake account
- [x] Set up `dashboard/` directory with `app.py` and `requirements.txt`
- [x] `dashboard/app.py` built:
  - Stock selector dropdown (AAPL, GOOGL, MSFT, TSLA, AMZN)
  - Date range picker (defaults to last 180 days)
  - Metrics row: latest close + delta, RSI, SMA-20, SMA-50, MACD
  - 3-panel Plotly chart: price + SMA-20/50 + Bollinger Bands / RSI with 70/30 lines / MACD with histogram and signal
  - Connects to Snowflake via `snowflake-connector-python` + RSA key auth, using `st.secrets`
  - Data cached with `@st.cache_data(ttl=3600)`
- [x] `.streamlit/secrets.toml` created locally (gitignored) — template with Snowflake credentials
  - Note: must live at `dashboard/.streamlit/secrets.toml` when running `streamlit run` from `dashboard/` subdirectory
- [x] Fixed JWT auth error — registered correct public key in Snowflake via DataExpert org UI
  - Snowflake UI breaks single-line keys; must paste public key with standard 64-char PEM line breaks (no headers)
  - Active fingerprint: `SHA256:PUw88mqTNMUdknWEr/g09N7A3zyyriszORxv2pgoKes=`
- [x] Tested dashboard locally — connected to Snowflake, charts rendered successfully
- [x] Deployed to Streamlit Community Cloud — publicly accessible, auto-deploys on `git push` to `main`
  - Secrets added via Streamlit Cloud UI settings (paste full `secrets.toml` contents)

---

## Phase 7: Scheduling & Monitoring
**Goal:** Automate the pipeline and add observability.

- [x] Created Databricks Job via Workflows → Jobs and Pipelines UI
  - Job name: `stock-pipeline-daily`
  - 4 tasks chained in order: `01_bronze_ingestion` → `02_silver_transform` → `03_gold_indicators` → `04_snowflake_load`
  - Each task: Git source (`bkdonnel/stock-streaming-pipeline`, `main` branch), DataExpert cluster
  - Notebook paths must omit `.py` extension (e.g. `databricks/notebooks/01_bronze_ingestion`)
  - Git credentials reused from existing Databricks Repos connection (no new PAT needed)
- [x] Schedule set: daily at 21:30 UTC (4:30pm ET)
- [x] Email notifications configured on failure
- [x] First manual test run — all 4 tasks completed successfully
- [x] Verified end-to-end: Bronze → Silver → Gold → Snowflake → Dashboard updated
- [x] Fixed weekend/holiday handling in `01_bronze_ingestion.py` (403 treated as graceful skip)
- [x] Fixed `indicators.py` import in `03_gold_indicators.py` for Job Git source context

---

## Previously Completed (Deprecated Kafka/Streaming Phases)
The original Phases 1-2 (local Kafka setup and Python Kafka producer) were built and tested successfully but are no longer part of the pipeline. The Polygon API fetch logic in `kafka/producer/utils.py` was reused in Phase 2 above.
