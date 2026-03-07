# Databricks notebook source
# Run this notebook once to initialize the Delta Lake databases and verify the environment.

# COMMAND ----------

# Verify required packages are available
import requests
import pandas as pd
from pyspark.sql import SparkSession

print("requests:", requests.__version__)
print("pandas:", pd.__version__)
print("pyspark: OK")

# COMMAND ----------

# Create medallion architecture databases
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

print("Databases created: bronze, silver, gold")

# COMMAND ----------

# Verify databases exist
display(spark.sql("SHOW DATABASES"))

# COMMAND ----------

# Verify the Polygon API key is accessible via Databricks Secrets
api_key = dbutils.secrets.get(scope="stock-pipeline", key="polygon-api-key")

if not api_key:
    raise ValueError("Secret returned empty. Check the scope and key name.")

print(f"POLYGON_API_KEY retrieved from secrets ({len(api_key)} chars)")

# COMMAND ----------

# Smoke test: call Polygon API for one symbol
import requests

symbol = "AAPL"
date = "2025-01-02"  # A known trading day
url = f"https://api.polygon.io/v1/open-close/{symbol}/{date}"
response = requests.get(url, params={"adjusted": "true", "apiKey": api_key})

if response.status_code == 200:
    data = response.json()
    print(f"Polygon API OK — {symbol} close on {date}: ${data.get('close')}")
elif response.status_code == 403:
    raise ValueError("API key rejected by Polygon. Check the secret value is correct.")
else:
    print(f"Unexpected status {response.status_code}: {response.text}")
