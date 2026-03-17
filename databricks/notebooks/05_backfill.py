# Databricks notebook source
# Backfill notebook — runs the full pipeline for each trading day in a date range.
# Use this to load historical data before the daily job was scheduled.
#
# Usage:
#   Set start_date and end_date widgets, then Run All.
#   Skips weekends automatically. Polygon will return 403/404 for holidays (skipped gracefully).

# COMMAND ----------

from datetime import date, timedelta

dbutils.widgets.text("start_date", "2025-12-01", "Start Date (YYYY-MM-DD)")
dbutils.widgets.text("end_date",   "2026-03-14", "End Date (YYYY-MM-DD)")

start = date.fromisoformat(dbutils.widgets.get("start_date").strip())
end   = date.fromisoformat(dbutils.widgets.get("end_date").strip())

print(f"Backfill range: {start} to {end}")

# COMMAND ----------

# Generate all weekdays in the range (Mon-Fri only — skip weekends)
trading_days = []
current = start
while current <= end:
    if current.weekday() < 5:  # 0=Mon, 4=Fri
        trading_days.append(current.isoformat())
    current += timedelta(days=1)

print(f"Trading days to process: {len(trading_days)}")
print(f"First: {trading_days[0]}  Last: {trading_days[-1]}")

# COMMAND ----------

# Run bronze ingestion for each date.
# Silver, Gold, and Snowflake load are run once at the end since they
# operate on the full table (not per-date).

success = []
skipped = []
failed  = []

for run_date in trading_days:
    try:
        result = dbutils.notebook.run(
            "01_bronze_ingestion",
            timeout_seconds=300,
            arguments={"run_date": run_date},
        )
        if "market closed" in result.lower():
            skipped.append(run_date)
            print(f"  SKIPPED {run_date} — market closed")
        else:
            success.append(run_date)
            print(f"  OK      {run_date}")
    except Exception as e:
        failed.append(run_date)
        print(f"  FAILED  {run_date} — {e}")

print(f"\nBronze ingestion complete.")
print(f"  Success: {len(success)} | Skipped: {len(skipped)} | Failed: {len(failed)}")
if failed:
    print(f"  Failed dates: {failed}")

# COMMAND ----------

# Run Silver transform once to clean and deduplicate all ingested data
dbutils.notebook.run("02_silver_transform", timeout_seconds=300)
print("Silver transform complete.")

# COMMAND ----------

# Run Gold indicators once to compute indicators across full history
dbutils.notebook.run("03_gold_indicators", timeout_seconds=300)
print("Gold indicators complete.")

# COMMAND ----------

# Load all Gold data to Snowflake
dbutils.notebook.run("04_snowflake_load", timeout_seconds=600)
print("Snowflake load complete.")

# COMMAND ----------

print(f"\nBackfill complete.")
print(f"  Dates loaded:  {len(success)}")
print(f"  Dates skipped: {len(skipped)} (holidays/market closed)")
print(f"  Dates failed:  {len(failed)}")
