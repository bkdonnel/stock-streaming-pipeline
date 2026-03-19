from dataclasses import dataclass
from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, min as spark_min, max as spark_max


@dataclass
class AuditResult:
    name: str
    passed: bool
    message: str


def check_row_count(df: DataFrame, min_rows: int = 1) -> AuditResult:
    count = df.count()
    passed = count >= min_rows
    return AuditResult("row_count", passed, f"{count} rows (min: {min_rows})")


def check_row_count_matches(df: DataFrame, expected: int) -> AuditResult:
    count = df.count()
    passed = count == expected
    return AuditResult("row_count_matches", passed, f"{count} rows (expected {expected})")


def check_no_nulls(df: DataFrame, columns: list[str]) -> list[AuditResult]:
    results = []
    for col_name in columns:
        null_count = df.filter(col(col_name).isNull()).count()
        results.append(AuditResult(
            f"no_nulls_{col_name}",
            null_count == 0,
            f"{null_count} nulls in {col_name}",
        ))
    return results


def check_symbols_complete(df: DataFrame, expected: list[str]) -> AuditResult:
    found = {row.symbol for row in df.select("symbol").distinct().collect()}
    missing = set(expected) - found
    passed = len(missing) == 0
    return AuditResult(
        "symbols_complete",
        passed,
        f"Missing: {missing}" if missing else "All symbols present",
    )


def check_column_range(df: DataFrame, col_name: str, min_val: float, max_val: float) -> AuditResult:
    non_null = df.filter(col(col_name).isNotNull())
    if non_null.count() == 0:
        return AuditResult(f"range_{col_name}", True, "No non-null values to check")
    result = non_null.select(spark_min(col_name).alias("mn"), spark_max(col_name).alias("mx")).collect()[0]
    passed = result.mn >= min_val and result.mx <= max_val
    return AuditResult(
        f"range_{col_name}",
        passed,
        f"{col_name}: min={result.mn:.4f}, max={result.mx:.4f} (expected [{min_val}, {max_val}])",
    )


def write_audit_publish(
    spark: SparkSession,
    df: DataFrame,
    live_table: str,
    staging_table: str,
    audit_fn: Callable[[DataFrame], list[AuditResult]],
) -> None:
    """
    Write-Audit-Publish pattern:
    1. Write df to a staging table (invisible to downstream consumers).
    2. Run audit_fn against the staged data.
    3. If all checks pass, overwrite the live table from staging and drop staging.
    4. If any checks fail, drop staging and raise RuntimeError — live table is untouched.
    """
    print(f"WAP: writing to staging table {staging_table}")
    df.write.format("delta").mode("overwrite").saveAsTable(staging_table)

    staged_df = spark.table(staging_table)
    audits = audit_fn(staged_df)

    failures = []
    print("Audit results:")
    for audit in audits:
        status = "PASS" if audit.passed else "FAIL"
        print(f"  [{status}] {audit.name}: {audit.message}")
        if not audit.passed:
            failures.append(audit)

    if failures:
        spark.sql(f"DROP TABLE IF EXISTS {staging_table}")
        raise RuntimeError(
            f"WAP audit failed — {len(failures)} check(s) did not pass. "
            f"Live table {live_table} was not updated."
        )

    print(f"All audits passed. Publishing to {live_table}.")
    spark.table(staging_table).write.format("delta").mode("overwrite").saveAsTable(live_table)
    spark.sql(f"DROP TABLE IF EXISTS {staging_table}")
    print("Publish complete.")
