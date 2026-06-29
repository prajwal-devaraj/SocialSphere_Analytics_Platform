"""
Data quality checks for the silver layer. Used by both:
    - airflow/dags/daily_product_analytics_dag.py (blocking: raises on failure)
    - airflow/dags/data_quality_dag.py (monitoring: logs/warns on failure)

Each check returns a dict: {"name": ..., "passed": bool, "detail": str}
run_all_checks() aggregates them into a report.

These checks operate on a PySpark DataFrame read from the silver Parquet
table, so they validate the *output* of bronze_to_silver -- i.e. they're
a second line of defense / regression test, not a replacement for the
cleaning logic itself.
"""

from datetime import datetime, timedelta, UTC

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

VALID_EVENT_TYPES = [
    "user_signup", "user_login", "profile_view", "post_created", "post_liked",
    "post_shared", "comment_created", "video_viewed", "message_sent",
    "friend_request_sent", "ad_impression", "ad_click", "purchase_completed",
    "subscription_started", "subscription_cancelled", "app_error",
    "session_started", "session_ended",
]


def _get_spark() -> SparkSession:
    return SparkSession.builder.appName("DataQualityChecks").getOrCreate()


def check_no_null_event_ids(df) -> dict:
    bad = df.filter(F.col("event_id").isNull() | (F.length("event_id") == 0)).count()
    return {"name": "no_null_event_ids", "passed": bad == 0, "detail": f"{bad} rows with null/empty event_id"}


def check_no_duplicate_event_ids(df) -> dict:
    total = df.count()
    distinct = df.select("event_id").distinct().count()
    dupes = total - distinct
    return {"name": "no_duplicate_event_ids", "passed": dupes == 0, "detail": f"{dupes} duplicate event_id rows"}


def check_valid_event_types_only(df) -> dict:
    bad = df.filter(~F.col("event_type").isin(VALID_EVENT_TYPES)).count()
    return {"name": "valid_event_types_only", "passed": bad == 0, "detail": f"{bad} rows with unrecognized event_type"}


def check_timestamps_not_future(df) -> dict:
    bad = df.filter(F.col("event_timestamp") > F.current_timestamp()).count()
    return {"name": "timestamps_not_in_future", "passed": bad == 0, "detail": f"{bad} rows with future event_timestamp"}


def check_revenue_not_negative(df) -> dict:
    bad = df.filter(F.col("revenue") < 0).count()
    return {"name": "revenue_not_negative", "passed": bad == 0, "detail": f"{bad} rows with negative revenue"}


def check_event_volume_not_unusually_low(df, drop_threshold: float = 0.5) -> dict:
    """
    Compares the most recent day's event count to the average of the
    previous 7 days. Fails if today's volume dropped more than
    `drop_threshold` (default 50%) relative to that average -- a common
    signal that ingestion broke somewhere upstream.
    """
    daily_counts = (
        df.groupBy("event_date").agg(F.count("*").alias("event_count"))
        .orderBy(F.desc("event_date"))
        .collect()
    )
    if len(daily_counts) < 2:
        return {"name": "event_volume_not_unusually_low", "passed": True, "detail": "not enough days of data to evaluate"}

    latest = daily_counts[0]["event_count"]
    trailing = daily_counts[1:8]
    avg_trailing = sum(r["event_count"] for r in trailing) / len(trailing)

    if avg_trailing == 0:
        return {"name": "event_volume_not_unusually_low", "passed": True, "detail": "trailing average is zero, skipping"}

    drop_pct = 1 - (latest / avg_trailing)
    passed = drop_pct < drop_threshold
    return {
        "name": "event_volume_not_unusually_low",
        "passed": passed,
        "detail": f"latest={latest}, trailing_avg={avg_trailing:.1f}, drop_pct={drop_pct:.1%}",
    }


def check_required_partitions_exist(silver_path: str) -> dict:
    """
    Verifies the silver table has at least one partition for the most
    recent date Spark can see (sanity check that the write actually
    happened and isn't an empty/partial table).
    """
    spark = _get_spark()
    try:
        df = spark.read.parquet(silver_path)
        partition_count = df.select("event_date").distinct().count()
        passed = partition_count > 0
        detail = f"{partition_count} distinct event_date partitions found"
    except Exception as e:
        passed = False
        detail = f"could not read silver table: {e}"
    return {"name": "required_partitions_exist", "passed": passed, "detail": detail}


def run_all_checks(silver_path: str) -> dict:
    spark = _get_spark()
    df = spark.read.parquet(silver_path)

    checks = [
        check_no_null_event_ids(df),
        check_no_duplicate_event_ids(df),
        check_valid_event_types_only(df),
        check_timestamps_not_future(df),
        check_revenue_not_negative(df),
        check_event_volume_not_unusually_low(df),
        check_required_partitions_exist(silver_path),
    ]

    passed_count = sum(1 for c in checks if c["passed"])
    summary = f"{passed_count}/{len(checks)} checks passed"

    return {"checks": checks, "summary": summary, "run_at": datetime.now(UTC).isoformat()}


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-path", required=True)
    args = parser.parse_args()

    report = run_all_checks(args.silver_path)
    print(json.dumps(report, indent=2))
