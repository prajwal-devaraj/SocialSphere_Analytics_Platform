"""
Bronze -> Silver Spark job.

Reads raw NDJSON events from the bronze layer and produces a cleaned,
deduplicated, validated Parquet table in the silver layer.

Cleaning rules applied:
    - drop records with null/empty event_id or user_id
    - drop duplicate event_id (keep first occurrence)
    - drop records with unparseable or future timestamps
    - drop records with negative revenue
    - drop records with an event_type outside the known set
    - standardize platform/device_type/country to lowercase/uppercase conventions
    - derive event_date (partition column) from event_timestamp

Usage:
    spark-submit bronze_to_silver.py \
        --input  s3a://socialsphere/bronze/events \
        --output s3a://socialsphere/silver/events_clean

    # or for local testing:
    spark-submit bronze_to_silver.py --input ./data/bronze_local --output ./data/silver_local
"""

import argparse

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

VALID_EVENT_TYPES = [
    "user_signup", "user_login", "profile_view", "post_created", "post_liked",
    "post_shared", "comment_created", "video_viewed", "message_sent",
    "friend_request_sent", "ad_impression", "ad_click", "purchase_completed",
    "subscription_started", "subscription_cancelled", "app_error",
    "session_started", "session_ended",
]


def get_spark_session(app_name: str = "BronzeToSilver") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_bronze(spark: SparkSession, input_path: str, start_date: str = None, end_date: str = None) -> DataFrame:
    # Bronze files are NDJSON; let Spark infer schema then we explicitly cast.
    df = spark.read.json(input_path)
    if start_date or end_date:
        df = df.withColumn("_ts_for_filter", F.to_timestamp("event_timestamp"))
        if start_date:
            df = df.filter(F.col("_ts_for_filter") >= F.lit(start_date))
        if end_date:
            df = df.filter(F.col("_ts_for_filter") <= F.expr(f"timestamp('{end_date} 23:59:59')"))
        df = df.drop("_ts_for_filter")
    return df


def clean_events(df: DataFrame):
    """
    Returns (clean_df, quality_report_dict). The quality report captures
    counts at each filtering stage so the data_quality layer / Airflow DAG
    can assert on it.
    """
    raw_count = df.count()

    # 1. Parse/validate timestamp -> drop unparseable
    df = df.withColumn(
        "event_ts_parsed", F.to_timestamp("event_timestamp")
    )
    after_ts_parse = df.filter(F.col("event_ts_parsed").isNotNull())
    dropped_bad_timestamp = raw_count - after_ts_parse.count()

    # 2. Drop future timestamps (allow 60s clock skew)
    now = F.current_timestamp()
    after_future_check = after_ts_parse.filter(
        F.col("event_ts_parsed") <= F.expr("current_timestamp() + INTERVAL 60 SECONDS")
    )
    dropped_future_ts = after_ts_parse.count() - after_future_check.count()

    # 3. Drop null/empty event_id or user_id
    before = after_future_check.count()
    after_required_fields = after_future_check.filter(
        F.col("event_id").isNotNull() & (F.length(F.col("event_id")) > 0)
        & F.col("user_id").isNotNull() & (F.length(F.col("user_id")) > 0)
    )
    dropped_missing_ids = before - after_required_fields.count()

    # 4. Drop unknown event types
    before = after_required_fields.count()
    after_valid_type = after_required_fields.filter(F.col("event_type").isin(VALID_EVENT_TYPES))
    dropped_bad_event_type = before - after_valid_type.count()

    # 5. Drop negative revenue
    before = after_valid_type.count()
    after_revenue_check = after_valid_type.filter(
        (F.col("revenue").isNull()) | (F.col("revenue") >= 0)
    )
    dropped_negative_revenue = before - after_revenue_check.count()

    # 6. Deduplicate on event_id, keep first by event_timestamp
    before = after_revenue_check.count()
    window = Window.partitionBy("event_id").orderBy(F.col("event_ts_parsed").asc())
    deduped = (
        after_revenue_check.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    dropped_duplicates = before - deduped.count()

    # 7. Standardize dimension fields + derive partition column
    clean = (
        deduped
        .withColumn("platform", F.lower(F.col("platform")))
        .withColumn("device_type", F.lower(F.col("device_type")))
        .withColumn("country", F.upper(F.col("country")))
        .withColumn("event_date", F.to_date("event_ts_parsed"))
        .withColumn("event_timestamp", F.col("event_ts_parsed"))
        .drop("event_ts_parsed")
        .fillna({"revenue": 0.0})
    )

    quality_report = {
        "raw_count": raw_count,
        "dropped_bad_timestamp": dropped_bad_timestamp,
        "dropped_future_timestamp": dropped_future_ts,
        "dropped_missing_ids": dropped_missing_ids,
        "dropped_bad_event_type": dropped_bad_event_type,
        "dropped_negative_revenue": dropped_negative_revenue,
        "dropped_duplicates": dropped_duplicates,
        "clean_count": clean.count(),
    }
    return clean, quality_report


def write_silver(df: DataFrame, output_path: str):
    (
        df.write.mode("overwrite")
        .partitionBy("event_date")
        .parquet(output_path)
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Bronze input path (local dir or s3a://...)")
    parser.add_argument("--output", required=True, help="Silver output path (local dir or s3a://...)")
    parser.add_argument("--start-date", default=None, help="Optional ISO date (YYYY-MM-DD) to filter bronze input from, for backfills")
    parser.add_argument("--end-date", default=None, help="Optional ISO date (YYYY-MM-DD) to filter bronze input through, for backfills")
    args = parser.parse_args()

    spark = get_spark_session()
    raw_df = read_bronze(spark, args.input, start_date=args.start_date, end_date=args.end_date)
    clean_df, report = clean_events(raw_df)
    write_silver(clean_df, args.output)

    print("=== Bronze -> Silver Quality Report ===")
    for k, v in report.items():
        print(f"  {k}: {v}")
    print("========================================")

    spark.stop()


if __name__ == "__main__":
    main()
