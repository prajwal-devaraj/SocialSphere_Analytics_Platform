"""
Silver -> Gold: Daily User Metrics.

Produces gold_daily_user_metrics with one row per event_date:
    event_date, daily_active_users, new_users, returning_users,
    total_sessions, avg_session_duration, total_events

WAU/MAU are computed as trailing 7-day / 30-day distinct-user counts per
day, which is the standard approach (a rolling window, not calendar weeks
or months) and is what the dashboard's DAU/MAU stickiness chart consumes.

Usage:
    spark-submit silver_to_gold_daily_metrics.py \
        --input  s3a://socialsphere/silver/events_clean \
        --output s3a://socialsphere/gold/daily_user_metrics
"""

import argparse

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def get_spark_session(app_name: str = "SilverToGoldDailyMetrics") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def compute_daily_active_users(df: DataFrame) -> DataFrame:
    return df.groupBy("event_date").agg(F.countDistinct("user_id").alias("daily_active_users"))


def compute_new_vs_returning(df: DataFrame) -> DataFrame:
    """
    A user's first-ever event_date (across the whole dataset) marks them as
    'new' on that date; every other active date for that user counts as
    'returning'.
    """
    first_seen = df.groupBy("user_id").agg(F.min("event_date").alias("first_seen_date"))
    joined = df.select("user_id", "event_date").distinct().join(first_seen, on="user_id")
    joined = joined.withColumn(
        "user_status",
        F.when(F.col("event_date") == F.col("first_seen_date"), F.lit("new")).otherwise(F.lit("returning")),
    )
    return (
        joined.groupBy("event_date", "user_status")
        .agg(F.countDistinct("user_id").alias("user_count"))
        .groupBy("event_date")
        .agg(
            F.sum(F.when(F.col("user_status") == "new", F.col("user_count")).otherwise(0)).alias("new_users"),
            F.sum(F.when(F.col("user_status") == "returning", F.col("user_count")).otherwise(0)).alias("returning_users"),
        )
    )


def compute_session_metrics(df: DataFrame) -> DataFrame:
    session_durations = (
        df.groupBy("event_date", "session_id")
        .agg(
            F.max("duration_seconds").alias("session_duration_proxy"),
            F.count("*").alias("events_in_session"),
        )
    )
    return session_durations.groupBy("event_date").agg(
        F.countDistinct("session_id").alias("total_sessions"),
        F.avg("session_duration_proxy").alias("avg_session_duration"),
    )


def compute_total_events(df: DataFrame) -> DataFrame:
    return df.groupBy("event_date").agg(F.count("*").alias("total_events"))


def compute_wau_mau(daily_metrics: DataFrame, df: DataFrame) -> DataFrame:
    """
    Computes rolling 7-day (WAU) and 30-day (MAU) distinct active users
    ending on each event_date, plus DAU/MAU stickiness ratio.

    Avoids a cross join between (all dates) x (all users) -- which is
    cheap with a handful of test users but becomes a real bottleneck at
    tens of thousands of users x dozens of dates (millions of intermediate
    rows). Instead, for each user we materialize the list of distinct
    dates they were active and use a range-join condition pushed into a
    join predicate (not a literal cross product), letting Spark's join
    planner avoid materializing the full cartesian product.
    """
    user_dates = df.select("user_id", F.col("event_date").alias("activity_date")).distinct()
    all_dates = daily_metrics.select("event_date").distinct()

    # Range join: for each target event_date, find user-activity-dates
    # within [event_date - 29, event_date]. Spark's catalyst optimizer
    # recognizes this as a range condition and uses a sort-merge / range
    # join strategy instead of a full cross join when both sides are
    # reasonably sized.
    joined = all_dates.join(
        user_dates,
        on=(
            (user_dates["activity_date"] <= all_dates["event_date"])
            & (user_dates["activity_date"] >= F.date_sub(all_dates["event_date"], 29))
        ),
        how="inner",
    ).select("event_date", "activity_date", "user_id")

    joined = joined.withColumn("days_diff", F.datediff(F.col("event_date"), F.col("activity_date")))

    wau = (
        joined.filter(F.col("days_diff") < 7)
        .groupBy("event_date").agg(F.countDistinct("user_id").alias("weekly_active_users"))
    )
    mau = (
        joined.groupBy("event_date").agg(F.countDistinct("user_id").alias("monthly_active_users"))
    )

    result = daily_metrics.join(wau, on="event_date", how="left").join(mau, on="event_date", how="left")
    result = result.withColumn(
        "dau_mau_stickiness",
        F.when(F.col("monthly_active_users") > 0, F.col("daily_active_users") / F.col("monthly_active_users")).otherwise(0.0),
    )
    return result


def build_daily_user_metrics(df: DataFrame) -> DataFrame:
    dau = compute_daily_active_users(df)
    new_returning = compute_new_vs_returning(df)
    sessions = compute_session_metrics(df)
    totals = compute_total_events(df)

    result = (
        dau.join(new_returning, on="event_date", how="left")
        .join(sessions, on="event_date", how="left")
        .join(totals, on="event_date", how="left")
    )
    result = compute_wau_mau(result, df)
    return result.orderBy("event_date")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = get_spark_session()
    silver_df = spark.read.parquet(args.input)
    gold_df = build_daily_user_metrics(silver_df)

    gold_df.write.mode("overwrite").parquet(args.output)

    print("=== gold_daily_user_metrics sample ===")
    gold_df.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
