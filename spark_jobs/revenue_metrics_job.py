"""
Silver -> Gold: Revenue Metrics.

Output: gold_revenue_metrics
    event_date, total_revenue, paying_users, arpu,
    subscription_starts, subscription_cancellations

ARPU here = total_revenue / daily_active_users (all active users that day,
not just payers) -- the standard "average revenue per user" definition.

Usage:
    spark-submit revenue_metrics_job.py \
        --input  s3a://socialsphere/silver/events_clean \
        --output s3a://socialsphere/gold/revenue_metrics
"""

import argparse

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def get_spark_session(app_name: str = "RevenueMetricsJob") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def build_revenue_metrics(df: DataFrame) -> DataFrame:
    daily_active_users = df.groupBy("event_date").agg(F.countDistinct("user_id").alias("daily_active_users"))

    revenue_events = df.filter(F.col("revenue") > 0)

    revenue_agg = revenue_events.groupBy("event_date").agg(
        F.sum("revenue").alias("total_revenue"),
        F.countDistinct("user_id").alias("paying_users"),
    )

    sub_starts = (
        df.filter(F.col("event_type") == "subscription_started")
        .groupBy("event_date").agg(F.count("*").alias("subscription_starts"))
    )
    sub_cancels = (
        df.filter(F.col("event_type") == "subscription_cancelled")
        .groupBy("event_date").agg(F.count("*").alias("subscription_cancellations"))
    )

    result = (
        daily_active_users.join(revenue_agg, on="event_date", how="left")
        .join(sub_starts, on="event_date", how="left")
        .join(sub_cancels, on="event_date", how="left")
        .fillna(0, subset=["total_revenue", "paying_users", "subscription_starts", "subscription_cancellations"])
        .withColumn(
            "arpu",
            F.when(F.col("daily_active_users") > 0, F.col("total_revenue") / F.col("daily_active_users")).otherwise(0.0),
        )
        .select(
            "event_date", "total_revenue", "paying_users", "arpu",
            "subscription_starts", "subscription_cancellations",
        )
        .orderBy("event_date")
    )
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = get_spark_session()
    silver_df = spark.read.parquet(args.input)
    gold_df = build_revenue_metrics(silver_df)

    gold_df.write.mode("overwrite").parquet(args.output)

    print("=== gold_revenue_metrics sample ===")
    gold_df.show(15, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
