"""
Silver -> Gold: Feature Usage.

Output: gold_feature_usage
    event_date, feature, event_count, unique_users, avg_events_per_user

Usage:
    spark-submit feature_usage_job.py \
        --input  s3a://socialsphere/silver/events_clean \
        --output s3a://socialsphere/gold/feature_usage
"""

import argparse

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def get_spark_session(app_name: str = "FeatureUsageJob") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def build_feature_usage(df: DataFrame) -> DataFrame:
    with_feature = df.filter(F.col("feature").isNotNull())

    per_user_counts = (
        with_feature.groupBy("event_date", "feature", "user_id")
        .agg(F.count("*").alias("user_event_count"))
    )

    result = (
        per_user_counts.groupBy("event_date", "feature")
        .agg(
            F.sum("user_event_count").alias("event_count"),
            F.countDistinct("user_id").alias("unique_users"),
            F.avg("user_event_count").alias("avg_events_per_user"),
        )
        .orderBy("event_date", F.desc("event_count"))
    )
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = get_spark_session()
    silver_df = spark.read.parquet(args.input)
    gold_df = build_feature_usage(silver_df)

    gold_df.write.mode("overwrite").parquet(args.output)

    print("=== gold_feature_usage sample ===")
    gold_df.show(15, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
