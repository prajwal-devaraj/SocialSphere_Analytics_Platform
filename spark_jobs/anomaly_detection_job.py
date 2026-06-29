"""
Silver -> Gold: Anomaly Detection.

Reads gold_daily_user_metrics and gold_revenue_metrics (or any daily metric
table) and flags days where a metric deviates significantly from its
trailing rolling average, using a z-score approach:

    z = (actual_value - rolling_mean) / rolling_stddev
    is_anomaly = abs(z) > threshold (default 2.0)

This is intentionally simple (no external ML library) so it runs anywhere
Spark runs, but the same anomaly_score column is exactly what a more
sophisticated model (e.g. seasonal decomposition, Prophet, isolation
forest) would replace later without changing the gold schema.

Output: gold_anomaly_detection
    event_date, metric_name, actual_value, expected_value, anomaly_score, is_anomaly

Usage:
    spark-submit anomaly_detection_job.py \
        --daily-metrics s3a://socialsphere/gold/daily_user_metrics \
        --revenue-metrics s3a://socialsphere/gold/revenue_metrics \
        --output s3a://socialsphere/gold/anomaly_detection \
        --window 7 --threshold 2.0
"""

import argparse

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def get_spark_session(app_name: str = "AnomalyDetectionJob") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def detect_anomalies_for_metric(df: DataFrame, metric_col: str, metric_name: str, window_size: int, threshold: float) -> DataFrame:
    w = Window.orderBy("event_date").rowsBetween(-window_size, -1)  # trailing window, excludes current day

    with_stats = (
        df.select("event_date", F.col(metric_col).alias("actual_value"))
        .withColumn("expected_value", F.avg("actual_value").over(w))
        .withColumn("rolling_stddev", F.stddev("actual_value").over(w))
    )

    scored = with_stats.withColumn(
        "anomaly_score",
        F.when(
            F.col("rolling_stddev").isNotNull() & (F.col("rolling_stddev") > 0),
            (F.col("actual_value") - F.col("expected_value")) / F.col("rolling_stddev"),
        ).otherwise(F.lit(0.0)),
    ).withColumn(
        "is_anomaly",
        F.abs(F.col("anomaly_score")) > threshold,
    ).withColumn(
        "metric_name", F.lit(metric_name)
    )

    return scored.select(
        "event_date", "metric_name", "actual_value", "expected_value", "anomaly_score", "is_anomaly"
    )


def build_anomaly_report(daily_metrics: DataFrame, revenue_metrics: DataFrame, window_size: int, threshold: float) -> DataFrame:
    parts = [
        detect_anomalies_for_metric(daily_metrics, "daily_active_users", "daily_active_users", window_size, threshold),
        detect_anomalies_for_metric(daily_metrics, "total_events", "total_events", window_size, threshold),
        detect_anomalies_for_metric(revenue_metrics, "total_revenue", "total_revenue", window_size, threshold),
    ]
    result = parts[0]
    for p in parts[1:]:
        result = result.unionByName(p)
    return result.orderBy("event_date", "metric_name")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--daily-metrics", required=True, dest="daily_metrics")
    parser.add_argument("--revenue-metrics", required=True, dest="revenue_metrics")
    parser.add_argument("--output", required=True)
    parser.add_argument("--window", type=int, default=7)
    parser.add_argument("--threshold", type=float, default=2.0)
    args = parser.parse_args()

    spark = get_spark_session()
    daily_df = spark.read.parquet(args.daily_metrics)
    revenue_df = spark.read.parquet(args.revenue_metrics)

    result = build_anomaly_report(daily_df, revenue_df, args.window, args.threshold)
    result.write.mode("overwrite").parquet(args.output)

    print("=== gold_anomaly_detection sample ===")
    result.show(30, truncate=False)
    print(f"Anomalies flagged: {result.filter(F.col('is_anomaly')).count()} / {result.count()} rows")

    spark.stop()


if __name__ == "__main__":
    main()
