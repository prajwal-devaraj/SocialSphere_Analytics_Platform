"""
Silver -> Gold: Retention Cohorts.

For each signup cohort (grouped by signup_date = each user's first-ever
event_date), computes how many of those users were still active on
day 1, day 7, and day 30 after signup.

Output: gold_retention_cohorts
    signup_date, retention_day, cohort_size, retained_users, retention_rate

retention_day is one of {0, 1, 7, 30} (0 = signup day itself, included as
a sanity-check baseline that should always be 100%).

Usage:
    spark-submit retention_cohort_job.py \
        --input  s3a://socialsphere/silver/events_clean \
        --output s3a://socialsphere/gold/retention_cohorts
"""

import argparse

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

RETENTION_DAYS = [0, 1, 7, 30]


def get_spark_session(app_name: str = "RetentionCohortJob") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def build_retention_cohorts(df: DataFrame) -> DataFrame:
    # Each user's signup_date = first date we ever see them active.
    signups = df.groupBy("user_id").agg(F.min("event_date").alias("signup_date"))

    cohort_sizes = signups.groupBy("signup_date").agg(F.countDistinct("user_id").alias("cohort_size"))

    active_dates = df.select("user_id", "event_date").distinct().join(signups, on="user_id")
    active_dates = active_dates.withColumn("days_since_signup", F.datediff(F.col("event_date"), F.col("signup_date")))

    results = []
    for day in RETENTION_DAYS:
        retained = (
            active_dates.filter(F.col("days_since_signup") == day)
            .groupBy("signup_date")
            .agg(F.countDistinct("user_id").alias("retained_users"))
            .withColumn("retention_day", F.lit(day))
        )
        results.append(retained)

    retained_all = results[0]
    for r in results[1:]:
        retained_all = retained_all.unionByName(r)

    final = (
        retained_all.join(cohort_sizes, on="signup_date", how="left")
        .withColumn(
            "retention_rate",
            F.when(F.col("cohort_size") > 0, F.col("retained_users") / F.col("cohort_size")).otherwise(0.0),
        )
        .select("signup_date", "retention_day", "cohort_size", "retained_users", "retention_rate")
        .orderBy("signup_date", "retention_day")
    )
    return final


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = get_spark_session()
    silver_df = spark.read.parquet(args.input)
    gold_df = build_retention_cohorts(silver_df)

    gold_df.write.mode("overwrite").parquet(args.output)

    print("=== gold_retention_cohorts sample ===")
    gold_df.show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
