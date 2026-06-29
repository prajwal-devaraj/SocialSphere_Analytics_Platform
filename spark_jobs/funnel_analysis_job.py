"""
Silver -> Gold: Funnel Analysis.

Tracks the product funnel:
    user_signup -> profile_view -> post_created -> post_liked -> subscription_started

For each user, finds whether/when they reached each step (any time at or
after the previous step), then aggregates per event_date (the date of the
*first* step, i.e. signup date) to show how many users from that day's
cohort reached each subsequent step, and conversion/drop-off rate between
consecutive steps.

Output: gold_funnel_metrics
    event_date, funnel_step, step_order, users_reached, conversion_rate, drop_off_rate

conversion_rate = users_reached at this step / users_reached at step 1 (signup)
drop_off_rate   = 1 - (users_reached at this step / users_reached at previous step)

Usage:
    spark-submit funnel_analysis_job.py \
        --input  s3a://socialsphere/silver/events_clean \
        --output s3a://socialsphere/gold/funnel_metrics
"""

import argparse

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

FUNNEL_STEPS = [
    "user_signup",
    "profile_view",
    "post_created",
    "post_liked",
    "subscription_started",
]


def get_spark_session(app_name: str = "FunnelAnalysisJob") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def build_funnel_metrics(df: DataFrame) -> DataFrame:
    relevant = df.filter(F.col("event_type").isin(FUNNEL_STEPS))

    # Earliest occurrence of each funnel step, per user
    first_step_events = (
        relevant.groupBy("user_id", "event_type")
        .agg(F.min("event_timestamp").alias("first_occurred_at"))
    )

    # Cohort date = user's signup date (their first user_signup event_date)
    signup_dates = (
        df.filter(F.col("event_type") == "user_signup")
        .groupBy("user_id").agg(F.min("event_date").alias("cohort_date"))
    )

    step_order_df = df.sparkSession.createDataFrame(
        [(step, i + 1) for i, step in enumerate(FUNNEL_STEPS)],
        schema=["event_type", "step_order"],
    )

    reached = (
        first_step_events.join(step_order_df, on="event_type")
        .join(signup_dates, on="user_id", how="inner")  # only count users who actually signed up
    )

    # Enforce TRUE funnel semantics: a user only counts at step N if they
    # also reached every step before it (timestamps strictly non-decreasing
    # through the sequence). Without this, a user who e.g. liked a post
    # before ever viewing their profile would inflate later-step counts
    # and produce nonsensical negative drop-off rates.
    pivoted = (
        reached.groupBy("user_id", "cohort_date")
        .pivot("step_order", values=list(range(1, len(FUNNEL_STEPS) + 1)))
        .agg(F.first("first_occurred_at"))
    )
    # pivoted columns are now named "1","2","3","4","5" (one per funnel step)
    step_cols = [str(i) for i in range(1, len(FUNNEL_STEPS) + 1)]

    reached_step_flags = pivoted
    for idx, col in enumerate(step_cols):
        if idx == 0:
            # Step 1 (signup) is reached iff the timestamp is non-null
            reached_step_flags = reached_step_flags.withColumn(
                f"reached_{col}", F.col(col).isNotNull()
            )
        else:
            prev_col = step_cols[idx - 1]
            # Reached step N iff reached step N-1 AND this step's timestamp
            # is at or after the previous step's timestamp.
            reached_step_flags = reached_step_flags.withColumn(
                f"reached_{col}",
                F.col(f"reached_{prev_col}")
                & F.col(col).isNotNull()
                & (F.col(col) >= F.col(prev_col)),
            )

    long_format = []
    for idx, col in enumerate(step_cols):
        step_name = FUNNEL_STEPS[idx]
        long_format.append(
            reached_step_flags.filter(F.col(f"reached_{col}"))
            .select(
                "cohort_date",
                "user_id",
                F.lit(step_name).alias("event_type"),
                F.lit(idx + 1).alias("step_order"),
            )
        )

    reached_ordered = long_format[0]
    for r in long_format[1:]:
        reached_ordered = reached_ordered.unionByName(r)

    per_cohort_step = (
        reached_ordered.groupBy("cohort_date", "event_type", "step_order")
        .agg(F.countDistinct("user_id").alias("users_reached"))
    )

    cohort_size = (
        signup_dates.groupBy("cohort_date").agg(F.countDistinct("user_id").alias("cohort_size"))
    )

    window_prev = Window.partitionBy("cohort_date").orderBy("step_order")

    result = (
        per_cohort_step.join(cohort_size, on="cohort_date")
        .withColumn("prev_step_users", F.lag("users_reached").over(window_prev))
        .withColumn(
            "conversion_rate",
            F.when(F.col("cohort_size") > 0, F.col("users_reached") / F.col("cohort_size")).otherwise(0.0),
        )
        .withColumn(
            "drop_off_rate",
            F.when(
                F.col("prev_step_users").isNotNull() & (F.col("prev_step_users") > 0),
                1 - (F.col("users_reached") / F.col("prev_step_users")),
            ).otherwise(F.lit(0.0)),
        )
        .withColumnRenamed("cohort_date", "event_date")
        .withColumnRenamed("event_type", "funnel_step")
        .select(
            "event_date", "funnel_step", "step_order", "users_reached",
            "conversion_rate", "drop_off_rate",
        )
        .orderBy("event_date", "step_order")
    )
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = get_spark_session()
    silver_df = spark.read.parquet(args.input)
    gold_df = build_funnel_metrics(silver_df)

    gold_df.write.mode("overwrite").parquet(args.output)

    print("=== gold_funnel_metrics sample ===")
    gold_df.show(25, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
