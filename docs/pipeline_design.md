# Pipeline Design Notes

## Bronze -> Silver: Cleaning Order

`spark_jobs/bronze_to_silver.py` applies cleaning rules in a specific order, each filtering down from the previous step's output. The quality report printed at the end of the job records exactly how many rows each step dropped:

1. **Parse timestamp** — drop rows where `event_timestamp` can't be parsed at all
2. **Drop future timestamps** — allows 60 seconds of clock skew, drops anything further in the future
3. **Drop missing IDs** — `event_id` or `user_id` null/empty
4. **Drop unknown event types** — anything not in the 18-type allowlist
5. **Drop negative revenue**
6. **Deduplicate** — on `event_id`, keeping the earliest timestamp if duplicated
7. **Standardize + derive `event_date`** — lowercase platform/device, uppercase country

This order matters: deduplication happens *after* the timestamp/ID/type filters so a corrupted duplicate doesn't get to "win" the dedup just because its row order happened to come first.

## Funnel Logic: Strict Step Ordering

The naive approach to funnel analysis — find each user's first occurrence of each step, independently — has a subtle bug: a user could reach a *later* step (e.g. `post_liked`) without ever reaching an *earlier* one (e.g. `post_created`), which would inflate later-step counts and produce **negative drop-off rates**. This was caught during testing (see `tests/test_spark_transformations.py::test_funnel_enforces_strict_step_order`) and fixed by pivoting each user's step timestamps and requiring strict, in-order progression: a user only counts at step N if they reached step N-1 first AND step N's timestamp is at or after step N-1's.

## Retention Cohort Definition

A user's cohort (`signup_date`) is their *first-ever* `event_date` across the whole dataset — not their first `user_signup` event specifically. This matters for synthetic/test data where a user might have other event types before/without an explicit signup event being present; the gold table reflects when they were first observed as active. `retention_day = 0` is always 100% by construction (every user is active on their own signup day) and is included as a sanity-check baseline.

## WAU/MAU: Rolling Windows, Not Calendar Periods

`gold_daily_user_metrics.weekly_active_users` and `monthly_active_users` are trailing 7-day and 30-day windows *ending on* each `event_date` — not calendar-week/calendar-month aggregates. This is the standard product-analytics definition (what most dashboards mean by "WAU"/"MAU") and is what makes `dau_mau_stickiness` a meaningful day-over-day metric rather than something that resets at the start of each month.

**Performance note (found during scale testing):** the first implementation computed this with a cross join between (distinct dates) x (distinct user/date pairs), then filtered down to the relevant range. That's fine with a few hundred test rows, but at ~50K users it produces a multi-million-row intermediate result before any filtering happens, and the job stalled. The fix was to push the date-range condition directly into the join predicate (`activity_date BETWEEN event_date - 29 AND event_date`) so Spark's planner does a range/sort-merge join instead of materializing the full cartesian product. Same output, verified identical on the test dataset before and after the change — see `tests/test_spark_transformations.py` and the manual verification in development notes. At ~1M silver rows / ~50K users this job now completes in under a minute on a single local core.

## Anomaly Detection: Why Z-Score on a Trailing Window

`anomaly_detection_job.py` uses a simple trailing-window z-score rather than a more sophisticated model (Prophet, isolation forest, etc.) deliberately:
- It requires no extra dependencies beyond Spark SQL window functions
- It's transparent — `expected_value` and `anomaly_score` are both interpretable on their own
- The output schema (`actual_value`, `expected_value`, `anomaly_score`, `is_anomaly`) is designed so a more sophisticated model could be swapped in later without changing anything downstream (the dashboard page, the data quality checks, etc.)

Known limitation: with a short trailing window and rapidly-growing synthetic data (a new platform in its growth phase), early days will show "anomalies" that are really just real growth, not problems. In production this is mitigated either with a longer history window or a model that accounts for trend (e.g. exponential weighting or seasonal decomposition).

## Idempotency

Every gold job writes with `.mode("overwrite")`, so re-running a job (e.g. via the backfill DAG) for the same input always produces the same output rather than accumulating duplicate rows. The bronze layer is the only place data is purely appended.
