"""
Backfill DAG.

Reprocesses an arbitrary historical date range through the full
bronze->silver->gold pipeline. Triggered manually with a date range
config, e.g.:

    airflow dags trigger backfill_product_analytics \
        --conf '{"start_date": "2026-06-01", "end_date": "2026-06-15"}'

Unlike the daily DAG (which processes "yesterday" on a fixed schedule),
this DAG reads the start_date/end_date from the trigger config and passes
them to each Spark job so jobs can filter the bronze input to just that
window before reprocessing silver/gold.

Note: schedule_interval=None means this DAG only runs when triggered
manually or via the CLI/API -- it is never picked up by the scheduler.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DATA_ROOT = "/opt/airflow/data"

default_args = {
    "owner": "socialsphere-data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def validate_backfill_range(**context):
    conf = context["dag_run"].conf or {}
    start_date = conf.get("start_date")
    end_date = conf.get("end_date")
    if not start_date or not end_date:
        raise ValueError(
            "backfill_dag requires --conf '{\"start_date\": \"YYYY-MM-DD\", \"end_date\": \"YYYY-MM-DD\"}'"
        )
    print(f"Backfilling product analytics for range {start_date} to {end_date}")
    return {"start_date": start_date, "end_date": end_date}


with DAG(
    dag_id="backfill_product_analytics",
    default_args=default_args,
    description="Manually-triggered backfill of bronze->silver->gold for a historical date range",
    schedule_interval=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["socialsphere", "backfill"],
) as dag:

    validate_range = PythonOperator(
        task_id="validate_backfill_range",
        python_callable=validate_backfill_range,
    )

    # {{ dag_run.conf['start_date'] }} / {{ dag_run.conf['end_date'] }} are
    # Jinja templates Airflow resolves at task execution time from the
    # trigger config validated above.
    backfill_bronze_to_silver = BashOperator(
        task_id="backfill_bronze_to_silver",
        bash_command=(
            "spark-submit /opt/airflow/spark_jobs/bronze_to_silver.py "
            f"--input {DATA_ROOT}/bronze/events "
            f"--output {DATA_ROOT}/silver/events_clean "
            "--start-date {{ dag_run.conf['start_date'] }} "
            "--end-date {{ dag_run.conf['end_date'] }}"
        ),
    )

    backfill_gold_daily_metrics = BashOperator(
        task_id="backfill_gold_daily_metrics",
        bash_command=(
            "spark-submit /opt/airflow/spark_jobs/silver_to_gold_daily_metrics.py "
            f"--input {DATA_ROOT}/silver/events_clean "
            f"--output {DATA_ROOT}/gold/daily_user_metrics"
        ),
    )

    backfill_retention = BashOperator(
        task_id="backfill_retention_cohorts",
        bash_command=(
            "spark-submit /opt/airflow/spark_jobs/retention_cohort_job.py "
            f"--input {DATA_ROOT}/silver/events_clean "
            f"--output {DATA_ROOT}/gold/retention_cohorts"
        ),
    )

    backfill_funnel = BashOperator(
        task_id="backfill_funnel_metrics",
        bash_command=(
            "spark-submit /opt/airflow/spark_jobs/funnel_analysis_job.py "
            f"--input {DATA_ROOT}/silver/events_clean "
            f"--output {DATA_ROOT}/gold/funnel_metrics"
        ),
    )

    validate_range >> backfill_bronze_to_silver >> [
        backfill_gold_daily_metrics,
        backfill_retention,
        backfill_funnel,
    ]
