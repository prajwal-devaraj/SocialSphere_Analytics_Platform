"""
Data Quality DAG.

Runs independently of the main pipeline DAG (e.g. on a tighter schedule)
purely to monitor data health and alert early if something's wrong with
the silver layer, without re-running the full Spark pipeline.

Checks (see data_quality/validation_rules.py for implementation):
    - no null event IDs
    - no duplicate event IDs
    - only valid event types
    - timestamps not in the future
    - revenue not negative
    - event volume not unusually low vs. trailing average
    - expected partitions exist for the latest date
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

DATA_ROOT = "/opt/airflow/data"

default_args = {
    "owner": "socialsphere-data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def run_quality_checks(**context):
    import sys
    sys.path.insert(0, "/opt/airflow/data_quality")
    from validation_rules import run_all_checks  # noqa

    report = run_all_checks(silver_path=f"{DATA_ROOT}/silver/events_clean")
    print(f"Data quality report: {report['summary']}")

    failed = [c for c in report["checks"] if not c["passed"]]
    if failed:
        # Don't raise here -- this DAG is monitoring-only. Surface failures
        # via logs/XCom so an alerting layer (e.g. Slack operator) can pick
        # them up without blocking the main pipeline.
        print(f"WARNING: {len(failed)} data quality check(s) failed: {failed}")
    return report


with DAG(
    dag_id="data_quality_dag",
    default_args=default_args,
    description="Standalone monitoring DAG for silver-layer data quality",
    schedule_interval="0 */4 * * *",  # every 4 hours
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["socialsphere", "data-quality", "monitoring"],
) as dag:

    quality_check_task = PythonOperator(
        task_id="run_quality_checks",
        python_callable=run_quality_checks,
    )
