"""
Daily Product Analytics DAG.

Orchestrates the full bronze -> silver -> gold pipeline once per day:

    1. check_raw_data_available   (sensor: today's bronze partition exists)
    2. bronze_to_silver            (Spark)
    3. silver_to_gold_daily_metrics (Spark)
    4. retention_cohort_job        (Spark)
    5. funnel_analysis_job         (Spark)
    6. feature_usage_job           (Spark)
    7. revenue_metrics_job         (Spark)
    8. anomaly_detection_job       (Spark, depends on daily_metrics + revenue_metrics)
    9. run_data_quality_checks     (PythonOperator)
   10. refresh_dashboard_tables    (PythonOperator, no-op placeholder /
       cache-busting hook for the Streamlit dashboard)

Steps 4-7 run in parallel since they all depend only on silver and not on
each other; anomaly detection waits for both daily_metrics and revenue.

Schedule: once per day at 02:00 UTC, processing the previous day's data
(Airflow's execution_date semantics: a run scheduled for day D processes
data for day D-1 by default with @daily).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor

DATA_ROOT = "/opt/airflow/data"  # mounted volume; in production this is s3a://socialsphere/...

default_args = {
    "owner": "socialsphere-data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def run_data_quality_checks(**context):
    """
    Wraps data_quality/validation_rules.py so failures here fail the DAG run and
    surface in Airflow's UI, rather than silently shipping bad gold data.
    """
    import sys
    sys.path.insert(0, "/opt/airflow/data_quality")
    from validation_rules import run_all_checks  # noqa

    report = run_all_checks(silver_path=f"{DATA_ROOT}/silver/events_clean")
    failed = [c for c in report["checks"] if not c["passed"]]
    if failed:
        raise ValueError(f"Data quality checks failed: {failed}")
    print(f"All {len(report['checks'])} data quality checks passed.")


def refresh_dashboard_tables(**context):
    """
    Placeholder hook: in this project the Streamlit dashboard reads gold
    Parquet directly, so there's no cache to invalidate. If gold tables
    were instead loaded into Postgres/Trino, this is where that load step
    would go.
    """
    print("Gold layer refreshed. Dashboard will pick up new Parquet files on next read.")


with DAG(
    dag_id="daily_product_analytics_dag",
    default_args=default_args,
    description="End-to-end bronze->silver->gold product analytics pipeline",
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["socialsphere", "analytics", "production"],
) as dag:

    check_raw_data_available = FileSensor(
        task_id="check_raw_data_available",
        filepath=f"{DATA_ROOT}/bronze/events",
        poke_interval=60,
        timeout=60 * 30,
        mode="reschedule",
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            "spark-submit /opt/airflow/spark_jobs/bronze_to_silver.py "
            f"--input {DATA_ROOT}/bronze/events "
            f"--output {DATA_ROOT}/silver/events_clean"
        ),
    )

    silver_to_gold_daily_metrics = BashOperator(
        task_id="silver_to_gold_daily_metrics",
        bash_command=(
            "spark-submit /opt/airflow/spark_jobs/silver_to_gold_daily_metrics.py "
            f"--input {DATA_ROOT}/silver/events_clean "
            f"--output {DATA_ROOT}/gold/daily_user_metrics"
        ),
    )

    retention_cohort_job = BashOperator(
        task_id="retention_cohort_job",
        bash_command=(
            "spark-submit /opt/airflow/spark_jobs/retention_cohort_job.py "
            f"--input {DATA_ROOT}/silver/events_clean "
            f"--output {DATA_ROOT}/gold/retention_cohorts"
        ),
    )

    funnel_analysis_job = BashOperator(
        task_id="funnel_analysis_job",
        bash_command=(
            "spark-submit /opt/airflow/spark_jobs/funnel_analysis_job.py "
            f"--input {DATA_ROOT}/silver/events_clean "
            f"--output {DATA_ROOT}/gold/funnel_metrics"
        ),
    )

    feature_usage_job = BashOperator(
        task_id="feature_usage_job",
        bash_command=(
            "spark-submit /opt/airflow/spark_jobs/feature_usage_job.py "
            f"--input {DATA_ROOT}/silver/events_clean "
            f"--output {DATA_ROOT}/gold/feature_usage"
        ),
    )

    revenue_metrics_job = BashOperator(
        task_id="revenue_metrics_job",
        bash_command=(
            "spark-submit /opt/airflow/spark_jobs/revenue_metrics_job.py "
            f"--input {DATA_ROOT}/silver/events_clean "
            f"--output {DATA_ROOT}/gold/revenue_metrics"
        ),
    )

    anomaly_detection_job = BashOperator(
        task_id="anomaly_detection_job",
        bash_command=(
            "spark-submit /opt/airflow/spark_jobs/anomaly_detection_job.py "
            f"--daily-metrics {DATA_ROOT}/gold/daily_user_metrics "
            f"--revenue-metrics {DATA_ROOT}/gold/revenue_metrics "
            f"--output {DATA_ROOT}/gold/anomaly_detection"
        ),
    )

    data_quality_checks = PythonOperator(
        task_id="run_data_quality_checks",
        python_callable=run_data_quality_checks,
    )

    refresh_dashboard = PythonOperator(
        task_id="refresh_dashboard_tables",
        python_callable=refresh_dashboard_tables,
    )

    # --- Dependency graph ---
    check_raw_data_available >> bronze_to_silver

    bronze_to_silver >> silver_to_gold_daily_metrics
    bronze_to_silver >> retention_cohort_job
    bronze_to_silver >> funnel_analysis_job
    bronze_to_silver >> feature_usage_job
    bronze_to_silver >> revenue_metrics_job

    [silver_to_gold_daily_metrics, revenue_metrics_job] >> anomaly_detection_job

    [retention_cohort_job, funnel_analysis_job, feature_usage_job, anomaly_detection_job] >> data_quality_checks
    data_quality_checks >> refresh_dashboard
