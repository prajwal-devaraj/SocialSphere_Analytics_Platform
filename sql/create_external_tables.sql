-- create_external_tables.sql
--
-- DDL for registering the gold Parquet tables as external tables in a
-- Hive Metastore / Trino catalog. NOT required for the MVP stack (the
-- Streamlit dashboard reads Parquet directly via pandas), but included so
-- the "Advanced Version" of this project (Trino + Hive Metastore) has a
-- ready-to-run schema definition.
--
-- Usage (once Trino + Hive Metastore are added to docker-compose.yml):
--   trino --execute "$(cat create_external_tables.sql)"
--   or run statement-by-statement in the Trino CLI / DBeaver.

CREATE SCHEMA IF NOT EXISTS hive.socialsphere
WITH (location = 's3a://socialsphere/gold/');

CREATE TABLE IF NOT EXISTS hive.socialsphere.daily_user_metrics (
    event_date            DATE,
    daily_active_users    BIGINT,
    new_users             BIGINT,
    returning_users       BIGINT,
    total_sessions        BIGINT,
    avg_session_duration  DOUBLE,
    total_events          BIGINT,
    weekly_active_users   BIGINT,
    monthly_active_users  BIGINT,
    dau_mau_stickiness    DOUBLE
)
WITH (
    external_location = 's3a://socialsphere/gold/daily_user_metrics/',
    format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS hive.socialsphere.feature_usage (
    event_date           DATE,
    feature               VARCHAR,
    event_count           BIGINT,
    unique_users           BIGINT,
    avg_events_per_user    DOUBLE
)
WITH (
    external_location = 's3a://socialsphere/gold/feature_usage/',
    format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS hive.socialsphere.retention_cohorts (
    signup_date     DATE,
    retention_day   INTEGER,
    cohort_size     BIGINT,
    retained_users  BIGINT,
    retention_rate  DOUBLE
)
WITH (
    external_location = 's3a://socialsphere/gold/retention_cohorts/',
    format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS hive.socialsphere.funnel_metrics (
    event_date        DATE,
    funnel_step        VARCHAR,
    step_order          INTEGER,
    users_reached       BIGINT,
    conversion_rate     DOUBLE,
    drop_off_rate       DOUBLE
)
WITH (
    external_location = 's3a://socialsphere/gold/funnel_metrics/',
    format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS hive.socialsphere.revenue_metrics (
    event_date                  DATE,
    total_revenue                 DOUBLE,
    paying_users                  BIGINT,
    arpu                            DOUBLE,
    subscription_starts             BIGINT,
    subscription_cancellations      BIGINT
)
WITH (
    external_location = 's3a://socialsphere/gold/revenue_metrics/',
    format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS hive.socialsphere.anomaly_detection (
    event_date       DATE,
    metric_name        VARCHAR,
    actual_value         DOUBLE,
    expected_value       DOUBLE,
    anomaly_score        DOUBLE,
    is_anomaly           BOOLEAN
)
WITH (
    external_location = 's3a://socialsphere/gold/anomaly_detection/',
    format = 'PARQUET'
);
