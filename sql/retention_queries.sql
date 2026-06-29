-- retention_queries.sql
-- Works against hive.socialsphere.retention_cohorts (Trino) or the
-- gold_retention_cohorts table in any SQL engine.

-- 1. Average Day 1 / Day 7 / Day 30 retention across all cohorts
SELECT
    retention_day,
    SUM(retained_users) AS total_retained,
    SUM(cohort_size) AS total_cohort_size,
    SUM(retained_users) * 1.0 / SUM(cohort_size) AS avg_retention_rate
FROM retention_cohorts
GROUP BY retention_day
ORDER BY retention_day;

-- 2. Retention rate by individual signup cohort, Day 7 only
SELECT signup_date, cohort_size, retained_users, retention_rate
FROM retention_cohorts
WHERE retention_day = 7
ORDER BY signup_date DESC;

-- 3. Cohorts with the worst Day 1 retention (candidates for onboarding investigation)
SELECT signup_date, cohort_size, retention_rate
FROM retention_cohorts
WHERE retention_day = 1
ORDER BY retention_rate ASC
LIMIT 10;

-- 4. Retention trend over time, pivoted by retention_day (for charting)
SELECT
    signup_date,
    MAX(CASE WHEN retention_day = 1 THEN retention_rate END) AS day_1_retention,
    MAX(CASE WHEN retention_day = 7 THEN retention_rate END) AS day_7_retention,
    MAX(CASE WHEN retention_day = 30 THEN retention_rate END) AS day_30_retention
FROM retention_cohorts
GROUP BY signup_date
ORDER BY signup_date;
