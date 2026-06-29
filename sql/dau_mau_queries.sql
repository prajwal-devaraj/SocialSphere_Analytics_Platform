-- dau_mau_queries.sql
-- Works against hive.socialsphere.daily_user_metrics (Trino) or any SQL
-- engine pointed at the gold_daily_user_metrics table (Spark SQL, DuckDB,
-- Postgres after loading).

-- 1. Latest DAU / WAU / MAU snapshot
SELECT event_date, daily_active_users, weekly_active_users, monthly_active_users, dau_mau_stickiness
FROM daily_user_metrics
ORDER BY event_date DESC
LIMIT 1;

-- 2. 30-day DAU trend
SELECT event_date, daily_active_users
FROM daily_user_metrics
ORDER BY event_date DESC
LIMIT 30;

-- 3. DAU/MAU stickiness trend (engagement health over time)
SELECT event_date, dau_mau_stickiness
FROM daily_user_metrics
ORDER BY event_date ASC;

-- 4. New vs returning users over the last 14 days
SELECT event_date, new_users, returning_users,
       new_users + returning_users AS total_active_users
FROM daily_user_metrics
ORDER BY event_date DESC
LIMIT 14;

-- 5. Week-over-week DAU growth rate
SELECT
    event_date,
    daily_active_users,
    LAG(daily_active_users, 7) OVER (ORDER BY event_date) AS dau_7_days_ago,
    (daily_active_users - LAG(daily_active_users, 7) OVER (ORDER BY event_date))
        / CAST(LAG(daily_active_users, 7) OVER (ORDER BY event_date) AS DOUBLE) AS wow_growth_rate
FROM daily_user_metrics
ORDER BY event_date;
