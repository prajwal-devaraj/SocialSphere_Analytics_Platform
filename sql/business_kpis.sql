-- business_kpis.sql
-- Cross-table queries combining daily_user_metrics, revenue_metrics, and
-- feature_usage into the "executive summary" view -- the kind of single
-- query a product manager would actually ask for.

-- 1. Daily business summary: DAU, revenue, ARPU, stickiness in one row per day
SELECT
    d.event_date,
    d.daily_active_users,
    d.monthly_active_users,
    d.dau_mau_stickiness,
    r.total_revenue,
    r.arpu,
    r.paying_users,
    r.subscription_starts,
    r.subscription_cancellations
FROM daily_user_metrics d
LEFT JOIN revenue_metrics r ON d.event_date = r.event_date
ORDER BY d.event_date DESC;

-- 2. Feature adoption rate (unique users on a feature / DAU that day)
SELECT
    f.event_date,
    f.feature,
    f.unique_users,
    d.daily_active_users,
    f.unique_users * 1.0 / d.daily_active_users AS feature_adoption_rate
FROM feature_usage f
JOIN daily_user_metrics d ON f.event_date = d.event_date
ORDER BY f.event_date DESC, feature_adoption_rate DESC;

-- 3. Churn proxy: users active 7-13 days ago who have NOT been active in the last 7 days
-- (Illustrative -- requires raw/silver event_id-level data, included here as a
--  reference query for when this project adds a dedicated churn job.)
-- SELECT user_id
-- FROM events_clean
-- WHERE user_id IN (
--     SELECT DISTINCT user_id FROM events_clean
--     WHERE event_date BETWEEN DATE '2026-06-08' AND DATE '2026-06-14'
-- )
-- AND user_id NOT IN (
--     SELECT DISTINCT user_id FROM events_clean
--     WHERE event_date BETWEEN DATE '2026-06-15' AND DATE '2026-06-21'
-- );

-- 4. Revenue per active user vs. revenue per paying user (ARPU vs ARPPU)
SELECT
    event_date,
    total_revenue,
    paying_users,
    total_revenue / NULLIF(paying_users, 0) AS arppu,
    arpu
FROM revenue_metrics
ORDER BY event_date DESC;
