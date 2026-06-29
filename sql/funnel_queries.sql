-- funnel_queries.sql
-- Works against hive.socialsphere.funnel_metrics (Trino) or the
-- gold_funnel_metrics table in any SQL engine.

-- 1. Latest cohort's full funnel
SELECT funnel_step, step_order, users_reached, conversion_rate, drop_off_rate
FROM funnel_metrics
WHERE event_date = (SELECT MAX(event_date) FROM funnel_metrics)
ORDER BY step_order;

-- 2. Average conversion rate per step, across all cohorts
SELECT funnel_step, step_order, AVG(conversion_rate) AS avg_conversion_rate
FROM funnel_metrics
GROUP BY funnel_step, step_order
ORDER BY step_order;

-- 3. Step with the worst average drop-off (biggest funnel leak)
SELECT funnel_step, step_order, AVG(drop_off_rate) AS avg_drop_off_rate
FROM funnel_metrics
WHERE step_order > 1
GROUP BY funnel_step, step_order
ORDER BY avg_drop_off_rate DESC
LIMIT 5;

-- 4. Signup-to-subscription conversion rate trend over time
SELECT event_date, conversion_rate AS signup_to_subscription_rate
FROM funnel_metrics
WHERE funnel_step = 'subscription_started'
ORDER BY event_date;
