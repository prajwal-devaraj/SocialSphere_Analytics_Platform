# Data Dictionary

## Bronze / Silver: Event Schema

Every event (raw in bronze, cleaned in silver) follows this schema:

| Field | Type | Description |
|---|---|---|
| `event_id` | string | Unique ID for this event. Deduplicated on in bronze->silver. |
| `user_id` | string | The user who triggered the event. Never null in silver. |
| `session_id` | string | Groups events into a single user session. |
| `event_type` | string | One of the 18 valid event types (see below). |
| `event_timestamp` | timestamp | UTC timestamp of the event. Never in the future in silver. |
| `platform` | string | `ios`, `android`, or `web`. Lowercased in silver. |
| `device_type` | string | `mobile`, `desktop`, or `tablet`. Lowercased in silver. |
| `country` | string | ISO-style 2-letter country code. Uppercased in silver. |
| `city` | string | City name. |
| `app_version` | string | Client app version string. |
| `page` | string (nullable) | Page/screen where the event occurred. |
| `feature` | string (nullable) | Specific UI feature interacted with. |
| `post_id` | string (nullable) | Set for post/video engagement events. |
| `creator_id` | string (nullable) | Set for engagement events — the creator of the content engaged with. |
| `duration_seconds` | int (nullable) | Duration relevant to the event (session length, video watch time, etc). |
| `revenue` | float | USD amount. 0.0 for non-revenue events. Never negative in silver. |
| `metadata.experiment_group` | string (nullable) | A/B test bucket. |
| `metadata.referrer` | string (nullable) | What led the user to this event. |
| `event_date` | date | **Silver-only.** Derived from `event_timestamp`; the partition column. |

### Valid Event Types

`user_signup`, `user_login`, `profile_view`, `post_created`, `post_liked`, `post_shared`, `comment_created`, `video_viewed`, `message_sent`, `friend_request_sent`, `ad_impression`, `ad_click`, `purchase_completed`, `subscription_started`, `subscription_cancelled`, `app_error`, `session_started`, `session_ended`

---

## Gold Tables

### `gold_daily_user_metrics`
One row per `event_date`.

| Column | Description |
|---|---|
| `event_date` | Date this row summarizes |
| `daily_active_users` | Distinct users active that day |
| `new_users` | Users whose *first-ever* event was on this date |
| `returning_users` | Active users who had been seen before this date |
| `total_sessions` | Distinct sessions that day |
| `avg_session_duration` | Mean session duration proxy (seconds) |
| `total_events` | Total event count that day |
| `weekly_active_users` | Distinct users active in the trailing 7 days (ending this date) |
| `monthly_active_users` | Distinct users active in the trailing 30 days (ending this date) |
| `dau_mau_stickiness` | `daily_active_users / monthly_active_users` |

### `gold_retention_cohorts`
One row per (`signup_date`, `retention_day`) pair, where `retention_day` is one of 0, 1, 7, 30.

| Column | Description |
|---|---|
| `signup_date` | The cohort's signup date (user's first-ever active date) |
| `retention_day` | Days after signup being measured |
| `cohort_size` | Total users who signed up on `signup_date` |
| `retained_users` | Of those, how many were active exactly `retention_day` days later |
| `retention_rate` | `retained_users / cohort_size` |

### `gold_funnel_metrics`
One row per (`event_date` = cohort signup date, `funnel_step`).

| Column | Description |
|---|---|
| `event_date` | Cohort's signup date |
| `funnel_step` | One of: `user_signup`, `profile_view`, `post_created`, `post_liked`, `subscription_started` |
| `step_order` | 1-5, the position of this step in the funnel |
| `users_reached` | Users from this cohort who reached this step in order (also reached every prior step) |
| `conversion_rate` | `users_reached / cohort_size` (cohort_size = step 1's users_reached) |
| `drop_off_rate` | `1 - (users_reached / previous_step_users_reached)` |

### `gold_feature_usage`
One row per (`event_date`, `feature`).

| Column | Description |
|---|---|
| `event_date` | Date |
| `feature` | Feature name (e.g. `like_button`, `dm_send`) |
| `event_count` | Total events involving this feature that day |
| `unique_users` | Distinct users who used this feature that day |
| `avg_events_per_user` | `event_count / unique_users` |

### `gold_revenue_metrics`
One row per `event_date`.

| Column | Description |
|---|---|
| `event_date` | Date |
| `total_revenue` | Sum of all `revenue` values that day |
| `paying_users` | Distinct users with revenue > 0 that day |
| `arpu` | `total_revenue / daily_active_users` (all active users, not just payers) |
| `subscription_starts` | Count of `subscription_started` events |
| `subscription_cancellations` | Count of `subscription_cancelled` events |

### `gold_anomaly_detection`
One row per (`event_date`, `metric_name`).

| Column | Description |
|---|---|
| `event_date` | Date |
| `metric_name` | One of: `daily_active_users`, `total_events`, `total_revenue` |
| `actual_value` | The metric's real value that day |
| `expected_value` | Trailing rolling-window average (excludes current day) |
| `anomaly_score` | Z-score: `(actual - expected) / rolling_stddev` |
| `is_anomaly` | True if `abs(anomaly_score) > threshold` (default 2.0) |
