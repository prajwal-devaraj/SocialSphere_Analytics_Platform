"""
Configuration and reference data for the synthetic event generator.
Centralizing this makes it easy to tweak realism without touching logic.
"""

import os

# ---- Output ----
OUTPUT_DIR = os.environ.get("EVENT_OUTPUT_DIR", "./data/bronze_local")
DEFAULT_NUM_EVENTS = 100_000
DEFAULT_NUM_USERS = 10_000
EVENTS_PER_FILE = 50_000  # chunk output files so they stay manageable

# ---- Time window for generated data ----
DAYS_OF_HISTORY = 60  # generate events spread across the last N days

# ---- Reference dimensions ----
COUNTRIES = ["US", "IN", "GB", "CA", "DE", "BR", "AU", "FR", "JP", "MX"]

CITIES_BY_COUNTRY = {
    "US": ["New York", "Los Angeles", "Chicago", "Austin", "Seattle"],
    "IN": ["Bangalore", "Mumbai", "Delhi", "Hyderabad", "Pune"],
    "GB": ["London", "Manchester", "Birmingham"],
    "CA": ["Toronto", "Vancouver", "Montreal"],
    "DE": ["Berlin", "Munich", "Hamburg"],
    "BR": ["Sao Paulo", "Rio de Janeiro"],
    "AU": ["Sydney", "Melbourne"],
    "FR": ["Paris", "Lyon"],
    "JP": ["Tokyo", "Osaka"],
    "MX": ["Mexico City", "Guadalajara"],
}

PLATFORMS = ["ios", "android", "web"]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
APP_VERSIONS = ["2.2.0", "2.3.0", "2.3.5", "2.4.0", "2.4.1"]
EXPERIMENT_GROUPS = ["A", "B", "control"]
REFERRERS = ["notification", "search", "home_feed", "direct", "ad_click", None]

PAGES = ["home_feed", "profile", "explore", "video_player", "messages", "settings", "checkout"]

FEATURES_BY_PAGE = {
    "home_feed": ["like_button", "comment_box", "share_button", "infinite_scroll"],
    "profile": ["edit_profile", "follow_button", "bio_section"],
    "explore": ["search_bar", "trending_tab", "hashtag_click"],
    "video_player": ["autoplay", "video_like", "video_comment", "video_share"],
    "messages": ["dm_send", "group_chat", "voice_message"],
    "settings": ["privacy_settings", "notification_settings"],
    "checkout": ["subscription_checkout", "payment_method"],
}

# Event type -> relative weight (how often it occurs). Tune to make
# common events (views/likes) far more frequent than rare ones (purchases).
EVENT_TYPE_WEIGHTS = {
    "session_started": 12,
    "session_ended": 12,
    "profile_view": 8,
    "post_created": 4,
    "post_liked": 18,
    "post_shared": 5,
    "comment_created": 7,
    "video_viewed": 15,
    "message_sent": 6,
    "friend_request_sent": 3,
    "ad_impression": 10,
    "ad_click": 2,
    "purchase_completed": 1,
    "subscription_started": 1,
    "subscription_cancelled": 0.5,
    "app_error": 1,
    "user_login": 9,
    "user_signup": 0.8,  # rare relative to total volume, common per-new-user
}

# Funnel-relevant event order (used by event_patterns / funnel job downstream)
FUNNEL_STEPS = [
    "user_signup",
    "profile_view",       # proxy for "profile_completed"
    "post_created",       # proxy for "first_post"
    "post_liked",         # proxy for "first_engagement"
    "subscription_started",
]

# Revenue-bearing event types and a realistic price range (USD)
REVENUE_EVENT_TYPES = {
    "purchase_completed": (0.99, 49.99),
    "subscription_started": (4.99, 19.99),
}

RANDOM_SEED = 42
