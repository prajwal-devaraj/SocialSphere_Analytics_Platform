"""
Generates realistic event sequences for a single user-day: sessions, the
events within each session (weighted by EVENT_TYPE_WEIGHTS), and revenue
events. Designed so downstream funnel/retention/revenue jobs have a
plausible, non-random signal to recover.
"""

import random
import uuid
from datetime import timedelta

from config import (
    EVENT_TYPE_WEIGHTS,
    PAGES,
    FEATURES_BY_PAGE,
    APP_VERSIONS,
    EXPERIMENT_GROUPS,
    REFERRERS,
    REVENUE_EVENT_TYPES,
)

_EVENT_TYPES = list(EVENT_TYPE_WEIGHTS.keys())
_EVENT_WEIGHTS = list(EVENT_TYPE_WEIGHTS.values())


def _new_event_id() -> str:
    return f"evt_{uuid.uuid4().hex[:16]}"


def _new_session_id() -> str:
    return f"sess_{uuid.uuid4().hex[:12]}"


def _pick_event_type() -> str:
    return random.choices(_EVENT_TYPES, weights=_EVENT_WEIGHTS, k=1)[0]


def _revenue_for(event_type: str) -> float:
    if event_type in REVENUE_EVENT_TYPES:
        lo, hi = REVENUE_EVENT_TYPES[event_type]
        return round(random.uniform(lo, hi), 2)
    return 0.0


def generate_session_events(user, session_start, is_first_day: bool, app_version=None):
    """
    Build the list of events for one session for one user, returned as plain
    dicts matching the platform event schema.
    """
    session_id = _new_session_id()
    num_events = max(1, int(random.gauss(6, 3)))  # avg ~6 events per session
    events = []
    current_time = session_start
    app_version = app_version or random.choice(APP_VERSIONS)
    experiment_group = random.choice(EXPERIMENT_GROUPS)

    # Force a signup event for brand-new users on their first session
    event_sequence = []
    if is_first_day and random.random() < 0.9:
        event_sequence.append("user_signup")
    event_sequence.append("session_started")

    for _ in range(num_events):
        event_sequence.append(_pick_event_type())
    event_sequence.append("session_ended")

    for event_type in event_sequence:
        page = random.choice(PAGES)
        feature = random.choice(FEATURES_BY_PAGE.get(page, ["unknown"]))
        duration = max(1, int(random.gauss(20, 12)))
        current_time = current_time + timedelta(seconds=random.randint(1, 45))

        event = {
            "event_id": _new_event_id(),
            "user_id": user.user_id,
            "session_id": session_id,
            "event_type": event_type,
            "event_timestamp": current_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "platform": user.platform,
            "device_type": user.device_type,
            "country": user.country,
            "city": user.city,
            "app_version": app_version,
            "page": page,
            "feature": feature,
            "post_id": f"post_{random.randint(1, 50000)}" if event_type in
                       ("post_liked", "post_shared", "comment_created", "video_viewed") else None,
            "creator_id": f"user_{random.randint(1, 9999999):07d}" if event_type in
                          ("post_liked", "post_shared", "video_viewed") else None,
            "duration_seconds": duration,
            "revenue": _revenue_for(event_type),
            "metadata": {
                "experiment_group": experiment_group,
                "referrer": random.choice(REFERRERS),
            },
        }
        events.append(event)

    return events


def inject_bad_records(events: list, bad_rate: float = 0.01) -> list:
    """
    Intentionally corrupt a small percentage of events so the
    bronze->silver Spark job has real data-quality problems to clean:
    missing user_id, null event_id duplicates, bad timestamps, negative
    revenue, junk event_type, etc. Mirrors real-world pipeline conditions.
    """
    corrupted = []
    for e in events:
        if random.random() < bad_rate:
            issue = random.choice(
                ["missing_user_id", "bad_timestamp", "negative_revenue", "duplicate", "bad_event_type"]
            )
            e = dict(e)  # copy
            if issue == "missing_user_id":
                e["user_id"] = None
            elif issue == "bad_timestamp":
                e["event_timestamp"] = "not-a-timestamp"
            elif issue == "negative_revenue":
                e["revenue"] = -round(random.uniform(1, 20), 2)
            elif issue == "bad_event_type":
                e["event_type"] = "unknown_event_xyz"
            elif issue == "duplicate":
                corrupted.append(e)  # add once now, again below -> creates a duplicate event_id
        corrupted.append(e)
    return corrupted
