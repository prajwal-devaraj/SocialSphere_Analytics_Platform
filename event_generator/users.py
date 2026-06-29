"""
Generates a synthetic user population with realistic behavioral attributes:
signup dates, country/device affinity, and a "churn propensity" that drives
whether a user keeps showing up in later sessions (so retention/funnel
analytics downstream actually have signal, not pure noise).
"""

import random
from datetime import datetime, timedelta, UTC
from dataclasses import dataclass, field

from config import (
    COUNTRIES,
    CITIES_BY_COUNTRY,
    PLATFORMS,
    DEVICE_TYPES,
    DAYS_OF_HISTORY,
)


@dataclass
class SyntheticUser:
    user_id: str
    signup_date: datetime
    country: str
    city: str
    platform: str
    device_type: str
    churn_propensity: float  # 0.0 = sticky/loyal, 1.0 = churns almost immediately
    is_paying: bool
    creator_weight: float = field(default=0.0)  # how often this user is a content creator


def generate_users(num_users: int, days_of_history: int = DAYS_OF_HISTORY, seed: int = None) -> list:
    """
    Create `num_users` synthetic users with a signup date spread across the
    history window, weighted toward more recent signups (mimics real growth).
    """
    if seed is not None:
        random.seed(seed)

    now = datetime.now(UTC).replace(tzinfo=None)
    users = []

    for i in range(num_users):
        # Skew signups toward more recent days using a simple power-law-ish weighting
        days_ago = int(random.triangular(0, days_of_history, 0))  # mode=0 -> recent-heavy
        signup_date = now - timedelta(days=days_ago, hours=random.randint(0, 23))

        country = random.choice(COUNTRIES)
        city = random.choice(CITIES_BY_COUNTRY[country])
        platform = random.choices(PLATFORMS, weights=[0.45, 0.45, 0.10])[0]
        device_type = "mobile" if platform in ("ios", "android") else random.choice(DEVICE_TYPES)

        # Most users are low-to-moderate churn risk; a tail is very high risk.
        churn_propensity = min(1.0, max(0.0, random.betavariate(2, 5)))

        is_paying = random.random() < 0.08  # ~8% conversion to paid
        creator_weight = random.random() if random.random() < 0.05 else 0.0  # ~5% are creators

        users.append(
            SyntheticUser(
                user_id=f"user_{i+1:07d}",
                signup_date=signup_date,
                country=country,
                city=city,
                platform=platform,
                device_type=device_type,
                churn_propensity=churn_propensity,
                is_paying=is_paying,
                creator_weight=creator_weight,
            )
        )

    return users


def is_active_on_day(user: SyntheticUser, day: datetime) -> bool:
    """
    Decide whether a user is active on a given day, based on tenure and churn
    propensity. Activity probability decays over time for high-churn users,
    stays roughly flat for sticky users.
    """
    if day < user.signup_date:
        return False

    tenure_days = (day - user.signup_date).days
    # Sticky users (low churn_propensity) stay near a high baseline activity rate.
    # High-churn users decay fast toward near-zero activity.
    base_rate = 0.55 * (1 - user.churn_propensity) + 0.05
    decay = max(0.0, 1 - (user.churn_propensity * tenure_days / 30))
    activity_prob = base_rate * decay
    return random.random() < max(activity_prob, 0.01)
