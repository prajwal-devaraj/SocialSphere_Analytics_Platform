"""
Main CLI entry point for the synthetic event generator.

Usage:
    python generate_events.py --events 1000000 --users 50000 --output ./data/bronze_local

Writes newline-delimited JSON (NDJSON) files partitioned by date into:
    <output>/year=YYYY/month=MM/day=DD/part-XXXXX.json

This mirrors how the FastAPI ingestion service and Spark jobs expect bronze
data to be laid out, so files generated here can be dropped straight into
the same folder MinIO/bronze would use, or uploaded to MinIO directly.
"""

import argparse
import json
import os
import random
import time
from collections import defaultdict
from datetime import datetime, timedelta, UTC

from config import (
    DEFAULT_NUM_EVENTS,
    DEFAULT_NUM_USERS,
    OUTPUT_DIR,
    EVENTS_PER_FILE,
    DAYS_OF_HISTORY,
    RANDOM_SEED,
)
from users import generate_users, is_active_on_day
from events import generate_session_events, inject_bad_records


def parse_args():
    parser = argparse.ArgumentParser(description="SocialSphere synthetic event generator")
    parser.add_argument("--events", type=int, default=DEFAULT_NUM_EVENTS, help="Approximate total number of events to generate")
    parser.add_argument("--users", type=int, default=DEFAULT_NUM_USERS, help="Number of synthetic users to create")
    parser.add_argument("--output", type=str, default=OUTPUT_DIR, help="Output directory for partitioned NDJSON files")
    parser.add_argument("--days", type=int, default=DAYS_OF_HISTORY, help="Number of days of history to spread events across")
    parser.add_argument("--bad-rate", type=float, default=0.01, help="Fraction of events to intentionally corrupt (data quality testing)")
    parser.add_argument("--seed", type=int, default=RANDOM_SEED, help="Random seed for reproducibility")
    return parser.parse_args()


def partition_path(base_dir: str, day: datetime) -> str:
    return os.path.join(
        base_dir,
        f"year={day.year:04d}",
        f"month={day.month:02d}",
        f"day={day.day:02d}",
    )


def write_partition_files(events_by_day: dict, base_dir: str, events_per_file: int):
    total_written = 0
    for day, day_events in events_by_day.items():
        out_dir = partition_path(base_dir, day)
        os.makedirs(out_dir, exist_ok=True)

        for i in range(0, len(day_events), events_per_file):
            chunk = day_events[i:i + events_per_file]
            part_num = i // events_per_file
            file_path = os.path.join(out_dir, f"part-{part_num:05d}.json")
            with open(file_path, "w") as f:
                for ev in chunk:
                    f.write(json.dumps(ev) + "\n")
            total_written += len(chunk)
    return total_written


def main():
    args = parse_args()
    random.seed(args.seed)

    print(f"[SocialSphere Generator] target_events={args.events} users={args.users} days={args.days} output={args.output}")
    start = time.time()

    users = generate_users(args.users, days_of_history=args.days, seed=args.seed)
    print(f"[SocialSphere Generator] generated {len(users)} synthetic users")

    now = datetime.now(UTC).replace(tzinfo=None)
    all_days = [now - timedelta(days=d) for d in range(args.days)]

    events_by_day = defaultdict(list)
    total_events = 0
    target = args.events

    # Walk backwards from most recent day so we stop as soon as we hit target,
    # keeping the most recent (most relevant) data if the user requests fewer events.
    for day in sorted(all_days, key=lambda d: d, reverse=True):
        if total_events >= target:
            break
        day_key = day.replace(hour=0, minute=0, second=0, microsecond=0)

        for user in users:
            if total_events >= target:
                break
            if not is_active_on_day(user, day):
                continue

            is_first_day = day_key.date() == user.signup_date.date()
            num_sessions = 1 if random.random() < 0.7 else 2  # most users: 1 session/day, some 2
            for _ in range(num_sessions):
                session_start = day_key + timedelta(
                    hours=random.randint(0, 23), minutes=random.randint(0, 59)
                )
                session_events = generate_session_events(user, session_start, is_first_day)
                events_by_day[day_key].extend(session_events)
                total_events += len(session_events)

    # Inject realistic bad records for data-quality testing downstream
    for day_key in list(events_by_day.keys()):
        events_by_day[day_key] = inject_bad_records(events_by_day[day_key], bad_rate=args.bad_rate)

    written = write_partition_files(events_by_day, args.output, EVENTS_PER_FILE)
    elapsed = time.time() - start

    print(f"[SocialSphere Generator] wrote {written} events across {len(events_by_day)} day-partitions")
    print(f"[SocialSphere Generator] output directory: {os.path.abspath(args.output)}")
    print(f"[SocialSphere Generator] done in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
