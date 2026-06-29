"""
Freshness-focused data quality checks: is the data current, did it arrive
on schedule, has volume dropped unexpectedly. These are the checks most
relevant to catching a broken/stalled pipeline (vs. a malformed-data
problem, which schema_checks.py covers).
"""

from validation_rules import (
    check_timestamps_not_future,
    check_event_volume_not_unusually_low,
    check_required_partitions_exist,
    _get_spark,
)


def run_freshness_checks(silver_path: str) -> dict:
    spark = _get_spark()
    df = spark.read.parquet(silver_path)

    checks = [
        check_timestamps_not_future(df),
        check_event_volume_not_unusually_low(df),
        check_required_partitions_exist(silver_path),
    ]
    passed_count = sum(1 for c in checks if c["passed"])
    return {"checks": checks, "summary": f"{passed_count}/{len(checks)} freshness checks passed"}


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-path", required=True)
    args = parser.parse_args()
    print(json.dumps(run_freshness_checks(args.silver_path), indent=2))
