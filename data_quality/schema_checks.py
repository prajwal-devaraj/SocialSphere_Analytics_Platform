"""
Schema-focused data quality checks: structural correctness of the silver
table (required fields present, event_type vocabulary, value ranges).

This module deliberately only imports from validation_rules rather than
duplicating logic, so there's a single source of truth for each check.
Use this when you want to run *only* schema checks (e.g. right after
bronze_to_silver, before the more expensive volume/freshness checks).
"""

from validation_rules import (
    check_no_null_event_ids,
    check_no_duplicate_event_ids,
    check_valid_event_types_only,
    check_revenue_not_negative,
    _get_spark,
)


def run_schema_checks(silver_path: str) -> dict:
    spark = _get_spark()
    df = spark.read.parquet(silver_path)

    checks = [
        check_no_null_event_ids(df),
        check_no_duplicate_event_ids(df),
        check_valid_event_types_only(df),
        check_revenue_not_negative(df),
    ]
    passed_count = sum(1 for c in checks if c["passed"])
    return {"checks": checks, "summary": f"{passed_count}/{len(checks)} schema checks passed"}


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-path", required=True)
    args = parser.parse_args()
    print(json.dumps(run_schema_checks(args.silver_path), indent=2))
