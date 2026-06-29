"""
Shared data-loading utilities for the SocialSphere Streamlit dashboard.
All pages import from here so there's one place that knows how to read
gold Parquet tables (and one place to swap in a Trino/Postgres connection
later without touching every page).
"""

import os
import pandas as pd
import streamlit as st

GOLD_ROOT = os.environ.get("GOLD_DATA_ROOT", "./data/gold_local")


@st.cache_data(ttl=300)
def load_gold_table(table_name: str) -> pd.DataFrame:
    """
    Loads a gold-layer table by name (e.g. 'daily_user_metrics') from
    Parquet. Cached for 5 minutes so repeated page interactions (filter
    changes, tab switches) don't re-read from disk every time.
    """
    path = os.path.join(GOLD_ROOT, table_name)
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        df = pd.read_parquet(path)
        if "event_date" in df.columns:
            df["event_date"] = pd.to_datetime(df["event_date"])
        if "signup_date" in df.columns:
            df["signup_date"] = pd.to_datetime(df["signup_date"])
        return df
    except Exception as e:
        st.error(f"Could not load gold table '{table_name}': {e}")
        return pd.DataFrame()


def format_number(n) -> str:
    if pd.isna(n):
        return "—"
    n = float(n)
    if abs(n) >= 1_000_000:
        return f"{n / 1_000_000:.2f}M"
    if abs(n) >= 1_000:
        return f"{n / 1_000:.1f}K"
    return f"{n:.0f}"


def format_currency(n) -> str:
    if pd.isna(n):
        return "—"
    return f"${n:,.2f}"


def format_percent(n) -> str:
    if pd.isna(n):
        return "—"
    return f"{n * 100:.1f}%"
