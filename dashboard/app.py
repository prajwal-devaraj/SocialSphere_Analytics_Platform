"""
SocialSphere Analytics Platform - Streamlit Dashboard (entry point).

Run with:
    streamlit run app.py

Pages live in pages/ and are auto-discovered by Streamlit's multi-page
app mechanism (the numeric prefix controls sidebar ordering).
"""

import streamlit as st
from data_loader import load_gold_table, format_number, format_currency

st.set_page_config(
    page_title="SocialSphere Analytics Platform",
    page_icon="📊",
    layout="wide",
)

st.title("📊 SocialSphere Analytics Platform")
st.caption("Large-scale product analytics data warehouse — synthetic social-media event data")

st.markdown(
    """
    Welcome to the SocialSphere internal analytics dashboard. Use the pages in the
    sidebar to explore:

    - **Product KPIs** — DAU/MAU, sessions, revenue at a glance
    - **Retention** — cohort retention heatmap (Day 1 / 7 / 30)
    - **Funnel Analysis** — signup → engagement → subscription conversion
    - **Feature Usage** — which features drive the most engagement
    - **Revenue** — ARPU, subscriptions, revenue trend
    - **Anomaly Detection** — traffic and revenue anomalies flagged by the pipeline
    """
)

st.divider()

daily = load_gold_table("daily_user_metrics")
revenue = load_gold_table("revenue_metrics")

if daily.empty:
    st.warning(
        "No gold data found yet. Run the event generator + Spark pipeline first, "
        "or check that GOLD_DATA_ROOT points to the right directory."
    )
else:
    latest = daily.sort_values("event_date").iloc[-1]
    latest_rev = revenue.sort_values("event_date").iloc[-1] if not revenue.empty else None

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Daily Active Users", format_number(latest["daily_active_users"]))
    col2.metric("Monthly Active Users", format_number(latest.get("monthly_active_users", 0)))
    col3.metric("Total Events (latest day)", format_number(latest["total_events"]))
    col4.metric(
        "Total Revenue (latest day)",
        format_currency(latest_rev["total_revenue"]) if latest_rev is not None else "—",
    )

    st.caption(f"Latest data: {latest['event_date'].date()}")
