"""
Page 1: Product KPIs.

Shows DAU, MAU, DAU/MAU stickiness, total events, average session
duration, total revenue, and conversion rate over time.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st
import plotly.express as px
from data_loader import load_gold_table, format_number, format_currency, format_percent

st.set_page_config(page_title="Product KPIs", page_icon="📈", layout="wide")
st.title("📈 Product KPIs")

daily = load_gold_table("daily_user_metrics")
revenue = load_gold_table("revenue_metrics")
funnel = load_gold_table("funnel_metrics")

if daily.empty:
    st.warning("No daily_user_metrics gold table found. Run the pipeline first.")
    st.stop()

daily = daily.sort_values("event_date")
latest = daily.iloc[-1]

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("DAU", format_number(latest["daily_active_users"]))
col2.metric("WAU", format_number(latest.get("weekly_active_users", 0)))
col3.metric("MAU", format_number(latest.get("monthly_active_users", 0)))
col4.metric("DAU/MAU Stickiness", format_percent(latest.get("dau_mau_stickiness", 0)))
col5.metric("Avg Session Duration", f"{latest.get('avg_session_duration', 0):.0f}s")

st.divider()

left, right = st.columns(2)

with left:
    st.subheader("DAU / WAU / MAU Trend")
    melted = daily.melt(
        id_vars=["event_date"],
        value_vars=["daily_active_users", "weekly_active_users", "monthly_active_users"],
        var_name="metric", value_name="users",
    )
    fig = px.line(melted, x="event_date", y="users", color="metric", markers=True)
    st.plotly_chart(fig, use_container_width=True)

with right:
    st.subheader("Total Events per Day")
    fig2 = px.bar(daily, x="event_date", y="total_events")
    st.plotly_chart(fig2, use_container_width=True)

left2, right2 = st.columns(2)

with left2:
    st.subheader("New vs Returning Users")
    melted2 = daily.melt(
        id_vars=["event_date"], value_vars=["new_users", "returning_users"],
        var_name="user_type", value_name="count",
    )
    fig3 = px.bar(melted2, x="event_date", y="count", color="user_type", barmode="stack")
    st.plotly_chart(fig3, use_container_width=True)

with right2:
    if not revenue.empty:
        st.subheader("Revenue Trend")
        fig4 = px.line(revenue.sort_values("event_date"), x="event_date", y="total_revenue", markers=True)
        st.plotly_chart(fig4, use_container_width=True)
    else:
        st.info("No revenue data available yet.")

if not funnel.empty:
    st.divider()
    st.subheader("Latest Funnel Conversion (Signup → Subscription)")
    latest_cohort_date = funnel["event_date"].max()
    latest_funnel = funnel[funnel["event_date"] == latest_cohort_date].sort_values("step_order")
    fig5 = px.funnel(latest_funnel, x="users_reached", y="funnel_step")
    st.plotly_chart(fig5, use_container_width=True)
    st.caption(f"Cohort: users who signed up on {latest_cohort_date.date()}")
