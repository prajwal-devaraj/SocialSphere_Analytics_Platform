"""
Page 5: Revenue Analytics.

Shows total revenue, ARPU, paying users, and subscription starts/cancels
over time.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st
import plotly.express as px
from data_loader import load_gold_table, format_number, format_currency

st.set_page_config(page_title="Revenue", page_icon="💰", layout="wide")
st.title("💰 Revenue Analytics")

revenue = load_gold_table("revenue_metrics")

if revenue.empty:
    st.warning("No revenue_metrics gold table found. Run the pipeline first.")
    st.stop()

revenue = revenue.sort_values("event_date")
latest = revenue.iloc[-1]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Revenue (latest day)", format_currency(latest["total_revenue"]))
col2.metric("Paying Users (latest day)", format_number(latest["paying_users"]))
col3.metric("ARPU (latest day)", format_currency(latest["arpu"]))
col4.metric(
    "Net Subscriptions (latest day)",
    int(latest["subscription_starts"] - latest["subscription_cancellations"]),
)

st.divider()

left, right = st.columns(2)
with left:
    st.subheader("Revenue Trend")
    fig = px.line(revenue, x="event_date", y="total_revenue", markers=True)
    st.plotly_chart(fig, use_container_width=True)

with right:
    st.subheader("ARPU Trend")
    fig2 = px.line(revenue, x="event_date", y="arpu", markers=True)
    st.plotly_chart(fig2, use_container_width=True)

st.subheader("Subscription Starts vs Cancellations")
melted = revenue.melt(
    id_vars=["event_date"], value_vars=["subscription_starts", "subscription_cancellations"],
    var_name="type", value_name="count",
)
fig3 = px.bar(melted, x="event_date", y="count", color="type", barmode="group")
st.plotly_chart(fig3, use_container_width=True)

with st.expander("Raw revenue data"):
    st.dataframe(revenue, use_container_width=True)
