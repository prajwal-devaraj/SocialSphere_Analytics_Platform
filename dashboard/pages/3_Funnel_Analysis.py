"""
Page 3: Funnel Analysis.

Shows the signup -> profile_view -> post_created -> post_liked ->
subscription_started funnel, with a cohort-date selector.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st
import plotly.express as px
from data_loader import load_gold_table, format_percent

st.set_page_config(page_title="Funnel Analysis", page_icon="🔻", layout="wide")
st.title("🔻 Funnel Analysis")

funnel = load_gold_table("funnel_metrics")

if funnel.empty:
    st.warning("No funnel_metrics gold table found. Run the pipeline first.")
    st.stop()

funnel = funnel.sort_values(["event_date", "step_order"])
cohort_dates = sorted(funnel["event_date"].unique(), reverse=True)

selected_date = st.selectbox(
    "Signup cohort date", cohort_dates, format_func=lambda d: d.strftime("%Y-%m-%d")
)

cohort_funnel = funnel[funnel["event_date"] == selected_date].sort_values("step_order")

col1, col2 = st.columns([2, 1])

with col1:
    fig = px.funnel(cohort_funnel, x="users_reached", y="funnel_step")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Step Conversion")
    for _, row in cohort_funnel.iterrows():
        st.metric(
            row["funnel_step"],
            f"{int(row['users_reached'])} users",
            f"{format_percent(row['conversion_rate'])} of cohort",
        )

st.divider()
st.subheader("Conversion Rate Trend by Step (all cohorts)")
fig2 = px.line(
    funnel, x="event_date", y="conversion_rate", color="funnel_step", markers=True,
)
st.plotly_chart(fig2, use_container_width=True)

st.subheader("Drop-off Rate by Step (all cohorts)")
fig3 = px.bar(
    funnel[funnel["step_order"] > 1], x="event_date", y="drop_off_rate", color="funnel_step", barmode="group",
)
st.plotly_chart(fig3, use_container_width=True)

with st.expander("Raw funnel data"):
    st.dataframe(funnel, use_container_width=True)
