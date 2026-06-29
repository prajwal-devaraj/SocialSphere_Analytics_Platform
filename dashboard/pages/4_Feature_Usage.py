"""
Page 4: Feature Usage.

Shows top features by usage, unique users per feature, and trend over
time for a selected feature.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st
import plotly.express as px
from data_loader import load_gold_table

st.set_page_config(page_title="Feature Usage", page_icon="🧩", layout="wide")
st.title("🧩 Feature Usage")

feature_usage = load_gold_table("feature_usage")

if feature_usage.empty:
    st.warning("No feature_usage gold table found. Run the pipeline first.")
    st.stop()

feature_usage = feature_usage.sort_values("event_date")

st.subheader("Top Features (all time)")
totals = (
    feature_usage.groupby("feature")
    .agg(total_events=("event_count", "sum"), total_unique_users=("unique_users", "sum"))
    .sort_values("total_events", ascending=False)
    .reset_index()
)

col1, col2 = st.columns(2)
with col1:
    fig = px.bar(totals.head(15), x="feature", y="total_events")
    st.plotly_chart(fig, use_container_width=True)
with col2:
    fig2 = px.bar(totals.head(15), x="feature", y="total_unique_users")
    st.plotly_chart(fig2, use_container_width=True)

st.divider()

selected_feature = st.selectbox("Select a feature to see its trend", totals["feature"].tolist())
feature_trend = feature_usage[feature_usage["feature"] == selected_feature]

fig3 = px.line(
    feature_trend, x="event_date", y=["event_count", "unique_users"], markers=True,
    labels={"value": "count", "variable": "metric"},
)
st.plotly_chart(fig3, use_container_width=True)

with st.expander("Raw feature usage data"):
    st.dataframe(feature_usage, use_container_width=True)
