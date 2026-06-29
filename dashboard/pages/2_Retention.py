"""
Page 2: Retention.

Shows Day 1 / Day 7 / Day 30 retention rates and a cohort heatmap.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st
import plotly.express as px
from data_loader import load_gold_table, format_percent

st.set_page_config(page_title="Retention", page_icon="🔁", layout="wide")
st.title("🔁 Retention Cohorts")

retention = load_gold_table("retention_cohorts")

if retention.empty:
    st.warning("No retention_cohorts gold table found. Run the pipeline first.")
    st.stop()

retention = retention.sort_values(["signup_date", "retention_day"])

# --- Overall averages by retention_day ---
overall = retention.groupby("retention_day").apply(
    lambda g: (g["retained_users"].sum() / g["cohort_size"].sum()) if g["cohort_size"].sum() > 0 else 0
).reset_index(name="avg_retention_rate")

day_labels = {0: "Day 0 (signup)", 1: "Day 1", 7: "Day 7", 30: "Day 30"}
cols = st.columns(len(overall))
for i, row in overall.iterrows():
    label = day_labels.get(int(row["retention_day"]), f"Day {int(row['retention_day'])}")
    cols[i % len(cols)].metric(label, format_percent(row["avg_retention_rate"]))

st.divider()

st.subheader("Retention Heatmap by Signup Cohort")
heatmap_data = retention.pivot_table(
    index="signup_date", columns="retention_day", values="retention_rate", aggfunc="mean"
)
heatmap_data.columns = [day_labels.get(int(c), f"Day {c}") for c in heatmap_data.columns]

fig = px.imshow(
    heatmap_data,
    labels=dict(x="Retention Period", y="Signup Cohort", color="Retention Rate"),
    color_continuous_scale="Blues",
    aspect="auto",
)
st.plotly_chart(fig, use_container_width=True)

st.subheader("Retention Rate Trend by Cohort")
fig2 = px.line(
    retention, x="signup_date", y="retention_rate", color="retention_day",
    markers=True, labels={"retention_day": "Retention Day"},
)
st.plotly_chart(fig2, use_container_width=True)

with st.expander("Raw retention data"):
    st.dataframe(retention, use_container_width=True)
