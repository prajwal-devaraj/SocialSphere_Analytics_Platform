"""
Page 6: Anomaly Detection.

Shows flagged anomalies in DAU, total events, and revenue, plus a chart
overlaying actual vs expected values so spikes/drops are visually obvious.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st
import plotly.graph_objects as go
from data_loader import load_gold_table

st.set_page_config(page_title="Anomaly Detection", page_icon="🚨", layout="wide")
st.title("🚨 Anomaly Detection")

anomalies = load_gold_table("anomaly_detection")

if anomalies.empty:
    st.warning("No anomaly_detection gold table found. Run the pipeline first.")
    st.stop()

anomalies = anomalies.sort_values("event_date")
metric_names = sorted(anomalies["metric_name"].unique())

flagged = anomalies[anomalies["is_anomaly"] == True]
st.metric("Total anomalies flagged", len(flagged))

selected_metric = st.selectbox("Select metric", metric_names)
metric_df = anomalies[anomalies["metric_name"] == selected_metric]

fig = go.Figure()
fig.add_trace(go.Scatter(x=metric_df["event_date"], y=metric_df["actual_value"], mode="lines+markers", name="Actual"))
fig.add_trace(go.Scatter(x=metric_df["event_date"], y=metric_df["expected_value"], mode="lines", name="Expected (rolling avg)", line=dict(dash="dash")))

anomaly_points = metric_df[metric_df["is_anomaly"] == True]
fig.add_trace(
    go.Scatter(
        x=anomaly_points["event_date"], y=anomaly_points["actual_value"],
        mode="markers", name="Anomaly", marker=dict(color="red", size=12, symbol="x"),
    )
)
fig.update_layout(title=f"{selected_metric}: Actual vs Expected", xaxis_title="Date", yaxis_title="Value")
st.plotly_chart(fig, use_container_width=True)

st.divider()
st.subheader("All Flagged Anomalies")
if flagged.empty:
    st.success("No anomalies currently flagged across any metric.")
else:
    st.dataframe(
        flagged[["event_date", "metric_name", "actual_value", "expected_value", "anomaly_score"]]
        .sort_values("event_date", ascending=False),
        use_container_width=True,
    )

with st.expander("Raw anomaly detection data"):
    st.dataframe(anomalies, use_container_width=True)
