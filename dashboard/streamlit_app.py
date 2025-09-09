import streamlit as st
import pandas as pd

st.title("📊 Financial Risk Analytics Dashboard (Trade Reports)")

uploaded_file = st.file_uploader("Upload parsed trade_report CSV", type=["csv"])

if uploaded_file:
    df = pd.read_csv(uploaded_file, parse_dates=["timestamp"])
    st.subheader("Sample of Trades")
    st.write(df.head())

    # Risk metrics
    st.subheader("Risk Exposure Metrics")
    exposure = df.groupby("symbol").agg(
        total_volume=("sale_size", "sum"),
        avg_price=("sale_price", "mean"),
        trade_count=("sale_size", "count"),
        vwap=("sale_price", lambda x: (x * df.loc[x.index, "sale_size"]).sum() / df.loc[x.index, "sale_size"].sum())
    ).reset_index()
    st.write(exposure)

    # Fraud alerts
    st.subheader("🚨 Fraud / Anomaly Alerts")
    large_trades = df[df["sale_size"] > df["sale_size"].mean() * 5]
    if not large_trades.empty:
        st.warning("Unusually large trades detected!")
        st.write(large_trades)
    else:
        st.success("No abnormal trades detected.")

    # Timeline
    st.subheader("Trade Timeline")
    st.line_chart(df.set_index("timestamp")["sale_price"])
