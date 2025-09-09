import streamlit as st
import pandas as pd

st.title("Financial Risk Analytics Dashboard")

st.subheader("Trade Exposures")
data = pd.DataFrame({
    "symbol": ["AAPL", "GOOG", "MSFT"],
    "exposure": [120000, 95000, 78000]
})
st.bar_chart(data.set_index("symbol"))

st.subheader("Fraud Alerts")
alerts = pd.DataFrame({
    "trade_id": [101, 102],
    "score": [0.92, 0.85]
})
st.table(alerts)
