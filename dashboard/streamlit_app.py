import streamlit as st
import pandas as pd

st.title("📊 Financial Risk Analytics Dashboard (Trade Reports)")

# Allow multiple CSV uploads
uploaded_files = st.file_uploader(
    "Upload one or more parsed trade_report CSV files",
    type=["csv"],
    accept_multiple_files=True
)

if uploaded_files:
    # Load all CSVs
    dfs = []
    for file in uploaded_files:
        df = pd.read_csv(file, parse_dates=["timestamp"])
        dfs.append(df)
    df = pd.concat(dfs, ignore_index=True)

    # Extract date
    df["date"] = df["timestamp"].dt.date

    st.subheader("Sample of Trades")

    # Show date range
    min_date = df["date"].min()
    max_date = df["date"].max()
    if min_date == max_date:
        st.write(f"Data for **{min_date}**")
    else:           
        st.write(f"Data covers from **{min_date}** to **{max_date}**")

    st.write(df.head())

    # ---- Overall Risk Metrics ----
    st.subheader("Risk Exposure Metrics (All Days Combined)")
    exposure = df.groupby("symbol").agg(
        total_volume=("sale_size", "sum"),
        avg_price=("sale_price", "mean"),
        trade_count=("trade_id", "count"),
        vwap=("sale_price", lambda x: (x * df.loc[x.index, "sale_size"]).sum() / df.loc[x.index, "sale_size"].sum())
    ).reset_index()
    st.write(exposure)

    # ---- Daily Metrics Table ----
    st.subheader("📊 Daily Metrics Table (Per Ticker, Per Day)")
    daily_metrics = df.groupby(["date", "symbol"]).agg(
        total_volume=("sale_size", "sum"),
        avg_price=("sale_price", "mean"),
        trade_count=("trade_id", "count"),
        vwap=("sale_price", lambda x: (x * df.loc[x.index, "sale_size"]).sum() / df.loc[x.index, "sale_size"].sum())
    ).reset_index()
    st.write(daily_metrics)

    # ---- Interactive Charts ----
    st.subheader("📈 Interactive Ticker Comparisons")

    tickers = df["symbol"].unique().tolist()
    selected_tickers = st.multiselect(
        "Select Tickers to Display",
        tickers,
        default=tickers[:1]  # default to first ticker
    )

    if selected_tickers:
        # Average Sale Price per Day
        st.markdown("### Average Sale Price")
        avg_price_trend = df.groupby(["date", "symbol"])["sale_price"].mean().reset_index()
        pivot_avg = avg_price_trend.pivot(index="date", columns="symbol", values="sale_price")
        st.line_chart(pivot_avg[selected_tickers])


        # Trade Volume per Day
        st.markdown("### Total Trade Volume")
        daily_volume = df.groupby(["date", "symbol"])["sale_size"].sum().reset_index()
        pivot_volume = daily_volume.pivot(index="date", columns="symbol", values="sale_size")
        st.line_chart(pivot_volume[selected_tickers])


        # VWAP per Day
        st.markdown("### Volume Weighted Average Price (VWAP)")
        vwap_daily = df.groupby(["date", "symbol"]).apply(
            lambda x: (x["sale_price"] * x["sale_size"]).sum() / x["sale_size"].sum()
        ).reset_index(name="vwap")
        pivot_vwap = vwap_daily.pivot(index="date", columns="symbol", values="vwap")
        st.line_chart(pivot_vwap[selected_tickers])

    # ---- Daily Trade Count ----
    st.subheader("📊 Total Number of Trades per Day")
    daily_trades = df.groupby("date")["trade_id"].count()
    st.bar_chart(daily_trades)

    # ---- Fraud Alerts ----
    st.subheader("🚨 Fraud / Anomaly Alerts")
    large_trades = df[df["sale_size"] > df["sale_size"].mean() * 5]
    if not large_trades.empty:
        st.warning("Unusually large trades detected!")
        st.write(large_trades)
    else:
        st.success("No abnormal trades detected.")
