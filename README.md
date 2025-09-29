# Financial Risk Analytics and Trading Behavior using IEX Market Data

## Overview
End-to-end financial risk analytics and trading behavior pipeline that ingests financial data (~300GB), processes via PySpark to obtain parsed trade_report files in AWS, builds fact tables with dbt, visualizes exposures and fraud alerts via Streamlit dashboard (and applies a PyTorch-based fraud detection model - optional).

## Workflow
1. Download raw market data by date from IEX Market Data website. We specifically need data from TOPS_1_5 and TOPS_1_6 (I have collected data between 2016-12-12 to 2018-07-31):

   ```bash
   https://iextrading.com/trading/market-data/ 
   ```
1. Parse IEX TOPS .pcap.gz into CSV:
   ```bash
   python etl/parse_iex_tops.py --input data_feeds_xxx.pcap.gz --output data/raw/iex/parsed.csv
   ```

2. Ingest parsed CSV into Spark:
   ```bash
   python etl/spark_ingest.py
   ```

<!-- 3. Orchestrate full pipeline with Airflow (ETL → dbt → fraud scoring → dashboard). -->

<!-- 4. For cloud-scale, move parsed CSV/Parquet into Snowflake and update configs accordingly. -->


