# Financial Risk Analytics Project
Financial Risk Analytics
Goal: Process 300GB of financial flat files (trades, transactions) into Snowflake.
Tools: PySpark for ETL, Pandas/NumPy for financial calculations, dbt for fact tables.
Add-on: PyTorch fraud detection model.
Visualization: Risk exposure and fraud alerts dashboard.

## Overview
End-to-end financial risk analytics pipeline that ingests financial trades/transactions (~300GB), processes via PySpark, stores curated data in Snowflake, builds fact tables with dbt, applies a PyTorch-based fraud detection model, and visualizes exposures and fraud alerts via Streamlit dashboard.

## Workflow
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


