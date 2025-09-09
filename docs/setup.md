# Setup Instructions

## Requirements
- Python 3.9+
- Spark 3.x
- dbt-core
- PyTorch
- Streamlit
- Airflow (optional, for orchestration)

## Steps
1. Install requirements:
   ```bash
   pip install -r requirements.txt
   ```

2. Test parsing on local file:
   ```bash
   python etl/parse_iex_tops.py --input data_feeds_xxx.pcap.gz --output data/raw/iex/parsed.csv
   ```

3. Run Spark ingestion locally:
   ```bash
   python etl/spark_ingest.py
   ```

4. Extend pipeline with dbt + PyTorch + dashboard.
