# etl/config.py
# Central configuration for the ETL scripts.

AWS_REGION = "us-east-1"

# ---------------------------
# S3 buckets and layout
# ---------------------------

# Raw data: all .pcap.gz files directly inside this bucket (no subfolders)
RAW_BUCKET = "iex-raw-data"
RAW_PREFIX = ""   # keep empty since files are in the root

# Parsed data: all outputs written as parsed_YYYYMMDD.parquet
PARSED_BUCKET = "iex-parsed-data"
PARSED_PREFIX = ""  # leave empty → files go directly to bucket root

# ---------------------------
# Glue job defaults
# ---------------------------
GLUE_NUM_DPUS = 2      # adjust depending on job size/performance
GLUE_ROLE = "AWSGlueServiceRole-IEXETL"  # IAM role for Glue job execution

# ---------------------------
# Parser versions (for metadata/logging, optional)
# ---------------------------
TOPS_1_5_MARK = "TOPS1.5"
TOPS_1_6_MARK = "TOPS1.6"

# ---------------------------
# Logging
# ---------------------------
# Every N trade_report messages, log progress
LOG_EVERY = 10000
