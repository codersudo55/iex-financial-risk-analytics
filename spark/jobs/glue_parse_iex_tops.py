import sys
import boto3
import gzip
import io
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from scapy.all import rdpcap
import tempfile

# ---------------------------
# Parse Glue job arguments
# ---------------------------
args = getResolvedOptions(sys.argv, ["RAW_INPUT", "OUTPUT_BUCKET"])
raw_input = args["RAW_INPUT"]   # e.g. s3://iex-raw-data/data_feeds/20161212_...pcap.gz
output_bucket = args["OUTPUT_BUCKET"]  # e.g. s3://iex-parsed-data/

# Derive output filename
date_str = raw_input.split("/")[-1].split("_")[1]  # 20161212 (depends on filename format)
output_path = f"{output_bucket}parsed_{date_str}.parquet"

# ---------------------------
# Initialize Spark
# ---------------------------
spark = SparkSession.builder.appName("IEX_Tops_Parser").getOrCreate()

# ---------------------------
# Schema for trade_report
# ---------------------------
schema = StructType([
    StructField("type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("sale_size", LongType(), True),
    StructField("sale_price", DoubleType(), True),
    StructField("trade_id", StringType(), True),
])

# ---------------------------
# Custom PCAP → Rows
# ---------------------------
def parse_pcap_gz_to_rows(s3_uri):
    s3 = boto3.client("s3")
    bucket, key = s3_uri.replace("s3://", "").split("/", 1)

    # Download and decompress
    obj = s3.get_object(Bucket=bucket, Key=key)
    gz_body = gzip.GzipFile(fileobj=io.BytesIO(obj["Body"].read()))

    # Write to temp file so scapy can read it
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(gz_body.read())
        tmp_path = tmp.name

    # Parse packets
    packets = rdpcap(tmp_path)
    rows = []

    for pkt in packets:
        try:
            if pkt.haslayer("Raw"):
                payload = bytes(pkt["Raw"].load).decode("utf-8", errors="ignore").strip()
                if payload.startswith("trade_report"):
                    parts = payload.split(",")
                    if len(parts) == 6:
                        rows.append(tuple(parts))
        except Exception:
            continue

    return rows


rows = parse_pcap_gz_to_rows(raw_input)

# ---------------------------
# Create DataFrame & Write Parquet
# ---------------------------
if rows:
    df = spark.createDataFrame(rows, schema=schema)
    df.write.mode("overwrite").parquet(output_path)
    print(f"✅ Parsed {df.count()} rows → {output_path}")
else:
    print(f"⚠️ No rows parsed from {raw_input} – check file format or parser logic.")
