# Glue IEX Data Job Parsing using Parallelism 
import sys
import boto3
import gzip
import io
import tempfile
import json
from decimal import Decimal
from datetime import datetime, timedelta

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from iex_parser import Parser, TOPS_1_5, TOPS_1_6

# ---------------------------
# Parse Glue job arguments
# ---------------------------
args = getResolvedOptions(
    sys.argv,
    ["OUTPUT_BUCKET", "RAW_PREFIX", "START_DATE", "END_DATE"]
)
output_bucket = args["OUTPUT_BUCKET"]
raw_prefix = args["RAW_PREFIX"]
start_date = datetime.strptime(args["START_DATE"], "%Y%m%d")
end_date   = datetime.strptime(args["END_DATE"], "%Y%m%d")

# ---------------------------
# Initialize Spark
# ---------------------------
spark = SparkSession.builder.appName("IEX_TOPS_Batch_Parser").getOrCreate()
schema = StructType([
    StructField("type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("sale_size", LongType(), True),
    StructField("sale_price", DoubleType(), True),
    StructField("trade_id", StringType(), True),
])

summary = {"success": [], "failed": [], "skipped": []}

# ---------------------------
# Non-picklable boto3 client workaround
# ---------------------------
def get_s3_client():
    """Create boto3 client locally in each executor, not on driver."""
    import boto3
    return boto3.client("s3")

# ---------------------------
# Checkpoint helper
# ---------------------------
def output_exists(output_path):
    s3 = get_s3_client()
    bucket = output_path.replace("s3://", "").split("/")[0]
    key = "/".join(output_path.replace("s3://", "").split("/")[1:])
    try:
        s3.head_object(Bucket=bucket, Key=key + "/_SUCCESS")
        return True
    except:
        return False

# ---------------------------
# Parser function
# ---------------------------
def parse_and_save(task):
    s3_uri, date_str, output_path = task
    s3 = get_s3_client()

    if output_exists(output_path):
        return ("skip", date_str, f"Already exists {output_path}")

    bucket, key = s3_uri.replace("s3://", "").split("/", 1)

    # Ensure file exists
    try:
        s3.head_object(Bucket=bucket, Key=key)
    except Exception as e:
        return ("fail", date_str, f"Missing file {s3_uri}: {e}")

    # Download & decompress
    obj = s3.get_object(Bucket=bucket, Key=key)
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        gz_body = gzip.GzipFile(fileobj=io.BytesIO(obj["Body"].read()))
        tmp.write(gz_body.read())
        tmp_path = tmp.name

    parser_version = TOPS_1_5 if date_str < "20171101" else TOPS_1_6
    rows = []
    try:
        with Parser(tmp_path, parser_version) as reader:
            for msg in reader:
                if msg["type"] == "trade_report":
                    rows.append((
                        msg["type"],
                        msg["timestamp"].isoformat(),
                        msg["symbol"].decode("utf-8"),
                        msg["size"],
                        float(msg["price"]) if isinstance(msg["price"], Decimal) else msg["price"],
                        msg["trade_id"]
                    ))
    except Exception as e:
        return ("fail", date_str, str(e))

    if not rows:
        return ("fail", date_str, "No trade_report rows")

    df = spark.createDataFrame(rows, schema=schema)
    df.write.mode("overwrite").parquet(output_path)
    return ("success", date_str, len(rows))

# ---------------------------
# Build list of files
# ---------------------------
files_to_process = []
curr = start_date
while curr <= end_date:
    if curr.weekday() < 5:  # weekdays only
        date_str = curr.strftime("%Y%m%d")
        parser_suffix = "5" if date_str < "20171101" else "6"
        key = f"data_feeds_{date_str}_{date_str}_IEXTP1_TOPS1.{parser_suffix}.pcap.gz"
        s3_uri = f"{raw_prefix}{key}"
        output_path = f"{output_bucket}parsed_{date_str}.parquet"
        files_to_process.append((s3_uri, date_str, output_path))
    curr += timedelta(days=1)

print(f"📂 Found {len(files_to_process)} files between {start_date} and {end_date}")

# ---------------------------
# Parallelize with Spark safely
# ---------------------------
rdd = spark.sparkContext.parallelize(files_to_process, min(len(files_to_process), 50))
results = rdd.map(parse_and_save).collect()

# ---------------------------
# Collect results
# ---------------------------
for status, date_str, info in results:
    if status == "success":
        summary["success"].append({"date": date_str, "rows": info})
    elif status == "fail":
        summary["failed"].append({"date": date_str, "reason": info})
    else:
        summary["skipped"].append({"date": date_str, "reason": info})

# ---------------------------
# Write summary
# ---------------------------
bucket_name = output_bucket.replace("s3://", "").split("/")[0]
prefix = "/".join(output_bucket.replace("s3://", "").split("/")[1:])
summary_key = f"summary_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.json"
s3 = boto3.client("s3")
s3.put_object(
    Bucket=bucket_name,
    Key="/".join([prefix, summary_key]),
    Body=json.dumps(summary, indent=2).encode("utf-8")
)
print(f"📄 Summary written → {output_bucket}{summary_key}")
