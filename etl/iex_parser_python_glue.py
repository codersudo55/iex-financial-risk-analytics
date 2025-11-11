import sys
import os
import uuid
import boto3
import gzip
import io
import tempfile
import json
from decimal import Decimal
from datetime import datetime, timedelta

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from iex_parser import Parser, TOPS_1_5, TOPS_1_6

# ---------- args ----------
args = getResolvedOptions(sys.argv, ["OUTPUT_BUCKET", "RAW_PREFIX", "START_DATE", "END_DATE"])
OUTPUT_BUCKET = args["OUTPUT_BUCKET"].rstrip("/") + "/"   # e.g., s3://iex-parsed-data/
RAW_PREFIX = args["RAW_PREFIX"].rstrip("/") + "/"         # e.g., s3://iex-raw-data/
START_DATE = datetime.strptime(args["START_DATE"], "%Y%m%d")
END_DATE   = datetime.strptime(args["END_DATE"], "%Y%m%d")

# ---------- Spark ----------
spark = SparkSession.builder.appName("IEX_TOPS_Parser_Parquet_Only").getOrCreate()
sc = spark.sparkContext

# ---------- helper: create local boto3 client inside executors ----------
def _get_s3_client():
    import boto3
    return boto3.client("s3")

# ---------- helper: write rows to s3 as parquet using pyarrow ----------
def _upload_rows_as_parquet_s3(rows, bucket, key_prefix):
    import pyarrow as pa
    import pyarrow.parquet as pq
    import tempfile

    data = [
        {
            "type": r[0],
            "timestamp": r[1],
            "symbol": r[2],
            "sale_size": int(r[3]) if r[3] is not None else None,
            "sale_price": float(r[4]) if r[4] is not None else None,
            "trade_id": str(r[5]) if r[5] is not None else None,
        }
        for r in rows
    ]
    table = pa.Table.from_pylist(data)

    local_tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    local_tmp.close()
    pq.write_table(table, local_tmp.name)

    s3 = _get_s3_client()
    part_name = f"part-{uuid.uuid4().hex}.parquet"
    dest_key = key_prefix + part_name
    s3.upload_file(local_tmp.name, bucket, dest_key)
    os.unlink(local_tmp.name)
    return dest_key

# ---------- worker function ----------
def _worker_task(task):
    s3_uri, date_str = task
    s3 = _get_s3_client()

    bucket = s3_uri.replace("s3://", "").split("/", 1)[0]
    key = s3_uri.replace("s3://", "").split("/", 1)[1]

    out_bucket = OUTPUT_BUCKET.replace("s3://", "").split("/", 1)[0]
    out_prefix_base = OUTPUT_BUCKET.replace("s3://", "").split("/", 1)[1]
    key_prefix = f"{out_prefix_base}parsed_{date_str}.parquet/"

    # checkpoint
    try:
        existing = s3.list_objects_v2(Bucket=out_bucket, Prefix=key_prefix, MaxKeys=1)
        if "Contents" in existing:
            return {"date": date_str, "status": "SKIPPED", "rows": None, "reason": "already exists"}
    except Exception:
        pass

    # get object
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            gz_body = gzip.GzipFile(fileobj=io.BytesIO(obj["Body"].read()))
            tmp.write(gz_body.read())
            tmp_path = tmp.name
    except Exception as e:
        return {"date": date_str, "status": "FAILED", "reason": f"download/decompress: {e}"}

    parser_version = TOPS_1_5 if date_str <= "20171031" else TOPS_1_6
    rows = []
    try:
        with Parser(tmp_path, parser_version) as reader:
            for msg in reader:
                if msg["type"] == "trade_report":
                    rows.append((
                        msg["type"],
                        msg["timestamp"].isoformat(),
                        msg["symbol"].decode("utf-8"),
                        msg.get("size", None),
                        float(msg["price"]) if isinstance(msg.get("price", None), Decimal) else msg.get("price", None),
                        str(msg.get("trade_id", None))
                    ))
    except Exception as e:
        return {"date": date_str, "status": "FAILED", "reason": f"parse error: {e}"}
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass

    if not rows:
        return {"date": date_str, "status": "FAILED", "reason": "no trade_report rows"}

    try:
        part_key = _upload_rows_as_parquet_s3(rows, out_bucket, key_prefix)
        return {"date": date_str, "status": "SUCCESS", "rows": len(rows), "part_key": part_key}
    except Exception as e:
        return {"date": date_str, "status": "FAILED", "reason": f"upload error: {e}"}

# ---------- build list of weekday files ----------
tasks = []
curr = START_DATE
while curr <= END_DATE:
    if curr.weekday() < 5:
        date_str = curr.strftime("%Y%m%d")
        parser_suffix = "5" if date_str <= "20171031" else "6"
        key_name = f"data_feeds_{date_str}_{date_str}_IEXTP1_TOPS1.{parser_suffix}.pcap.gz"
        s3_uri = RAW_PREFIX + key_name
        tasks.append((s3_uri, date_str))
    curr += timedelta(days=1)

print(f"Found {len(tasks)} tasks to process")

# ---------- Spark parallelism ----------
partitions = min(len(tasks), 40)
rdd = sc.parallelize(tasks, partitions)
results = rdd.map(_worker_task).collect()

# ---------- checkpoint summary ----------
s3_driver = boto3.client("s3")
out_bucket_name = OUTPUT_BUCKET.replace("s3://", "").split("/", 1)[0]
out_prefix_base = OUTPUT_BUCKET.replace("s3://", "").split("/", 1)[1]

summary = {"success": [], "failed": [], "skipped": []}
for r in results:
    d = r["date"]
    if r["status"] == "SUCCESS":
        summary["success"].append(r)
        success_key = f"{out_prefix_base}parsed_{d}.parquet/_SUCCESS"
        s3_driver.put_object(Bucket=out_bucket_name, Key=success_key, Body=b"")
    elif r["status"] == "SKIPPED":
        summary["skipped"].append(r)
    else:
        summary["failed"].append(r)

summary_key = f"summary_{START_DATE.strftime('%Y%m%d')}_{END_DATE.strftime('%Y%m%d')}.json"
s3_driver.put_object(Bucket=out_bucket_name, Key="/".join([out_prefix_base, summary_key]), Body=json.dumps(summary, indent=2).encode("utf-8"))
print("Summary written; job complete.")
