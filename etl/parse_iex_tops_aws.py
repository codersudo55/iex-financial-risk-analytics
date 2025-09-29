import argparse
import os
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from iex_parser import TOPS_1_5, TOPS_1_6
from scapy.utils import PcapReader
from datetime import datetime
from io import BytesIO
from config import RAW_BUCKET, PARSED_BUCKET, RAW_PREFIX, PARSED_PREFIX


def parse_pcap_to_parquet(s3_input_key, s3_output_prefix, tops_version="1.5"):
    s3 = boto3.client("s3")
    raw_bucket = RAW_BUCKET
    parsed_bucket = PARSED_BUCKET

    # Download file from S3 into memory
    obj = s3.get_object(Bucket=raw_bucket, Key=s3_input_key)
    file_bytes = BytesIO(obj['Body'].read())

    # Choose parser version
    if tops_version == "1.5":
        parser = TOPS_1_5()
    else:
        parser = TOPS_1_6()

    trade_reports = []
    with PcapReader(file_bytes) as pcap_reader:
        for packet in pcap_reader:
            try:
                msg = parser.parse_message(packet.load)
                if msg and msg["type"] == "trade_report":
                    trade_reports.append({
                        "timestamp": msg["timestamp"],
                        "symbol": msg["symbol"],
                        "sale_size": msg["sale_size"],
                        "sale_price": msg["sale_price"],
                        "trade_id": msg["trade_id"],
                    })
            except Exception:
                continue

    # Convert to Parquet
    if trade_reports:
        df = pd.DataFrame(trade_reports)
        table = pa.Table.from_pandas(df)

        # Example partition: /date=YYYY-MM-DD/
        trading_day = s3_input_key.split("/")[0].split("_")[0]  # adjust if filename format differs
        output_key = f"{s3_output_prefix}/date={trading_day}/parsed_{trading_day}.parquet"

        out_buffer = BytesIO()
        pq.write_table(table, out_buffer)
        s3.put_object(Bucket=parsed_bucket, Key=output_key, Body=out_buffer.getvalue())
        print(f"✅ Wrote {len(df)} rows to s3://{parsed_bucket}/{output_key}")
    else:
        print(f"⚠️ No trade_report messages found in {s3_input_key}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_key", required=True, help="S3 key of raw pcap.gz file")
    parser.add_argument("--output_prefix", required=True, help="S3 prefix for output parquet files")
    parser.add_argument("--tops_version", choices=["1.5", "1.6"], default="1.5")
    args = parser.parse_args()

    parse_pcap_to_parquet(args.input_key, args.output_prefix, args.tops_version)
