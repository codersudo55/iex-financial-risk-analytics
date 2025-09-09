import csv
from decimal import Decimal
from iex_parser import Parser, TOPS_1_5

def parse_pcap_to_csv(input_file: str, output_file: str):
    """Parse IEX TOPS pcap.gz file and write trade_report messages to CSV."""
    trades_count = 0
    with Parser(input_file, TOPS_1_5) as reader, open(output_file, "w", newline="") as csvfile:
        fieldnames = ["type", "timestamp", "symbol", "sale_size", "sale_price", "trade_id"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for msg in reader:
            if msg["type"] == "trade_report":
                row = {"type": msg["type"],
                       "timestamp": msg["timestamp"].isoformat(),
                       "symbol": msg["symbol"].decode("utf-8"),
                       "sale_size": msg["size"],
                       "sale_price": float(msg["price"]) if isinstance(msg["price"], Decimal) else msg["price"],
                       "trade_id": msg["trade_id"]}
                writer.writerow(row)
                trades_count += 1
                # log progress every 100000 trades
                if trades_count % 100000 == 0:
                    print(f"✅ Parsed {trades_count:,} trades so far...")

    print(f"🎉 Finished parsing. Total trades written: {trades_count:,}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Parse IEX TOPS .pcap.gz into CSV (trade_report only)")
    parser.add_argument("--input", required=True, help="Path to input .pcap.gz file")
    parser.add_argument("--output", required=True, help="Path to output CSV file")
    args = parser.parse_args()

    parse_pcap_to_csv(args.input, args.output)
