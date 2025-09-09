import csv
from decimal import Decimal
from iex_parser import Parser, TOPS_1_5

def parse_pcap_to_csv(input_file: str, output_file: str):
    """Parse IEX TOPS pcap.gz file and write quote_update + trade_report messages to CSV."""
    with Parser(input_file, TOPS_1_5) as reader, open(output_file, "w", newline="") as csvfile:
        fieldnames = ["type", "timestamp", "symbol",
                      "bid_size", "bid_price", "ask_size", "ask_price",
                      "last_sale_size", "last_sale_price"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for msg in reader:
            row = {"type": msg["type"],
                   "timestamp": msg["timestamp"].isoformat(),
                   "symbol": msg["symbol"].decode("utf-8")}
            # Defaults
            row.update({
                "bid_size": 0, "bid_price": 0.0,
                "ask_size": 0, "ask_price": 0.0,
                "last_sale_size": 0, "last_sale_price": 0.0
            })
            if msg["type"] == "quote_update":
                row["bid_size"] = msg["bid_size"]
                row["bid_price"] = float(msg["bid_price"]) if isinstance(msg["bid_price"], Decimal) else msg["bid_price"]
                row["ask_size"] = msg["ask_size"]
                row["ask_price"] = float(msg["ask_price"]) if isinstance(msg["ask_price"], Decimal) else msg["ask_price"]
            elif msg["type"] == "trade_report":
                row["last_sale_size"] = msg["size"]
                row["last_sale_price"] = float(msg["price"]) if isinstance(msg["price"], Decimal) else msg["price"]
            writer.writerow(row)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Parse IEX TOPS .pcap.gz into CSV (quotes + trades)")
    parser.add_argument("--input", required=True, help="Path to input .pcap.gz file")
    parser.add_argument("--output", required=True, help="Path to output CSV file")
    args = parser.parse_args()

    parse_pcap_to_csv(args.input, args.output)
