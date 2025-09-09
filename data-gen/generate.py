import csv, random, argparse, uuid
from datetime import datetime, timedelta

def generate_trade(ts, symbol):
    return {
        "trade_id": str(uuid.uuid4()),
        "timestamp": ts.isoformat(),
        "symbol": symbol,
        "price": round(random.uniform(10, 1000), 2),
        "quantity": random.randint(1, 5000),
        "side": random.choice(["BUY", "SELL"]),
        "buyer_id": "B-"+str(random.randint(1,100000)),
        "seller_id": "S-"+str(random.randint(1,100000)),
        "venue": random.choice(["NYSE","NASDAQ","IEX"])
    }

def stream_write_csv(filename, symbols, total_rows):
    ts = datetime.utcnow()
    with open(filename, "w", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(generate_trade(ts, symbols[0]).keys()))
        writer.writeheader()
        for i in range(total_rows):
            ts += timedelta(milliseconds=random.randint(1,1000))
            symbol = random.choice(symbols)
            writer.writerow(generate_trade(ts, symbol))
            if (i+1) % 100000 == 0:
                print("generated", i+1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="trades.csv")
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--symbols", nargs='+', default=["AAPL","MSFT","GOOG"])
    args = parser.parse_args()
    stream_write_csv(args.out, args.symbols, args.rows)
