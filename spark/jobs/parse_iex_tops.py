# Parser: converts IEX TOPS .pcap.gz into CSV with both quotes and trades
import csv
from decimal import Decimal
from iex_parser import Parser, TOPS_1_5

def parse_pcap_to_csv(input_file: str, output_file: str):
    with Parser(input_file, TOPS_1_5) as reader, open(output_file, "w", newline='') as csvfile:
        fieldnames = ['type','timestamp','symbol',
                      'bid_size','bid_price','ask_size','ask_price',
                      'last_sale_size','last_sale_price']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for msg in reader:
            # Some messages may not include symbol etc; guard.
            try:
                t = msg.get('type')
                sym = msg.get('symbol')
                if sym is None:
                    continue
                symbol = sym.decode('utf-8') if isinstance(sym, bytes) else str(sym)
                row = dict.fromkeys(fieldnames, 0)
                row.update({'type': t, 'timestamp': msg['timestamp'].isoformat(), 'symbol': symbol})
                if t == 'quote_update':
                    row['bid_size'] = msg.get('bid_size', 0)
                    row['bid_price'] = float(msg.get('bid_price', 0) if isinstance(msg.get('bid_price'), Decimal) else msg.get('bid_price', 0))
                    row['ask_size'] = msg.get('ask_size', 0)
                    row['ask_price'] = float(msg.get('ask_price', 0) if isinstance(msg.get('ask_price'), Decimal) else msg.get('ask_price', 0))
                elif t == 'trade_report':
                    # trade messages may use 'price' and 'size'
                    row['last_sale_size'] = msg.get('size', 0)
                    row['last_sale_price'] = float(msg.get('price', 0) if isinstance(msg.get('price'), Decimal) else msg.get('price', 0))
                writer.writerow(row)
            except Exception as e:
                # skip malformed messages but continue parsing
                print('skip msg', e)
                continue

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True, help='input .pcap.gz file')
    parser.add_argument('--output', required=True, help='output CSV file (e.g. data/raw/iex/parsed_20161212.csv)')
    args = parser.parse_args()
    parse_pcap_to_csv(args.input, args.output)
