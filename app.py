from flask import Flask, render_template
import json
import re
from collections import defaultdict

app = Flask(__name__)

# Month abbreviation to month number mapping
month_map = {
    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4,
    'MAY': 5, 'JUN': 6, 'JUL': 7, 'AUG': 8,
    'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
}

def extract_month(ticker):
    match = re.search(r'\d{2}([A-Z]{3})FUT', ticker)
    if match:
        month_str = match.group(1)
        return month_map.get(month_str.upper(), None)
    return None

def calculate_differences(data):
    grouped = defaultdict(list)
    for instrument in data.values():
        display_name = instrument['displayName']
        ticker = instrument['ticker']
        month = extract_month(ticker)
        if month:
            instrument['ExpiryMonth'] = month
            grouped[display_name].append(instrument)
        else:
            print(f"Could not extract month from ticker: {ticker}")

    differences = {}
    for display_name, instruments in grouped.items():
        if len(instruments) < 2:
            # Not enough data to compare near and far month
            continue

        # Sort instruments by ExpiryMonth
        sorted_instruments = sorted(instruments, key=lambda x: x['ExpiryMonth'])

        # Assume the first is near month and the second is far month
        near = sorted_instruments[0]
        far = sorted_instruments[1]

        price_diff = near['LastTradedPrice'] - far['LastTradedPrice']
        differences[display_name] = {
            'NearMonthTicker': near['ticker'],
            'NearMonthPrice': near['LastTradedPrice'],
            'FarMonthTicker': far['ticker'],
            'FarMonthPrice': far['LastTradedPrice'],
            'PriceDifference': round(price_diff, 2)
        }
    return differences

@app.route('/')
def index():
    # Load JSON data
    with open('touchline_data.json', 'r') as f:
        data = json.load(f)

    differences = calculate_differences(data)
    return render_template('index.html', differences=differences)

if __name__ == '__main__':
    app.run(debug=True)
