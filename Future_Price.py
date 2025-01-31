import json
import re
import time
from collections import defaultdict

# Month abbreviation to month number mapping
month_map = {
    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4,
    'MAY': 5, 'JUN': 6, 'JUL': 7, 'AUG': 8,
    'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
}

# Function to extract month from ticker
def extract_month(ticker):
    match = re.search(r'\d{2}([A-Z]{3})FUT', ticker)
    if match:
        month_str = match.group(1)
        return month_map.get(month_str.upper(), None)
    return None

# Main function to process and write data
def update_price_differences():
    # Load touchline data
    with open('touchline_datas.json', 'r') as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            print("Invalid JSON data")
            data = {}

    # Group instruments by displayName
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

    # Calculate differences
    differences = {}
    for display_name, instruments in grouped.items():
        if len(instruments) < 2:
            # Not enough data to compare near and far month
            continue
        
        # Sort instruments by ExpiryMonth
        sorted_instruments = sorted(instruments, key=lambda x: x['ExpiryMonth'])
        print(sorted_instruments)
        # Assume the first is near month and second is far month
        near = sorted_instruments[0]
        far = sorted_instruments[1]
        
        price_diff = far['LastTradedPrice'] - near['LastTradedPrice']
        differences[display_name] = {
            'NearMonthTicker': near['ticker'],
            'NearMonthPrice': near['LastTradedPrice'],
            'FarMonthTicker': far['ticker'],
            'FarMonthPrice': far['LastTradedPrice'],
            'PriceDifference': round((price_diff / near['LastTradedPrice'] * 100), 2)
        }

    # Write the differences to a JSON file
    with open('price_differences.json', 'w') as f:
        json.dump(differences, f, indent=4)
        print("Price difference data written to price_difference.json")

# Continuous update loop
print("Press Ctrl+C to stop the script")
try:
    while True:
        update_price_differences()
        time.sleep(5)  # Update every 5 seconds (adjust as needed)
except KeyboardInterrupt:
    print("Script stopped")
