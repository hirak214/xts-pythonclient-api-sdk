from datetime import datetime
from flask import Flask, request, render_template
from flask_socketio import SocketIO
from Connect import XTSConnect
from MarketDataSocketClient import MDSocket_io
import pandas as pd
import io as StringIO
import json
import os

# display max rows and columns
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
app = Flask(__name__)
socketio = SocketIO(app)
touchline_data = {}

# MarketData API Credentials
API_KEY = '3642a41c547ee103920202'
API_SECRET = 'Qkbc461#aF'
source = 'WebAPI'
# ROOT_URL = 'http://ctrade.jainam.in:3000/apimarketdata'

# Initialise
xt = XTSConnect(API_KEY, API_SECRET, source)

# Login for authorization token
response = xt.marketdata_login()

# Store the token and userid
set_marketDataToken = response['result']['token']
set_muserID = response['result']['userID']
print("Login: ", response)


exchangesegments = [xt.EXCHANGE_NSEFO]
def get_formatted_masters(exchangeSegment: list, series = "FUTSTK"):
        response = xt.get_master(exchangeSegmentList=exchangesegments)
        response = response['result'].split('\n')
        # # print(response)   
        nifty50_symbols = [
                            'RELIANCE', 'TCS', 'HDFCBANK', 'INFY', 'ICICIBANK', 'HINDUNILVR', 'SBIN',
                            'BHARTIARTL', 'BAJFINANCE', 'ITC', 'KOTAKBANK', 'LT', 'ASIANPAINT', 'AXISBANK',
                            'HCLTECH', 'MARUTI', 'SUNPHARMA', 'TITAN', 'ULTRACEMCO', 'WIPRO', 'NESTLEIND',
                            'M&M', 'TECHM', 'POWERGRID', 'BAJAJFINSV', 'ADANIENT', 'ADANIPORTS', 'COALINDIA',
                            'ONGC', 'NTPC', 'JSWSTEEL', 'TATAMOTORS', 'TATASTEEL', 'HDFCLIFE', 'SBILIFE',
                            'DRREDDY', 'CIPLA', 'DIVISLAB', 'GRASIM', 'HINDALCO', 'BPCL', 'IOC', 'HEROMOTOCO',
                            'BRITANNIA', 'SHREECEM', 'EICHERMOT', 'APOLLOHOSP', 'BAJAJ-AUTO', 'INDUSINDBK',
                            'UPL'
                        ]
#         nifty50_symbols = [
#     'ADANIENT', 'ADANIPORTS', 'APOLLOHOSP', 'AXISBANK', 'BAJAJ-AUTO',
#     'BAJAJFINSV', 'BEL', 'BPCL', 'BRITANNIA', 'CIPLA',
#     'COALINDIA', 'DIVISLAB', 'DRREDDY', 'EICHERMOT', 'GAIL',
#     'GRASIM', 'HCLTECH', 'HEROMOTOCO', 'HINDCOPPER', 'HINDPETRO',
#     'ICICIPRULI', 'INDUSINDBK', 'INFY', 'ITC', 'JSWSTEEL',
#     'KOTAKBANK', 'LICHSGFIN', 'M&MFIN', 'MARICO',
#     'NMDC', 'ONGC', 'POWERGRID', 'RECLTD',
#     'SHREECEM', 'SUNTV', 'SUNPHARMA', 'TATACOMM',
#     'TATACONSUM', 'TATAMOTORS', 'TATASTEEL', 'TECHM',
#     'TITAN', 'TORNTPOWER', 'ULTRACEMCO', 'VOLTAS', 'WIPRO',
#     'ZEEL', 'BHEL', 'COLPAL',
#     'GODREJCP', 'ICICIGI', 'IDFCFIRSTB', 'INDIGO',
#     'JUBLFOOD', 'L&TFH', 'LICHSGFIN', 'MARUTI', 'MGL',
#     'MINDTREE', 'MOTHERSUMI', 'NATIONALUM',
#     'PEL', 'PIDILITIND', 'PNB', 'POLYCAB',
#     'SAIL', 'SBILIFE', 'SIEMENS', 'SRF', 'UBL',
#     'UPL', 'VEDL',
#     # Add more symbols as needed
# ]                
                                
        # split at ,
        response = [x.split(',') for x in response]
        # print(response)
        # in each sublist split at |
        response = [[y.split('|') for y in x] for x in response]
        columns = ['ExchangeSegment', 'ExchangeInstrumentID', 'InstrumentType', 'Name', 'Description', 'Series', 'NameWithSeries', 'InstrumentID', 'PriceBand.High', 'PriceBand.Low', 'FreezeQty', 'TickSize', 'LotSize', 'Multiplier', 'Ignore', 'displayName', 'ExpiryDate', 'TickerName', 'Ignore2', 'Ignore3', 'ticker','ignore4','ignore5']
        # # print(response)
        # response = [sublist for sublist in response if len(sublist[0]) > 5 and sublist[0][5] == 'FUTSTK']
        # response = [sublist for sublist in response if len(sublist[0]) > 5 and sublist[0][3].isin(nifty50_symbols)]
        # # print(response[1][0][5])
        # print(response)
        # create a DataFrame from the list of lists
        # quit()
        df = pd.DataFrame([sublist[0] for sublist in response])
        df = df[df[3].isin(nifty50_symbols)]
        df = df[df[5] == 'FUTSTK']
        # df = df[~df[17].str.contains('SPD')]
        # assign column names to the DataFrame
        df.columns = columns
        
        df.reset_index(drop=True, inplace=True)
        df['ExpiryDate'] = pd.to_datetime(df['ExpiryDate'])
        # drop columns
        df.drop(columns=['Ignore','Ignore2', 'Ignore3','ignore4', 'ignore5'], inplace=True)
        # print(df['ExpiryDate'].unique())
        # print(df)    
        # quit()
        # keep columns with only near month expiry and near month next expiry
        near_month_expiry = df['ExpiryDate'].min()
        near_month_next_expiry = df['ExpiryDate'].unique()[1]
        df = df[df['ExpiryDate'].isin([near_month_expiry, near_month_next_expiry])]
        # keep only rows where OptionType is 1
        
        # print(df['ExpiryDate'].unique())

        df.to_csv('nifty50_fut.csv', index=False)
        return df
response = get_formatted_masters(exchangesegments, series = "FUTSTK")
# print(response.tail())
# print(response.head())
# quit()

instrument_displayName = response.set_index('ExchangeInstrumentID')[['displayName', 'Description']].apply(tuple,axis=1).to_dict()
print(instrument_displayName)    

#  keep where 'displayName' is not 

# response = response[response['displayName'].notnull()]
# response = response.drop(response[response['displayName'] == ''].index)
# quit()
# print(response)
# quit()
# Connecting to Marketdata socket
soc = MDSocket_io(set_marketDataToken, set_muserID)

# Instruments for subscribing

# Creating list of dictionaries from DataFrame
Instruments = [{'exchangeSegment': 2, 'exchangeInstrumentID': int(row['ExchangeInstrumentID'])} for _, row in response.iterrows()]
# Instruments = [{'exchangeSegment': 2, 'exchangeInstrumentID': 10355472}]


# dict_count = 0

# for i in Instruments:
#     if isinstance(i, dict):
#         dict_count += 1
    
# print(dict_count)
# quit()
# Callback for connection
def on_connect():
    """Connect from the socket."""
    print('Market Data Socket connected successfully!')

    # # Subscribe to instruments
    print('Sending subscription request for Instruments - \n' + str(Instruments))
    response = xt.send_subscription(Instruments, 1501)
    print('Sent Subscription request!')
    print("Subscription response: ", response)

# Callback on receiving message
def on_message(data):
    print('I received a message!')

# Callback for message code 1501 FULL
# Callback for message code 1501 FULL
def on_message1501_json_full(data):
    if isinstance(data, str):
        data = json.loads(data)
    if isinstance(data, dict):
        # Get the ExchangeInstrumentID and look up the displayName
        instrument_id = str(data.get('ExchangeInstrumentID'))
        print('Instrument ID: ', instrument_id)
        display_name, ticker = instrument_displayName.get(instrument_id, ('None', 'None'))
        
        
        # Add the displayName to the data
        data['displayName'] = display_name
        data['ticker'] = ticker
        print('I received a 1501 Touchline message!' + json.dumps(data))
        
        # Update touchline_data
        touchline_data[instrument_id] = {
            "ExchangeInstrumentID": instrument_id,
            "displayName": display_name,
            "ticker": ticker,
            "LastTradedPrice": data.get("Touchline",{}).get("LastTradedPrice"),
            "LastTradedQuantity": data.get("Touchline",{}).get("LastTradedQunatity"),
            "TotalTradedQuantity": data.get("Touchline",{}).get("TotalTradedQuantity"),
        }
        
        # Save touchline_data to JSON
        file_path = "touchline_datas.json"
        if os.path.exists(file_path):
            with open(file_path, "r") as file:
                try:
                    json_data = json.load(file)
                    if not isinstance(json_data, dict):
                        json_data = {}
                except json.JSONDecodeError:
                    json_data = {}
        else:
            json_data = {}

        # Update json_data with the latest touchline_data
        json_data.update(touchline_data)
        
        with open(file_path, "w") as file:
            json.dump(json_data, file, indent=4)
        
        print("JSON file updated successfully with the latest touchline data.")
        socketio.emit('market_data', data)

    else:
        print("Error: Received data is not in expected dictionary format.")

# Callback for message code 1502 FULL
def on_message1502_json_full(data):
    print('I received a 1502 Market depth message!' + data)

# Callback for message code 1505 FULL
def on_message1505_json_full(data):
    print('I received a 1505 Candle data message!' + data)

# Callback for message code 1507 FULL
def on_message1507_json_full(data):
    print('I received a 1507 MarketStatus data message!' + data)

# Callback for message code 1510 FULL
def on_message1510_json_full(data):
    print('I received a 1510 Open interest message!' + data)

# Callback for message code 1512 FULL
def on_message1512_json_full(data):
    print('I received a 1512 Level1,LTP message!' + data)

# Callback for message code 1105 FULL
# def on_message1105_json_full(data):
#     print('I received a 1105, Instrument Property Change Event message!' + data)
#   # Callback for message code 1105 FULL
# def on_message1105_json_partial(data):
#     print('I received a 1105, Instrument Property Change Event message!' + data)



# Callback for message code 1501 PARTIAL
def on_message1501_json_partial(data):
    print('I received a 1501, Touchline Event message!' + data)

# Callback for message code 1502 PARTIAL
def on_message1502_json_partial(data):
    print('I received a 1502 Market depth message!' + data)

# Callback for message code 1505 PARTIAL
def on_message1505_json_partial(data):
    print('I received a 1505 Candle data message!' + data)

# Callback for message code 1510 PARTIAL
def on_message1510_json_partial(data):
    print('I received a 1510 Open interest message!' + data)

# Callback for message code 1512 PARTIAL
def on_message1512_json_partial(data):
    print('I received a 1512, LTP Event message!' + data)



# Callback for message code 1105 PARTIAL
# def on_message1105_json_partial(data):
#     print('I received a 1105, Instrument Property Change Event message!' + data)

# Callback for disconnection
def on_disconnect():
    print('Market Data Socket disconnected!')


# Callback for error
def on_error(data):
    """Error from the socket."""
    print('Market Data Error', data)


# Assign the callbacks.
soc.on_connect = on_connect
soc.on_message = on_message
soc.on_message1502_json_full = on_message1502_json_full
soc.on_message1505_json_full = on_message1505_json_full
soc.on_message1507_json_full = on_message1507_json_full
soc.on_message1510_json_full = on_message1510_json_full
soc.on_message1501_json_full = on_message1501_json_full
soc.on_message1512_json_full = on_message1512_json_full
# soc.on_message1105_json_full = on_message1105_json_full
soc.on_message1502_json_partial = on_message1502_json_partial
soc.on_message1505_json_partial = on_message1505_json_partial
soc.on_message1510_json_partial = on_message1510_json_partial
soc.on_message1501_json_partial = on_message1501_json_partial
soc.on_message1512_json_partial = on_message1512_json_partial
# soc.on_message1105_json_partial = on_message1105_json_partial
soc.on_disconnect = on_disconnect
soc.on_error = on_error


# Event listener
el = soc.get_emitter()
el.on('connect', on_connect)
el.on('1501-json-full', on_message1501_json_full)
el.on('1502-json-full', on_message1502_json_full)
el.on('1507-json-full', on_message1507_json_full)
el.on('1512-json-full', on_message1512_json_full)
# el.on('1105-json-full', on_message1105_json_full)
# el.on('1105-json-partial', on_message1105_json_full)


# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
soc.connect()

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000, debug=True)
