from datetime import datetime, timedelta, timezone
import time
import ccxt  # API for accessing Bybit and Binance exchanges
import csv
import config
import urllib3
import json
import requests
import time
import hashlib
import hmac
import urllib.parse
from datetime import datetime
import concurrent.futures

from datetime import datetime, timedelta, timezone
import uuid

# Initialize the API clients for Bybit and Binance exchanges


# Replace with your API keys
api_key_binance = config.api_key_binance
secret_key_binance = config.secret_key_binance

api_key_bybit = config.api_key_bybit
secret_key_bybit = config.secret_key_bybit


def store_when_order_0_to_csv(params, amount):
    # Check if the variable has a value of 0.0
    if amount == 0.0:
        # Open the CSV file in append mode
        with open('order0binance.csv', 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(params.values())


def binance_futures_get_exchange_info():
    response = requests.get('https://fapi.binance.com/fapi/v1/exchangeInfo')
    data = response.json()
    print("Binance's data for precision is loaded.")
    return data


def binance_futures_get_precision(symbol, exchange_info):
    for pair in exchange_info['symbols']:
        if pair['symbol'] == symbol:
            return pair['quantityPrecision']


# Retrieve exchange info
binance_exchange_info = binance_futures_get_exchange_info()
binance_executor = concurrent.futures.ThreadPoolExecutor()
bybit_executor = concurrent.futures.ThreadPoolExecutor()


def adjust_precision(quantity, precision):
    format_str = "{:." + str(precision) + "f}"
    return float(format_str.format(quantity))


def binance_generate_signature(query_string, api_secret):
    return hmac.new(api_secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()


def binance_open_order(api_key, api_secret, symbol, side, order_type, usdt_amount, leverage, price=None):
    precision = binance_futures_get_precision(symbol, binance_exchange_info)
    binance_base_url = 'https://fapi.binance.com'
    binance_endpoint = '/fapi/v1/order'
    binance_url = f'{binance_base_url}{binance_endpoint}'

    timestamp = int(time.time() * 1000)  # Generate current timestamp in milliseconds

    binance_headers = {
        'X-MBX-APIKEY': api_key
    }
    price = round(price, 7)
    print('hat is happening?', usdt_amount, price, 'what is happening?')
    print()
    amount = usdt_amount / price
    print('test69line', amount, price)
    print()
    amount = adjust_precision(amount, precision)
    print(amount, precision, 'wait a second', precision)
    print()
    print('test71line', amount, price, 'lol', amount)
    print('test71line', amount, price, 'lol', amount)
    binance_params = {
        'symbol': symbol,
        'side': side.upper(),
        'type': order_type.upper(),
        'quantity': amount,
        'leverage': leverage,
        'timestamp': timestamp  # Include the timestamp parameter
    }
    print(binance_params, 868686)
    if amount == 0:
        store_when_order_0_to_csv(binance_params, amount)
        print('We found it 97')
        return

    if price is not None and order_type.upper() != 'MARKET':
        binance_params['price'] = price

    if order_type.upper() != 'MARKET':
        binance_params['timeInForce'] = 'GTC'

    query_string = urllib.parse.urlencode(binance_params)
    signature = binance_generate_signature(query_string, api_secret)
    binance_params['signature'] = signature

    http = urllib3.PoolManager()
    encoded_params = urllib.parse.urlencode(binance_params)
    full_url = f"{binance_url}?{encoded_params}"
    response = http.request('POST', full_url, headers=binance_headers)
    response_data = json.loads(response.data.decode('utf-8'))

    print(response_data, 'response_data_binance')


binance_session = requests.Session()


def binance_close_position(api_key, api_secret, symbol):
    # Generate current timestamp in milliseconds
    timestamp = int(time.time() * 1000)

    # Prepare headers
    binance_headers = {
        'X-MBX-APIKEY': api_key
    }

    # Get position information
    params = {
        'symbol': symbol,
        'timestamp': timestamp
    }
    query_string = urllib.parse.urlencode(params)
    signature = binance_generate_signature(query_string, api_secret)
    params['signature'] = signature

    http = urllib3.PoolManager()
    response = http.request('GET', 'https://fapi.binance.com/fapi/v2/positionRisk', headers=binance_headers,
                            fields=params)
    position = json.loads(response.data)

    if position:
        quantity = abs(float(position[0]['positionAmt']))
        if quantity > 0:
            # Create a new order
            params = {
                'symbol': symbol,
                'side': 'SELL' if float(position[0]['positionAmt']) > 0 else 'BUY',
                'type': 'MARKET',
                'quantity': quantity,
                'timestamp': timestamp,
                'reduceOnly': 'true'  # Add this line
            }
            print(params, 'binance closing 160')
            query_string = urllib.parse.urlencode(params)
            signature = binance_generate_signature(query_string, api_secret)
            params['signature'] = signature

            encoded_params = urllib.parse.urlencode(params)
            full_url = f"https://fapi.binance.com/fapi/v1/order?{encoded_params}"
            response = http.request('POST', full_url, headers=binance_headers)
            order = json.loads(response.data)
            # print('167 binance closed order successfully', order)
            return order
        else:
            return "No position to close."
    else:
        return "No position information found."


http = urllib3.PoolManager()
bybit_recv_window = str(5000)
bybit_url = "https://api.bybit.com"  # Testnet endpoint


def bybit_genSignature(api_key, secret_key, payload):
    bybit_time_stamp = str(int(time.time() * 10 ** 3))
    param_str = bybit_time_stamp + api_key + bybit_recv_window + payload
    hash = hmac.new(bytes(secret_key, "utf-8"), param_str.encode("utf-8"), hashlib.sha256)
    signature = hash.hexdigest()
    return signature, bybit_time_stamp


def bybit_HTTP_Request(api_key, secret_key, endPoint, method, payload, Info):
    signature, time_stamp = bybit_genSignature(api_key, secret_key, payload)
    headers = {
        'X-BAPI-API-KEY': api_key,
        'X-BAPI-SIGN': signature,
        'X-BAPI-SIGN-TYPE': '2',
        'X-BAPI-TIMESTAMP': time_stamp,
        'X-BAPI-RECV-WINDOW': bybit_recv_window,
        'Content-Type': 'application/json'
    }

    if method == "POST":
        response = http.request(method, bybit_url + endPoint, headers=headers, body=payload)
    else:
        full_url = bybit_url + endPoint + "?" + payload
        response = http.request(method, full_url, headers=headers)

    response_data = json.loads(response.data.decode('utf-8'))
    # print(response_data)
    return response_data
    # print(Info + " Elapsed Time : " + str(response.elapsed))


def bybit_open_order(api_key, api_secret, symbol, side, order_type, usdt_amount, leverage, price=None):
    endpoint = "/contract/v3/private/order/create"
    method = "POST"
    orderLinkId = uuid.uuid4().hex
    time_in_force = 'GoodTillCancel'
    amount = usdt_amount / price
    amount = round(amount, 8)
    params = f'{{"symbol":"{symbol}","orderType":"{order_type}","side":"{side.capitalize()}","qty":"{amount:.8f}","price":"{price:.8f}","timeInForce":"{time_in_force}","category":"linear","orderLinkId":"{orderLinkId}"}}'
    print('bybit_open_order', params)
    return bybit_HTTP_Request(api_key, api_secret, endpoint, method, params, "Create")


def bybit_close_position(api_key, api_secret, symbol):
    def bybit_open_order_x(api_key, api_secret, symbol, side, order_type, amount, leverage):
        endpoint = "/contract/v3/private/order/create"
        method = "POST"
        orderLinkId = uuid.uuid4().hex
        time_in_force = 'GoodTillCancel'
        amount = round(amount, 8)
        params = f'{{"symbol":"{symbol}","orderType":"{order_type}","side":"{side.capitalize()}","qty":"{amount:.8f}","timeInForce":"{time_in_force}","category":"linear","orderLinkId":"{orderLinkId}"}}'
        # print('bybit_open_order', params)
        return bybit_HTTP_Request(api_key, api_secret, endpoint, method, params, "Create")

    def bybit_get_open_orders(api_key, api_secret, symbol):
        try:
            endpoint = "/contract/v3/private/position/list"
            method = "GET"
            payload = f'symbol={symbol}'
            return bybit_HTTP_Request(api_key, api_secret, endpoint, method, payload, "Fetch open orders")
        except Exception as e:
            print(f"Exception in bybit_get_open_orders: {e}")

    # First, get the open order
    open_orders = bybit_get_open_orders(api_key, api_secret, symbol)
    # print(open_orders, 'test')

    if open_orders is None:
        print("Error retrieving open orders.")
        return

    if open_orders['retCode'] == 0 and open_orders['result']['list']:
        # Assuming there's only one open order for the symbol
        open_order = open_orders['result']['list'][0]

        # Get the size (quantity) of the open order
        size = float(open_order['size'])

        # Get the side of the open order. The side for the close order will be the opposite.
        open_order_side = open_order['side']
        close_order_side = 'Sell' if open_order_side == 'Buy' else 'Buy'

        # Close the order by placing an opposite order with the same size.
        # Assuming the order_type, usdt_amount, leverage and price are same as those in bybit_open_order
        order_type = 'Market'  # For simplicity, we are using 'Market' order to close the position immediately.
        usdt_amount = size  # The size of the open order
        leverage = 10  # The leverage used for the open order
        # print(api_key, api_secret, symbol, close_order_side, order_type, usdt_amount, leverage)
        bybit_open_order_x(api_key, api_secret, symbol, close_order_side, order_type, usdt_amount, leverage)

    else:
        print("No open order to close.")


# arbitrage_calculator.py
from config import ARBITRAGE_THRESHOLD, MAX_POSITIONS_PER_PAIR, MAX_TOTAL_POSITIONS, PERCENT_ACCEPTANCE, ORDER_AMOUNT

open_positions = {}


def close_position(symbol, long_exchange, short_exchange, amount, long_price, short_price):
    if symbol not in open_positions:
        print(f"No open positions found for {symbol}")
        return
    percent_profit = ((long_price - short_price) / short_price) * 100
    position_to_close = None
    for position in open_positions[symbol]:
        if position["long_exchange"] == long_exchange and position["short_exchange"] == short_exchange:
            position_to_close = position
            break

    if position_to_close is None:
        print(f"No matching position found for {symbol}")
        return

    if long_exchange == "binance":
        # long_order = place_binance_order(symbol, "sell", amount)
        # short_order = place_bybit_order(symbol, "buy", amount)
        # result_binance = binance_close_position(api_key_binance, secret_key_binance, symbol)
        # print(result_binance)
        # result_bybit = bybit_close_position(api_key_bybit, secret_key_bybit, symbol)
        print('closing long_exchange binance 274')
        # bybit_executor.submit(bybit_close_position, api_key_bybit, secret_key_bybit, symbol)
        # binance_executor.submit(binance_close_position, api_key_binance, secret_key_binance, symbol)
        print('after closing long_exchange binance 277')
        # print(result_bybit)
        # close_positions_concurrently(api_key_binance, secret_key_binance, api_key_bybit, secret_key_bybit, symbol)
        pass
    else:
        # long_order = place_bybit_order(symbol, "sell", amount)
        # short_order = place_binance_order(symbol, "buy", amount)
        # result_binance = binance_close_position(api_key_binance, secret_key_binance, symbol)
        # print(result_binance)
        # result_bybit = bybit_close_position(api_key_bybit, secret_key_bybit, symbol)
        print('closing long_exchange bybit 287')
        # bybit_executor.submit(bybit_close_position, api_key_bybit, secret_key_bybit, symbol)
        # binance_executor.submit(binance_close_position, api_key_binance, secret_key_binance, symbol)
        print('closing long_exchange bybit 290')
        # print(result_bybit)
        # close_positions_concurrently(api_key_binance, secret_key_binance, api_key_bybit, secret_key_bybit, symbol)
        pass
    # if long_order and short_order:
    if True:
        position_to_close["close_time"] = datetime.now(timezone.utc)
        open_positions[symbol].remove(position_to_close)
        print(f"Closed position: long on {long_exchange}, short on {short_exchange}")
        write_open_positions_to_csv()
        write_trading_history_to_csv("trade_closed", symbol, long_exchange, short_exchange, amount,
                                     datetime.now(timezone.utc), long_price, short_price, percent_profit)

    else:
        print("Failed to close position")


def display_open_positions():
    print("\nCurrent open positions:")
    for symbol, positions in open_positions.items():
        if len(positions) > 0:
            print(f"{symbol}: {len(positions)} open trades")
            for i, position in enumerate(positions, start=1):
                print(
                    f"  {i}. Long on {position['long_exchange']}, short on {position['short_exchange']}, amount: {position['amount']}")


def write_trading_history_to_csv(trade_type, symbol, long_exchange, short_exchange, amount, timestamp, long_price,
                                 short_price, percent_profit, filename="trading_history.csv"):
    with open(filename, mode="a", newline="") as csvfile:
        fieldnames = ["trade_type", "symbol", "long_exchange", "short_exchange", "amount", "timestamp", "long_price",
                      "short_price", "percent_profit"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Write header only if the file is empty
        if csvfile.tell() == 0:
            writer.writeheader()

        trade_data = {
            "trade_type": trade_type,
            "symbol": symbol,
            "long_exchange": long_exchange,
            "short_exchange": short_exchange,
            "amount": amount,
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "long_price": long_price,
            "short_price": short_price,
            "percent_profit": percent_profit,
        }
        writer.writerow(trade_data)


def write_open_positions_to_csv(filename="open_positions.csv"):
    with open(filename, mode="w", newline="") as csvfile:
        fieldnames = ["symbol", "long_exchange", "short_exchange", "amount", "open_time", "long_price", "short_price",
                      "percent_profit"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for symbol, positions in open_positions.items():
            for position in positions:
                position_data = position.copy()
                position_data["symbol"] = symbol
                position_data["open_time"] = position_data["open_time"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                writer.writerow(position_data)


# def place_binance_order(symbol, side, amount, price=None):
#     symbol = symbol.replace('-', '')  # remove dash from symbol
#     order_type = 'limit' if price else 'market'
#     try:
#         order = binance.create_order(
#             symbol,
#             type=order_type,
#             side=side,
#             amount=amount,
#             price=price,
#             params={'timeInForce': 'GTC'}
#         )
#         return order
#     except Exception as e:
#         print(f"Binance order error: {e}")
#         return None
#
#
# def place_bybit_order(symbol, side, amount, price=None):
#     order_type = 'Limit' if price else 'Market'
#     try:
#         order = bybit.create_order(
#             symbol,
#             type=order_type,
#             side=side,
#             amount=amount,
#             price=price,
#             params={'time_in_force': 'GoodTillCancel'}
#         )
#         return order
#     except Exception as e:
#         print(f"Bybit order error: {e}")
#         return None


def execute_arbitrage_trade(symbol, long_exchange, short_exchange, amount, long_price, short_price):
    usdt_amount = amount
    if symbol not in open_positions:
        open_positions[symbol] = []

    percent_profit = ((short_price - long_price) / long_price) * 100
    positions_per_pair = len(open_positions[symbol])
    total_positions = sum(len(positions) for positions in open_positions.values())

    if positions_per_pair < MAX_POSITIONS_PER_PAIR and total_positions < MAX_TOTAL_POSITIONS:
        if long_exchange == "binance":
            print(
                f"long_exchange: {long_exchange}, positions_per_pair: {positions_per_pair}, MAX_POSITIONS_PER_PAIR: {MAX_POSITIONS_PER_PAIR}, total_positions: {total_positions}, MAX_TOTAL_POSITIONS: {MAX_TOTAL_POSITIONS}")

            # long_order = place_binance_order(symbol, "buy", amount)
            # short_order = place_bybit_order(symbol, "sell", amount)
            print(long_exchange, symbol, 'buy', 'market', usdt_amount, 3,
                  long_price, datetime.now(), '374')
            # binance_open_order(api_key_binance, secret_key_binance, symbol, 'buy', 'market', usdt_amount, 3,
            #                    long_price)
            # binance_executor.submit(binance_open_order, api_key_binance, secret_key_binance, symbol, 'buy', 'market',
            #                         usdt_amount, 3,
            #                         long_price)
            print('380 haha I was launched')
            print(short_exchange, symbol, 'sell', 'market', usdt_amount, 3,
                  short_price, datetime.now())
            # bybit_open_order(api_key_bybit, secret_key_bybit, symbol, 'sell', 'market', usdt_amount, 3, short_price)
            # bybit_executor.submit(bybit_open_order, api_key_bybit, secret_key_bybit, symbol, 'Sell', 'Market',
            #                       usdt_amount, 3, short_price)
            print(f"long_exchange_x: {long_exchange}, AfterBothOrdersTheTime", datetime.now())

            pass
        else:
            print(
                f"long_exchange: {long_exchange}, positions_per_pair: {positions_per_pair}, MAX_POSITIONS_PER_PAIR: {MAX_POSITIONS_PER_PAIR}, total_positions: {total_positions}, MAX_TOTAL_POSITIONS: {MAX_TOTAL_POSITIONS}")
            # long_order = place_bybit_order(symbol, "buy", amount)
            # short_order = place_binance_order(symbol, "sell", amount)
            print(long_exchange, symbol, 'buy', 'market', usdt_amount, 3, long_exchange, datetime.now())
            # bybit_open_order(api_key_bybit, secret_key_bybit, symbol, 'buy', 'market', usdt_amount, 3, long_price)
            # bybit_executor.submit(bybit_open_order, api_key_bybit, secret_key_bybit, symbol, 'Buy', 'Market',
            #                       usdt_amount, 3, long_price)
            print(short_exchange, symbol, 'sell', 'market', usdt_amount, 3,
                  short_price, datetime.now())
            # binance_open_order(api_key_binance, secret_key_binance, symbol, 'sell', 'market', usdt_amount, 3,
            #                    short_price)
            # binance_executor.submit(binance_open_order, api_key_binance, secret_key_binance, symbol, 'sell', 'market',
            #                         usdt_amount, 3,
            #                         short_price)
            print(f"long_exchange_x: {long_exchange}, AfterBothOrdersTheTime", datetime.now())

            pass
        # if long_order and short_order: add it back later
        if True:
            open_positions[symbol].append({
                "long_exchange": long_exchange,
                "short_exchange": short_exchange,
                "amount": amount,
                "open_time": datetime.now(timezone.utc),
                "long_price": long_price,
                "short_price": short_price,
                "percent_profit": percent_profit
            })
            print(f"Arbitrage trade executed: long on {long_exchange}, short on {short_exchange} {datetime.now()}")
            write_trading_history_to_csv("trade_open", symbol, long_exchange, short_exchange, amount,
                                         datetime.now(timezone.utc), long_price, short_price, percent_profit)
            display_open_positions()
            print()
        else:
            print("Failed to execute arbitrage trade")
    else:
        limits_reached = []
        if positions_per_pair >= MAX_POSITIONS_PER_PAIR:
            limits_reached.append(f"maximum positions per pair ({MAX_POSITIONS_PER_PAIR})")
        if total_positions >= MAX_TOTAL_POSITIONS:
            limits_reached.append(f"maximum total positions ({MAX_TOTAL_POSITIONS})")

        limits_str = " and ".join(limits_reached)
        # print(f"Cannot execute arbitrage trade for {symbol}: reached {limits_str}.")
        # print(f"Open trades for {symbol}: {positions_per_pair}")

        write_open_positions_to_csv()


class ThrottledPrinter:
    def __init__(self, min_interval_seconds=1):
        self.min_interval_seconds = min_interval_seconds
        self.last_print_timestamp = datetime.utcnow()

    def print_throttled(self, message):
        now = datetime.utcnow()
        seconds_since_last_print = (now - self.last_print_timestamp).total_seconds()
        if seconds_since_last_print >= self.min_interval_seconds:
            print(message)
            self.last_print_timestamp = now
            return True
        else:
            return False


throttled_printer = ThrottledPrinter()


def check_websocket_health(latest_prices, max_delay_ms=20000):  # max_delay is in milliseconds
    now = datetime.utcnow()
    most_recent_timestamp = None
    second_most_recent_timestamp = None
    most_recent_exchange = None
    second_most_recent_exchange = None
    most_recent_pair = None
    second_most_recent_pair = None

    for pair, exchange_data in latest_prices.items():
        for exchange, data in exchange_data.items():
            timestamp = data[0]['timestamp']
            if timestamp is not None:
                if most_recent_timestamp is None or timestamp > most_recent_timestamp:
                    second_most_recent_timestamp = most_recent_timestamp
                    second_most_recent_exchange = most_recent_exchange
                    second_most_recent_pair = most_recent_pair
                    most_recent_timestamp = timestamp
                    most_recent_exchange = exchange
                    most_recent_pair = pair
                elif second_most_recent_timestamp is None or timestamp > second_most_recent_timestamp:
                    second_most_recent_timestamp = timestamp
                    second_most_recent_exchange = exchange
                    second_most_recent_pair = pair

    max_delay_s = max_delay_ms / 1000  # convert to seconds

    if most_recent_timestamp is not None:
        delay_s = (now - most_recent_timestamp).total_seconds()
        delay_ms = delay_s * 1000  # convert back to milliseconds for display
        if delay_s > max_delay_s:
            if throttled_printer.print_throttled(
                    f"Warning: Most recent data is outdated by {delay_ms} ms. Pair: {most_recent_pair}, Exchange: {most_recent_exchange}, Most recent timestamp: {most_recent_timestamp}"):
                if second_most_recent_timestamp is not None:
                    second_delay_s = (now - second_most_recent_timestamp).total_seconds()
                    second_delay_ms = second_delay_s * 1000  # convert back to milliseconds for display
                    print(
                        f"Second most recent data is outdated by {second_delay_ms} ms. Pair: {second_most_recent_pair}, Exchange: {second_most_recent_exchange}, Most recent timestamp: {second_most_recent_timestamp}")
            return False

    return True


def calculate_percent_profit(long_price, short_price):
    return ((short_price - long_price) / long_price) * 100


# This dictionary keeps track of the last print time for each pair
last_print_times_1 = {}
last_print_times_2 = {}


def process_arbitrage_data(pair, latest_prices, last_arbitrage_opportunities, delayed_prints):
    binance_data = latest_prices[pair]['binance'][0]
    bybit_data = latest_prices[pair]['bybit'][0]

    if (binance_data['bid_price'] is None or binance_data['ask_price'] is None or
            bybit_data['bid_price'] is None or bybit_data['ask_price'] is None):
        return

    bybit_timestamp = latest_prices[pair]['bybit'][0]['timestamp']
    binance_timestamp = latest_prices[pair]['binance'][0]['timestamp']

    # Check if the data is outdated
    now = datetime.utcnow()
    max_allowed_delay = timedelta(seconds=1300)
    if (now - bybit_timestamp > max_allowed_delay) or (now - binance_timestamp > max_allowed_delay):
        print(
            f"Warning: Data for {pair} is outdated. Binance timestamp: {binance_timestamp}, Bybit timestamp: {bybit_timestamp}")
        return

    check_websocket_health(latest_prices, max_delay_ms=3000)

    bybit_timestamp = latest_prices[pair]['bybit'][0]['timestamp'].strftime("%Y-%m-%d %H:%M:%S.%f")
    binance_timestamp = latest_prices[pair]['binance'][0]['timestamp'].strftime("%Y-%m-%d %H:%M:%S.%f")
    percent_profit = calculate_percent_profit(binance_data['ask_price'], bybit_data['bid_price'])
    # print(percent_profit, '1x')
    print_delay = 1  # delay in seconds
    # Check if Bybit's bid price is higher than Binance's ask price
    if bybit_data['bid_price'] > binance_data['ask_price']:
        long_exchange = 'binance'
        short_exchange = 'bybit'
        percent_profit = calculate_percent_profit(binance_data['ask_price'], bybit_data['bid_price'])
        # print(percent_profit, '1xx')
        if percent_profit >= ARBITRAGE_THRESHOLD:
            if pair not in last_print_times_1 or time.time() - last_print_times_1[pair] >= print_delay:
                print(pair, long_exchange, binance_data['ask_price'], short_exchange, bybit_data['bid_price'], 'amount',
                      f'percent_profit1xxc: {percent_profit:.2f}%, Bybit timestamp: {bybit_timestamp}, Binance timestamp: {binance_timestamp}')
                last_print_times_1[pair] = time.time()  # update the last print time for this pair
            execute_arbitrage_trade(pair, long_exchange, short_exchange, ORDER_AMOUNT, binance_data['ask_price'],
                                    bybit_data['bid_price'])
            percent_profit = calculate_percent_profit(bybit_data['bid_price'], binance_data['ask_price'])
            # print(percent_profit, '1xxz', pair)
    elif bybit_data['ask_price'] < binance_data['bid_price']:
        percent_profit = calculate_percent_profit(bybit_data['ask_price'], binance_data['bid_price'])
        # print(percent_profit, '1xxz', pair)
        if percent_profit >= PERCENT_ACCEPTANCE:
            # print('testtest', pair, percent_profit)
            if pair in open_positions:
                # Find the corresponding long and short exchanges for the open position
                for position in open_positions[pair]:
                    # do something with position
                    if position['long_exchange'] == 'binance':
                        close_position(pair, position['long_exchange'], position['short_exchange'], position['amount'],
                                       binance_data['bid_price'], bybit_data['ask_price'])
                        print('xtest', position)
                        print(
                            f"Closing position for1xxc {pair, position['long_exchange'], binance_data['bid_price'], position['short_exchange'], bybit_data['ask_price']} as price difference is {percent_profit:.2f}% bybit_data['bid_price'] > binance_data['ask_price']",
                            bybit_data['bid_price'], binance_data['ask_price'])

    # Check if Binance's bid price is higher than Bybit's ask price

    # print(calculate_percent_profit(bybit_data['ask_price'], binance_data['bid_price']), '2xy')
    if binance_data['bid_price'] > bybit_data['ask_price']:
        long_exchange = 'bybit'
        short_exchange = 'binance'
        percent_profit = calculate_percent_profit(bybit_data['ask_price'], binance_data['bid_price'])
        # print(percent_profit, '2x')
        if percent_profit >= ARBITRAGE_THRESHOLD:

            if pair not in last_print_times_2 or time.time() - last_print_times_2[pair] >= print_delay:
                print(pair, long_exchange, bybit_data['ask_price'], short_exchange, binance_data['bid_price'], 'amount',
                      f'percent_profit2xxc: {percent_profit:.2f}%, Bybit timestamp: {bybit_timestamp}, Binance timestamp: {binance_timestamp}')
                last_print_times_2[pair] = time.time()  # update the last print time for this pair
            execute_arbitrage_trade(pair, long_exchange, short_exchange, ORDER_AMOUNT, bybit_data['ask_price'],
                                    binance_data['bid_price'])
            percent_profit = calculate_percent_profit(binance_data['bid_price'], bybit_data['ask_price'])
            # print(percent_profit, '2xxz', pair)
    elif binance_data['ask_price'] < bybit_data['bid_price']:
        percent_profit = calculate_percent_profit(binance_data['ask_price'], bybit_data['bid_price'])
        # print(percent_profit, '2xxz', pair)
        if percent_profit >= PERCENT_ACCEPTANCE:
            # print('testtest', pair, percent_profit)
            if pair in open_positions:
                # Find the corresponding long and short exchanges for the open position
                for position in open_positions[pair]:
                    # do something with position
                    if position['long_exchange'] == 'bybit':
                        close_position(pair, position['long_exchange'], position['short_exchange'], position['amount'],
                                       bybit_data['bid_price'], binance_data['ask_price'])
                        print('xtest', position)
                        print(
                            f"Closing position for2xxc {pair, position['long_exchange'], bybit_data['bid_price'], position['short_exchange'], binance_data['ask_price']} as price "
                            f"difference is {percent_profit:.2f}%,binance_data['bid_price'] > bybit_data['ask_price']",
                            binance_data['bid_price'], bybit_data['ask_price'])



