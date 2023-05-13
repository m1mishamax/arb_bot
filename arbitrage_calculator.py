from datetime import datetime, timedelta, timezone
import time
import ccxt  # API for accessing Bybit and Binance exchanges
import csv
import threading
import config

# Initialize the API clients for Bybit and Binance exchanges
bybit = ccxt.bybit({
    'enableRateLimit': True,
    'options': {
        'defaultType': 'future',
    },
    'rateLimit': 2000,  # Bybit allows 50 requests per second
    'apiKey': 'TT6Hq7UlERJFiOYUy2ilrz2qFU2KTS6MBAU8Ca3v6tbEgMtu5GyEhtlhcyUgFzAd',
    'secret': 'HZqhoK14NHEMOZrOmYMThK9SDXKhoQ72ZZdYAhDevw8i31ZF04qUwL5kfMLDZlKk',
})
binance = ccxt.binance({
    'enableRateLimit': True,
    'options': {
        'defaultType': 'future',
    },
    'rateLimit': 2000,  # Binance allows 1200 requests per minute
    'apiKey': 'un8gN8r1sJuvEWQ9wD',
    'secret': 'oBtFDAlJtmLFoHTdt8P80GTpYO6FBEEmMztD',
})

# Replace with your API keys
api_key_binance = config.api_key_binance
secret_key_binance = config.secret_key_binance

api_key_bybit = config.api_key_bybit
secret_key_bybit = config.secret_key_bybit


def open_order_binance(api_key, api_secret, symbol, side, order_type, usdt_amount, leverage, price=None):
    binance_futures = ccxt.binance({
        'apiKey': api_key,
        'secret': api_secret,
        'enableRateLimit': True,
        'options': {
            'defaultType': 'future'
        }
    })
    if price is None:
        # Get the current price if not provided
        ticker = binance_futures.fetch_ticker(symbol)
        price = ticker['last']

    # Calculate the amount of the asset to buy
    amount = usdt_amount / price
    # Binance's minimum order size precision is typically to 3 decimal places, but this can vary per asset
    amount = round(amount, 5)

    market_order = {
        'symbol': symbol,
        'side': side,
        'type': order_type,
        'amount': amount,
    }

    if price is not None:
        market_order['price'] = price

    return binance_futures.create_order(**market_order)


from binance.client import Client
from binance.exceptions import BinanceAPIException


def binance_close_position(api_key, api_secret, symbol):
    client = Client(api_key, api_secret)
    try:
        position = client.futures_position_information(symbol=symbol)
        if position:
            quantity = abs(float(position[0]['positionAmt']))
            if quantity > 0:
                order = client.futures_create_order(
                    symbol=symbol,
                    side='SELL' if float(position[0]['positionAmt']) > 0 else 'BUY',
                    type='MARKET',
                    quantity=quantity
                )
                return order
            else:
                return "No position to close."
        else:
            return "No position information found."
    except BinanceAPIException as e:
        return f"Error closing position: {e.message}"


def bybit_close_position(api_key, api_secret, symbol):
    bybit_futures = ccxt.bybit()
    bybit_futures.apiKey = api_key
    bybit_futures.secret = api_secret

    # Fetch position details
    response = bybit_futures.private_get_private_linear_position_list({'symbol': symbol})
    position = response['result'][0]
    qty = abs(int(position['size']))  # Absolute value of size

    # Close position
    if position['side'] == 'Buy':  # Long position
        order = bybit_futures.create_market_sell_order(symbol=symbol, amount=qty)
    elif position['side'] == 'Sell':  # Short position
        order = bybit_futures.create_market_buy_order(symbol=symbol, amount=qty)

    # Return the order result or any other desired output
    return order


def open_order_bybit(api_key, api_secret, symbol, side, order_type, usdt_amount, leverage, price=None):
    bybit_futures = ccxt.bybit({
        'apiKey': api_key_bybit,
        'secret': secret_key_bybit,
        'enableRateLimit': True,
        'options': {
            'defaultType': 'future'
        }
    })

    bybit_futures.apiKey = api_key
    bybit_futures.secret = api_secret

    if price is None:
        # Get the current price if not provided
        ticker = bybit_futures.fetch_ticker(symbol)
        price = ticker['last']

    # Calculate the quantity of the asset to buy
    quantity = usdt_amount / price
    # Bybit's order quantity precision is typically to 2 decimal places
    quantity = round(quantity, 5)

    market_order = {
        'symbol': symbol,
        'side': side,
        'type': order_type,
        'amount': quantity,
    }

    if price is not None:
        market_order['price'] = price

    return bybit_futures.create_order(**market_order)


def binance_long_bybit_short(api_key_binance, secret_key_binance, api_key_bybit, secret_key_bybit, symbol, usdt_amount,
                             leverage, long_price, short_price):
    # Create two threads, one for each function call
    binance_thread = threading.Thread(target=open_order_binance, args=(
        api_key_binance, secret_key_binance, symbol, 'buy', 'market', usdt_amount, leverage, long_price))
    bybit_thread = threading.Thread(target=open_order_bybit, args=(
        api_key_bybit, secret_key_bybit, symbol, 'sell', 'market', usdt_amount, leverage, short_price))

    # Start the threads
    binance_thread.start()
    bybit_thread.start()

    # Wait for both threads to finish
    binance_thread.join()
    bybit_thread.join()


def bybit_long_binance_short(api_key_bybit, secret_key_bybit, api_key_binance, secret_key_binance, symbol, usdt_amount,
                             leverage, short_price, long_price):
    # Create two threads, one for each function call
    bybit_thread = threading.Thread(target=open_order_bybit, args=(
        api_key_bybit, secret_key_bybit, symbol, 'buy', 'market', usdt_amount, leverage, short_price))
    binance_thread = threading.Thread(target=open_order_binance, args=(
        api_key_binance, secret_key_binance, symbol, 'sell', 'market', usdt_amount, leverage, long_price))

    # Start the threads
    bybit_thread.start()
    binance_thread.start()

    # Wait for both threads to finish
    bybit_thread.join()
    binance_thread.join()


def close_positions_concurrently(api_key_binance, secret_key_binance, api_key_bybit, secret_key_bybit, symbol):
    # Create two threads, one for each function call
    binance_thread = threading.Thread(target=binance_close_position, args=(api_key_binance, secret_key_binance, symbol))
    bybit_thread = threading.Thread(target=bybit_close_position, args=(api_key_bybit, secret_key_bybit, symbol))

    # Start the threads
    binance_thread.start()
    bybit_thread.start()

    # Wait for both threads to finish
    binance_thread.join()
    bybit_thread.join()


# arbitrage_calculator.py
from config import ARBITRAGE_THRESHOLD, MAX_POSITIONS_PER_PAIR, MAX_TOTAL_POSITIONS, PERCENT_ACCEPTANCE

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
        # print(result_bybit)
        # close_positions_concurrently(api_key_binance, secret_key_binance, api_key_bybit, secret_key_bybit, symbol)
        pass
    else:
        # long_order = place_bybit_order(symbol, "sell", amount)
        # short_order = place_binance_order(symbol, "buy", amount)
        # result_binance = binance_close_position(api_key_binance, secret_key_binance, symbol)
        # print(result_binance)
        # result_bybit = bybit_close_position(api_key_bybit, secret_key_bybit, symbol)
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


def place_binance_order(symbol, side, amount, price=None):
    symbol = symbol.replace('-', '')  # remove dash from symbol
    order_type = 'limit' if price else 'market'
    try:
        order = binance.create_order(
            symbol,
            type=order_type,
            side=side,
            amount=amount,
            price=price,
            params={'timeInForce': 'GTC'}
        )
        return order
    except Exception as e:
        print(f"Binance order error: {e}")
        return None


def place_bybit_order(symbol, side, amount, price=None):
    order_type = 'Limit' if price else 'Market'
    try:
        order = bybit.create_order(
            symbol,
            type=order_type,
            side=side,
            amount=amount,
            price=price,
            params={'time_in_force': 'GoodTillCancel'}
        )
        return order
    except Exception as e:
        print(f"Bybit order error: {e}")
        return None


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
                  long_price, datetime.now())
            # open_order_binance(api_key_binance, secret_key_binance, symbol, 'buy', 'market', usdt_amount, 3,
            #                    long_price)
            print(long_exchange, symbol, 'sell', 'market', usdt_amount, 3,
                  short_price, datetime.now())
            open_order_bybit(api_key_bybit, secret_key_bybit, symbol, 'sell', 'market', usdt_amount, 3, short_price)
            print(f"long_exchange_x: {long_exchange}, AfterBothOrdersTheTime", datetime.now())

            # binance_long_bybit_short(api_key_binance, secret_key_binance, api_key_bybit, secret_key_bybit, symbol,
            #                          usdt_amount, 3, long_price, short_price)

            pass
        else:
            print(
                f"long_exchange: {long_exchange}, positions_per_pair: {positions_per_pair}, MAX_POSITIONS_PER_PAIR: {MAX_POSITIONS_PER_PAIR}, total_positions: {total_positions}, MAX_TOTAL_POSITIONS: {MAX_TOTAL_POSITIONS}")
            # long_order = place_bybit_order(symbol, "buy", amount)
            # short_order = place_binance_order(symbol, "sell", amount)
            print(long_exchange, symbol, 'buy', 'market', usdt_amount, 3, short_price, datetime.now())
            open_order_bybit(api_key_bybit, secret_key_bybit, symbol, 'buy', 'market', usdt_amount, 3, short_price)

            print(long_exchange, symbol, 'sell', 'market', usdt_amount, 3,
                  long_price, datetime.now())
            # open_order_binance(api_key_binance, secret_key_binance, symbol, 'sell', 'market', usdt_amount, 3,
            #                    long_price)
            print(f"long_exchange_x: {long_exchange}, AfterBothOrdersTheTime", datetime.now())

            # bybit_long_binance_short(api_key_bybit, secret_key_bybit, api_key_binance, secret_key_binance, symbol,
            #                          usdt_amount, 3, short_price, long_price)
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
        print(f"Cannot execute arbitrage trade for {symbol}: reached {limits_str}.")
        print(f"Open trades for {symbol}: {positions_per_pair}")
        display_open_positions()
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
    # Check if Bybit's bid price is higher than Binance's ask price
    if bybit_data['bid_price'] > binance_data['ask_price']:
        long_exchange = 'binance'
        short_exchange = 'bybit'
        percent_profit = calculate_percent_profit(binance_data['ask_price'], bybit_data['bid_price'])
        # print(percent_profit, '1xx')
        if percent_profit >= ARBITRAGE_THRESHOLD:
            print(pair, long_exchange, binance_data['ask_price'], short_exchange, bybit_data['bid_price'], 'amount',
                  f'percent_profit1xxc: {percent_profit:.2f}%, Bybit timestamp: {bybit_timestamp}, Binance timestamp: {binance_timestamp}')
            execute_arbitrage_trade(pair, long_exchange, short_exchange, 6, binance_data['ask_price'],
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
            print(pair, long_exchange, bybit_data['ask_price'], short_exchange, binance_data['bid_price'], 'amount',
                  f'percent_profit2xxc: {percent_profit:.2f}%, Bybit timestamp: {bybit_timestamp}, Binance timestamp: {binance_timestamp}')
            execute_arbitrage_trade(pair, long_exchange, short_exchange, 6, bybit_data['ask_price'],
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
