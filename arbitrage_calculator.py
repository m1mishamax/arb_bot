from datetime import datetime, timedelta, timezone
import time
import ccxt  # API for accessing Bybit and Binance exchanges
import csv

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

# arbitrage_calculator.py
ARBITRAGE_THRESHOLD = 0.25
MAX_POSITIONS_PER_PAIR = 1
MAX_TOTAL_POSITIONS = 5
PERCENT_ACCEPTANCE = 0.05

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
        pass
    else:
        # long_order = place_bybit_order(symbol, "sell", amount)
        # short_order = place_binance_order(symbol, "buy", amount)
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
    if symbol not in open_positions:
        open_positions[symbol] = []

    percent_profit = ((short_price - long_price) / long_price) * 100
    positions_per_pair = len(open_positions[symbol])
    total_positions = sum(len(positions) for positions in open_positions.values())

    if positions_per_pair < MAX_POSITIONS_PER_PAIR and total_positions < MAX_TOTAL_POSITIONS:
        if long_exchange == "binance":
            # long_order = place_binance_order(symbol, "buy", amount)
            # short_order = place_bybit_order(symbol, "sell", amount)
            pass
        else:
            # long_order = place_bybit_order(symbol, "buy", amount)
            # short_order = place_binance_order(symbol, "sell", amount)
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
            print(f"Arbitrage trade executed: long on {long_exchange}, short on {short_exchange}")
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
    max_allowed_delay = timedelta(seconds=300)
    if (now - bybit_timestamp > max_allowed_delay) or (now - binance_timestamp > max_allowed_delay):
        print(
            f"Warning: Data for {pair} is outdated. Binance timestamp: {binance_timestamp}, Bybit timestamp: {bybit_timestamp}")
        return

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
            execute_arbitrage_trade(pair, long_exchange, short_exchange, 10, binance_data['ask_price'],
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
            execute_arbitrage_trade(pair, long_exchange, short_exchange, 10, bybit_data['ask_price'],
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
