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
ARBITRAGE_THRESHOLD = 0.20
MAX_POSITIONS_PER_PAIR = 2
MAX_TOTAL_POSITIONS = 6
PERCENT_ACCEPTANCE = -0.10

open_positions = {}


def close_position(symbol, long_exchange, short_exchange, amount):
    if symbol not in open_positions:
        print(f"No open positions found for {symbol}")
        return

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
                                     datetime.now(timezone.utc))

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


def write_trading_history_to_csv(trade_type, symbol, long_exchange, short_exchange, amount, timestamp,
                                 filename="trading_history.csv"):
    with open(filename, mode="a", newline="") as csvfile:
        fieldnames = ["trade_type", "symbol", "long_exchange", "short_exchange", "amount", "timestamp"]
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
        }
        writer.writerow(trade_data)


def write_open_positions_to_csv(filename="open_positions.csv"):
    with open(filename, mode="w", newline="") as csvfile:
        fieldnames = ["symbol", "long_exchange", "short_exchange", "amount", "open_time"]  # Add open_time here
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


def execute_arbitrage_trade(symbol, long_exchange, short_exchange, amount):
    if symbol not in open_positions:
        open_positions[symbol] = []

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
                "open_time": datetime.now(timezone.utc)
            })
            print(f"Arbitrage trade executed: long on {long_exchange}, short on {short_exchange}")
            write_trading_history_to_csv("trade_open", symbol, long_exchange, short_exchange, amount,
                                         datetime.now(timezone.utc))
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

    bybit_timestamp = latest_prices[pair]['bybit'][0]['timestamp'].strftime("%Y-%m-%d %H:%M:%S.%f")
    binance_timestamp = latest_prices[pair]['binance'][0]['timestamp'].strftime("%Y-%m-%d %H:%M:%S.%f")

    # Check if Bybit's bid price is higher than Binance's ask price
    if bybit_data['bid_price'] > binance_data['ask_price']:
        long_exchange = 'binance'
        short_exchange = 'bybit'
        percent_profit = calculate_percent_profit(binance_data['ask_price'], bybit_data['bid_price'])
        if percent_profit >= ARBITRAGE_THRESHOLD:
            print(pair, long_exchange, binance_data['ask_price'], short_exchange, bybit_data['bid_price'], 'amount',
                  f'percent_profit: {percent_profit:.2f}%, Bybit timestamp: {bybit_timestamp}, Binance timestamp: {binance_timestamp}')
            execute_arbitrage_trade(pair, long_exchange, short_exchange, 10)
    # Check if Binance's bid price is higher than Bybit's ask price
    if binance_data['bid_price'] > bybit_data['ask_price']:
        long_exchange = 'bybit'
        short_exchange = 'binance'
        percent_profit = calculate_percent_profit(bybit_data['ask_price'], binance_data['bid_price'])
        if percent_profit >= ARBITRAGE_THRESHOLD:
            print(pair, long_exchange, bybit_data['ask_price'], short_exchange, binance_data['bid_price'], 'amount',
                  f'percent_profit: {percent_profit:.2f}%, Bybit timestamp: {bybit_timestamp}, Binance timestamp: {binance_timestamp}')
            execute_arbitrage_trade(pair, long_exchange, short_exchange, 10)

def calculate_arbitrage(pair, latest_prices, last_arbitrage_opportunities, delayed_prints):
    # print('latest_prices',latest_prices)
    # The function implementation remains the same
    # print(latest_prices[pair]['bybit'])

    bybit_data = latest_prices[pair]['bybit']
    binance_data = latest_prices[pair]['binance']

    if bybit_data is None or binance_data is None:
        return

    bybit_price = bybit_data['price']
    # print(bybit_price)
    binance_price = binance_data['price']
    # print(binance_price)
    # print(latest_prices[pair]['bybit']['timestamp'].replace(tzinfo=timezone.utc),'testaks')
    bybit_timestamp = latest_prices[pair]['bybit']['timestamp'].replace(tzinfo=timezone.utc)
    binance_timestamp = latest_prices[pair]['binance']['timestamp'].replace(tzinfo=timezone.utc)
    bybit_timestamp_str = bybit_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    binance_timestamp_str = binance_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    percentage_diff = ((bybit_price - binance_price) / binance_price) * 100
    # print(percentage_diff)
    current_time = datetime.now(timezone.utc)
    current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    bybit_diff_local = abs((bybit_timestamp - current_time).total_seconds()) * 1000
    binance_diff_local = abs((binance_timestamp - current_time).total_seconds()) * 1000
    average_diff_local = (bybit_diff_local + binance_diff_local) / 2

    # Inside the calculate_arbitrage function
    if 0 <= abs(percentage_diff) <= PERCENT_ACCEPTANCE:
        # print('testaaaaaaaaaaaa')
        # Find the corresponding long and short exchanges for the open position
        if pair in open_positions:
            for position in open_positions[pair]:
                # do something with position
                close_position(pair, position['long_exchange'], position['short_exchange'], position['amount'])
                print('test', position)
                print(f"Closing position for {pair} as price difference is {percentage_diff:.2f}%")

    if abs(percentage_diff) >= ARBITRAGE_THRESHOLD:

        print(f"Arbitrage opportunity for {pair}: {percentage_diff:.2f}% at {current_time_str}")
        print(f"Bybit price: {bybit_price}, Binance price: {binance_price}")
        print(f"Bybit timestamp: {bybit_timestamp_str}, Binance timestamp: {binance_timestamp_str}")

        # print(f"Bybit local time difference: {bybit_diff_local} ms")
        # print(f"Binance local time difference: {binance_diff_local} ms")
        print(f"Average local time difference: {average_diff_local} ms")
        # print('test1117',latest_prices[pair]['bybit'],latest_prices[pair]['binance'])
        # Calculate price change between previous and current price for both exchanges
        bybit_prev_data = latest_prices[pair]['bybit']
        binance_prev_data = latest_prices[pair]['binance']

        if bybit_prev_data is not None and binance_prev_data is not None:
            bybit_price_change = ((bybit_price - bybit_prev_data['price']) / bybit_prev_data['price']) * 100
            binance_price_change = ((binance_price - binance_prev_data['price']) / binance_prev_data['price']) * 100

            print(
                f"Bybit price change: {bybit_price_change:.2f}%, Binance price change: {binance_price_change:.2f}%")

            # Identify which exchange created the arbitrage opportunity
            if abs(bybit_price_change) > abs(binance_price_change):
                print("Arbitrage opportunity created by Bybit.")
            else:
                print("Arbitrage opportunity created by Binance.")

        print()
        last_arbitrage_opportunities[pair] = {
            'bybit_price': bybit_price,
            'binance_price': binance_price,
            'percentage_diff': percentage_diff,
            'printed': False
        }

    else:
        if pair in last_arbitrage_opportunities and not last_arbitrage_opportunities[pair]['printed']:
            last_opportunity = last_arbitrage_opportunities[pair]
            delayed_prints[pair] = {
                'bybit_price': bybit_price,
                'binance_price': binance_price,
                'percentage_diff': percentage_diff,
                'timestamp': time.time(),
            }
            print(f"Update for {pair} at {current_time_str}:")
            print(f"Previous arbitrage opportunity: {last_opportunity['percentage_diff']:.2f}%")
            print(f"Current price difference: {percentage_diff:.2f}%")
            print(f"Bybit price: {bybit_price}, Binance price: {binance_price}")
            print(f"Bybit timestamp: {bybit_timestamp_str}, Binance timestamp: {binance_timestamp_str}")
            print(f"Average local time difference: {average_diff_local} ms")

            # Calculate price change between previous and current price for both exchanges
            bybit_price_change = ((bybit_price - last_opportunity['bybit_price']) / last_opportunity[
                'bybit_price']) * 100
            binance_price_change = ((binance_price - last_opportunity['binance_price']) / last_opportunity[
                'binance_price']) * 100

            print(
                f"Bybit price change: {bybit_price_change:.2f}%, Binance price change: {binance_price_change:.2f}%")
            print()

            last_arbitrage_opportunities[pair]['printed'] = True

    if abs(percentage_diff) >= ARBITRAGE_THRESHOLD:
        print(
            f"{current_time_str}: ARBITRAGE OPPORTUNITY DETECTED: {pair} {binance_price} (Binance) vs {bybit_price} (Bybit) - {percentage_diff}%")
        # Execute the arbitrage trade
        amount = 0.01  # Define the amount to trade, you can use a more sophisticated approach
        if binance_price < bybit_price:
            # print('execute_arbitrage_trade(pair, "binance", "bybit", amount)')
            execute_arbitrage_trade(pair, 'binance', 'bybit', '10')

        else:
            # print('execute_arbitrage_trade(pair, "bybit", "binance", amount)')
            execute_arbitrage_trade(pair, 'bybit', 'binance', '10')
