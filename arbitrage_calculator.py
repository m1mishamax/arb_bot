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
PERCENT_ACCEPTANCE = 0.03

open_positions = {}


def display_open_positions():
    print("\nCurrent open positions:")
    for symbol, positions in open_positions.items():
        if len(positions) > 0:
            print(f"{symbol}: {len(positions)} open trades")
            for i, position in enumerate(positions, start=1):
                print(
                    f"  {i}. Long on {position['long_exchange']}, short on {position['short_exchange']}, amount: {position['amount']}")


def write_open_positions_to_csv(filename="open_positions.csv"):
    with open(filename, mode="w", newline="") as csvfile:
        fieldnames = ["symbol", "long_exchange", "short_exchange", "amount"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for symbol, positions in open_positions.items():
            for position in positions:
                position_data = position.copy()
                position_data["symbol"] = symbol
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
                "amount": amount
            })
            print(f"Arbitrage trade executed: long on {long_exchange}, short on {short_exchange}")
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


def calculate_arbitrage(pair, latest_prices, last_arbitrage_opportunities, delayed_prints):
    # The function implementation remains the same
    bybit_data = latest_prices[pair]['bybit'][-1]
    binance_data = latest_prices[pair]['binance'][-1]

    if bybit_data is None or binance_data is None:
        return

    bybit_price = bybit_data['price']
    binance_price = binance_data['price']
    bybit_timestamp = latest_prices[pair]['bybit'][-1]['timestamp'].replace(tzinfo=timezone.utc)
    binance_timestamp = latest_prices[pair]['binance'][-1]['timestamp'].replace(tzinfo=timezone.utc)
    bybit_timestamp_str = bybit_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    binance_timestamp_str = binance_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    percentage_diff = ((bybit_price - binance_price) / binance_price) * 100

    current_time = datetime.now(timezone.utc)
    current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    bybit_diff_local = abs((bybit_timestamp - current_time).total_seconds()) * 1000
    binance_diff_local = abs((binance_timestamp - current_time).total_seconds()) * 1000
    average_diff_local = (bybit_diff_local + binance_diff_local) / 2
    if abs(percentage_diff) >= ARBITRAGE_THRESHOLD:

        print(f"Arbitrage opportunity for {pair}: {percentage_diff:.2f}% at {current_time_str}")
        print(f"Bybit price: {bybit_price}, Binance price: {binance_price}")
        print(f"Bybit timestamp: {bybit_timestamp_str}, Binance timestamp: {binance_timestamp_str}")

        # print(f"Bybit local time difference: {bybit_diff_local} ms")
        # print(f"Binance local time difference: {binance_diff_local} ms")
        print(f"Average local time difference: {average_diff_local} ms")

        # Calculate price change between previous and current price for both exchanges
        bybit_prev_data = latest_prices[pair]['bybit'][0]
        binance_prev_data = latest_prices[pair]['binance'][0]

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
