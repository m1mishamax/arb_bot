from datetime import datetime, timedelta, timezone
import time


# arbitrage_calculator.py
ARBITRAGE_THRESHOLD = 0.40


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
