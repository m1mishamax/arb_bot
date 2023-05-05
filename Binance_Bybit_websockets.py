import asyncio
import websockets
import json
import requests
from typing import List
from datetime import datetime
import time

# Constants
API_BASE_BINANCE = "https://fapi.binance.com"
API_BASE_BYBIT = "https://api.bybit.com"


# REST API calls to get trading pairs
def get_binance_pairs() -> List[str]:
    url = f"{API_BASE_BINANCE}/fapi/v1/exchangeInfo"
    response = requests.get(url)
    data = response.json()
    return [symbol['symbol'] for symbol in data['symbols']]


def get_bybit_pairs() -> List[str]:
    url = f"{API_BASE_BYBIT}/v2/public/symbols"
    response = requests.get(url)
    data = response.json()
    return [symbol['name'] for symbol in data['result']]


# Fetch and process trading pairs
binance_pairs = get_binance_pairs()
bybit_pairs = get_bybit_pairs()
matching_pairs = list(set(binance_pairs).intersection(bybit_pairs))


# Get Binance trading pair volumes
def get_binance_volumes() -> List[dict]:
    url = f"{API_BASE_BINANCE}/fapi/v1/ticker/24hr"
    response = requests.get(url)
    data = response.json()
    return data


binance_volumes = get_binance_volumes()
filtered_volumes = [pair for pair in binance_volumes if pair['symbol'] in matching_pairs]
sorted_volumes = sorted(filtered_volumes, key=lambda x: float(x['quoteVolume']))
selected_pairs = [pair['symbol'] for pair in sorted_volumes[:200]]

# Store latest mark prices
latest_prices = {pair: {'binance': [None, None], 'bybit': [None, None]} for pair in selected_pairs}
# Store last known arbitrage opportunities
last_arbitrage_opportunities = {}
# Store delayed prints
delayed_prints = {}


def process_binance_data(data):
    if 's' not in data or 'p' not in data or 'E' not in data:
        return

    pair = data['s']
    price = float(data['p'])
    timestamp = datetime.utcfromtimestamp(data['E'] / 1000)  # Convert to seconds with milliseconds
    latest_prices[pair]['binance'].pop(0)  # Remove the oldest price data
    latest_prices[pair]['binance'].append({'price': price, 'timestamp': timestamp})  # Add the new price data
    calculate_arbitrage(pair)


def process_bybit_data(data):
    if 'data' not in data or 'update' not in data['data']:
        return

    update_data = data['data']['update'][0]
    if 'symbol' not in update_data or 'mark_price' not in update_data or 'timestamp_e6' not in data:
        return

    pair = update_data['symbol']
    price = float(update_data['mark_price'])
    timestamp = datetime.utcfromtimestamp(int(data['timestamp_e6']) / 1000000)  # Convert to seconds with milliseconds
    latest_prices[pair]['bybit'].pop(0)  # Remove the oldest price data
    latest_prices[pair]['bybit'].append({'price': price, 'timestamp': timestamp})  # Add the new price data
    calculate_arbitrage(pair)


ARBITRAGE_THRESHOLD = 0.35


def calculate_arbitrage(pair):
    bybit_data = latest_prices[pair]['bybit'][-1]
    binance_data = latest_prices[pair]['binance'][-1]

    if bybit_data is None or binance_data is None:
        return

    bybit_price = bybit_data['price']
    binance_price = binance_data['price']
    bybit_timestamp = bybit_data['timestamp']
    binance_timestamp = binance_data['timestamp']

    percentage_diff = ((bybit_price - binance_price) / binance_price) * 100

    if abs(percentage_diff) >= ARBITRAGE_THRESHOLD:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # Include milliseconds in the output
        print(f"Arbitrage opportunity for {pair}: {percentage_diff:.2f}% at {current_time}")
        print(f"Bybit price: {bybit_price}, Binance price: {binance_price}")
        print(f"Bybit timestamp: {bybit_timestamp}, Binance timestamp: {binance_timestamp}")
        print(f"Timestamp difference: {abs((bybit_timestamp - binance_timestamp).total_seconds()) * 1000} ms")

        # Calculate price change between previous and current price for both exchanges
        bybit_prev_data = latest_prices[pair]['bybit'][0]
        binance_prev_data = latest_prices[pair]['binance'][0]

        if bybit_prev_data is not None and binance_prev_data is not None:
            bybit_price_change = ((bybit_price - bybit_prev_data['price']) / bybit_prev_data['price']) * 100
            binance_price_change = ((binance_price - binance_prev_data['price']) / binance_prev_data['price']) * 100

            print(f"Bybit price change: {bybit_price_change:.2f}%, Binance price change: {binance_price_change:.2f}%")

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
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # Include milliseconds in the output
            print(f"Update for {pair} at {current_time}:")
            print(f"Previous arbitrage opportunity: {last_opportunity['percentage_diff']:.2f}%")
            print(f"Current price difference: {percentage_diff:.2f}%")
            print(f"Bybit price: {bybit_price}, Binance price: {binance_price}")
            print(f"Bybit timestamp: {bybit_timestamp}, Binance timestamp: {binance_timestamp}")
            print(f"Timestamp difference: {abs((bybit_timestamp - binance_timestamp).total_seconds()) * 1000} ms")

            # Calculate price change between previous and current price for both exchanges
            bybit_price_change = ((bybit_price - last_opportunity['bybit_price']) / last_opportunity[
                'bybit_price']) * 100
            binance_price_change = ((binance_price - last_opportunity['binance_price']) / last_opportunity[
                'binance_price']) * 100

            print(f"Bybit price change: {bybit_price_change:.2f}%, Binance price change: {binance_price_change:.2f}%")
            print()

            last_arbitrage_opportunities[pair]['printed'] = True


async def print_delayed_updates():
    while True:
        await asyncio.sleep(1)  # Use non-blocking sleep
        pairs_to_remove = []
        for pair, data in delayed_prints.items():
            if time.time() - data['timestamp'] >= 20:
                last_opportunity = last_arbitrage_opportunities[pair]
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # Include milliseconds in the output
                print(f"*20 seconds after Update for {pair} at {current_time}:")
                print(f"Previous arbitrage opportunity: {last_opportunity['percentage_diff']:.2f}%")
                print(f"Current price difference: {data['percentage_diff']:.2f}%")
                print(f"Bybit price: {data['bybit_price']}, Binance price: {data['binance_price']}")
                print(
                    f"Bybit timestamp: {latest_prices[pair]['bybit'][-1]['timestamp']}, Binance timestamp: {latest_prices[pair]['binance'][-1]['timestamp']}")
                timestamp_difference = abs((latest_prices[pair]['bybit'][-1]['timestamp'] -
                                            latest_prices[pair]['binance'][-1]['timestamp']).total_seconds()) * 1000
                print(f"Timestamp difference: {timestamp_difference} ms")

                # Calculate price change between previous and current price for both exchanges
                bybit_price_change = ((data['bybit_price'] - last_opportunity['bybit_price']) / last_opportunity[
                    'bybit_price']) * 100
                binance_price_change = ((data['binance_price'] - last_opportunity['binance_price']) / last_opportunity[
                    'binance_price']) * 100

                print(f"Bybit price change: {bybit_price_change:.2f}%, Binance price change: {binance_price_change:.2f}%")

                # Identify which exchange created the arbitrage opportunity
                if abs(bybit_price_change) > abs(binance_price_change):
                    print("Arbitrage opportunity created by Bybit.")
                else:
                    print("Arbitrage opportunity created by Binance.")
                print()

                last_arbitrage_opportunities[pair]['printed'] = True
                pairs_to_remove.append(pair)

        for pair in pairs_to_remove:
            delayed_prints.pop(pair)



# Update websocket handling
async def binance_websocket():
    uri = "wss://fstream.binance.com/ws"
    async with websockets.connect(uri) as websocket:
        for pair in selected_pairs:
            payload = {
                "method": "SUBSCRIBE",
                "params": [f"{pair.lower()}@markPrice"],
                "id": 1
            }
            await websocket.send(json.dumps(payload))
            await asyncio.sleep(0.2)  # Add a 200 ms delay between subscription requests

        while True:
            message = await websocket.recv()
            data = json.loads(message)
            process_binance_data(data)


async def bybit_websocket():
    uri = "wss://stream.bybit.com/realtime_public"
    async with websockets.connect(uri) as websocket:
        for pair in selected_pairs:
            payload = {
                "op": "subscribe",
                "args": [f"instrument_info.100ms.{pair}"]
            }
            await websocket.send(json.dumps(payload))

        while True:
            message = await websocket.recv()
            data = json.loads(message)
            process_bybit_data(data)


async def main():
    tasks = [
        asyncio.create_task(binance_websocket()),
        asyncio.create_task(bybit_websocket()),
        asyncio.create_task(print_delayed_updates()),  # Add this task
    ]
    await asyncio.gather(*tasks)
    while True:
        await asyncio.gather(*tasks)
        await asyncio.sleep(1)
        print_delayed_updates()

if __name__ == "__main__":
    asyncio.run(main())