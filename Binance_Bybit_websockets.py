import asyncio
import websockets
import json
import requests
from typing import List
from datetime import datetime, timedelta, timezone
import time
from arbitrage_calculator import process_arbitrage_data

# Constants
API_BASE_BINANCE = "https://fapi.binance.com"
API_BASE_BYBIT = "https://api.bybit.com"
ignored_tokens = []


# REST API calls to get trading pairs
def get_binance_pairs() -> List[str]:
    url = f"{API_BASE_BINANCE}/fapi/v1/exchangeInfo"
    response = requests.get(url)
    data = response.json()

    filtered_symbols = []
    for symbol in data['symbols']:
        if not any(token in symbol['symbol'] for token in ignored_tokens):
            filtered_symbols.append(symbol['symbol'])

    return filtered_symbols[:200]  # Limit to the first 200 symbols after filtering


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

# Remove the first latest_prices initialization
# latest_prices = {pair: {'binance': [None, None], 'bybit': [None, None]} for pair in selected_pairs}

# Store last known arbitrage opportunities
last_arbitrage_opportunities = {}
# Store delayed prints
delayed_prints = {}

# Initialize the latest_prices dictionary with None values
latest_prices = {pair: {'binance': [{'bid_price': None, 'ask_price': None, 'timestamp': None}],
                        'bybit': [{'bid_price': None, 'ask_price': None, 'timestamp': None}]} for pair in
                 selected_pairs}

last_received_timestamps = {pair: {'binance': None, 'bybit': None} for pair in selected_pairs}


def process_binance_data(data):
    if 's' not in data or 'b' not in data or 'a' not in data or 'E' not in data:
        return

    pair = data['s']
    bid_price = float(data['b'])
    ask_price = float(data['a'])
    seconds, milliseconds = divmod(data['E'], 1000)
    timestamp = datetime.utcfromtimestamp(seconds) + timedelta(milliseconds=milliseconds)
    latest_prices[pair]['binance'].pop(0)  # Remove the oldest price data
    latest_prices[pair]['binance'].append(
        {'bid_price': bid_price, 'ask_price': ask_price, 'timestamp': timestamp})  # Add the new price data
    last_received_timestamps[pair]['binance'] = timestamp  # Store the timestamp of the received data
    # print('process_binance_data', pair, latest_prices, last_arbitrage_opportunities, delayed_prints)
    # print('Binance', pair, timestamp.strftime(
    #     "%Y-%m-%d %H:%M:%S.%f"))
    process_arbitrage_data(pair, latest_prices, last_arbitrage_opportunities, delayed_prints)


def process_bybit_data(data):
    if 'topic' not in data or 'data' not in data:
        return
    # print("Bybit raw data:", data)
    pair = data['topic'].split('.')[2]
    # timestamp = data['timestamp_e6']
    # print(timestamp)
    timestamp = datetime.utcfromtimestamp(int(data['timestamp_e6']) / 1_000_000)
    if data['type'] == 'snapshot':
        return  # Ignore snapshot data
    elif data['type'] == 'delta':
        bid_price = float(data['data']['update'][0]['bid1_price']) if 'bid1_price' in data['data']['update'][
            0] else None
        ask_price = float(data['data']['update'][0]['ask1_price']) if 'ask1_price' in data['data']['update'][
            0] else None
    else:
        return

    # print(f"Before popping data: {latest_prices[pair]['bybit']}")  # Add this print statement
    if len(latest_prices[pair]['bybit']) > 1:
        latest_prices[pair]['bybit'].pop(0)  # Remove the oldest price data

    # print(f"After popping data: {latest_prices[pair]['bybit']}")  # Add this print statement

    # Use the last known prices if not updated in this update
    if bid_price is None:
        bid_price = latest_prices[pair]['bybit'][-1]['bid_price']

    if ask_price is None:
        ask_price = latest_prices[pair]['bybit'][-1]['ask_price']

    latest_prices[pair]['bybit'].append(
        {'bid_price': bid_price, 'ask_price': ask_price, 'timestamp': timestamp})  # Add the new price data
    last_received_timestamps[pair]['bybit'] = timestamp  # Store the timestamp of the received data
    # print(pair, last_received_timestamps[pair]['bybit'])
    # print('Bybit',pair,timestamp.strftime(
    #                 "%Y-%m-%d %H:%M:%S.%f"))

    process_arbitrage_data(pair, latest_prices, last_arbitrage_opportunities, delayed_prints)


async def print_heartbeat():
    while True:
        await asyncio.sleep(60 * 20)  # Print the heartbeat every 600 seconds (10 minutes)
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"Heartbeat at {current_time}")

        for pair in selected_pairs:
            binance_last_received = last_received_timestamps[pair]['binance']
            bybit_last_received = last_received_timestamps[pair]['bybit']

            if binance_last_received:
                binance_diff = (datetime.utcnow() - binance_last_received).total_seconds()
                print(f"{pair} Binance: {binance_diff:.2f}s since last update")

            if bybit_last_received:
                bybit_diff = (datetime.utcnow() - bybit_last_received).total_seconds()
                print(f"{pair} Bybit: {bybit_diff:.2f}s since last update")

        print()


async def print_delayed_updates():
    while True:
        await asyncio.sleep(1)  # Use non-blocking sleep
        pairs_to_remove = []
        for pair, data in delayed_prints.items():
            if time.time() - data['timestamp'] >= 50:
                last_opportunity = last_arbitrage_opportunities[pair]
                current_time = datetime.now(timezone.utc)
                current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                timestamp_difference = abs((latest_prices[pair]['bybit'][-1]['timestamp'] -
                                            latest_prices[pair]['binance'][-1]['timestamp']).total_seconds()) * 1000
                bybit_timestamp = latest_prices[pair]['bybit'][-1]['timestamp'].replace(tzinfo=timezone.utc)
                binance_timestamp = latest_prices[pair]['binance'][-1]['timestamp'].replace(tzinfo=timezone.utc)
                bybit_timestamp_str = latest_prices[pair]['bybit'][-1]['timestamp'].strftime("%Y-%m-%d %H:%M:%S.%f")[
                                      :-3]
                binance_timestamp_str = latest_prices[pair]['binance'][-1]['timestamp'].strftime(
                    "%Y-%m-%d %H:%M:%S.%f")[:-3]

                bybit_diff_local = abs((bybit_timestamp - current_time).total_seconds()) * 1000
                binance_diff_local = abs((binance_timestamp - current_time).total_seconds()) * 1000
                average_diff_local = (bybit_diff_local + binance_diff_local) / 2

                print(f"*50 seconds after Update for {pair} at {current_time_str}:")
                print(f"Previous arbitrage opportunity: {last_opportunity['percentage_diff']:.2f}%")
                print(f"Current price difference: {data['percentage_diff']:.2f}%")
                print(f"Bybit price: {data['bybit_price']}, Binance price: {data['binance_price']}")
                print(
                    f"Bybit timestamp: {bybit_timestamp_str}, Binance timestamp: {binance_timestamp_str}")

                print(f"Average local time difference: {average_diff_local} ms")

                # Calculate price change between previous and current price for both exchanges
                bybit_price_change = ((data['bybit_price'] - last_opportunity['bybit_price']) / last_opportunity[
                    'bybit_price']) * 100
                binance_price_change = ((data['binance_price'] - last_opportunity['binance_price']) / last_opportunity[
                    'binance_price']) * 100

                print(
                    f"Bybit price change: {bybit_price_change:.2f}%, Binance price change: {binance_price_change:.2f}%")

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




async def binance_websocket():
    uri = "wss://fstream.binance.com/ws"
    while True:
        try:
            async with websockets.connect(uri, ping_interval=10) as websocket:
                for pair in selected_pairs:
                    payload = {
                        "method": "SUBSCRIBE",
                        "params": [f"{pair.lower()}@bookTicker"],
                        "id": 1
                    }
                    await websocket.send(json.dumps(payload))
                    await asyncio.sleep(0.2)

                while True:
                    message = await websocket.recv()
                    data = json.loads(message)
                    # print(data)
                    process_binance_data(data)
        except websockets.ConnectionClosed as cc:
            print(f"Binance websocket disconnected: {cc}. Reconnecting...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"Binance websocket connection error: {e}. Reconnecting...")
            await asyncio.sleep(5)



async def bybit_websocket():
    uri = "wss://stream.bybit.com/realtime_public"
    while True:
        try:
            async with websockets.connect(uri, ping_interval=30) as websocket:
                for pair in selected_pairs:
                    payload = {
                        "op": "subscribe",
                        "args": [f"instrument_info.100ms.{pair}"]
                    }
                    await websocket.send(json.dumps(payload))

                while True:
                    message = await websocket.recv()
                    data = json.loads(message)
                    # print(f"Received data from Bybit websocket: {data}")  # Add this print statement here
                    process_bybit_data(data)

        except asyncio.CancelledError:
            print("Bybit websocket connection cancelled. Reconnecting...")
            await asyncio.sleep(5)  # Sleep for 5 seconds before reconnecting

        except websockets.exceptions.ConnectionClosedError as e:
            print(f"Bybit websocket connection error: {e}. Reconnecting...")
            await asyncio.sleep(5)  # Sleep for 5 seconds before reconnecting


async def print_statement():
    # Set the start time
    start_time = time.time()

    # Initialize a variable to keep track of the number of times the statement has been printed
    count = 0

    # Print the statement at the launch of the code
    print("Code launched at", datetime.now())

    # Loop indefinitely
    while True:
        # Wait for 10 minutes using asyncio.sleep
        await asyncio.sleep(300)

        # Print the statement
        count += 1
        print("Statement printed at {} and has been printed {} times.".format(datetime.now(), count))


async def main():
    tasks = []

    tasks.append(asyncio.create_task(binance_websocket()))
    tasks.append(asyncio.create_task(bybit_websocket()))
    tasks.append(asyncio.create_task(print_delayed_updates()))
    tasks.append(asyncio.create_task(print_heartbeat()))
    tasks.append(asyncio.create_task(print_statement()))  # Add this task

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
