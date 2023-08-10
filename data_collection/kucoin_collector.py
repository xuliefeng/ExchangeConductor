import asyncio
import ssl
import time

import aiohttp
import requests

from data_processing.kucoin_processor import filter_symbols, insert_to_db


def kucoin(coins_s, coins_r):
    start_time = time.time()
    url = "https://api.kucoin.com/api/v1/symbols"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        data = data['data']
        found_records, not_found_coins = filter_symbols(coins_s, coins_r, data)
        prices = asyncio.run(kucoin_depth(found_records))
        insert_to_db(prices)
    else:
        print(f"Request failed with status code {response.status_code}")

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    print(f"---------------------------------------------------------------------------------------------------- kucoin executed in {elapsed_time} seconds.")


async def kucoin_depth(found_records):
    url = "https://api.kucoin.com/api/v1/market/orderbook/level1?symbol="
    tasks = []
    prices = {}

    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
        for pair in found_records:
            task = fetch(pair, session, url)
            tasks.append(task)

        results = await asyncio.gather(*tasks)

        for pair, result in results:
            if result:
                prices[pair] = result

    return prices


async def fetch(pair, session, url):
    async with session.get(url + pair) as response:
        if response.status == 200:
            return pair, await response.json()
        else:
            print(f"Request failed {pair} status code {response.status} - kucoin")
            return pair, None
