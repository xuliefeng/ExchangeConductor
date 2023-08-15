import asyncio
import ssl
import time

import aiohttp
import requests
from data_processing.discarded_kraken_processor import insert_to_db, filter_symbols


def kraken(coins_s, coins_r):
    start_time = time.time()
    url = "https://api.kraken.com/0/public/AssetPairs"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        data = data['result']
        found_records = filter_symbols(coins_s, coins_r, data)
        prices = asyncio.run(kraken_depth(found_records))
        insert_to_db(prices, coins_r)
    else:
        print(f"Request failed with status code {response.status_code}")

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    print(f"---------------------------------------------------------------------------------------------------- kraken executed in {elapsed_time} seconds.")


async def kraken_depth(found_records):
    url = "https://api.kraken.com/0/public/Depth?count=1&pair="
    tasks = []
    prices = []

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
                prices.append(result['result'])

    return prices


async def fetch(pair, session, url):
    async with session.get(url + pair) as response:
        if response.status == 200:
            return pair, await response.json()
        else:
            print(f"Request failed {pair} status code {response.status} - kraken")
            return pair, None