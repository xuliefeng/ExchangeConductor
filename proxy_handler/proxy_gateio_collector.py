import asyncio
import ssl
import time
import random

import aiohttp
import requests

from data_processing.gateio_processor import filter_symbols, insert_to_db
from proxy_handler.proxy_loader import load_proxies_from_file

proxies = load_proxies_from_file()


def select_proxy():
    return random.choice(proxies)


def gateio(coins_s, coins_r):
    start_time = time.time()
    url = "https://api.gateio.ws/api/v4/spot/tickers"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        found_records = filter_symbols(coins_s, coins_r, data)
        prices = asyncio.run(gateio_depth(found_records))
        insert_to_db(prices)
    else:
        print(f"Request failed with status code {response.status_code}")

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    print(
        f"---------------------------------------------------------------------------------------------------- gateio executed in {elapsed_time} seconds.")


async def gateio_depth(found_records):
    url = "https://api.gateio.ws/api/v4/spot/order_book?currency_pair="
    tasks = []
    prices = {}

    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
        for pair in found_records:
            proxy = select_proxy()
            task = fetch(pair, session, url, proxy)
            tasks.append(task)

        results = await asyncio.gather(*tasks)

        for pair, result in results:
            if result:
                prices[pair] = result

    return prices


async def fetch(pair, session, url, proxy):
    async with session.get(url + pair, proxy=proxy) as response:
        if response.status == 200:
            return pair, await response.json()
        else:
            print(f"Request failed {pair} status code {response.status} - gateio")
            return pair, None
