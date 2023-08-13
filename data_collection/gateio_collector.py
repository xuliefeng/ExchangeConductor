import asyncio
import random
import time
import httpx
from data_processing.gateio_processor import filter_symbols, insert_to_db
from proxy_handler.proxy_loader import load_proxies_from_file

proxies = load_proxies_from_file()


def select_proxy():
    return random.choice(proxies)


async def gateio_tickers(url: str, proxy: dict = None):
    async with httpx.AsyncClient(proxies=proxy, verify=False, timeout=10) as client:
        response = await client.get(url)
        return response.json() if response.status_code == 200 else None


def gateio(symbols):
    start_time = time.time()
    url = "https://api.gateio.ws/api/v4/spot/tickers"
    proxy = select_proxy()
    print(proxy)
    data = asyncio.run(gateio_tickers(url, proxy))
    if data:
        found_records = filter_symbols(symbols, data)
        prices = asyncio.run(gateio_depth(found_records))
        insert_to_db(prices)
    else:
        print("Failed to get tickers from gateio")

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    print(
        f"---------------------------------------------------------------------------------------------------- gateio executed in {elapsed_time} seconds."
    )


async def gateio_depth(found_records):
    url = "https://api.gateio.ws/api/v4/spot/order_book?currency_pair="
    tasks = [fetch(pair, url) for pair in found_records]
    results = await asyncio.gather(*tasks)

    prices = {pair: result for pair, result in results if result}
    return prices


async def fetch(pair, url):
    proxy = select_proxy()
    print(proxy)
    async with httpx.AsyncClient(proxies=proxy, verify=False) as client:
        response = await client.get(url + pair)
        if response.status_code == 200:
            return pair, response.json()
        else:
            print(f"Request failed {pair} status code {response.status_code} - gateio")
            return pair, None
