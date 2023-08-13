import asyncio
import random
import time
import httpx
from data_processing.gate_io_processor import filter_symbols, insert_to_db
from proxy_handler.proxy_loader import load_proxies_from_file

proxies = load_proxies_from_file()


def select_proxy():
    return random.choice(proxies)


def gate_io(symbols):
    start_time = time.time()
    url = "https://api.gateio.ws/api/v4/spot/tickers"
    data = asyncio.run(gate_io_tickers(url, select_proxy()))
    if data:
        found_records = filter_symbols(symbols, data)
        result = asyncio.run(gate_io_depth(found_records))
        insert_to_db(result)
    else:
        print("Failed to get tickers from gate_io")

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    print(
        f"---------------------------------------------------------------------------------------------------- gate_io executed in {elapsed_time} seconds."
    )


async def gate_io_tickers(url: str, proxy: dict = None):
    async with httpx.AsyncClient(proxies=proxy, verify=False, timeout=10) as client:
        response = await client.get(url)
        return response.json() if response.status_code == 200 else None


async def gate_io_depth(found_records):
    url = "https://api.gateio.ws/api/v4/spot/order_book?currency_pair="
    tasks = [fetch(symbol, url) for symbol in found_records]
    results = await asyncio.gather(*tasks)
    return {symbol: result for symbol, result in results if result}


async def fetch(symbol, url):
    proxy = select_proxy()
    async with httpx.AsyncClient(proxies=proxy, verify=False, timeout=10) as client:
        response = await client.get(url + symbol)
        if response.status_code == 200:
            return symbol, response.json()
        else:
            print(f"Request failed {symbol} status code {response.status_code} - gate_io")
            return symbol, None
