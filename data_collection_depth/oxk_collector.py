import asyncio
import random
import time
import httpx

from config.logger_config import setup_logger
from data_processing_depth.okx_processor import filter_symbols, insert_to_db
from proxy_handler.proxy_loader import load_proxies_from_file

proxies = load_proxies_from_file()

logger = setup_logger("okx_collector", "log/app.log")


def select_proxy():
    return random.choice(proxies)


def okx(symbols, temp_table_name):
    start_time = time.time()
    data = asyncio.run(okx_symbols())
    data = data['data']
    if data:
        found_records = filter_symbols(symbols, data)
        result = asyncio.run(okx_depth(found_records))
        insert_to_db(result, temp_table_name)
    else:
        logger.error("Failed to get tickers from gate_io")

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    logger.info(f"-------------------------------------------------- okx executed in {elapsed_time} seconds.")


async def okx_symbols():
    proxy = select_proxy()
    url = "https://www.okx.com/api/v5/market/tickers?instType=SPOT"
    async with httpx.AsyncClient(proxies=proxy, verify=False, timeout=10) as client:
        response = await client.get(url)
        return response.json() if response.status_code == 200 else None


async def okx_depth(found_records):
    url = "https://www.okx.com/api/v5/market/books?sz=10&instId="
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
            logger.info(f"Request failed {symbol} status code {response.status_code} - okx")
            return symbol, None
