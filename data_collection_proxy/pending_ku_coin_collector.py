import asyncio
import time
import httpx

from config.logger_config import setup_logger
from data_processing_proxy.pending_ku_coin_processor import insert_to_db, filter_symbols
from proxy_handler.proxy_loader import ProxyRotator

rotator = ProxyRotator()
logger = setup_logger("ku_coin_collector", "log/app.log")


def ku_coin(symbols, temp_table_name):
    start_time = time.time()
    data = asyncio.run(ku_coin_symbols())
    data = data['data']
    if data:
        found_records = filter_symbols(symbols, data)
        result = asyncio.run(ku_coin_depth(found_records))
        insert_to_db(result, temp_table_name)
    else:
        logger.error("Failed to get tickers from ku_coin")

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    logger.info(f"-------------------------------------------------- ku_coin executed in {elapsed_time} seconds.")


async def ku_coin_symbols():
    proxy = rotator.get_next_proxy()
    url = "https://api.kucoin.com/api/v1/symbols"
    async with httpx.AsyncClient(proxies=proxy, verify=False, timeout=10) as client:
        response = await client.get(url)
        return response.json() if response.status_code == 200 else None


async def ku_coin_depth(found_records):
    url = "https://api.kucoin.com/api/v1/market/orderbook/level2_20?symbol="
    tasks = [fetch(symbol, url) for symbol in found_records]
    results = await asyncio.gather(*tasks)
    return {symbol: result for symbol, result in results if result}


async def fetch(symbol, url):
    proxy = rotator.get_next_proxy()
    async with httpx.AsyncClient(proxies=proxy, verify=False, timeout=10) as client:
        response = await client.get(url + symbol)
        if response.status_code == 200:
            return symbol, response.json()
        else:
            logger.info(f"Request failed {symbol} status code {response.status_code} - ku_coin")
            return symbol, None
