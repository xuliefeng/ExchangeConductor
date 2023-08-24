import asyncio
import time
import httpx

from config.logger_config import setup_logger
from data_processing_depth.mod5_bitfinex_processor import filter_symbols, insert_to_db
from proxy_handler.proxy_loader import ProxyRotator

rotator = ProxyRotator()
logger = setup_logger("bitfinex_collector", "log/app.log")


def bitfinex(symbols, temp_table_name, reference):
    start_time = time.time()
    data = asyncio.run(bitfinex_symbols())
    if data:
        found_records = filter_symbols(symbols, data)
        result = asyncio.run(bitfinex_depth(found_records))
        insert_to_db(result, temp_table_name, reference)
    else:
        logger.error("Failed to get tickers from bitfinex")

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    logger.info(f"-------------------------------------------------- bitfinex executed in {elapsed_time} seconds.")


async def bitfinex_symbols():
    proxy = rotator.get_next_proxy()
    url = "https://api-pub.bitfinex.com/v2/tickers?symbols=ALL"
    async with httpx.AsyncClient(proxies=proxy, verify=False, timeout=10) as client:
        response = await client.get(url)
        return response.json() if response.status_code == 200 else None


async def bitfinex_depth(found_records):
    tasks = [fetch(symbol, f"https://api-pub.bitfinex.com/v2/book/t{symbol}/R0?len=25") for symbol in found_records]
    results = await asyncio.gather(*tasks)
    return {symbol: result for symbol, result in results if result}


async def fetch(symbol, url):
    proxy = rotator.get_next_proxy()
    async with httpx.AsyncClient(proxies=proxy, verify=False, timeout=10) as client:
        response = await client.get(url)
        if response.status_code == 200:
            return symbol, response.json()
        else:
            logger.info(f"Request failed {symbol} status code {response.status_code} - bitfinex")
            return symbol, None
