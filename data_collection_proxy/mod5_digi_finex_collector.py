import asyncio
import os
import time
import httpx

from config.logger_config import setup_logger
from data_processing_proxy.mod5_digi_finex_processor import filter_symbols, insert_to_db
from proxy_handler.proxy_loader import ProxyRotator

logger = setup_logger("digi_finex_collector", "log/app.log")
# rotator = ProxyRotator(os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'proxy_handler', 'mod5_digi_finex.txt'))
rotator = ProxyRotator()
max_concurrent_requests = 250
retry_limit = 3


def digi_finex(temp_table_name):
    start_time = time.time()
    data = asyncio.run(digi_finex_symbols())
    if data:
        found_records = filter_symbols(data)
        result = asyncio.run(digi_finex_depth(found_records))
        insert_to_db(result, temp_table_name)

        end_time = time.time()
        elapsed_time = round(end_time - start_time, 3)
        logger.info(
            f"-------------------------------------------------- digi_finex executed in {elapsed_time} seconds. ----- symbols : {len(found_records)} success : {len(result)}")
    else:
        logger.error("Failed to get tickers from digi_finex")


async def digi_finex_symbols():
    proxy = rotator.get_next_proxy()
    url = "https://openapi.digifinex.com/v3/ticker"
    async with httpx.AsyncClient(proxies=proxy, verify=False, timeout=10) as client:
        response = await client.get(url)
        return response.json()['ticker'] if response.status_code == 200 else None


async def digi_finex_depth(found_records):
    url = "https://openapi.digifinex.com/v3/order_book?limit=1&symbol="
    semaphore = asyncio.Semaphore(max_concurrent_requests)
    tasks = [fetch(symbol, url, semaphore) for symbol in found_records]
    results = await asyncio.gather(*tasks)
    return {symbol: result for symbol, result in results if result}


async def fetch(symbol, url, semaphore):
    proxy = rotator.get_next_proxy()

    for retry in range(retry_limit):
        async with semaphore:
            try:
                async with httpx.AsyncClient(proxies=proxy, verify=False, timeout=20) as client:
                    response = await client.get(url + symbol)
                    if response.status_code == 200:
                        # logger.info("success: " + symbol)
                        return symbol, response.json()
                    else:
                        logger.info(f"Request failed {symbol} status code {response.status_code} - digi_finex")
                        await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error fetching {symbol}. {repr(e)} - digi_finex")
                await asyncio.sleep(0.1)

    return symbol, None
