import asyncio
import os
import time
import httpx

from config.logger_config import setup_logger
from data_processing_proxy.mod3_bi_ka_processor import filter_symbols, insert_to_db
from proxy_handler.proxy_loader import ProxyRotator

logger = setup_logger("bi_ka_collector", "log/app.log")
# rotator = ProxyRotator(os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'proxy_handler', 'mod3_bi_ka.txt'))
# rotator = ProxyRotator()
max_concurrent_requests = 1
retry_limit = 3


def bi_ka(temp_table_name):
    start_time = time.time()
    try:
        data = asyncio.run(bi_ka_symbols())
        if data:
            found_records = filter_symbols(data)
            result = asyncio.run(bi_ka_depth(found_records))
            insert_to_db(result, temp_table_name)

            end_time = time.time()
            elapsed_time = round(end_time - start_time, 3)
            logger.info(
                f"-------------------------------------------------- bi_ka executed in {elapsed_time} seconds. ----- symbols : {len(found_records)} success : {len(result)}")
    except Exception as e:
        logger.error("Failed to get tickers from bi_ka", e)


async def bi_ka_symbols():
    # proxy = rotator.get_next_proxy()
    url = "https://www.bika.one/cmc/spot/summary"
    async with httpx.AsyncClient(verify=False, timeout=10) as client:
        response = await client.get(url)
        return response.json() if response.status_code == 200 else None


async def bi_ka_depth(found_records):
    url = "https://www.bika.one/cmc/spot/orderbook/"
    semaphore = asyncio.Semaphore(max_concurrent_requests)
    tasks = [fetch(symbol, url, semaphore) for symbol in found_records]
    results = await asyncio.gather(*tasks)
    return {symbol: result for symbol, result in results if result}


async def fetch(symbol, url, semaphore):
    async with semaphore:
        for retry in range(retry_limit):
            try:
                async with httpx.AsyncClient(verify=False, timeout=20) as client:
                    response = await client.get(url + symbol)
                    if response.status_code == 200:
                        await asyncio.sleep(0.1)
                        return symbol, response.json()
                    else:
                        logger.info(f"Request failed {symbol} status code {response.status_code} - bi_ka")

            except Exception as e:
                logger.error(f"Error fetching {symbol}. {repr(e)} - bi_ka")
    return symbol, None
