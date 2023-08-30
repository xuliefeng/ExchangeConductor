import asyncio
import os
import time
import httpx

from config.logger_config import setup_logger
from data_processing_proxy.mod1_gate_io_processor import filter_symbols, insert_to_db
from proxy_handler.proxy_loader import ProxyRotator

logger = setup_logger("gate_io_collector", "log/app.log")
rotator = ProxyRotator(os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'proxy_handler', 'mod1_gate_io.txt'))
# rotator = ProxyRotator()
max_concurrent_requests = 500
retry_limit = 3


def gate_io(temp_table_name):
    start_time = time.time()
    data = asyncio.run(gate_io_symbols())
    if data:
        found_records = filter_symbols(data)
        result = asyncio.run(gate_io_depth(found_records))
        insert_to_db(result, temp_table_name)

        end_time = time.time()
        elapsed_time = round(end_time - start_time, 3)
        logger.info(
            f"-------------------------------------------------- gate_io executed in {elapsed_time} seconds. ----- symbols : {len(found_records)} success : {len(result)}")
    else:
        logger.error("Failed to get tickers from gate_io")


async def gate_io_symbols():
    proxy = rotator.get_next_proxy()
    url = "https://api.gateio.ws/api/v4/spot/tickers"
    async with httpx.AsyncClient(proxies=proxy, verify=False, timeout=10) as client:
        response = await client.get(url)
        return response.json() if response.status_code == 200 else None


async def gate_io_depth(found_records):
    url = "https://api.gateio.ws/api/v4/spot/order_book?limit=1&currency_pair="
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
                        logger.info(f"Request failed {symbol} status code {response.status_code} - gate_io")
                        await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error fetching {symbol}. {repr(e)} - gate_io")
                await asyncio.sleep(0.1)

    return symbol, None
