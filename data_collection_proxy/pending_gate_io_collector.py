import asyncio
import time
import httpx

from config.logger_config import setup_logger
from data_processing_proxy.pending_gate_io_processor import filter_symbols, insert_to_db
from proxy_handler.proxy_loader import ProxyRotator

rotator = ProxyRotator()
logger = setup_logger("gate_io_collector", "log/app.log")


def gate_io(symbols, temp_table_name):
    start_time = time.time()
    data = asyncio.run(gate_io_symbols())
    if data:
        found_records = filter_symbols(symbols, data)
        result = asyncio.run(gate_io_depth(found_records))
        insert_to_db(result, temp_table_name)
    else:
        logger.error("Failed to get tickers from gate_io")

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    logger.info(f"-------------------------------------------------- gate_io executed in {elapsed_time} seconds.")


async def gate_io_symbols():
    proxy = rotator.get_next_proxy()
    url = "https://api.gateio.ws/api/v4/spot/tickers"
    async with httpx.AsyncClient(proxies=proxy, verify=False, timeout=10) as client:
        response = await client.get(url)
        return response.json() if response.status_code == 200 else None


async def gate_io_depth(found_records):
    url = "https://api.gateio.ws/api/v4/spot/order_book?limit=10&currency_pair="
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
            logger.info(f"Request failed {symbol} status code {response.status_code} - gate_io")
            return symbol, None
