import time

import requests

from config.logger_config import setup_logger
from data_processing.bybit_processor import filter_symbols, insert_to_db

logger = setup_logger("bybit_collector", "log/app.log")


def bybit(symbols, reference):
    start_time = time.time()
    url = "https://api.bybit.com/v5/market/tickers?category=spot"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        data = data['result']['list']
        found_records = filter_symbols(symbols, data)
        insert_to_db(found_records, reference)
    else:
        logger.error(f"Request failed with status code {response.status_code}")

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    logger.info(f"-------------------------------------------------- bybit executed in {elapsed_time} seconds.")