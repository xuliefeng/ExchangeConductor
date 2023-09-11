import time

import requests

from config.logger_config import setup_logger
from data_processing.mod2_binance_processor import filter_symbols, insert_to_db

logger = setup_logger("binance_collector", "log/app.log")


def binance(temp_table_name):
    start_time = time.time()
    try:
        url = "https://api.binance.com/api/v3/ticker/bookTicker"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            found_records = filter_symbols(data)
            insert_to_db(found_records, temp_table_name)
            end_time = time.time()
            elapsed_time = round(end_time - start_time, 3)
            logger.info(f"-------------------------------------------------- binance executed in {elapsed_time} seconds.")
    except Exception as e:
        logger.error("Failed to get tickers from binance", e)

