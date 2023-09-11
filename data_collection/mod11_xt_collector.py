import time

import requests

from config.logger_config import setup_logger
from data_processing.mod11_xt_processor import filter_symbols, insert_to_db

logger = setup_logger("xt_collector", "log/app.log")


def xt(temp_table_name):
    start_time = time.time()
    try:
        url = "https://sapi.xt.com/v4/public/ticker/book"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            data = data['result']
            found_records = filter_symbols(data)
            insert_to_db(found_records, temp_table_name)
            end_time = time.time()
            elapsed_time = round(end_time - start_time, 3)
            logger.info(f"-------------------------------------------------- xt executed in {elapsed_time} seconds.")
    except Exception as e:
        logger.error("Failed to get tickers from xt", e)

