import time

import requests

from config.logger_config import setup_logger
from data_processing.mod4_bit_get_processor import filter_symbols, insert_to_db

logger = setup_logger("bit_get_collector", "log/app.log")


def bit_get(temp_table_name):
    start_time = time.time()
    try:
        url = "https://api.bitget.com/api/mix/v1/market/tickers?productType=umcbl"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            data = data['data']
            found_records = filter_symbols(data)
            insert_to_db(found_records, temp_table_name)
            end_time = time.time()
            elapsed_time = round(end_time - start_time, 3)
            logger.info(f"-------------------------------------------------- bit_get executed in {elapsed_time} seconds.")
    except Exception as e:
        logger.error("Failed to get tickers from bit_get", e)

