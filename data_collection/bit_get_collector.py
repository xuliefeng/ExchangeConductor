import time

import requests

from config.logger_config import setup_logger
from data_processing.bit_get_processor import filter_symbols, insert_to_db

logger = setup_logger("bit_get_collector", "log/app.log")


def bit_get(symbols, reference):
    start_time = time.time()
    url = "https://api.bitget.com/api/mix/v1/market/tickers?productType=umcbl"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        data = data['data']
        found_records = filter_symbols(symbols, data)
        insert_to_db(found_records, reference)
    else:
        logger.error(f"Request failed with status code {response.status_code}")

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    logger.info(f"-------------------------------------------------- bit_get executed in {elapsed_time} seconds.")
