import time

import requests

from config.logger_config import setup_logger
from data_processing.xt_processor import filter_symbols, insert_to_db

logger = setup_logger("xt_collector", "log/app.log")


def xt(symbols):
    start_time = time.time()
    url = "https://sapi.xt.com/v4/public/ticker/book"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        data = data['result']
        found_records = filter_symbols(symbols, data)
        insert_to_db(found_records)
    else:
        logger.error(f"Request failed with status code {response.status_code}")

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    logger.info(f"-------------------------------------------------- xt executed in {elapsed_time} seconds.")
