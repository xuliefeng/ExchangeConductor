import time

import requests

from config.logger_config import setup_logger
from data_processing.mod7_bit_venus_processor import filter_symbols, insert_to_db

logger = setup_logger("bit_venus_collector", "log/app.log")


def bit_venus(temp_table_name):
    start_time = time.time()
    url = "https://www.bitvenus.me/openapi/quote/v1/ticker/bookTicker"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        found_records = filter_symbols(data)
        insert_to_db(found_records, temp_table_name)
    else:
        logger.error(f"Request failed with status code {response.status_code}")

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    logger.info(f"-------------------------------------------------- bit_venus executed in {elapsed_time} seconds.")
