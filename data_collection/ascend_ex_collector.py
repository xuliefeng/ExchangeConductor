import time

import requests

from config.logger_config import setup_logger
from data_processing.ascend_ex_processor import filter_symbols, insert_to_db

logger = setup_logger("ascend_ex_collector", "log/app.log")


def ascend_ex(symbols, temp_table_name):
    start_time = time.time()
    url = "https://ascendex.com/api/pro/v1/spot/ticker"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        data = data['data']
        found_records = filter_symbols(symbols, data)
        insert_to_db(found_records, temp_table_name)
    else:
        logger.error(f"Request failed with status code {response.status_code}")

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    logger.info(f"-------------------------------------------------- ascend_ex executed in {elapsed_time} seconds.")
