import time

import requests

from config.logger_config import setup_logger
from data_processing.mod16_la_token_processor import filter_symbols, insert_to_db

logger = setup_logger("la_token_collector", "log/app.log")


def la_token(temp_table_name):
    start_time = time.time()
    try:
        url = "https://api.latoken.com/v2/ticker"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            found_records = filter_symbols(data)
            insert_to_db(found_records, temp_table_name)
            end_time = time.time()
            elapsed_time = round(end_time - start_time, 3)
            logger.info(f"-------------------------------------------------- la_token executed in {elapsed_time} seconds.")
    except Exception as e:
        logger.error("Failed to get tickers from la_token", e)
