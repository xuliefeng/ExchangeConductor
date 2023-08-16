import time

import requests

from config.logger_config import setup_logger
from data_processing import okx_processor as okx_module
from data_processing.okx_processor import filter_symbols

logger = setup_logger("okx_collector", "log/app.log")


def okx(symbols):
    start_time = time.time()
    url = "https://www.okx.com/api/v5/market/tickers?instType=SPOT"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        data = data['data']
        found_records = filter_symbols(symbols, data)
        okx_module.insert_to_db(found_records)
    else:
        logger.error(f"Request failed with status code {response.status_code}")

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    logger.info(f"-------------------------------------------------- okx executed in {elapsed_time} seconds.")
