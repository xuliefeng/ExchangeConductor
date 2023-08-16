import time

import requests

from config.logger_config import setup_logger
from data_processing import huobi_processor as huobi_module
from data_processing.huobi_processor import filter_symbols

logger = setup_logger("huobi_collector", "log/app.log")


def huobi(symbols, reference):
    start_time = time.time()
    url = "https://api.huobi.pro/market/tickers"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        data = data['data']
        found_records = filter_symbols(symbols, data)
        huobi_module.insert_to_db(found_records, reference)
    else:
        logger.error(f"Request failed with status code {response.status_code}")

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    logger.info(f"-------------------------------------------------- huobi executed in {elapsed_time} seconds.")
