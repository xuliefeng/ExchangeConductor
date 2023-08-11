import time

import requests
from data_processing import okx_processor as okx_module
from data_processing.okx_processor import filter_symbols


def okx(coins_s, coins_r):
    start_time = time.time()
    url = "https://www.okx.com/api/v5/market/tickers?instType=SPOT"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        data = data['data']
        found_records = filter_symbols(coins_s, coins_r, data)
        okx_module.insert_to_db(found_records)
    else:
        print(f"Request failed with status code {response.status_code}")

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    print(f"---------------------------------------------------------------------------------------------------- okx executed in {elapsed_time} seconds.")
