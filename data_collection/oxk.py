import requests
from data_processing import okx as okx_module


def okx(coins_s, coins_r):
    url = "https://www.okx.com/api/v5/market/tickers?instType=SPOT"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        data = data['data']
        found_records, not_found_coins = okx_module.filter_symbols(coins_s, coins_r, data)
        okx_module.insert_to_db(found_records)
    else:
        print(f"Request failed with status code {response.status_code}")
