import requests
from data_processing import huobi_processor as huobi_module


def huobi(coins_s, coins_r):
    url = "https://api.huobi.pro/market/tickers"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        data = data['data']
        found_records, not_found_coins = huobi_module.filter_symbols(coins_s, coins_r, data)
        huobi_module.insert_to_db(found_records, coins_r)
    else:
        print(f"Request failed with status code {response.status_code}")