import asyncio
import ssl

import aiohttp
import requests

from data_processing import okx as okx_module
from data_processing import huobi as huobi_module
from data_processing import kucoin as kucoin_module
from data_processing import kraken as kraken_module










def kucoin(coins_s, coins_r):
    url = "https://api.kucoin.com/api/v1/symbols"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        data = data['data']
        found_records, not_found_coins = kucoin_module.filter_symbols(coins_s, coins_r, data)
        kucoin_module.insert_to_db(found_records)
    else:
        print(f"Request failed with status code {response.status_code}")
