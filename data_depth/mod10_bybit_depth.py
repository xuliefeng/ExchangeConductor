import requests

from config.logger_config import setup_logger

logger = setup_logger("bybit_depth", "log/app.log")


def bybit(symbol_name, reference):
    symbol_name = symbol_name + reference
    url = f"https://api.bybit.com/v5/market/orderbook?category=spot&limit=20&symbol={symbol_name}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()['result']
            data['asks'] = data.pop('a')
            data['bids'] = data.pop('b')
            return data
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from bybit Error: {repr(e)}")
