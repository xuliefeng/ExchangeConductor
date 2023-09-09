import requests

from config.logger_config import setup_logger

logger = setup_logger("deep_coin_depth", "log/app.log")


def deep_coin(symbol_name, reference):
    symbol_name = symbol_name + '-' + reference
    url = f"https://api.deepcoin.com/deepcoin/market/books?sz=20&instId={symbol_name}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()['data']
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from deep_coin Error: {repr(e)}")
