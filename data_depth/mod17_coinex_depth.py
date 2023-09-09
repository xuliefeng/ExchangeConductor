import requests

from config.logger_config import setup_logger

logger = setup_logger("coinex_depth", "log/app.log")


def coinex(symbol_name, reference):
    symbol_name = symbol_name + reference
    url = f"https://api.coinex.com/v1/market/depth?limit=20&merge=0&market={symbol_name}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()['data']
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from coinex Error: {repr(e)}")
