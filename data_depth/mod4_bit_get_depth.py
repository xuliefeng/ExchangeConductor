import requests

from config.logger_config import setup_logger

logger = setup_logger("bit_get_depth", "log/app.log")


def bit_get(symbol_name, reference):
    symbol_name = symbol_name + reference + '_UMCBL'
    url = f"https://api.bitget.com/api/mix/v1/market/depth?limit=15&precision=scale0&symbol={symbol_name}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()['data']
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from bit_get Error: {repr(e)}")
