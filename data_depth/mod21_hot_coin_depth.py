import requests

from config.logger_config import setup_logger

logger = setup_logger("hot_coin_depth", "log/app.log")


def hot_coin(symbol_name, reference):
    symbol_name = str(symbol_name).lower() + '_' + str(reference).lower()
    url = f"https://api.hotcoinfin.com/v1/depth?symbol={symbol_name}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()['data']['depth']
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from hot_coin Error: {repr(e)}")
