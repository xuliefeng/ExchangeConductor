import requests

from config.logger_config import setup_logger

logger = setup_logger("huobi_depth", "log/app.log")


def huobi(symbol_name, reference):
    symbol_name = str(symbol_name).lower() + str(reference).lower()
    url = f"https://api.huobi.pro/market/depth?depth=20&type=step0&symbol={symbol_name}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()['tick']
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from huobi Error: {repr(e)}")
