import requests

from config.logger_config import setup_logger

logger = setup_logger("digi_finex_depth", "log/app.log")


def digi_finex(symbol_name, reference):
    symbol_name = str(symbol_name).lower() + '_' + str(reference).lower()
    url = f"https://openapi.digifinex.com/v3/order_book?limit=20&symbol={symbol_name}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from digi_finex Error: {repr(e)}")
