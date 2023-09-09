import requests

from config.logger_config import setup_logger

logger = setup_logger("bit_mark_depth", "log/app.log")


def bit_mark(symbol_name, reference):
    symbol_name = symbol_name + '_' + reference
    url = f"https://api-cloud.bitmart.com/spot/quotation/v3/books?limit=20&symbol={symbol_name}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()['data']
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from bit_mark Error: {repr(e)}")
