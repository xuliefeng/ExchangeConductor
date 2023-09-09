import requests

from config.logger_config import setup_logger

logger = setup_logger("jubi_depth", "log/app.log")


def jubi(symbol_name, reference):
    symbol_name = symbol_name + reference
    url = f"https://api.jbex.com/openapi/quote/v1/depth?limit=20&symbol={symbol_name}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from jubi Error: {repr(e)}")
