import requests

from config.logger_config import setup_logger

logger = setup_logger("ascend_ex_depth", "log/app.log")


def ascend_ex(symbol_name, reference):
    symbol_name = symbol_name + '/' + reference
    url = f"https://ascendex.com/api/pro/v1/depth?symbol={symbol_name}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()['data']['data']
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from ascend_ex Error: {repr(e)}")
