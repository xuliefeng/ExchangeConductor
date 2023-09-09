import requests

from config.logger_config import setup_logger

logger = setup_logger("l_bank_depth", "log/app.log")


def l_bank(symbol_name, reference):
    symbol_name = str(symbol_name).lower() + '_' + str(reference).lower()
    url = f"https://api.lbkex.com/v2/depth.do?size=20&symbol={symbol_name}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()['data']
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from l_bank Error: {repr(e)}")
