import requests

from config.logger_config import setup_logger

logger = setup_logger("hitbtc_depth", "log/app.log")


def hitbtc(symbol_name, reference):
    symbol_name = symbol_name + reference
    url = f"https://api.hitbtc.com/api/3/public/orderbook/{symbol_name}?depth=20"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            data['asks'] = data.pop('ask')
            data['bids'] = data.pop('bid')
            return data
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from hitbtc Error: {repr(e)}")
