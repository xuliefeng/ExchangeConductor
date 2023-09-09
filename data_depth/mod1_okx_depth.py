import requests

from config.logger_config import setup_logger

logger = setup_logger("okx_depth", "log/app.log")


def okx(symbol_name, reference):
    symbol_name = symbol_name + '-' + reference
    url = f"https://www.okx.com/api/v5/market/books?sz=20&instId={symbol_name}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            first_entry = data['data'][0]
            parsed_data = {
                'asks': [ask[:2] for ask in first_entry['asks']],
                'bids': [bid[:2] for bid in first_entry['bids']]
            }
            return parsed_data
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from okx Error: {repr(e)}")
