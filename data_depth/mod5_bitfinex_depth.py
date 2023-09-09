import requests

from config.logger_config import setup_logger

logger = setup_logger("bitfinex_depth", "log/app.log")


def bitfinex(symbol_name, reference):
    symbol_name = symbol_name + reference
    url = f"https://api-pub.bitfinex.com/v2/book/t{symbol_name}/P0?len=25"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            bids = []
            asks = []
            for entry in data:
                price = entry[0]
                quantity = entry[2]

                if quantity > 0 and len(bids) < 20:
                    bids.append([str(price), str(quantity)])
                elif quantity < 0 and len(asks) < 20:
                    asks.append([str(price), str(abs(quantity))])

                if len(bids) == 20 and len(asks) == 20:
                    break

            transformed_data = {
                "bids": bids,
                "asks": asks
            }
            return transformed_data
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from bitfinex Error: {repr(e)}")
