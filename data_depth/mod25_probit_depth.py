import requests

from config.logger_config import setup_logger

logger = setup_logger("probit_depth", "log/app.log")


def probit(symbol_name, reference):
    symbol_name = symbol_name + '-' + reference
    url = f"https://api.probit.com/api/exchange/v1/order_book?market_id={symbol_name}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()['data']
            bids = []
            asks = []
            for entry in reversed(data):
                if entry["side"] == "buy" and len(bids) < 20:
                    bids.append([entry["price"], entry["quantity"]])
                elif entry["side"] == "sell" and len(asks) < 20:
                    asks.append([entry["price"], entry["quantity"]])

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
        logger.error(f"Failed to get depth from probit Error: {repr(e)}")
