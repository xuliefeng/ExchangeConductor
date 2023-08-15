import ccxt


def get_exchange():
    exchanges = ccxt.exchanges
    for exchange in exchanges:
        print(exchange)


get_exchange()


def get_fetch_tickers():
    for exchange_id in ccxt.exchanges:
        exchange = getattr(ccxt, exchange_id)()
        if exchange.has['fetchTickers']:
            print(exchange_id)


# get_fetch_tickers()


def get_fetch_order_book():
    for exchange_id in ccxt.exchanges:
        exchange = getattr(ccxt, exchange_id)()
        if exchange.has['fetchOrderBook']:
            print(exchange_id)


# get_fetch_order_book()
