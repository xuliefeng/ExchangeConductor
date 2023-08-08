import ccxt


def get_exchange():
    exchanges = ccxt.exchanges
    for exchange in exchanges:
        print(exchange)


# get_exchange()
def get_fetch_tickers():
    for exchange_id in ccxt.exchanges:
        exchange = getattr(ccxt, exchange_id)()
        if exchange.has['fetchTickers']:
            print(exchange_id)


get_fetch_tickers()


def get_all_symbols():
    exchange_list = ccxt.exchanges

    all_symbols = set()
    for exchange_name in exchange_list:
        exchange = getattr(ccxt, exchange_name)()
        try:
            markets = exchange.load_markets()
            symbols = markets.keys()
            all_symbols.update(symbols)
        except Exception as e:
            print(f'Error loading markets from exchange {exchange_name}: {str(e)}')
            pass

    for symbol in all_symbols:
        print(symbol)


# get_all_symbols()

def get_one_symbols():
    exchange = ccxt.bitstamp()
    tickers = exchange.fetchTickers()

    for symbol, ticker in tickers.items():
        bid_price = ticker['bid']
        ask_price = ticker['ask']
        bid_volume = ticker['bidVolume']
        ask_volume = ticker['askVolume']
        print(f'{symbol}:')
        print(f'  买价: {bid_price}, 买量: {bid_volume}')
        print(f'  卖价: {ask_price}, 卖量: {ask_volume}')


get_one_symbols()
