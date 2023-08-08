
def filter_symbols(data, symbols_to_match):

    return {k: v for k, v in data.items() if k in symbols_to_match}