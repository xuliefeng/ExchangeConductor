# from database.db_pool import release_connection, get_connection
#
#
# def filter_symbols(symbols, data):
#     found_records = []
#     inst_ids_set = set(data.keys())
#
#     for symbol in symbols:
#         combined_id = str(symbol).replace('-', '')
#         if combined_id in inst_ids_set:
#             found_records.append(combined_id)
#
#     print(f"kraken - symbols      : {len(data)}")
#     print(f"kraken - symbols found: {len(found_records)}")
#     return found_records
#
#
# def insert_to_db(data, reference):
#     connection = get_connection()
#     cursor = connection.cursor()
#
#     query = """
#         INSERT INTO trade_data (
#             coin_name, bid, bid_size, ask, ask_size, update_time, exchange_name
#         ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, 'kraken');
#     """
#
#     records_to_insert = []
#     for item in data:
#         for symbol, details in item.items():
#             coin_name = transform_symbol(symbol, reference)
#             bid = details['bids'][0][0]
#             bid_size = details['bids'][0][1]
#             ask = details['asks'][0][0]
#             ask_size = details['asks'][0][1]
#             records_to_insert.append((coin_name, bid, bid_size, ask, ask_size))
#
#     cursor.executemany(query, records_to_insert)
#     connection.commit()
#
#     release_connection(connection)
#
#
# def transform_symbol(symbol, reference):
#     for match in reference:
#         if symbol.endswith(match):
#             return str(symbol[:-len(match)]) + '-' + match
#     return symbol
