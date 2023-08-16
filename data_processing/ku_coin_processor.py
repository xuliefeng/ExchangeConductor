# from database.db_pool import release_connection, get_connection
#
#
# def filter_symbols(coins_s, coins_r, data):
#     found_records = []
#
#     inst_ids_set = set(item['symbol'] for item in data)
#
#     for coin_stable in coins_s:
#         for coin_reference in coins_r:
#             combined_id = f"{coin_stable}-{coin_reference}"
#             if combined_id in inst_ids_set:
#                 found_records.append(combined_id)
#
#     print(f"kucoin - symbols found: {len(found_records)}")
#     return found_records
#
#
# def insert_to_db(data):
#     connection = get_connection()
#     cursor = connection.cursor()
#
#     query = """
#         INSERT INTO trade_data (
#             coin_name, bid, bid_size, ask, ask_size, last, last_size, update_time, exchange_name
#         ) VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, 'kucoin');
#     """
#
#     records_to_insert = []
#     for pair, result in data.items():
#         coin_data = result['data']
#         record = (
#             pair,
#             coin_data['bestBid'],
#             coin_data['bestBidSize'],
#             coin_data['bestAsk'],
#             coin_data['bestAskSize'],
#             coin_data['price'],
#             coin_data['size']
#         )
#         records_to_insert.append(record)
#
#     batch_size = 1000
#     for i in range(0, len(records_to_insert), batch_size):
#         batch = records_to_insert[i:i + batch_size]
#         cursor.executemany(query, batch)
#         connection.commit()
#
#     release_connection(connection)