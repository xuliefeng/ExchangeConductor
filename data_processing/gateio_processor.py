from database.db_pool import release_connection, get_connection


def filter_symbols(coins_s, coins_r, data):
    found_records = []
    not_found_coins = set()

    inst_ids_set = set(item['currency_pair'] for item in data)

    for coin_stable in coins_s:
        found = False
        for coin_reference in coins_r:
            combined_id = f"{coin_stable}_{coin_reference}"
            if combined_id in inst_ids_set:
                found_records.append(combined_id)
                found = True
                break
        if not found:
            not_found_coins.add(coin_stable)

    print(f"gateio - symbols found: {len(found_records)} symbols not found : {len(not_found_coins)}")
    return found_records, not_found_coins


def insert_to_db(data):
    connection = get_connection()
    cursor = connection.cursor()

    query = """
        INSERT INTO trade_data (
            coin_name, ask, ask_size, bid, bid_size, update_time, exchange_name
        ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, 'gateio');
    """

    records_to_insert = []
    for pair, result in data.items():
        pair = pair.replace("_", "-")
        ask_price, ask_size = result['asks'][0]
        bid_price, bid_size = result['bids'][0]

        record = (
            pair,
            ask_price,
            ask_size,
            bid_price,
            bid_size
        )
        records_to_insert.append(record)

    batch_size = 1000
    for i in range(0, len(records_to_insert), batch_size):
        batch = records_to_insert[i:i + batch_size]
        cursor.executemany(query, batch)
        connection.commit()

    release_connection(connection)

