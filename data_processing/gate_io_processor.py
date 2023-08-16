from database.db_pool import release_connection, get_connection


def filter_symbols(symbols, data):
    found_records = []
    inst_ids_set = set(item['currency_pair'] for item in data)

    for symbol in symbols:
        symbol = str(symbol).replace('-', '_')
        if symbol in inst_ids_set:
            found_records.append(symbol)

    print(f"gate_io - symbols      : {len(data)}")
    print(f"gate_io - symbols found: {len(found_records)}")
    return found_records


def insert_to_db(data):
    connection = get_connection()
    cursor = connection.cursor()

    query = """
        INSERT INTO trade_data (
            symbol_name, ask, ask_size, bid, bid_size, update_time, exchange_name
        ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, 'gate_io');
    """

    records_to_insert = []
    for symbol, result in data.items():
        symbol = symbol.replace("_", "-")
        ask_price, ask_size = result['asks'][0]
        bid_price, bid_size = result['bids'][0]

        record = (
            symbol,
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

