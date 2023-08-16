from database.db_pool import release_connection, get_connection


def filter_symbols(symbols, data):
    found_records = []

    inst_ids_set = set(item['symbol'] for item in data)

    for symbol in symbols:
        combined_id = str(symbol).replace('-', '').lower()
        if combined_id in inst_ids_set:
            found_records.append([item for item in data if item['symbol'] == combined_id][0])

    print(f"huobi - symbols      : {len(data)}")
    print(f"huobi - symbols found: {len(found_records)}")
    return found_records


def insert_to_db(found_records, reference):
    connection = get_connection()
    cursor = connection.cursor()

    for item in found_records:
        item['symbol'] = transform_symbol(item['symbol'], reference)

    query = """
        INSERT INTO trade_data (
            symbol_name, bid, bid_size, ask, ask_size, update_time, exchange_name
        ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, 'huobi');
    """

    batch_size = 1000
    for i in range(0, len(found_records), batch_size):
        batch = found_records[i:i + batch_size]
        records_to_insert = [
            (
                record['symbol'],
                record['bid'],
                record['bidSize'],
                record['ask'],
                record['askSize']
            ) for record in batch
        ]

        cursor.executemany(query, records_to_insert)
        connection.commit()

    release_connection(connection)


def transform_symbol(symbol, reference):
    for match in reference:
        if symbol.endswith(match.lower()):
            return str(symbol[:-len(match)]).upper() + '-' + match
    return symbol.upper()
