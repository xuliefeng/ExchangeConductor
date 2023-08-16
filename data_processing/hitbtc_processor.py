from config.logger_config import setup_logger
from database.db_pool import release_connection, get_connection

logger = setup_logger("hitbtc_processor", "log/app.log")


def filter_symbols(symbols, data):
    found_records = []

    inst_ids_set = set(data.keys())

    for symbol in symbols:
        combined_id = str(symbol).replace('-', '')
        if combined_id in inst_ids_set:
            found_records.append({combined_id: data[combined_id]})

    logger.info(f"hitbtc - symbols       : {len(data)}")
    logger.info(f"hitbtc - symbols found : {len(found_records)}")
    return found_records


def insert_to_db(found_records, reference):
    connection = get_connection()
    cursor = connection.cursor()

    for item in found_records:
        key = list(item.keys())[0]
        transformed_key = transform_symbol(key, reference)
        item[transformed_key] = item.pop(key)

    query = """
        INSERT INTO trade_data (
            symbol_name, bid, bid_size, ask, ask_size, update_time, exchange_name
        ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, 'hitbtc');
    """

    records_to_insert = []
    for record in found_records:
        symbol = list(record.keys())[0]
        data = record[symbol]

        if data.get('ask') and len(data['ask']) > 0:
            ask_price = data['ask'][0][0]
            ask_size = data['ask'][0][1]
        else:
            ask_price = None
            ask_size = None

        if data.get('bid') and len(data['bid']) > 0:
            bid_price = data['bid'][0][0]
            bid_size = data['bid'][0][1]
        else:
            bid_price = None
            bid_size = None

        records_to_insert.append(
            (
                symbol,
                bid_price,
                bid_size,
                ask_price,
                ask_size
            )
        )

    batch_size = 1000
    for i in range(0, len(records_to_insert), batch_size):
        batch = records_to_insert[i:i + batch_size]
        cursor.executemany(query, batch)
        connection.commit()

    release_connection(connection)


def transform_symbol(symbol, reference):
    for match in reference:
        if symbol.endswith(match):
            return str(symbol[:-len(match)]) + '-' + match
    return symbol
