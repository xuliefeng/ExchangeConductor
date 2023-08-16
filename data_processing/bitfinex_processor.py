from config.logger_config import setup_logger
from database.db_pool import release_connection, get_connection

logger = setup_logger("bitfinex_processor", "log/app.log")


def filter_symbols(symbols, data):
    found_records = []

    inst_ids_set = set(item[0] for item in data)

    for symbol in symbols:
        combined_id = 't' + str(symbol).replace('-', '')
        if combined_id in inst_ids_set:
            found_records.append([item for item in data if item[0] == combined_id][0])

    logger.info(f"bitfinex - symbols      : {len(data)}")
    logger.info(f"bitfinex - symbols found: {len(found_records)}")
    return found_records


def insert_to_db(found_records, reference):
    connection = get_connection()
    cursor = connection.cursor()

    for item in found_records:
        item[0] = transform_symbol(item[0], reference)

    query = """
        INSERT INTO trade_data (
            symbol_name, bid, bid_size, ask, ask_size, update_time, exchange_name
        ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, 'bitfinex');
    """

    batch_size = 1000
    for i in range(0, len(found_records), batch_size):
        batch = found_records[i:i + batch_size]
        records_to_insert = [
            (
                record[0],
                record[1],
                record[2],
                record[3],
                record[4]
            ) for record in batch
        ]

        cursor.executemany(query, records_to_insert)
        connection.commit()

    release_connection(connection)


def transform_symbol(symbol, reference):
    for match in reference:
        if symbol.endswith(match):
            symbol = str(symbol).replace('t', '')
            symbol = str(symbol[:-len(match)]) + '-' + match
            return symbol
    return symbol
