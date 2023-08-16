from config.logger_config import setup_logger
from database.db_pool import release_connection, get_connection

logger = setup_logger("bit_mark_processor", "log/app.log")


def filter_symbols(symbols, data):
    found_records = []
    inst_ids_set = set(item['symbol'] for item in data)

    for symbol in symbols:
        symbol = str(symbol).replace('-', '_')
        if symbol in inst_ids_set:
            found_records.append([item for item in data if item['symbol'] == symbol][0])

    logger.info(f"bit_mark - symbols       : {len(data)}")
    logger.info(f"bit_mark - symbols found : {len(found_records)}")
    return found_records


def insert_to_db(found_records):
    connection = get_connection()
    cursor = connection.cursor()

    query = """
        INSERT INTO trade_data (
            symbol_name, bid, bid_size, ask, ask_size, update_time, exchange_name
        ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, 'bitmark');
    """

    batch_size = 1000
    for i in range(0, len(found_records), batch_size):
        batch = found_records[i:i + batch_size]
        records_to_insert = [
            (
                str(record['symbol']).replace('_', '-'),
                record['best_bid'],
                record['best_bid_size'],
                record['best_ask'],
                record['best_ask_size']
            ) for record in batch
        ]

        cursor.executemany(query, records_to_insert)
        connection.commit()

    release_connection(connection)
