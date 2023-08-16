from config.logger_config import setup_logger
from database.db_pool import release_connection, get_connection

logger = setup_logger("xt_processor", "log/app.log")


def filter_symbols(symbols, data):
    found_records = []

    inst_ids_set = set(item['s'] for item in data)

    for symbol in symbols:
        symbol = str(symbol).replace('-', '_').lower()
        if symbol in inst_ids_set:
            found_records.append([item for item in data if item['s'] == symbol][0])

    logger.info(f"xt - symbols       : {len(data)}")
    logger.info(f"xt - symbols found : {len(found_records)}")
    return found_records


def insert_to_db(found_records):
    connection = get_connection()
    cursor = connection.cursor()

    for item in found_records:
        item['s'] = str(item['s']).replace('_', '-').upper()

    query = """
        INSERT INTO trade_data (
            symbol_name, bid, bid_size, ask, ask_size, update_time, exchange_name
        ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, 'xt');
    """

    batch_size = 1000
    for i in range(0, len(found_records), batch_size):
        batch = found_records[i:i + batch_size]
        records_to_insert = [
            (
                record['s'],
                record['bp'],
                record['bq'],
                record['ap'],
                record['aq']
            ) for record in batch
        ]

        cursor.executemany(query, records_to_insert)
        connection.commit()

    release_connection(connection)
