from config.logger_config import setup_logger
from database.db_pool import release_connection, get_connection

logger = setup_logger("ascend_ex_processor", "log/app.log")


def filter_symbols(symbols, data):
    found_records = []

    inst_ids_set = set(item['symbol'] for item in data)

    for symbol in symbols:
        symbol = str(symbol).replace('-', '/')
        if symbol in inst_ids_set:
            found_records.append([item for item in data if item['symbol'] == symbol][0])

    logger.info(f"ascend_ex - symbols       : {len(data)}")
    logger.info(f"ascend_ex - symbols found : {len(found_records)}")
    return found_records


def insert_to_db(found_records):
    connection = get_connection()
    cursor = connection.cursor()

    for item in found_records:
        item['symbol'] = str(item['symbol']).replace('/', '-')

    query = """
        INSERT INTO trade_data (
            symbol_name, bid, bid_size, ask, ask_size, update_time, exchange_name
        ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, 'ascend_ex');
    """

    batch_size = 1000
    for i in range(0, len(found_records), batch_size):
        batch = found_records[i:i + batch_size]
        records_to_insert = [
            (
                record['symbol'],
                record['bid'][0],
                record['bid'][1],
                record['ask'][0],
                record['ask'][1]
            ) for record in batch
        ]

        cursor.executemany(query, records_to_insert)
        connection.commit()

    release_connection(connection)
