from config.logger_config import setup_logger
from database.db_pool import release_connection, get_connection

logger = setup_logger("bitfinex_processor", "log/app.log")


def filter_symbols(symbols, data):
    found_records = []

    inst_ids_set = set(item[0] for item in data)

    for symbol in symbols:
        combined_id = 't' + str(symbol).replace('-', '')
        if combined_id in inst_ids_set:
            matched_item = [item for item in data if item[0] == combined_id][0]
            found_records.append((symbol, matched_item))

    logger.info(f"bitfinex - symbols       : {len(data)}")
    logger.info(f"bitfinex - symbols found : {len(found_records)}")
    return found_records


def insert_to_db(found_records, temp_table_name):
    connection = get_connection()
    cursor = connection.cursor()

    query = f"""
        INSERT INTO {temp_table_name} (
            symbol_name, bid, bid_size, ask, ask_size, update_time, exchange_name
        ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, 'bitfinex');
    """

    batch_size = 1000
    for i in range(0, len(found_records), batch_size):
        batch = found_records[i:i + batch_size]
        records_to_insert = [
            (
                record[0],
                record[1][1],
                record[1][2],
                record[1][3],
                record[1][4]
            ) for record in batch
        ]

        cursor.executemany(query, records_to_insert)
        connection.commit()

    release_connection(connection)
