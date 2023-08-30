from config.logger_config import setup_logger
from database.db_pool import release_connection, get_connection
from my_tools.time_util import get_current_time
from web_interaction.exchange import get_filtered_symbols_for_exchange

logger = setup_logger("okx_processor", "log/app.log")


def filter_symbols(data):
    found_records = []
    inst_ids_set = set(item['instId'] for item in data)
    symbols = get_filtered_symbols_for_exchange('okx')

    for symbol in symbols:
        if symbol in inst_ids_set:
            found_records.append([item for item in data if item['instId'] == symbol][0])

    logger.info(f"okx - symbols       : {len(data)}")
    logger.info(f"okx - symbols found : {len(found_records)}")
    return found_records


def insert_to_db(found_records, temp_table_name):
    current_time = get_current_time()
    connection = get_connection()
    cursor = connection.cursor()

    query = f"""
        INSERT INTO {temp_table_name} (
            symbol_name, bid, bid_size, ask, ask_size, update_time, exchange_name
        ) VALUES (%s, %s, %s, %s, %s, '{current_time}', 'okx');
    """

    batch_size = 1000
    for i in range(0, len(found_records), batch_size):
        batch = found_records[i:i + batch_size]
        records_to_insert = [
            (
                record['instId'],
                record['bidPx'],
                record['bidSz'],
                record['askPx'],
                record['askSz']
            ) for record in batch
        ]

        cursor.executemany(query, records_to_insert)
        connection.commit()

    release_connection(connection)
