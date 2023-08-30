from config.logger_config import setup_logger
from database.db_pool import release_connection, get_connection
from my_tools.time_util import get_current_time
from web_interaction.exclusion import get_filtered_symbols_for_exchange

logger = setup_logger("xt_processor", "log/app.log")


def filter_symbols(data):
    found_records = []
    inst_ids_set = set(item['s'] for item in data)
    symbols = get_filtered_symbols_for_exchange('xt')

    for symbol in symbols:
        combined_id = str(symbol).replace('-', '_').lower()
        if combined_id in inst_ids_set:
            matched_item = [item for item in data if item['s'] == combined_id][0]
            found_records.append((symbol, matched_item))

    logger.info(f"xt - symbols       : {len(data)}")
    logger.info(f"xt - symbols found : {len(found_records)}")
    return found_records


def insert_to_db(found_records, temp_table_name):
    current_time = get_current_time()
    connection = get_connection()
    cursor = connection.cursor()

    query = f"""
        INSERT INTO {temp_table_name} (
            symbol_name, bid, bid_size, ask, ask_size, update_time, exchange_name
        ) VALUES (%s, %s, %s, %s, %s, '{current_time}', 'xt');
    """

    batch_size = 1000
    for i in range(0, len(found_records), batch_size):
        batch = found_records[i:i + batch_size]
        records_to_insert = [
            (
                record[0],
                record[1]['bp'],
                record[1]['bq'],
                record[1]['ap'],
                record[1]['aq']
            ) for record in batch
        ]

        cursor.executemany(query, records_to_insert)
        connection.commit()

    release_connection(connection)
