import uuid

from config.logger_config import setup_logger
from database.db_pool import release_connection, get_connection
from my_tools.time_util import get_current_time

logger = setup_logger("okx_processor", "log/app.log")


def filter_symbols(symbols, data):
    found_records = []
    inst_ids_set = set(item['instId'] for item in data)

    for symbol in symbols:
        if symbol in inst_ids_set:
            found_records.append(symbol)

    logger.info(f"okx - symbols       : {len(data)}")
    logger.info(f"okx - symbols found : {len(found_records)}")
    return found_records


def insert_to_db(found_records, temp_table_name):
    current_time = get_current_time()
    connection = get_connection()
    cursor = connection.cursor()

    query_temp_table = f"""
        INSERT INTO {temp_table_name} (
            symbol_id, symbol_name, ask, bid, ask_size, bid_size, update_time, exchange_name
        ) VALUES (%s, %s, %s, %s, %s, %s, '{current_time}', 'okx');
    """

    query_temp_table_depth = f"""
        INSERT INTO {temp_table_name + '_depth'} (
            symbol_id, symbol_name, price, size, type
        ) VALUES (%s, %s, %s, %s, %s);
    """

    records_to_insert_temp = []
    records_to_insert_temp_depth = []

    for symbol_name, result in found_records.items():
        result = result.get('data', [])[0]
        asks = result.get('asks', [])
        bids = result.get('bids', [])

        if not asks or not bids:
            continue

        symbol_id = str(uuid.uuid4())
        symbol_name = symbol_name.replace("_", "-")

        try:
            ask_price = asks[0][0] if asks else None
            bid_price = bids[0][0] if bids else None
            ask_size = asks[0][1] if asks else None
            bid_size = bids[0][1] if bids else None

            records_to_insert_temp.append(
                (symbol_id, symbol_name, ask_price, bid_price, ask_size, bid_size)
            )
        except (IndexError, TypeError, ValueError) as e:
            logger.error(f"Error processing ask/bid price for symbol {symbol_name}. Error: {str(e)}")
            continue

        try:
            for ask in asks:
                records_to_insert_temp_depth.append(
                    (symbol_id, symbol_name, ask[0], ask[1], 'ask')
                )

            for bid in bids:
                records_to_insert_temp_depth.append(
                    (symbol_id, symbol_name, bid[0], bid[1], 'bid')
                )
        except (IndexError, TypeError, ValueError) as e:
            logger.error(f"Error processing asks/bids for symbol {symbol_name}. Error: {str(e)}")
            continue

    cursor.executemany(query_temp_table, records_to_insert_temp)
    cursor.executemany(query_temp_table_depth, records_to_insert_temp_depth)
    connection.commit()
    cursor.close()
    release_connection(connection)
