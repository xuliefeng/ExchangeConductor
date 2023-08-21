import uuid

from config.logger_config import setup_logger
from database.db_pool import release_connection, get_connection
from mytools.time_util import get_current_time
logger = setup_logger("gate_io_processor", "log/app.log")


def filter_symbols(symbols, data):
    found_records = []
    inst_ids_set = set(item['currency_pair'] for item in data)

    for symbol in symbols:
        symbol = str(symbol).replace('-', '_')
        if symbol in inst_ids_set:
            found_records.append(symbol)

    logger.info(f"gate_io - symbols       : {len(data)}")
    logger.info(f"gate_io - symbols found : {len(found_records)}")
    return found_records


def insert_to_db(found_records, temp_table_name):
    current_time = get_current_time()
    connection = get_connection()
    cursor = connection.cursor()

    query_temp_table = f"""
        INSERT INTO {temp_table_name} (
            symbol_id, symbol_name, ask, bid, update_time, exchange_name
        ) VALUES (%s, %s, %s, %s, '{current_time}', 'gateio');
    """

    query_temp_table_depth = f"""
        INSERT INTO {temp_table_name + '_depth'} (
            symbol_id, symbol_name, price, size, type
        ) VALUES (%s, %s, %s, %s, %s);
    """

    records_to_insert_temp = []
    records_to_insert_temp_depth = []

    for symbol_name, result in found_records.items():
        if not result.get('asks') or not result.get('bids'):
            continue
        symbol_id = str(uuid.uuid4())
        symbol_name = symbol_name.replace("_", "-")

        try:
            ask_price = result['asks'][0][0] if result['asks'] else None
            bid_price = result['bids'][0][0] if result['bids'] else None

            records_to_insert_temp.append(
                (symbol_id, symbol_name, ask_price, bid_price)
            )
        except IndexError:
            continue

        for ask in result['asks']:
            records_to_insert_temp_depth.append(
                (symbol_id, symbol_name, ask[0], ask[1], 'ask')
            )

        for bid in result['bids']:
            records_to_insert_temp_depth.append(
                (symbol_id, symbol_name, bid[0], bid[1], 'bid')
            )

    cursor.executemany(query_temp_table, records_to_insert_temp)
    cursor.executemany(query_temp_table_depth, records_to_insert_temp_depth)
    connection.commit()
    cursor.close()
    release_connection(connection)
