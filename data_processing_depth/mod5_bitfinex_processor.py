import uuid

from config.logger_config import setup_logger
from database.db_pool import release_connection, get_connection
from my_tools.time_util import get_current_time

logger = setup_logger("bitfinex_processor", "log/app.log")


def filter_symbols(symbols, data):
    found_records = []
    inst_ids_set = {item[0][1:] for item in data if item[0].startswith('t')}

    for symbol in symbols:
        symbol = str(symbol).replace('-', '')
        if symbol in inst_ids_set:
            found_records.append(symbol)

    logger.info(f"bitfinex - symbols       : {len(data)}")
    logger.info(f"bitfinex - symbols found : {len(found_records)}")
    return found_records


def insert_to_db(found_records, temp_table_name, reference):
    current_time = get_current_time()
    connection = get_connection()
    cursor = connection.cursor()

    query_temp_table = f"""
        INSERT INTO {temp_table_name} (
            symbol_id, symbol_name, ask, bid, ask_size, bid_size, update_time, exchange_name
        ) VALUES (%s, %s, %s, %s, %s, %s, '{current_time}', 'bitfinex');
    """

    query_temp_table_depth = f"""
        INSERT INTO {temp_table_name + '_depth'} (
            symbol_id, symbol_name, price, size, type
        ) VALUES (%s, %s, %s, %s, %s);
    """

    records_to_insert_temp = []
    records_to_insert_temp_depth = []

    for symbol_name, result in found_records.items():

        asks = [item for item in result if item[2] < 0]
        bids = [item for item in result if item[2] > 0]

        if not asks or not bids:
            continue

        symbol_id = str(uuid.uuid4())
        symbol_name = symbol_name.replace("_", "-")
        symbol_name = transform_symbol(symbol_name, reference)

        try:
            ask_price = asks[0][1] if asks else None
            bid_price = bids[0][1] if bids else None
            ask_size = abs(asks[0][2]) if asks else None
            bid_size = bids[0][2] if bids else None

            records_to_insert_temp.append(
                (symbol_id, symbol_name, ask_price, bid_price, ask_size, bid_size)
            )
        except (IndexError, TypeError, ValueError) as e:
            logger.error(f"Error processing ask/bid price for symbol {symbol_name}. Error: {str(e)}")
            continue

        try:
            for ask in asks:
                records_to_insert_temp_depth.append(
                    (symbol_id, symbol_name, ask[1], abs(ask[2]), 'ask')
                )
        except (IndexError, TypeError, ValueError) as e:
            logger.error(f"Error processing asks for symbol {symbol_name}. Error: {str(e)}")
            continue

        try:
            for bid in bids:
                records_to_insert_temp_depth.append(
                    (symbol_id, symbol_name, bid[1], bid[2], 'bid')
                )
        except (IndexError, TypeError) as e:
            logger.error(f"Error processing bids for symbol {symbol_name}. Error: {str(e)}")
            continue

    cursor.executemany(query_temp_table, records_to_insert_temp)
    cursor.executemany(query_temp_table_depth, records_to_insert_temp_depth)
    connection.commit()
    cursor.close()
    release_connection(connection)


def transform_symbol(symbol, reference):
    for match in reference:
        if symbol.endswith(str(match)):
            return str(symbol[:-len(match)]) + '-' + match
