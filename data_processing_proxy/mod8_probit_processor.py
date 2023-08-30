
from config.logger_config import setup_logger
from database.db_pool import release_connection, get_connection
from my_tools.time_util import get_current_time
from web_interaction.exchange import get_filtered_symbols_for_exchange

logger = setup_logger("probit_processor", "log/app.log")


def filter_symbols(data):
    found_records = []
    inst_ids_set = set(item['market_id'] for item in data)
    symbols = get_filtered_symbols_for_exchange('probit')

    for symbol in symbols:
        if symbol in inst_ids_set:
            found_records.append(symbol)

    logger.info(f"probit - symbols       : {len(data)}")
    logger.info(f"probit - symbols found : {len(found_records)}")
    return found_records


def insert_to_db(found_records, temp_table_name):
    current_time = get_current_time()
    connection = get_connection()
    cursor = connection.cursor()

    query_temp_table = f"""
        INSERT INTO {temp_table_name} (
            symbol_name, ask, bid, ask_size, bid_size, update_time, exchange_name
        ) VALUES (%s, %s, %s, %s, %s, '{current_time}', 'probit');
    """

    records_to_insert_temp = []

    for symbol_name, result in found_records.items():
        data = result.get('data', [])

        if not data:
            continue

        try:
            buy_data = [item for item in data if item['side'] == 'buy']
            sell_data = [item for item in data if item['side'] == 'sell']

            if not buy_data or not sell_data:
                continue

            ask_price = sell_data[-1]['price'] if sell_data else None
            bid_price = buy_data[-1]['price'] if buy_data else None
            ask_size = sell_data[-1]['quantity'] if sell_data else None
            bid_size = buy_data[-1]['quantity'] if buy_data else None

            records_to_insert_temp.append(
                (symbol_name, ask_price, bid_price, ask_size, bid_size)
            )
        except (IndexError, TypeError, ValueError) as e:
            logger.error(f"Error processing ask/bid price for symbol {symbol_name}. Error: {repr(e)}")
            continue

    cursor.executemany(query_temp_table, records_to_insert_temp)
    connection.commit()
    cursor.close()
    release_connection(connection)
