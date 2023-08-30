from config.logger_config import setup_logger
from database.db_pool import release_connection, get_connection
from my_tools.time_util import get_current_time
from web_interaction.exclusion import get_filtered_symbols_for_exchange

logger = setup_logger("coinex_processor", "log/app.log")


def filter_symbols(data):
    found_records = []
    inst_ids_set = set(data.keys())
    symbols = get_filtered_symbols_for_exchange('coinex')

    for symbol in symbols:
        combined_id = str(symbol).replace('-', '')
        if combined_id in inst_ids_set:
            found_records.append({symbol: data[combined_id]})

    logger.info(f"coinex - symbols       : {len(data)}")
    logger.info(f"coinex - symbols found : {len(found_records)}")
    return found_records


def insert_to_db(found_records, temp_table_name):
    current_time = get_current_time()
    connection = get_connection()
    cursor = connection.cursor()

    query = f"""
        INSERT INTO {temp_table_name} (
            symbol_name, bid, bid_size, ask, ask_size, update_time, exchange_name
        ) VALUES (%s, %s, %s, %s, %s, '{current_time}', 'coinex');
    """

    records_to_insert = []
    for record in found_records:
        symbol_name, data = list(record.items())[0]

        sell = data.get('sell')
        sell_amount = data.get('sell_amount')
        buy = data.get('buy')
        buy_amount = data.get('buy_amount')

        records_to_insert.append(
            (
                symbol_name,
                buy,
                buy_amount,
                sell,
                sell_amount
            )
        )

    batch_size = 1000
    for i in range(0, len(records_to_insert), batch_size):
        batch = records_to_insert[i:i + batch_size]
        cursor.executemany(query, batch)
        connection.commit()

    release_connection(connection)
