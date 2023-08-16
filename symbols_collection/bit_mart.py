import requests

from config.logger_config import setup_logger
from database.db_pool import get_connection, release_connection
from database.db_service import get_symbols

symbols, reference = get_symbols()
logger = setup_logger("bit_mart", "../log/app.log")


def bit_mart():
    url = "https://api-cloud.bitmart.com/spot/v2/ticker"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        data = data['data']['tickers']
        insert_to_db(data, reference)
    else:
        logger.error(f"Request failed with status code {response.status_code}")


def insert_to_db(data, ref):
    connection = get_connection()
    cursor = connection.cursor()

    filtered_symbols = transform_and_filter_symbols(data, ref)

    inst_ids_tuples = [(inst_id,) for inst_id in filtered_symbols]
    sql = "INSERT INTO symbols (symbol_name, remark) VALUES (%s, 'bit_mart')"

    cursor.executemany(sql, inst_ids_tuples)

    connection.commit()
    release_connection(connection)
    logger.info(f"{len(filtered_symbols)} record(s) inserted bit_mart")


def transform_and_filter_symbols(data, ref):
    transformed_symbols = []
    unmatched_symbols = []

    base_symbols = set()
    for item in data:
        symbol = item['symbol']
        base_currency, _ = symbol.split('_')
        base_symbols.add(base_currency)

    for base_currency in base_symbols:
        found = False
        for ref_currency in ref:
            matched_symbol = f"{base_currency}_{ref_currency}"
            if matched_symbol in [item['symbol'] for item in data]:
                transformed_symbols.append(base_currency + '-' + ref_currency)
                found = True
                break
        if not found:
            unmatched_symbols.append(base_currency)

    for unmatched in unmatched_symbols:
        logger.info(f"bit_mart_unmatched_symbols : {unmatched}")

    with open('bit_mart_unmatched_symbols.txt', 'w') as file:
        for unmatched in unmatched_symbols:
            file.write(f"bit_mart_unmatched_symbols : {unmatched}" + '\n')
        file.write(f"symbols :               {len(data)}" + '\n')
        file.write(f"base_symbols :          {len(base_symbols)}" + '\n')
        file.write(f"transformed_symbols :   {len(transformed_symbols)}" + '\n')

    logger.info(f"symbols :               {len(data)}")
    logger.info(f"base_symbols :          {len(base_symbols)}")
    logger.info(f"transformed_symbols :   {len(transformed_symbols)}")
    return transformed_symbols
