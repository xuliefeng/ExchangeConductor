import requests

from database.db_pool import get_connection, release_connection
from database.db_service import get_symbols

coins_stable, coins_reference = get_symbols()


def gateio():
    url = "https://api.gateio.ws/api/v4/spot/tickers"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        insert_to_db(data, coins_reference)
    else:
        print(f"Request failed with status code {response.status_code}")


def insert_to_db(data, coins_r):
    connection = get_connection()
    cursor = connection.cursor()

    filtered_symbols = transform_and_filter_symbols(data, coins_r)

    inst_ids_tuples = [(inst_id,) for inst_id in filtered_symbols]
    sql = "INSERT INTO coins_stable (coin_name, remark) VALUES (%s, 'gateio')"

    cursor.executemany(sql, inst_ids_tuples)

    connection.commit()
    release_connection(connection)
    print(f"{len(filtered_symbols)} record(s) inserted.")


def transform_and_filter_symbols(data, coins_r):
    transformed_symbols = []
    unmatched_symbols = []
    seen_prefixes = set()

    for item in data:
        symbol = item['currency_pair']

        for match in seen_prefixes:
            if symbol.startswith(match):
                continue

        matched = False
        for match in coins_r:
            if symbol.endswith(match):
                transformed_symbol = str(symbol[:-len(match) - 1]) + '-' + match
                transformed_symbols.append(transformed_symbol)
                prefix = str(symbol[:-len(match) - 1])
                seen_prefixes.add(prefix)
                matched = True
                break

        if not matched:
            unmatched_symbols.append(symbol)

    for unmatched in unmatched_symbols:
        print(f"gateio_unmatched_symbols : {unmatched}")

    with open('gateio_unmatched_symbols.txt', 'w') as file:
        for unmatched in unmatched_symbols:
            file.write(f"gateio_unmatched_symbols : {unmatched}" + '\n')
        file.write(str(len(data)) + '\n')
        file.write(str(len(transformed_symbols)) + '\n')

    print(len(data))
    print(len(transformed_symbols))
    return transformed_symbols


gateio()
