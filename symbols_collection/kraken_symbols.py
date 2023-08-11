import requests

from database.db_pool import get_connection, release_connection
from database.db_service import get_symbols

coins_stable, coins_reference = get_symbols()


def kraken():
    url = "https://api.kraken.com/0/public/AssetPairs"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        data = data['result']
        insert_to_db(data, coins_reference)
    else:
        print(f"Request failed with status code {response.status_code}")


def insert_to_db(data, coins_r):
    connection = get_connection()
    cursor = connection.cursor()

    print(len(data))
    filtered_symbols = transform_and_filter_symbols(data, coins_r)
    print(len(filtered_symbols))

    inst_ids_tuples = [(inst_id,) for inst_id in filtered_symbols]
    sql = "INSERT INTO coins_stable (coin_name, remark) VALUES (%s, 'kraken')"

    cursor.executemany(sql, inst_ids_tuples)

    connection.commit()
    release_connection(connection)
    print(f"{len(filtered_symbols)} record(s) inserted.")


def transform_and_filter_symbols(data, coins_r):
    transformed_symbols = []

    for item in data:
        for match in coins_r:
            if item.endswith(match):
                transformed_symbol = str(item[:-len(match)])
                transformed_symbols.append(transformed_symbol)
                break

    return transformed_symbols


kraken()
