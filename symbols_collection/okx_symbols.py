import requests
from database.db_pool import get_connection, release_connection
from database.db_service import get_symbols

coins_stable, coins_reference = get_symbols()


def okx():
    url = "https://www.okx.com/api/v5/market/tickers?instType=SPOT"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        data = data['data']
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
    sql = "INSERT INTO coins_stable (coin_name, remark) VALUES (%s, 'okx')"

    cursor.executemany(sql, inst_ids_tuples)

    connection.commit()
    release_connection(connection)
    print(f"{len(filtered_symbols)} record(s) inserted.")


def transform_and_filter_symbols(data, coins_r):
    transformed_symbols = []
    for item in data:
        symbol = item['instId']
        for match in coins_r:
            if symbol.endswith(match):
                transformed_symbol = str(symbol[:-len(match) - 1])
                transformed_symbols.append(transformed_symbol)
                break
    return transformed_symbols


okx()
