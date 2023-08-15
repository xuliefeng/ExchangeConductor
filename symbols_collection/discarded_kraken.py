import requests
from database.db_pool import get_connection, release_connection
from database.db_service import get_symbols

coins_stable, coins_reference = get_symbols()


def kraken():
    url = "https://api.kraken.com/0/public/Assets"
    response = requests.get(url)
    if response.status_code == 200:
        assets_data = response.json()['result']
        kraken_assets = [key for key in assets_data.keys()]

        url_pairs = "https://api.kraken.com/0/public/AssetPairs"
        response_pairs = requests.get(url_pairs)
        if response_pairs.status_code == 200:
            data_pairs = response_pairs.json()['result']
            insert_to_db(data_pairs, kraken_assets, coins_reference)
        else:
            print(f"Request for AssetPairs failed with status code {response_pairs.status_code}")
    else:
        print(f"Request failed with status code {response.status_code}")


def insert_to_db(data, kraken_assets, coins_r):
    connection = get_connection()
    cursor = connection.cursor()

    filtered_symbols = transform_and_filter_symbols(data, kraken_assets, coins_r)

    inst_ids_tuples = [(inst_id,) for inst_id in filtered_symbols]
    sql = "INSERT INTO coins_stable (coin_name, remark) VALUES (%s, 'kraken')"

    cursor.executemany(sql, inst_ids_tuples)

    connection.commit()
    release_connection(connection)
    print(f"{len(filtered_symbols)} record(s) inserted.")


def transform_and_filter_symbols(data, kraken_assets, coins_r):
    transformed_symbols = []
    unmatched_symbols = []

    for asset in kraken_assets:
        found = False
        for ref_currency in coins_r:
            if f"{asset}{ref_currency}" in data.keys():
                transformed_symbols.append(f"{asset}-{ref_currency}")
                found = True
                break
        if not found:
            unmatched_symbols.append(asset)

    for unmatched in unmatched_symbols:
        print(f"kraken_unmatched_symbols : {unmatched}")

    with open('kraken_unmatched_symbols.txt', 'w') as file:
        for unmatched in unmatched_symbols:
            file.write(f"kraken_unmatched_symbols : {unmatched}" + '\n')
        file.write(f"symbols :               {len(data)}" + '\n')
        file.write(f"base_symbols :          {len(kraken_assets)}" + '\n')
        file.write(f"transformed_symbols :   {len(transformed_symbols)}" + '\n')

    print(f"symbols :               {len(data)}")
    print(f"base_symbols :          {len(kraken_assets)}")
    print(f"transformed_symbols :   {len(transformed_symbols)}")
    return transformed_symbols


kraken()
