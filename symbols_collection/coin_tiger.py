import requests
from database.db_pool import get_connection, release_connection
from database.db_service import get_symbols

symbols, reference = get_symbols()


def coin_tiger():
    url = "https://www.cointiger.com/exchange/api/public/market/detail"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        insert_to_db(data, reference)
    else:
        print(f"Request failed with status code {response.status_code}")


def insert_to_db(data, ref):
    connection = get_connection()
    cursor = connection.cursor()

    filtered_symbols = transform_and_filter_symbols(data, ref)

    inst_ids_tuples = [(inst_id,) for inst_id in filtered_symbols]
    sql = "INSERT INTO symbols (symbol_name, remark) VALUES (%s, 'coin_tiger')"

    cursor.executemany(sql, inst_ids_tuples)

    connection.commit()
    release_connection(connection)
    print(f"{len(filtered_symbols)} record(s) inserted coin_tiger")


def transform_and_filter_symbols(data, ref):
    transformed_symbols = []
    unmatched_symbols = []

    base_symbols = set()
    for item in data.keys():
        for match in ref:
            if item.endswith(match):
                base_symbols.add(str(item[:-len(match)]))

    for base_currency in base_symbols:
        found = False
        for ref_currency in ref:
            matched_symbol = f"{base_currency}{ref_currency}"
            if matched_symbol in [item for item in data.keys()]:
                transformed_symbols.append(base_currency + '-' + ref_currency)
                found = True
                break
        if not found:
            unmatched_symbols.append(base_currency)

    for unmatched in unmatched_symbols:
        print(f"coin_tiger_unmatched_symbols : {unmatched}")

    with open('coin_tiger_unmatched_symbols.txt', 'w') as file:
        for unmatched in unmatched_symbols:
            file.write(f"coin_tiger_unmatched_symbols : {unmatched}" + '\n')
        file.write(f"symbols :               {len(data)}" + '\n')
        file.write(f"base_symbols :          {len(base_symbols)}" + '\n')
        file.write(f"transformed_symbols :   {len(transformed_symbols)}" + '\n')

    print(f"symbols :               {len(data)}")
    print(f"base_symbols :          {len(base_symbols)}")
    print(f"transformed_symbols :   {len(transformed_symbols)}")
    return transformed_symbols