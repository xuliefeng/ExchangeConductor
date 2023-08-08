from database.db_pool import release_connection, get_connection


def filter_symbols(coins_s, coins_r, inst_data):
    found_records = []
    not_found_coins = set()

    inst_ids_set = set(item['instId'] for item in inst_data)

    for coin_stable in coins_s:
        found = False
        for coin_reference in coins_r:
            combined_id = f"{coin_stable}-{coin_reference}"
            if combined_id in inst_ids_set:
                found_records.append([item for item in inst_data if item['instId'] == combined_id][0])
                # print(f"Data found for stable coin: {combined_id} in OKX")
                found = True
                break
        if not found:
            not_found_coins.add(coin_stable)
            print(f"Data not found for stable coin: {coin_stable} in OKX")

    return found_records, not_found_coins


def insert_to_db(data):
    connection = get_connection()
    cursor = connection.cursor()

    query = """
        INSERT INTO trade_data (
            coin_name, bid, bid_size, ask, ask_size, last, last_size, update_time, remark
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, 'OKX');
    """

    batch_size = 1000
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        records_to_insert = [
            (
                record['instId'],
                record['bidPx'],
                record['bidSz'],
                record['askPx'],
                record['askSz'],
                record['last'],
                record['lastSz'],
            ) for record in batch
        ]

        cursor.executemany(query, records_to_insert)
        connection.commit()

    release_connection(connection)