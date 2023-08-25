import time
import uuid

import requests
from flask import jsonify

from config.logger_config import setup_logger
from database.db_pool import get_connection, release_connection

logger = setup_logger("db_service", "log/app.log")


def fetch_coin_names(table_name):
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute(f"SELECT symbol_name FROM {table_name} order by symbol_id;")
    coins = [row[0] for row in cursor.fetchall()]
    release_connection(connection)
    return coins


def get_symbols():
    symbols = fetch_coin_names("symbols")
    reference = fetch_coin_names("reference")
    return symbols, reference


def create_temp_table():
    connection = get_connection()
    cursor = connection.cursor()

    temp_table_name = "temp_" + str(uuid.uuid4()).replace("-", "_")
    create_table = f"""CREATE TABLE {temp_table_name} AS SELECT * FROM trade_data;"""
    # create_table_depth = f"""CREATE TABLE {temp_table_name + '_depth'} AS SELECT * FROM trade_data_depth;"""

    cursor.execute(create_table)
    # cursor.execute(create_table_depth)
    connection.commit()
    cursor.close()
    connection.close()

    return temp_table_name


def delete_temp_table(temp_table_name):
    connection = get_connection()
    cursor = connection.cursor()

    drop_table_query = f"DROP TABLE IF EXISTS {temp_table_name};"
    cursor.execute(drop_table_query)
    cursor.close()
    connection.commit()
    connection.close()

    return jsonify({"message": f"Table {temp_table_name} deleted successfully!"})


def get_reference_price():
    start_time = time.time()
    symbols = ['TUSD',
               'BUSD',
               'USDC',
               'DAI',
               'USD',
               'BTC',
               'ETH',
               'LUSD',
               'USDP',
               'XAUT',
               'GUSD',
               'EUROC',
               'EURS',
               'RSR']

    joined_symbols = ','.join(symbols)
    url = f"https://min-api.cryptocompare.com/data/pricemulti?fsyms={joined_symbols}&tsyms=USDT"
    response = requests.get(url)
    data = response.json()
    prices_list = [(key, value['USDT']) for key, value in data.items()]

    connection = get_connection()
    cursor = connection.cursor()

    for currency, price in prices_list:
        cursor.execute("""
            UPDATE reference 
            SET price = %s
            WHERE symbol_name = %s;
        """, (price, currency))

    connection.commit()
    cursor.close()
    connection.close()

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    logger.info(
        f"-------------------------------------------------- get_reference_price executed in {elapsed_time} seconds.")


def get_usd_to_cny_rate():
    start_time = time.time()
    url = "https://open.er-api.com/v6/latest/USD"
    response = requests.get(url)

    data = response.json()
    cny_rate = data['rates']['CNY']

    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute("""
        UPDATE usd_to_cny_rate 
        SET rate = %s
        WHERE name = 'CNY';
    """, (cny_rate,))

    connection.commit()
    cursor.close()
    connection.close()

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    logger.info(
        f"-------------------------------------------------- get_usd_to_cny_rate executed in {elapsed_time} seconds.")
