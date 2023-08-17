import uuid

from flask import jsonify

from database.db_pool import get_connection, release_connection


def fetch_coin_names(table_name):
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute(f"SELECT symbol_name FROM {table_name};")
    coins = [row[0] for row in cursor.fetchall()]
    release_connection(connection)
    return coins


def get_symbols():
    symbols = fetch_coin_names("symbols")
    reference = fetch_coin_names("reference")
    return symbols, reference


def create_temp_table():
    temp_table_name = "temp_" + str(uuid.uuid4()).replace("-", "_")

    connection = get_connection()
    cursor = connection.cursor()

    create_table_query = f"""
    CREATE TABLE {temp_table_name} AS SELECT * FROM trade_data;
    """
    cursor.execute(create_table_query)
    cursor.close()
    connection.commit()
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
