import uuid

from psycopg2.extras import DictCursor

from config.logger_config import setup_logger
from database.db_pool import get_connection, release_connection

logger = setup_logger("db_service", "log/app.log")


def fetch_symbol_names(table_name):
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute(f"SELECT symbol_name FROM {table_name} order by symbol_id;")
    coins = [row[0] for row in cursor.fetchall()]
    cursor.close()
    release_connection(connection)
    return coins


def get_symbols():
    symbols = fetch_symbol_names("symbols")
    reference = fetch_symbol_names("reference")
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
    release_connection(connection)

    return temp_table_name


def delete_temp_table(temp_table_name):
    connection = get_connection()
    cursor = connection.cursor()

    drop_table_query = f"DROP TABLE IF EXISTS {temp_table_name};"
    cursor.execute(drop_table_query)
    connection.commit()
    cursor.close()
    release_connection(connection)


def usd_to_cny_rate():
    connection = get_connection()
    cursor = connection.cursor(cursor_factory=DictCursor)

    sql_script = """
        SELECT * FROM usd_to_cny_rate
    """

    cursor.execute(sql_script)
    result = cursor.fetchall()
    cursor.close()
    release_connection(connection)

    return result
