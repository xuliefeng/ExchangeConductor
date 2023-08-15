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
