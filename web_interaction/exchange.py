from psycopg2.extras import DictCursor

from database.db_pool import get_connection, release_connection
from database.db_service import get_symbols

global_exclusion_dict = None


def exchange_list():
    connection = get_connection()
    cursor = connection.cursor(cursor_factory=DictCursor)

    sql_script = """
    SELECT * FROM exchange ORDER BY exchange_id
"""

    cursor.execute(sql_script)
    result = cursor.fetchall()
    release_connection(connection)

    return result


def update_status(exchange_id, status):
    connection = get_connection()
    cursor = connection.cursor(cursor_factory=DictCursor)

    sql_script = f"""
    UPDATE exchange set status = {status} where exchange_id = {exchange_id}
"""

    cursor.execute(sql_script)
    connection.commit()
    release_connection(connection)


def exchange_list_used():
    connection = get_connection()
    cursor = connection.cursor(cursor_factory=DictCursor)

    sql_script = """
    SELECT * FROM exchange WHERE status = 1 ORDER BY exchange_id
"""

    cursor.execute(sql_script)
    result = cursor.fetchall()
    release_connection(connection)

    return result


def load_exclusion_list():
    global global_exclusion_dict

    connection = get_connection()
    cursor = connection.cursor(cursor_factory=DictCursor)

    cursor.execute("SELECT exchange_name, excluded_pair FROM exclusion_list WHERE status = 1")
    rows = cursor.fetchall()

    release_connection(connection)

    exclusion_dict = {}
    for row in rows:
        exchange, symbol = row
        if exchange not in exclusion_dict:
            exclusion_dict[exchange] = []
        exclusion_dict[exchange].append(symbol)

    global_exclusion_dict = exclusion_dict


all_symbols, _ = get_symbols()


def get_filtered_symbols_for_exchange(exchange_name):
    excluded_symbols = global_exclusion_dict.get(exchange_name, [])
    return [s for s in all_symbols if s not in excluded_symbols]
