from psycopg2.extras import DictCursor

from database.db_pool import get_connection, release_connection
from database.db_service import get_symbols

global_exclusion_dict = None

all_symbols, _ = get_symbols()


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


def get_filtered_symbols_for_exchange(exchange_name):
    excluded_symbols = global_exclusion_dict.get(exchange_name, [])
    return [s for s in all_symbols if s not in excluded_symbols]


def exclusion_list():
    connection = get_connection()
    cursor = connection.cursor(cursor_factory=DictCursor)

    cursor.execute("SELECT * FROM exclusion_list WHERE status = 1 ORDER BY exclusion_id")
    result = cursor.fetchall()

    release_connection(connection)

    return result


def delete_exclusion_record(exclusion_id):
    connection = get_connection()
    cursor = connection.cursor(cursor_factory=DictCursor)

    sql_script = f"""DELETE FROM exclusion_list where exclusion_id = {exclusion_id}"""

    cursor.execute(sql_script)
    connection.commit()
    release_connection(connection)


def insert_exclusion_record(data):
    exchange_name = data['exchangeName']
    symbol_name = data['symbolName']
    exclusion_type = data['type']
    expiry_days = data['expiryDays']

    connection = get_connection()
    cursor = connection.cursor()

    sql_script = """INSERT INTO exclusion_list (exchange_name, excluded_pair, type, status, expire_date) VALUES (%s, %s, %s, 1, %s)"""

    values = (
        [exchange_name,
         symbol_name,
         exclusion_type,
         expiry_days]
    )
    cursor.execute(sql_script, values)
    connection.commit()
    release_connection(connection)
