from psycopg2.extras import DictCursor

from database.db_pool import get_connection, release_connection
from database.db_service import get_symbols

_, reference = get_symbols()


def symbol_list():
    connection = get_connection()
    cursor = connection.cursor(cursor_factory=DictCursor)

    sql_script = """
    SELECT * FROM symbols ORDER BY symbol_id
"""

    cursor.execute(sql_script)
    result = cursor.fetchall()
    release_connection(connection)

    return result


def delete_record(symbol_id):
    connection = get_connection()
    cursor = connection.cursor(cursor_factory=DictCursor)

    sql_script = f"""DELETE FROM symbols where symbol_id = {symbol_id}"""

    cursor.execute(sql_script)
    connection.commit()
    release_connection(connection)


def insert_record(symbol_name):
    connection = get_connection()
    cursor = connection.cursor()

    sql_script = """INSERT INTO symbols (symbol_name, remark) VALUES (%s, %s)"""

    for ref in reference:
        combined_symbol_name = f"{symbol_name}-{ref}"
        values = (
            combined_symbol_name,
            ''
        )
        cursor.execute(sql_script, values)

    connection.commit()
    release_connection(connection)
