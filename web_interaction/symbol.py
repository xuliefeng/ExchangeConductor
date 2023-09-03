from psycopg2.extras import DictCursor

from database.db_pool import get_connection, release_connection

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

    values = (
        symbol_name,
        ''
    )
    cursor.execute(sql_script, values)

    connection.commit()
    release_connection(connection)
