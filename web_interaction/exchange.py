from psycopg2.extras import DictCursor

from database.db_pool import get_connection, release_connection


def load_exchanges():
    connection = get_connection()
    cursor = connection.cursor(cursor_factory=DictCursor)

    sql_script = """
    SELECT * FROM exchange WHERE status = 1 ORDER BY exchange_id
"""
    cursor.execute(sql_script)
    result = cursor.fetchall()
    cursor.close()
    release_connection(connection)

    return result


def exchange_list():
    connection = get_connection()
    cursor = connection.cursor(cursor_factory=DictCursor)

    sql_script = """
    SELECT * FROM exchange ORDER BY exchange_id
"""
    cursor.execute(sql_script)
    result = cursor.fetchall()
    cursor.close()
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
    cursor.close()
    release_connection(connection)
