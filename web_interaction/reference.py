from psycopg2.extras import DictCursor
from database.db_pool import get_connection, release_connection


def reference_list():
    connection = get_connection()
    cursor = connection.cursor(cursor_factory=DictCursor)

    sql_script = """
    SELECT * FROM reference ORDER BY symbol_id
"""

    cursor.execute(sql_script)
    result = cursor.fetchall()
    cursor.close()
    release_connection(connection)

    return result
