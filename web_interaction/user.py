import codecs

import bcrypt
from psycopg2.extras import DictCursor

from database.db_pool import get_connection, release_connection


def hash_password(password):
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password.encode('utf-8'), salt)


def check_password(password, hashed):
    bytes_hash = codecs.decode(hashed.replace('\\x', ''), 'hex')
    return bcrypt.checkpw(password.encode('utf-8'), bytes_hash)


def get_all_users():
    connection = get_connection()
    cursor = connection.cursor(cursor_factory=DictCursor)
    cursor.execute("SELECT * FROM users WHERE user_name != 'long123' ORDER BY user_id")
    result = cursor.fetchall()
    release_connection(connection)
    return result


def add_user_into_db(data):
    user_name = data['userName']
    password = data['password']
    expiry_days = data['expiryDays']
    hashed_password = hash_password(password)
    connection = get_connection()
    cursor = connection.cursor(cursor_factory=DictCursor)
    cursor.execute("INSERT INTO users (user_name, password, type, status, expire_date) VALUES (%s, %s, %s, %s, %s)",
                   (user_name, hashed_password, '普通用户', 1, expiry_days))
    connection.commit()
    release_connection(connection)


def update_user_info(data):
    user_id = data['userId']
    new_password = data.get('password', None)
    new_status = data.get('status', None)
    new_expire_date = data.get('expiryDays', None)

    connection = get_connection()
    cursor = connection.cursor(cursor_factory=DictCursor)

    updates = []
    values = []

    if new_password is not None:
        hashed_password = hash_password(new_password)
        updates.append("password = %s")
        values.append(hashed_password)

    if new_status is not None:
        updates.append("status = %s")
        if new_status == 'normal':
            new_status = 1
        else:
            new_status = 0
        values.append(new_status)

    if new_expire_date is not None:
        updates.append("expire_date = %s")
        values.append(new_expire_date)

    if updates:
        sql_update_query = "UPDATE users SET " + ", ".join(updates) + " WHERE user_id = %s"
        values.append(user_id)
        cursor.execute(sql_update_query, values)
        connection.commit()

    release_connection(connection)


def delete_user_record(user_id):
    connection = get_connection()
    cursor = connection.cursor(cursor_factory=DictCursor)
    cursor.execute("DELETE FROM users WHERE user_id = %s", (user_id,))
    connection.commit()
    release_connection(connection)


def user_login(user_name, input_password):
    connection = get_connection()
    cursor = connection.cursor(cursor_factory=DictCursor)

    cursor.execute("SELECT * FROM users WHERE user_name = %s", (user_name,))
    user = cursor.fetchone()
    release_connection(connection)
    if user:
        stored_password = user[2]
        status = user[4]
        if status != 1:
            return False, "用户已过期", None
        if check_password(input_password, stored_password):
            return True, "登陆成功", user
        else:
            return False, "密码错误", None
    else:
        return False, "用户不存在", None
