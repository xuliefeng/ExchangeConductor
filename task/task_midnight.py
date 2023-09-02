import sched
import time
from datetime import datetime, timedelta
from database.db_pool import get_connection, release_connection


def schedule_midnight_task():
    s = sched.scheduler(time.time, time.sleep)

    def run_task(sc):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"task midnight executed {current_time}")
        update_exclusion()
        update_user()
        next_run_time = datetime.now() + timedelta(days=1)
        next_run_time = next_run_time.replace(hour=0, minute=0, second=0, microsecond=0)
        s.enterabs(time.mktime(next_run_time.timetuple()), 1, run_task, (sc,))

    now = datetime.now()
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    time_to_midnight = (midnight - now).total_seconds()

    s.enter(time_to_midnight, 1, run_task, (s,))
    s.run()


def update_exclusion():
    try:
        connection = get_connection()
        cursor = connection.cursor()

        cursor.execute(
            "SELECT exclusion_id, status, expire_date FROM exclusion_list WHERE status = 1 AND expire_date != ''")
        rows = cursor.fetchall()

        for row in rows:
            exclusion_id, status, expire_date = row

            expire_date = int(expire_date) - 1

            if expire_date == 0:
                cursor.execute("UPDATE exclusion_list SET status = 0 WHERE exclusion_id = %s", (exclusion_id,))

            cursor.execute("UPDATE exclusion_list SET expire_date = %s WHERE exclusion_id = %s",
                           (expire_date, exclusion_id))

        connection.commit()
        cursor.close()
        release_connection(connection)
    except Exception as e:
        print(f"An error occurred: {e}")


def update_user():
    try:
        connection = get_connection()
        cursor = connection.cursor()

        cursor.execute("SELECT user_id, status, expire_date FROM users WHERE status = 1 AND expire_date != ''")
        rows = cursor.fetchall()

        for row in rows:
            user_id, status, expire_date = row

            expire_date = int(expire_date) - 1

            if expire_date == 0:
                cursor.execute("UPDATE users SET status = 0 WHERE user_id = %s", (user_id,))

            cursor.execute("UPDATE users SET expire_date = %s WHERE user_id = %s", (expire_date, user_id))

        connection.commit()
        cursor.close()
        release_connection(connection)
    except Exception as e:
        print(f"An error occurred: {e}")
