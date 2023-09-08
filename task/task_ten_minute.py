import requests
import sched, time
from datetime import datetime, timedelta
from pytz import timezone
from config.logger_config import setup_logger
from database.db_pool import get_connection, release_connection

logger = setup_logger("task_ten_minute", "log/app.log")


def schedule_ten_minute_task():
    s = sched.scheduler(time.time, time.sleep)
    shanghai_tz = timezone('Asia/Shanghai')

    def run_task(sc):
        current_time = datetime.now(shanghai_tz).strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Task executed every 10 minutes at {current_time}")
        get_reference_price()
        get_usd_to_cny_rate()
        next_run_time = datetime.now(shanghai_tz) + timedelta(minutes=10)
        s.enterabs(time.mktime(next_run_time.timetuple()), 1, run_task, (sc,))

    now = datetime.now(shanghai_tz)
    minutes_to_next_interval = 10 - (now.minute % 10)
    next_interval = now + timedelta(minutes=minutes_to_next_interval)
    time_to_next_interval = (next_interval - now).total_seconds()

    s.enter(time_to_next_interval, 1, run_task, (s,))
    s.run()


def get_reference_price():
    try:
        start_time = time.time()
        symbols = ['TUSD',
                   'BUSD',
                   'USDC',
                   'DAI',
                   'USD',
                   'BTC',
                   'ETH',
                   'LUSD',
                   'USDP',
                   'XAUT',
                   'GUSD',
                   'EUROC',
                   'EURS',
                   'RSR']

        joined_symbols = ','.join(symbols)
        url = f"https://min-api.cryptocompare.com/data/pricemulti?fsyms={joined_symbols}&tsyms=USDT&api_key=ed80e9409bc7c66b626320cfc2ae325213349b3614595035fb28920e40736433"
        response = requests.get(url)
        data = response.json()
        prices_list = [(key, value['USDT']) for key, value in data.items()]

        connection = get_connection()
        cursor = connection.cursor()

        for currency, price in prices_list:
            cursor.execute("""
                UPDATE reference 
                SET price = %s
                WHERE symbol_name = %s;
            """, (price, currency))

        connection.commit()
        cursor.close()
        release_connection(connection)

        end_time = time.time()
        elapsed_time = round(end_time - start_time, 3)
        logger.info(
            f"-------------------------------------------------- get_reference_price executed in {elapsed_time} seconds.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")


def get_usd_to_cny_rate():
    try:
        start_time = time.time()
        url = "https://open.er-api.com/v6/latest/USD"
        response = requests.get(url)

        data = response.json()
        cny_rate = data['rates']['CNY']

        connection = get_connection()
        cursor = connection.cursor()

        cursor.execute("""
            UPDATE usd_to_cny_rate 
            SET rate = %s
            WHERE name = 'CNY';
        """, (cny_rate,))

        connection.commit()
        cursor.close()
        release_connection(connection)

        end_time = time.time()
        elapsed_time = round(end_time - start_time, 3)
        logger.info(
            f"-------------------------------------------------- get_usd_to_cny_rate executed in {elapsed_time} seconds.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
