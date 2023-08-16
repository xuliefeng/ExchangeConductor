import time
from concurrent.futures import ThreadPoolExecutor, wait

from flask import Flask
from flask_cors import CORS

from config.logger_config import setup_logger
from data_collection.ascend_ex_collector import ascend_ex
from data_collection.bigone_collector import bigone
from data_collection.binance_collector import binance
from data_collection.bit_get_collector import bit_get
from data_collection.bit_mark_collector import bit_mark
from data_collection.bit_venus_collector import bit_venus
from data_collection.bitfinex_collector import bitfinex
from data_collection.bybit_collector import bybit
from data_collection.deep_coin_collector import deep_coin
from data_collection.hitbtc_collector import hitbtc
from data_collection.huobi_collector import huobi
from data_collection.jubi_collector import jubi
from data_collection.mexc_collector import mexc
from data_collection.okx_collector import okx
from data_collection.xt_collector import xt
from database.db_service import get_symbols

app = Flask(__name__)
CORS(app)
logger = setup_logger("app", "log/app.log")


def execute_in_parallel(symbols, reference):
    start_time = time.time()

    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(okx, symbols),
            executor.submit(huobi, symbols, reference),
            executor.submit(bitfinex, symbols, reference),
            executor.submit(bit_get, symbols, reference),
            executor.submit(mexc, symbols, reference),
            executor.submit(bit_venus, symbols, reference),
            executor.submit(deep_coin, symbols),
            executor.submit(ascend_ex, symbols),
            executor.submit(bybit, symbols, reference),
            executor.submit(xt, symbols),
            executor.submit(hitbtc, symbols, reference),
            executor.submit(bit_mark, symbols),
            executor.submit(bigone, symbols),
            executor.submit(jubi, symbols, reference),
            executor.submit(binance, symbols, reference)
        ]
        wait(futures)

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    logger.info(f"-------------------------------------------------- all executed in {elapsed_time} seconds.")


@app.route("/api/get", methods=["GET"])
def test():
    symbols, reference = get_symbols()
    execute_in_parallel(symbols, reference)
    # okx(symbols)
    # huobi(symbols, reference)
    # bitfinex(symbols, reference)
    # bit_get(symbols, reference)
    # mexc(symbols, reference)
    # bit_venus(symbols, reference)
    # deep_coin(symbols)
    # ascend_ex(symbols)
    # bybit(symbols, reference)
    # xt(symbols)
    # hitbtc(symbols, reference)
    # bit_mark(symbols)
    # bigone(symbols)
    # jubi(symbols, reference)
    # binance(symbols, reference)
    return "1", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
