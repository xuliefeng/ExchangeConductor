from concurrent.futures import ThreadPoolExecutor, wait

from flask import Flask
from flask_cors import CORS

from data_collection.bit_get_collector import bit_get
from data_collection.bitfinex_collector import bitfinex
from data_collection.huobi_collector import huobi
from data_collection.mexc_collector import mexc
from data_collection.okx_collector import okx
from database.db_service import get_symbols

app = Flask(__name__)
CORS(app)


def execute_in_parallel(symbols, reference):
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(okx, symbols),
            executor.submit(huobi, symbols, reference),

        ]
        wait(futures)


@app.route("/api/get", methods=["GET"])
def test():
    symbols, reference = get_symbols()
    # execute_in_parallel(symbols, reference)
    # okx(symbols)
    # huobi(symbols, reference)
    # bitfinex(symbols, reference)
    # bit_get(symbols, reference)
    mexc(symbols, reference)
    return "1", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
