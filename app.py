from concurrent.futures import ThreadPoolExecutor, wait

from flask import Flask
from flask_cors import CORS

from data_collection.gate_io_collector import gate_io
from data_collection.huobi_collector import huobi
from data_collection.okx_collector import okx
from database.db_service import get_symbols

app = Flask(__name__)
CORS(app)


def execute_in_parallel(symbols, reference):
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(okx, symbols),
            executor.submit(huobi, symbols, reference),
            # executor.submit(gate_io, symbols)

        ]
        wait(futures)


@app.route("/api/get", methods=["GET"])
def test():
    symbols, reference = get_symbols()
    # execute_in_parallel(symbols, reference)
    # okx(symbols)
    # huobi(symbols, reference)
    # gate_io(symbols)
    return "1", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
