from concurrent.futures import ThreadPoolExecutor, wait
from flask import Flask
from flask_cors import CORS

from data_collection.huobi_collector import huobi
from data_collection.kraken_collector import kraken
from data_collection.kucoin_collector import kucoin
from data_collection.okx_collector import okx
from database.db_service import get_symbols

app = Flask(__name__)
CORS(app)


def execute_in_parallel(coins_stable, coins_reference):
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(okx, coins_stable, coins_reference),
            executor.submit(huobi, coins_stable, coins_reference),
            executor.submit(kraken, coins_stable, coins_reference),
            executor.submit(kucoin, coins_stable, coins_reference)
        ]

        wait(futures)


@app.route("/api/get", methods=["GET"])
def test():
    coins_stable, coins_reference = get_symbols()

    execute_in_parallel(coins_stable, coins_reference)

    return "1", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
