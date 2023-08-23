import time
from concurrent.futures import ThreadPoolExecutor, wait

from flask import Flask, jsonify, request
from flask_cors import CORS

from config.logger_config import setup_logger
from data_analysis.trade_analysis import fetch_combined_analysis_data
from data_collection_depth.mod3_gate_io_collector import gate_io
from data_collection_depth.mod2_huobi_collector import huobi

from data_collection_depth.mod1_okx_collector import okx
from database.db_service import get_symbols, create_temp_table, delete_temp_table, get_reference_price, \
    get_usd_to_cny_rate
from web_interaction.exchange import exchange_list, update_status, exchange_list_used
from web_interaction.symbol import symbol_list, delete_record, insert_record

app = Flask(__name__)
CORS(app)
logger = setup_logger("app", "log/app.log")

exchange_functions = {
    "okx": okx,
    "huobi": huobi,
    "gateio": gate_io,

    # "bitfinex": bitfinex,
    # "bitget": bit_get,
    # "mexc": mexc,
    # "bitvenus": bit_venus,
    # "deepcoin": deep_coin,
    # "ascendex": ascend_ex,
    # "bybit": bybit,
    # "xt": xt,
    # "hitbtc": hitbtc,
    # "bitmark": bit_mark,
    # "bigone": bigone,
    # "jubi": jubi,
    # "binance": binance,
}

special_exchanges = ['okx', 'gateio', 'deepcoin', 'ascendex', 'xt', 'bitmark', 'bigone']


def execute_in_parallel(symbols, reference, temp_table_name, exchanges):
    start_time = time.time()
    with ThreadPoolExecutor() as executor:

        futures = [executor.submit(get_reference_price), executor.submit(get_usd_to_cny_rate)]
        for item in exchanges:
            exchange_name = item[1]
            if exchange_name in exchange_functions:
                if exchange_name in special_exchanges:
                    futures.append(executor.submit(exchange_functions[exchange_name], symbols, temp_table_name))
                else:
                    futures.append(executor.submit(exchange_functions[exchange_name], symbols, reference, temp_table_name))
        wait(futures)

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    logger.info(f"-------------------------------------------------- all executed in {elapsed_time} seconds.")


@app.route("/api/get", methods=["GET"])
def test():
    symbols, reference = get_symbols()
    temp_table_name = create_temp_table()

    okx(symbols, temp_table_name)
    huobi(symbols, temp_table_name, reference)
    gate_io(symbols, temp_table_name)

    return "Success", 200


@app.route('/api/get-analysis-data', methods=['GET'])
def get_analysis_data():
    symbols, reference = get_symbols()
    exchanges = exchange_list_used()
    temp_table_name = create_temp_table()
    execute_in_parallel(symbols, reference, temp_table_name, exchanges)
    data = fetch_combined_analysis_data(temp_table_name)
    # delete_temp_table(temp_table_name)
    return jsonify(data)


@app.route('/api/get-exchange-list', methods=['GET'])
def get_exchange_list():
    data = exchange_list()
    return jsonify(data)


@app.route('/api/update-exchange-status', methods=['POST'])
def update_exchange_status():
    data = request.json
    exchange_id = data['exchangeId']
    status = data['status']
    update_status(exchange_id, status)
    return "Success", 200


@app.route('/api/get-symbol-list', methods=['GET'])
def get_symbol_list():
    data = symbol_list()
    return jsonify(data)


@app.route('/api/delete-symbol', methods=['POST'])
def delete_symbol():
    data = request.json
    symbol_id = data['symbolId']
    delete_record(symbol_id)
    return "Success", 200


@app.route('/api/add-symbol', methods=['POST'])
def add_symbol():
    data = request.json
    symbol_name = data['symbolName']
    insert_record(symbol_name)
    return "Success", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
