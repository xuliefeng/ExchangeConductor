import time
from concurrent.futures import ThreadPoolExecutor, wait, ProcessPoolExecutor

from flask import Flask, jsonify, request
from flask_cors import CORS

from config.logger_config import setup_logger
from data_analysis.trade_analysis import fetch_combined_analysis_data
from data_collection.mod16_la_token_collector import la_token
from data_collection.mod17_coinex_collector import coinex
from data_collection.mod9_ascend_ex_collector import ascend_ex
from data_collection.mod14_bigone_collector import bigone
from data_collection.mod2_binance_collector import binance
from data_collection.mod4_bit_get_collector import bit_get
from data_collection.mod13_bit_mark_collector import bit_mark
from data_collection.mod7_bit_venus_collector import bit_venus
from data_collection.mod5_bitfinex_collector import bitfinex
from data_collection.mod10_bybit_collector import bybit
from data_collection.mod8_deep_coin_collector import deep_coin
from data_collection.mod12_hitbtc_collector import hitbtc
from data_collection.mod3_huobi_collector import huobi
from data_collection.mod15_jubi_collector import jubi
from data_collection.mod6_mexc_collector import mexc
from data_collection.mod1_okx_collector import okx
from data_collection.mod11_xt_collector import xt
from data_collection_proxy.mod1_gate_io_collector import gate_io
from data_collection_proxy.mod2_coin_w_collector import coin_w
from data_collection_proxy.mod3_bi_ka_collector import bi_ka
from data_collection_proxy.mod4_hot_coin_collector import hot_coin
from data_collection_proxy.mod5_digi_finex_collector import digi_finex
from data_collection_proxy.mod6_l_bank_collector import l_bank
from data_collection_proxy.mod7_bing_x_collector import bing_x
from data_collection_proxy.mod8_probit_collector import probit

from database.db_service import get_symbols, create_temp_table, get_reference_price, \
    get_usd_to_cny_rate, delete_temp_table
from web_interaction.exchange import exchange_list, update_status, exchange_list_used
from web_interaction.exclusion import load_exclusion_list, exclusion_list, delete_exclusion_record, \
    insert_exclusion_record
from web_interaction.reference import reference_list
from web_interaction.symbol import symbol_list, delete_record, insert_record
from web_interaction.user import get_all_users, add_user_into_db, update_user_info, delete_user_record

app = Flask(__name__)
CORS(app)
logger = setup_logger("app", "log/app.log")

exchange_functions = {
    'okx': okx,
    'binance': binance,
    'huobi': huobi,
    'bitget': bit_get,
    'bitfinex': bitfinex,
    'mexc': mexc,
    'bitvenus': bit_venus,
    'deepcoin': deep_coin,
    'ascendex': ascend_ex,
    'bybit': bybit,
    'xt': xt,
    'hitbtc': hitbtc,
    'bitmark': bit_mark,
    'bigone': bigone,
    'jubi': jubi,
    'latoken': la_token,
    'coinex': coinex,
    'gateio': gate_io,
    'coinw': coin_w,
    'bika': bi_ka,
    'hotcoin': hot_coin,
    'digifinex': digi_finex,
    'lbank': l_bank,
    'bingx': bing_x,
    'probit': probit
}

special_exchanges = ['gateio', 'coinw', 'bika', 'hotcoin', 'digifinex', 'lbank', 'bingx', 'probit']


def execute_in_parallel(temp_table_name, exchanges):
    with ThreadPoolExecutor() as thread_executor, ProcessPoolExecutor() as process_executor:

        futures = [
            thread_executor.submit(get_reference_price),
            thread_executor.submit(get_usd_to_cny_rate)
        ]

        for item in exchanges:
            exchange_name = item[1]

            if exchange_name in exchange_functions:
                if exchange_name in special_exchanges:
                    futures.append(process_executor.submit(exchange_functions[exchange_name], temp_table_name))
                else:
                    futures.append(thread_executor.submit(exchange_functions[exchange_name], temp_table_name))

        wait(futures)


@app.route('/api/get-analysis-data', methods=['GET'])
def get_analysis_data():
    start_time = time.time()

    exchanges = exchange_list_used()
    temp_table_name = create_temp_table()
    load_exclusion_list()

    execute_in_parallel(temp_table_name, exchanges)

    data = fetch_combined_analysis_data(temp_table_name)
    # delete_temp_table(temp_table_name)

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    logger.info(f"-------------------------------------------------- all executed in {elapsed_time} seconds.")

    return jsonify(data)


@app.route('/api/get-exchange-list', methods=['GET'])
def get_exchange_list():
    data = exchange_list()
    return jsonify(data)


@app.route('/api/get-exclusion-list', methods=['GET'])
def get_exclusion_list():
    data = exclusion_list()
    return jsonify(data)


@app.route('/api/get-symbol-list', methods=['GET'])
def get_symbol_list():
    data = symbol_list()
    return jsonify(data)


@app.route('/api/get-reference-list', methods=['GET'])
def get_reference_list():
    data = reference_list()
    return jsonify(data)


@app.route('/api/get-user-list', methods=['GET'])
def get_user_list():
    data = get_all_users()
    return jsonify(data)


@app.route('/api/add-user', methods=['POST'])
def add_user():
    data = request.json
    data = add_user_into_db(data)
    return jsonify(data)


@app.route('/api/update-user', methods=['POST'])
def update_user():
    data = request.json
    update_user_info(data)
    return "Success", 200


@app.route('/api/delete-user', methods=['POST'])
def delete_user():
    data = request.json
    user_id = data['userId']
    delete_user_record(user_id)
    return "Success", 200


@app.route('/api/update-exchange-status', methods=['POST'])
def update_exchange_status():
    data = request.json
    exchange_id = data['exchangeId']
    status = data['status']
    update_status(exchange_id, status)
    return "Success", 200


@app.route('/api/delete-symbol', methods=['POST'])
def delete_symbol():
    data = request.json
    symbol_id = data['symbolId']
    delete_record(symbol_id)
    return "Success", 200


@app.route('/api/delete-exclusion', methods=['POST'])
def delete_exclusion():
    data = request.json
    exclusion_id = data['exclusionId']
    delete_exclusion_record(exclusion_id)
    return "Success", 200


@app.route('/api/add-symbol', methods=['POST'])
def add_symbol():
    data = request.json
    symbol_name = data['symbolName']
    insert_record(symbol_name)
    return "Success", 200


@app.route('/api/add-exclusion', methods=['POST'])
def add_exclusion():
    data = request.json
    insert_exclusion_record(data)
    return "Success", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
