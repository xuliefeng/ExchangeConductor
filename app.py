import json

from multiprocessing import Process
from flask import Flask, jsonify, request, make_response
from flask_cors import CORS
from config.logger_config import setup_logger
from config.redis_config import RedisConfig
from data_depth.mod10_bybit_depth import bybit
from data_depth.mod11_xt_depth import xt
from data_depth.mod12_hitbtc_depth import hitbtc
from data_depth.mod13_bit_mark_depth import bit_mark
from data_depth.mod14_bigone_depth import bigone
from data_depth.mod15_jubi_depth import jubi
from data_depth.mod16_la_token_depth import la_token
from data_depth.mod17_coinex_depth import coinex
from data_depth.mod18_gate_io_depth import gate_io
from data_depth.mod19_coin_w_depth import coin_w
from data_depth.mod1_okx_depth import okx
from data_depth.mod20_bi_ka_depth import bi_ka
from data_depth.mod21_hot_coin_depth import hot_coin
from data_depth.mod22_digi_finex_depth import digi_finex
from data_depth.mod23_l_bank_depth import l_bank
from data_depth.mod24_bing_x_depth import bing_x
from data_depth.mod25_probit_depth import probit
from data_depth.mod2_binance_depth import binance
from data_depth.mod3_huobi_depth import huobi
from data_depth.mod4_bit_get_depth import bit_get
from data_depth.mod5_bitfinex_depth import bitfinex
from data_depth.mod6_mexc_depth import mexc
from data_depth.mod7_bit_venus_depth import bit_venus
from data_depth.mod8_deep_coin_depth import deep_coin
from data_depth.mod9_ascend_ex_depth import ascend_ex
from database.db_service import usd_to_cny_rate
from task.task_core import run_task
from task.task_midnight import schedule_midnight_task
from task.task_ten_minute import schedule_ten_minute_task
from web_interaction.exchange import exchange_list, update_status
from web_interaction.exclusion import exclusion_list, delete_exclusion_record, insert_exclusion_record
from web_interaction.reference import reference_list
from web_interaction.symbol import symbol_list, delete_record, insert_record
from web_interaction.user import get_all_users, add_user_into_db, update_user_info, delete_user_record, user_login

app = Flask(__name__)
CORS(app)
logger = setup_logger("app", "log/app.log")
redis_config = RedisConfig()


@app.route('/api/get-analysis-data', methods=['GET'])
def get_analysis_data():
    data = redis_config.get_data('result')
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


@app.route('/api/update-exchange-status', methods=['POST'])
def update_exchange_status():
    data = request.json
    exchange_id = data['exchangeId']
    status = data['status']
    update_status(exchange_id, status)
    return "Success", 200


@app.route('/api/add-exclusion', methods=['POST'])
def add_exclusion():
    data = request.json
    insert_exclusion_record(data)
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


@app.route('/api/delete-symbol', methods=['POST'])
def delete_symbol():
    data = request.json
    symbol_id = data['symbolId']
    delete_record(symbol_id)
    return "Success", 200


@app.route('/api/add-user', methods=['POST'])
def add_user():
    data = request.json
    data = add_user_into_db(data)
    return jsonify(data)


@app.route('/api/delete-user', methods=['POST'])
def delete_user():
    data = request.json
    user_id = data['userId']
    delete_user_record(user_id)
    return "Success", 200


@app.route('/api/update-user', methods=['POST'])
def update_user():
    data = request.json
    update_user_info(data)
    return "Success", 200


@app.route('/api/login', methods=['POST'])
def login():
    data = request.json
    user_name = data['username']
    input_password = data['password']

    success, message, user = user_login(user_name, input_password)
    if success:
        del user[2]
        response_data = {
            "status": "success",
            "message": "登陆成功",
            "user": user
        }
        response = make_response(json.dumps(response_data, ensure_ascii=False), 200)
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response
    else:
        return jsonify({"status": "failure", "message": message}), 401


@app.route('/api/get-rate', methods=['GET'])
def get_rate():
    data = usd_to_cny_rate()
    return jsonify(data)


@app.route('/api/get-depth', methods=['POST'])
def get_depth():
    data = request.json
    exchange_name = data['exchange']
    symbol_name = data['symbol']
    reference = data['reference']
    if exchange_name == 'okx':
        return jsonify(okx(symbol_name, reference))
    if exchange_name == 'binance':
        return jsonify(binance(symbol_name, reference))
    if exchange_name == 'huobi':
        return jsonify(huobi(symbol_name, reference))
    if exchange_name == 'bitget':
        return jsonify(bit_get(symbol_name, reference))
    if exchange_name == 'bitfinex':
        return jsonify(bitfinex(symbol_name, reference))
    if exchange_name == 'mexc':
        return jsonify(mexc(symbol_name, reference))
    if exchange_name == 'bitvenus':
        return jsonify(bit_venus(symbol_name, reference))
    if exchange_name == 'deepcoin':
        return jsonify(deep_coin(symbol_name, reference))
    if exchange_name == 'ascendex':
        return jsonify(ascend_ex(symbol_name, reference))
    if exchange_name == 'bybit':
        return jsonify(bybit(symbol_name, reference))
    if exchange_name == 'xt':
        return jsonify(xt(symbol_name, reference))
    if exchange_name == 'hitbtc':
        return jsonify(hitbtc(symbol_name, reference))
    if exchange_name == 'bitmark':
        return jsonify(bit_mark(symbol_name, reference))
    if exchange_name == 'bigone':
        return jsonify(bigone(symbol_name, reference))
    if exchange_name == 'jubi':
        return jsonify(jubi(symbol_name, reference))
    if exchange_name == 'latoken':
        return jsonify(la_token(symbol_name, reference))
    if exchange_name == 'coinex':
        return jsonify(coinex(symbol_name, reference))
    if exchange_name == 'gateio':
        return jsonify(gate_io(symbol_name, reference))
    if exchange_name == 'coinw':
        return jsonify(coin_w(symbol_name, reference))
    if exchange_name == 'bika':
        return jsonify(bi_ka(symbol_name, reference))
    if exchange_name == 'hotcoin':
        return jsonify(hot_coin(symbol_name, reference))
    if exchange_name == 'digifinex':
        return jsonify(digi_finex(symbol_name, reference))
    if exchange_name == 'lbank':
        return jsonify(l_bank(symbol_name, reference))
    if exchange_name == 'bingx':
        return jsonify(bing_x(symbol_name, reference))
    if exchange_name == 'probit':
        return jsonify(probit(symbol_name, reference))


if __name__ == "__main__":
    p = Process(target=run_task, args=(10,))
    p.start()
    p2 = Process(target=schedule_midnight_task)
    p2.start()
    p3 = Process(target=schedule_ten_minute_task)
    p3.start()
    app.run(host="0.0.0.0", port=8080, debug=True, use_reloader=False)
