import os

from symbols_collection.mod9_ascend_ex import ascend_ex
from symbols_collection.mod14_bigone import bigone
from symbols_collection.mod2_binance import binance
from symbols_collection.mod4_bit_get import bit_get
from symbols_collection.mod13_bit_mart import bit_mart
from symbols_collection.mod7_bit_venus import bit_venus
from symbols_collection.mod5_bitfinex import bitfinex
from symbols_collection.mod10_bybit import bybit
from symbols_collection.mod8_deep_coin import deep_coin
from symbols_collection.mod12_hitbtc import hitbtc
from symbols_collection.mod3_huobi import huobi
from symbols_collection.mod15_jubi import jubi
from symbols_collection.mod6_mexc import mexc
from symbols_collection.mod1_okx import okx
from symbols_collection.mod11_xt import xt

okx()
binance()
huobi()
bit_get()
bitfinex()
mexc()
bit_venus()
deep_coin()
ascend_ex()
bybit()
xt()
hitbtc()
bit_mart()
bigone()
jubi()

current_directory = os.getcwd()
all_files = os.listdir(current_directory)
for file_name in all_files:
    if file_name.endswith('.txt'):
        os.remove(os.path.join(current_directory, file_name))
print("done")
