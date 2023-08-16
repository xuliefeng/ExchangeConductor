# import asyncio
# import random
# import time
#
# import httpx
#
# from data_processing.kraken_processor import insert_to_db, filter_symbols
# from proxy_handler.proxy_loader import load_proxies_from_file
#
# proxies = load_proxies_from_file()
#
#
# def select_proxy():
#     return random.choice(proxies)
#
#
# def kraken(symbols, reference):
#     start_time = time.time()
#     url = "https://api.kraken.com/0/public/AssetPairs"
#     data = asyncio.run(kraken_tickers(url, select_proxy()))
#
#     if data:
#         data = data['result']
#         found_records = filter_symbols(symbols, data)
#         result = asyncio.run(kraken_depth(found_records))
#         insert_to_db(result, reference)
#     else:
#         print("Failed to get tickers from kraken")
#
#     end_time = time.time()
#     elapsed_time = round(end_time - start_time, 3)
#     print(
#         f"---------------------------------------------------------------------------------------------------- kraken executed in {elapsed_time} seconds.")
#
#
# async def kraken_tickers(url, proxy):
#     async with httpx.AsyncClient(proxies=proxy, verify=False, timeout=10) as client:
#         response = await client.get(url)
#         return response.json() if response.status_code == 200 else None
#
#
# async def kraken_depth(found_records):
#     url = "https://api.kraken.com/0/public/Depth?count=1&pair="
#     tasks = [fetch(symbol, url) for symbol in found_records]
#     results = await asyncio.gather(*tasks)
#     return {symbol: result for symbol, result in results if result}
#
#
# async def fetch(symbol, url):
#     proxy = select_proxy()
#     async with httpx.AsyncClient(proxies=proxy, verify=False, timeout=10) as client:
#         response = await client.get(url + symbol)
#         if response.status_code == 200:
#             return symbol, response.json()
#         else:
#             print(f"Request failed {symbol} status code {response.status_code} - kraken")
#             return symbol, None
