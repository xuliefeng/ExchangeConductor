import asyncio
import json
import time
from concurrent.futures import ThreadPoolExecutor

import httpx

from config.logger_config import setup_logger
from data_processing_proxy.pending_kraken_processor import filter_symbols, insert_to_db
from proxy_handler.proxy_loader import ProxyRotator
from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options


rotator = ProxyRotator()
logger = setup_logger("kraken_collector", "log/app.log")


def kraken(symbols, temp_table_name):
    start_time = time.time()
    data = asyncio.run(kraken_symbols())
    data = data['result']
    if data:
        found_records = filter_symbols(symbols, data)
        result = asyncio.run(kraken_depth(found_records))
        insert_to_db(result, temp_table_name)
    else:
        logger.error("Failed to get tickers from kraken")

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    logger.info(f"-------------------------------------------------- kraken executed in {elapsed_time} seconds.")


executor = ThreadPoolExecutor()


def fetch_with_selenium(proxy):
    chrome_opts = Options()
    # chrome_opts.add_argument("--headless")  # 暂时注释掉，以便进行调试
    chrome_opts.add_argument("--ignore-certificate-errors")
    chrome_opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

    # 设置代理
    selenium_wire_options = {
        'verify_ssl': False,
        'proxy': {
            'http': 'http://o2xyEg6WcPDM74M:9cztIoIrxXHJuJh@195.210.122.134:41563',
            'https': 'http://o2xyEg6WcPDM74M:9cztIoIrxXHJuJh@195.210.122.134:41563'
        }
    }

    driver = webdriver.Chrome(options=chrome_opts, seleniumwire_options=selenium_wire_options)

    try:
        driver.get("https://api.kraken.com/0/public/AssetPairs")
        json_content = driver.page_source
        print(driver.page_source)
        return json.loads(json_content)
    finally:
        driver.quit()



async def kraken_symbols():
    proxy = rotator.get_next_proxy()
    loop = asyncio.get_running_loop()
    data = await loop.run_in_executor(executor, fetch_with_selenium, proxy)
    return data


async def kraken_depth(found_records):
    url = "https://api.kraken.com/0/public/Depth?count=20&pair="
    tasks = [fetch(symbol, url) for symbol in found_records]
    results = await asyncio.gather(*tasks)
    return {symbol: result for symbol, result in results if result}


async def fetch(symbol, url):
    proxy = rotator.get_next_proxy()
    async with httpx.AsyncClient(proxies=proxy, verify=False, timeout=10) as client:
        response = await client.get(url + symbol)
        if response.status_code == 200:
            return symbol, response.json()
        else:
            logger.info(f"Request failed {symbol} status code {response.status_code} - kraken")
            return symbol, None
