import asyncio
import json
import time
import httpx
from selenium.webdriver.chrome.options import Options
from seleniumwire import webdriver

from config.logger_config import setup_logger
from data_processing_proxy.pending_bit_get_processor import filter_symbols, insert_to_db
from proxy_handler.proxy_loader import ProxyRotator

rotator = ProxyRotator()
logger = setup_logger("bit_get_collector", "log/app.log")


def bit_get(symbols, temp_table_name, reference):
    start_time = time.time()
    data = asyncio.run(bit_get_symbols())
    data = data['data']
    if data:
        found_records = filter_symbols(symbols, data)
        result = asyncio.run(bit_get_depth(found_records))
        insert_to_db(result, temp_table_name, reference)
    else:
        logger.error("Failed to get tickers from bit_get")

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    logger.info(f"-------------------------------------------------- bit_get executed in {elapsed_time} seconds.")


async def bit_get_symbols():
    chrome_opts = Options()
    # chrome_opts.add_argument("--headless")  # 暂时注释掉，以便进行调试
    chrome_opts.add_argument("--disable-extensions")
    chrome_opts.add_argument("--disable-webrtc")
    chrome_opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_opts.add_experimental_option('useAutomationExtension', False)
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--disable-dev-shm-usage")
    chrome_opts.add_argument("--disable-cache")
    chrome_opts.add_argument("--disable-application-cache")
    chrome_opts.add_argument("--disable-offline-load-stale-cache")
    chrome_opts.add_argument("--disk-cache-size=0")

    chrome_opts.add_argument("--ignore-certificate-errors")
    chrome_opts.add_argument('--single-process')
    chrome_opts.add_argument('--disable-dev-shm-usage')
    chrome_opts.add_argument("--start-maximized")
    chrome_opts.add_argument('--auto-open-devtools-for-tabs')
    chrome_opts.add_argument('--log-level=2')
    chrome_opts.add_argument('--disable-features=IsolateOrigins,site-per-process')
    chrome_opts.add_argument("--ignore_ssl")
    chrome_opts.add_argument('--ignore-ssl-errors')
    chrome_opts.add_argument('--ignore-certificate-errors')
    chrome_opts.add_argument('--allow-insecure-localhost')
    chrome_opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
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
        driver.get("https://api.bitget.com/api/mix/v1/market/tickers?productType=umcbl")
        json_content = driver.page_source
        return json.loads(json_content)
    finally:
        driver.quit()


async def bit_get_depth(found_records):
    url = "https://api.bitget.com/api/mix/v1/market/depth?limit=15&symbol="
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
            logger.info(f"Request failed {symbol} status code {response.status_code} - bit_get")
            return symbol, None
