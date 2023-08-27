from psycopg2.extras import DictCursor

from database.db_pool import get_connection, release_connection


def fetch_combined_analysis_data(temp_table_name):
    connection = get_connection()
    cursor = connection.cursor(cursor_factory=DictCursor)

    sql_script = f"""
WITH Combined AS (
    SELECT 
        b.symbol_name, 
        b.bid AS max_bid, 
        b.bid_size AS bid_size,
        a.ask AS min_ask, 
        a.ask_size AS ask_size,
        b.exchange_name AS max_bid_exchange, 
        a.exchange_name AS min_ask_exchange,
        CASE WHEN a.ask = 0 THEN NULL ELSE ROUND((((b.bid - a.ask) / a.ask) * 100), 2) END AS price_diff_percentage,
        ROW_NUMBER() OVER(PARTITION BY b.symbol_name ORDER BY 
            CASE WHEN a.ask = 0 THEN NULL ELSE (b.bid - a.ask) / a.ask END DESC) AS rn
    FROM {temp_table_name} b
    JOIN {temp_table_name} a ON b.symbol_name = a.symbol_name
    WHERE b.exchange_name <> a.exchange_name
    AND b.bid > a.ask
    ),ExtendedResults AS (
    SELECT 
        c.*,
        r.price AS reference_price,
        u.rate
    FROM Combined c
    JOIN reference r ON split_part(c.symbol_name, '-', 2) = r.symbol_name
    JOIN usd_to_cny_rate u ON 1 = 1
    WHERE c.price_diff_percentage >= 1
)
SELECT 
    symbol_name,
    price_diff_percentage,
    min_ask_exchange,
    max_bid_exchange,
    min_ask,
    max_bid,
    ask_size,
    bid_size,
    ROUND(COALESCE(min_ask * ask_size * reference_price, 0), 2) AS total_ask_amount,
    ROUND(COALESCE(max_bid * bid_size * reference_price, 0), 2) AS total_bid_amount,
    ROUND(COALESCE(min_ask * ask_size * reference_price * rate, 0), 2) AS total_ask_amount_cny,
    ROUND(COALESCE(max_bid * bid_size * reference_price * rate, 0), 2) AS total_bid_amount_cny
FROM ExtendedResults
-- WHERE rn = 1
ORDER BY price_diff_percentage DESC;
"""
    cursor.execute(sql_script)
    result = cursor.fetchall()
    release_connection(connection)

    return result
