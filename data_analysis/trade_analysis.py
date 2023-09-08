from psycopg2.extras import DictCursor

from database.db_pool import get_connection, release_connection


def fetch_combined_analysis_data(temp_table_name):
    connection = get_connection()
    cursor = connection.cursor(cursor_factory=DictCursor)

    sql_script = f"""
WITH AdjustedPrices AS (
    SELECT 
        t.*,
        COALESCE(r.price, 1) AS price,
        u.rate
    FROM {temp_table_name} t
    LEFT JOIN reference r ON t.reference = r.symbol_name
    LEFT JOIN usd_to_cny_rate u ON 1 = 1
),
Combined AS (
    SELECT 
        b.symbol_name,
        b.bid AS max_bid,
        b.bid_size AS bid_size,
        a.ask AS min_ask,
        a.ask_size AS ask_size,
        b.exchange_name AS max_bid_exchange,
        a.exchange_name AS min_ask_exchange,
        b.reference bid_reference,
        a.reference ask_reference,
        b.price bid_reference_price,
        a.price ask_reference_price,
        b.rate,
        CASE WHEN a.ask = 0 THEN NULL ELSE ROUND((((b.bid * b.price - a.ask * a.price) / (a.ask * a.price)) * 100), 2) END AS price_diff_percentage,
        ROW_NUMBER() OVER(PARTITION BY b.symbol_name ORDER BY 
            CASE WHEN a.ask = 0 THEN NULL ELSE (b.bid * b.price - a.ask * a.price) / (a.ask * a.price) END DESC) AS rn
    FROM AdjustedPrices b
    JOIN AdjustedPrices a ON b.symbol_name = a.symbol_name
    WHERE b.exchange_name <> a.exchange_name
    AND (b.bid * b.price) > (a.ask * a.price)
    )
SELECT 
    symbol_name,
    price_diff_percentage,
    min_ask_exchange,
    max_bid_exchange,
    min_ask,
    ask_reference,
    max_bid,
    bid_reference,
    ask_size,
    bid_size,
--     ROUND(COALESCE(min_ask * ask_size * ask_reference_price, 0), 2) AS total_ask_amount,
--     ROUND(COALESCE(max_bid * bid_size * bid_reference_price, 0), 2) AS total_bid_amount,
    ROUND(COALESCE(min_ask * ask_size * ask_reference_price * rate, 0), 2) AS total_ask_amount_cny,
    ROUND(COALESCE(max_bid * bid_size * bid_reference_price * rate, 0), 2) AS total_bid_amount_cny
FROM Combined
WHERE price_diff_percentage IS NOT NULL 
-- AND rn = 1
ORDER BY price_diff_percentage DESC;
"""
    cursor.execute(sql_script)
    result = cursor.fetchall()
    release_connection(connection)

    return result
