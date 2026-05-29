{{ config(
    materialized='table'
) }}

WITH raw_stream AS (
    SELECT 
        symbol,
        price,
        volume,
        timestamp,
        trade_time
    FROM default.crypto_trades_raw
)

SELECT
    symbol,
    -- Since trade_time is a standard DateTime, it is already grouped by the second!
    trade_time AS analytical_second,
    
    avg(price) AS average_price_usd,
    sum(volume) AS total_volume,
    count(*) AS transaction_count,
    
    max(price) AS high_price,
    min(price) AS low_price

FROM raw_stream
GROUP BY 
    symbol, 
    analytical_second
ORDER BY 
    analytical_second DESC