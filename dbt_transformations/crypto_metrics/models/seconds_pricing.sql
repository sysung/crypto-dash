{{ config(
    materialized='incremental',
    unique_key=['symbol', 'analytical_second']
) }}

WITH raw_stream AS (
    SELECT 
        symbol,
        price,
        volume,
        timestamp,
        trade_time
    FROM default.crypto_trades_raw
    {% if is_incremental() %}
        -- Only process new trades that have arrived since the last run
        WHERE trade_time > (SELECT max(analytical_second) FROM {{ this }})
    {% endif %}
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