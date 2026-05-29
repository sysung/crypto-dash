{{ config(
    materialized='incremental',
    unique_key=['symbol', 'analytical_minute']
) }}

WITH raw_stream AS (
    -- Pull directly from the Vault table ClickHouse is filling
    SELECT 
        symbol,
        price,
        volume,
        timestamp,
        trade_time
    FROM default.crypto_trades_raw
    {% if is_incremental() %}
        -- Only process new trades that have arrived since the last run
        WHERE trade_time > (SELECT max(analytical_minute) FROM {{ this }})
    {% endif %}
)

SELECT
    symbol,
    -- Round the exact second down to the nearest minute to group trades together
    toStartOfMinute(trade_time) AS analytical_minute,
    
    -- Calculate our core business metrics
    avg(price) AS average_price_usd,
    sum(volume) AS total_volume,
    count(*) AS transaction_count,
    
    -- Capture the highest and lowest price within that minute
    max(price) AS high_price,
    min(price) AS low_price

FROM raw_stream
GROUP BY 
    symbol, 
    analytical_minute
ORDER BY 
    analytical_minute DESC