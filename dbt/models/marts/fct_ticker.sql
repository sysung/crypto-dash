{{ config(
    materialized='incremental',
    engine='MergeTree()',
    order_by=['symbol', 'server_time', 'sequence_num'],
    incremental_strategy='append'
) }}

select
    symbol,
    price,
    volume_24h,
    low_24h,
    high_24h,
    low_52w,
    high_52w,
    price_percent_chg_24h,
    best_bid,
    best_ask,
    best_bid_quantity,
    best_ask_quantity,
    sequence_num,
    timestamp,
    server_time
from {{ source('default', 'crypto_ticks_raw') }}

{% if is_incremental() %}
    where server_time >= (select max(server_time) from {{ this }}) - interval 1 hour
{% endif %}
