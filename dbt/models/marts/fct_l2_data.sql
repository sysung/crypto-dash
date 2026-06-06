{{ config(
    materialized='incremental',
    engine='MergeTree()',
    order_by=['symbol', 'side', 'price', 'trade_time', 'sequence_num'],
    incremental_strategy='append'
) }}

select
    event_type,
    symbol,
    side,
    price,
    volume,
    event_time,
    sequence_num,
    timestamp,
    trade_time,
    server_time,
    ingest_time
from {{ source('default', 'crypto_l2_raw') }}

{% if is_incremental() %}
    where server_time >= (select max(server_time) from {{ this }}) - interval 1 hour
{% endif %}
