-- ============================================================================
-- 2. L2 ORDER BOOK DEPTH PIPELINE (raw_crypto_l2)
-- ============================================================================

-- Permanent columnar analytics vault for Order Book L2 Depth
CREATE TABLE IF NOT EXISTS crypto_l2_raw (
    event_type String,
    symbol String,
    side String,
    price Float64,
    volume Float64,
    event_time String,
    sequence_num Int64,
    timestamp String,
    trade_time DateTime64(6) MATERIALIZED parseDateTime64BestEffort(event_time),
    server_time DateTime64(9) MATERIALIZED parseDateTime64BestEffort(timestamp),
    ingest_time DateTime MATERIALIZED now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(trade_time)
ORDER BY (symbol, side, price, trade_time);

-- Virtual queue consumer connecting to raw_crypto_l2
CREATE TABLE IF NOT EXISTS kafka_crypto_l2 (
    event_type String,
    symbol String,
    side String,
    price Float64,
    volume Float64,
    event_time String,
    sequence_num Int64,
    timestamp String
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092',
         kafka_topic_list = 'raw_crypto_l2',
         kafka_group_name = 'clickhouse-l2-consumer-group',
         kafka_format = 'JSONEachRow',
         kafka_skip_broken_messages = 100;

-- Materialized View Pump
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_crypto_l2_raw TO crypto_l2_raw AS
SELECT 
    event_type,
    symbol,
    side,
    price,
    volume,
    event_time,
    sequence_num,
    timestamp
FROM kafka_crypto_l2;
