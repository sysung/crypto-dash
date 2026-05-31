-- ============================================================================
-- 1. TICKS TICKER INGESTION PIPELINE (raw_crypto_trades)
-- ============================================================================

-- Permanent columnar analytics vault for Ticks Ticker
CREATE TABLE IF NOT EXISTS crypto_ticks_raw (
    symbol String,
    price Float64,
    volume_24h Float64,
    low_24h Float64,
    high_24h Float64,
    low_52w Float64,
    high_52w Float64,
    price_percent_chg_24h Float64,
    best_bid Float64,
    best_ask Float64,
    best_bid_quantity Float64,
    best_ask_quantity Float64,
    sequence_num Int64,
    timestamp String,
    server_time DateTime64(9) MATERIALIZED parseDateTime64BestEffort(timestamp)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(server_time)
ORDER BY (symbol, server_time);

-- Virtual queue consumer connecting to raw_crypto_trades
CREATE TABLE IF NOT EXISTS kafka_crypto_ticks (
    symbol String,
    price Float64,
    volume_24h Float64,
    low_24h Float64,
    high_24h Float64,
    low_52w Float64,
    high_52w Float64,
    price_percent_chg_24h Float64,
    best_bid Float64,
    best_ask Float64,
    best_bid_quantity Float64,
    best_ask_quantity Float64,
    sequence_num Int64,
    timestamp String
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092',
         kafka_topic_list = 'raw_crypto_trades',
         kafka_group_name = 'clickhouse-trades-consumer-group',
         kafka_format = 'JSONEachRow',
         kafka_skip_broken_messages = 100;

-- Materialized View Pump
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_crypto_ticks_raw TO crypto_ticks_raw AS
SELECT 
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
    timestamp
FROM kafka_crypto_ticks;
