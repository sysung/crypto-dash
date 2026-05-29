-- 1. Create the permanent Vault
CREATE TABLE IF NOT EXISTS crypto_trades_raw (
    symbol String,
    price Float64,
    volume Float64,
    timestamp Int64,
    trade_time DateTime MATERIALIZED toDateTime(timestamp / 1000)
) ENGINE = MergeTree()
ORDER BY (symbol, trade_time);

-- 2. Create the Pipe to Kafka
CREATE TABLE IF NOT EXISTS kafka_crypto_trades (
    symbol String,
    price Float64,
    volume Float64,
    timestamp Int64
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092',
         kafka_topic_list = 'raw_crypto_trades',
         kafka_group_name = 'clickhouse-consumer-group',
         kafka_format = 'JSONEachRow';

-- 3. Create the automated Pump
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_crypto_trades_raw TO crypto_trades_raw AS
SELECT * FROM kafka_crypto_trades;