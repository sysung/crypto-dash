-- ============================================================================
-- 3. CLICKHOUSE PRE-AGGREGATION & STATEFUL ANALYTICS LAYER
-- ============================================================================

-- ============================================================================
-- PART 1: MOMENTUM VIEWS (REAL-TIME OHLCV AGGREGATIONS)
-- ============================================================================

-- A. 1-Minute OHLCV Target Table
CREATE TABLE IF NOT EXISTS crypto_ohlcv_1m (
    symbol String,
    window_start DateTime,
    open SimpleAggregateFunction(any, Float64),
    high SimpleAggregateFunction(max, Float64),
    low SimpleAggregateFunction(min, Float64),
    close SimpleAggregateFunction(anyLast, Float64),
    volume SimpleAggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (symbol, window_start);

-- B. 1-Minute Materialized View Pump
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_crypto_ohlcv_1m TO crypto_ohlcv_1m AS
SELECT
    symbol,
    toStartOfMinute(server_time) AS window_start,
    any(price) AS open,
    max(price) AS high,
    min(price) AS low,
    anyLast(price) AS close,
    sum(volume_24h) AS volume
FROM crypto_ticks_raw
GROUP BY symbol, window_start;

-- C. 5-Minute OHLCV Target Table
CREATE TABLE IF NOT EXISTS crypto_ohlcv_5m (
    symbol String,
    window_start DateTime,
    open SimpleAggregateFunction(any, Float64),
    high SimpleAggregateFunction(max, Float64),
    low SimpleAggregateFunction(min, Float64),
    close SimpleAggregateFunction(anyLast, Float64),
    volume SimpleAggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (symbol, window_start);

-- D. 5-Minute Materialized View Pump
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_crypto_ohlcv_5m TO crypto_ohlcv_5m AS
SELECT
    symbol,
    toStartOfFiveMinutes(server_time) AS window_start,
    any(price) AS open,
    max(price) AS high,
    min(price) AS low,
    anyLast(price) AS close,
    sum(volume_24h) AS volume
FROM crypto_ticks_raw
GROUP BY symbol, window_start;

-- E. 24-Hour (1-Day) OHLCV Target Table
CREATE TABLE IF NOT EXISTS crypto_ohlcv_24h (
    symbol String,
    window_start DateTime,
    open SimpleAggregateFunction(any, Float64),
    high SimpleAggregateFunction(max, Float64),
    low SimpleAggregateFunction(min, Float64),
    close SimpleAggregateFunction(anyLast, Float64),
    volume SimpleAggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (symbol, window_start);

-- F. 24-Hour Materialized View Pump
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_crypto_ohlcv_24h TO crypto_ohlcv_24h AS
SELECT
    symbol,
    toStartOfDay(server_time) AS window_start,
    any(price) AS open,
    max(price) AS high,
    min(price) AS low,
    anyLast(price) AS close,
    sum(volume_24h) AS volume
FROM crypto_ticks_raw
GROUP BY symbol, window_start;

-- G. Real-time Volume Spike Detection View
-- Computes the ratio of current 5-minute volume relative to a rolling 30-day average baseline.
CREATE OR REPLACE VIEW view_volume_spikes AS
SELECT
    symbol,
    window_start,
    volume,
    avg(volume) OVER (
        PARTITION BY symbol 
        ORDER BY window_start 
        ROWS BETWEEN 8640 PRECEDING AND 1 PRECEDING -- 30 days of 5-minute intervals (30 * 24 * 12 = 8640)
    ) AS rolling_30d_avg_volume,
    if(rolling_30d_avg_volume > 0, round(volume / rolling_30d_avg_volume, 4), 1.0) AS volume_spike_ratio
FROM crypto_ohlcv_5m;


-- ============================================================================
-- PART 2: RISK & VOLATILITY VIEWS
-- ============================================================================

-- A. Stateful Volatility & Maximum Drawdown View
-- Computes rolling 30-day volatility (stddev of log returns) and Max Drawdown over hourly closes.
CREATE OR REPLACE VIEW view_risk_and_volatility AS
WITH hourly_prices AS (
    SELECT
        symbol,
        toStartOfHour(server_time) AS window_start,
        anyLast(price) AS close
    FROM crypto_ticks_raw
    GROUP BY symbol, window_start
),
log_returns AS (
    SELECT
        symbol,
        window_start,
        close,
        lagInFrame(close, 1) OVER (PARTITION BY symbol ORDER BY window_start) AS prev_close,
        if(prev_close > 0, log(close / prev_close), 0.0) AS log_return
    FROM hourly_prices
),
peaks AS (
    SELECT
        symbol,
        window_start,
        close,
        log_return,
        max(close) OVER (
            PARTITION BY symbol 
            ORDER BY window_start 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_peak
    FROM log_returns
)
SELECT
    symbol,
    window_start,
    close,
    -- Hourly volatility over 30 days (30 * 24 = 720 hours)
    stddevSamp(log_return) OVER (
        PARTITION BY symbol 
        ORDER BY window_start 
        ROWS BETWEEN 720 PRECEDING AND CURRENT ROW
    ) AS rolling_30d_volatility_hourly,
    -- Drop from peak percentage
    if(cumulative_peak > 0, (close - cumulative_peak) / cumulative_peak, 0.0) AS current_drawdown,
    -- Maximum peak-to-trough drop over 30 days
    min(current_drawdown) OVER (
        PARTITION BY symbol 
        ORDER BY window_start 
        ROWS BETWEEN 720 PRECEDING AND CURRENT ROW
    ) AS rolling_30d_max_drawdown
FROM peaks;

-- B. Altcoin Beta View (vs BTC-USD)
-- Computes covariance and variance over rolling 24-hour windows to calculate Beta relative to BTC.
CREATE OR REPLACE VIEW view_altcoin_beta AS
WITH hourly_returns AS (
    SELECT
        symbol,
        toStartOfHour(server_time) AS window_start,
        anyLast(price) AS close
    FROM crypto_ticks_raw
    GROUP BY symbol, window_start
),
log_returns AS (
    SELECT
        symbol,
        window_start,
        close,
        lagInFrame(close, 1) OVER (PARTITION BY symbol ORDER BY window_start) AS prev_close,
        if(prev_close > 0, log(close / prev_close), 0.0) AS log_return
    FROM hourly_returns
),
aligned_btc AS (
    SELECT
        a.symbol AS symbol,
        a.window_start AS window_start,
        a.log_return AS r_alt,
        b.log_return AS r_btc
    FROM log_returns a
    INNER JOIN log_returns b ON a.window_start = b.window_start
    WHERE b.symbol = 'BTC-USD' AND a.symbol != 'BTC-USD'
)
SELECT
    symbol,
    window_start,
    r_alt,
    r_btc,
    covarSamp(r_alt, r_btc) OVER (
        PARTITION BY symbol 
        ORDER BY window_start 
        ROWS BETWEEN 24 PRECEDING AND CURRENT ROW
    ) AS covariance_24h,
    varSamp(r_btc) OVER (
        PARTITION BY symbol 
        ORDER BY window_start 
        ROWS BETWEEN 24 PRECEDING AND CURRENT ROW
    ) AS variance_btc_24h,
    if(variance_btc_24h > 0, covariance_24h / variance_btc_24h, 1.0) AS hourly_beta_24h
FROM aligned_btc;


-- ============================================================================
-- PART 3: STATIC METADATA SETUP
-- ============================================================================

CREATE TABLE IF NOT EXISTS token_metadata (
    symbol String,
    name String,
    utility String,
    consensus_mechanism String,
    security_checklist String,
    launched_year UInt16,
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
ORDER BY symbol;

-- Pre-populate asset metrics for our RAG context
INSERT INTO token_metadata (symbol, name, utility, consensus_mechanism, security_checklist, launched_year) VALUES
('BTC-USD', 'Bitcoin', 'Decentralized digital currency / Store of value', 'Proof of Work (PoW)', 'No smart contract vulnerabilities, highly secure, global miner hash rate protection.', 2009),
('ETH-USD', 'Ethereum', 'Smart contract platform gas asset', 'Proof of Stake (PoS)', 'Vetted contract execution environment, complex network validators, highly secure.', 2015),
('USDT-USD', 'Tether', 'Fiat-collateralized stablecoin pegged to USD', 'Pegged Asset', 'Centralized issuer risk, custodial reserves verification checklist standard.', 2014),
('BNB-USD', 'Binance Coin', 'Utility token for transaction fee discounts', 'Proof of Staked Authority (PoSA)', 'Exchange ecosystem dependency, active validator group governance.', 2017),
('XRP-USD', 'Ripple', 'Global real-time settlement and liquidity network', 'Ripple Protocol Consensus Algorithm (RPCA)', 'Highly centralized validator node pool, high speed settlement utility.', 2012),
('USDC-USD', 'USD Coin', 'Regulated fiat-collateralized stablecoin pegged to USD', 'Pegged Asset', 'Regulated issuer (Circle), fully-backed transparent audit reserves checklist.', 2018),
('SOL-USD', 'Solana', 'Ultra-fast smart contract execution platform', 'Proof of History (PoH) / Proof of Stake (PoS)', 'Parallel transaction execution risk, historical node uptime security checklist.', 2020),
('DOGE-USD', 'Dogecoin', 'Peer-to-peer meme-based cryptocurrency', 'Proof of Work (PoW)', 'Merge-mined with Litecoin, inflationary supply economics, high liquidity.', 2013);
