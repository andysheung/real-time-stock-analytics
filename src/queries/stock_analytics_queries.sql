-- Stock Analytics Queries for Trino
-- These queries demonstrate analytical capabilities on the Iceberg data lakehouse

-- ============================================================================
-- Basic Queries
-- ============================================================================

-- Get latest prices for all symbols
SELECT 
    symbol,
    price,
    timestamp,
    price_change,
    price_change_pct
FROM iceberg.stock_analytics.stock_prices
WHERE date = CURRENT_DATE
ORDER BY timestamp DESC
LIMIT 100;

-- Get daily aggregates for a specific symbol
SELECT 
    symbol,
    date,
    avg_price,
    max_price,
    min_price,
    total_volume,
    price_range,
    price_range_pct
FROM iceberg.stock_analytics.daily_aggregates
WHERE symbol = 'AAPL'
ORDER BY date DESC
LIMIT 30;

-- ============================================================================
-- Aggregation Queries
-- ============================================================================

-- Average price by symbol for the last 7 days
SELECT 
    symbol,
    AVG(avg_price) as avg_daily_price,
    MAX(max_price) as max_price_7d,
    MIN(min_price) as min_price_7d,
    SUM(total_volume) as total_volume_7d
FROM iceberg.stock_analytics.daily_aggregates
WHERE date >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY symbol
ORDER BY avg_daily_price DESC;

-- Top 5 symbols by volume for today
SELECT 
    symbol,
    SUM(total_volume) as total_volume
FROM iceberg.stock_analytics.daily_aggregates
WHERE date = CURRENT_DATE
GROUP BY symbol
ORDER BY total_volume DESC
LIMIT 5;

-- ============================================================================
-- Time Series Analysis
-- ============================================================================

-- Price trend over time for a symbol
SELECT 
    date,
    avg_price,
    max_price,
    min_price,
    price_range,
    total_volume
FROM iceberg.stock_analytics.daily_aggregates
WHERE symbol = 'AAPL'
    AND date >= CURRENT_DATE - INTERVAL '30' DAY
ORDER BY date ASC;

-- Moving average (7-day) for a symbol
SELECT 
    date,
    avg_price,
    AVG(avg_price) OVER (
        ORDER BY date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as moving_avg_7d
FROM iceberg.stock_analytics.daily_aggregates
WHERE symbol = 'AAPL'
    AND date >= CURRENT_DATE - INTERVAL '30' DAY
ORDER BY date ASC;

-- ============================================================================
-- Comparative Analysis
-- ============================================================================

-- Compare price changes across symbols
SELECT 
    symbol,
    AVG(avg_price_change_pct) as avg_daily_change_pct,
    STDDEV(avg_price_change_pct) as volatility,
    MAX(avg_price_change_pct) as max_daily_change_pct,
    MIN(avg_price_change_pct) as min_daily_change_pct
FROM iceberg.stock_analytics.daily_aggregates
WHERE date >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY symbol
ORDER BY volatility DESC;

-- Correlation between symbols (example: AAPL vs MSFT)
WITH aapl_data AS (
    SELECT date, avg_price as aapl_price
    FROM iceberg.stock_analytics.daily_aggregates
    WHERE symbol = 'AAPL'
        AND date >= CURRENT_DATE - INTERVAL '30' DAY
),
msft_data AS (
    SELECT date, avg_price as msft_price
    FROM iceberg.stock_analytics.daily_aggregates
    WHERE symbol = 'MSFT'
        AND date >= CURRENT_DATE - INTERVAL '30' DAY
)
SELECT 
    CORR(aapl_data.aapl_price, msft_data.msft_price) as price_correlation
FROM aapl_data
JOIN msft_data ON aapl_data.date = msft_data.date;

-- ============================================================================
-- Volume Analysis
-- ============================================================================

-- Volume distribution by symbol
SELECT 
    symbol,
    COUNT(*) as trading_days,
    AVG(total_volume) as avg_daily_volume,
    MAX(total_volume) as max_daily_volume,
    MIN(total_volume) as min_daily_volume,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_volume) as median_volume
FROM iceberg.stock_analytics.daily_aggregates
WHERE date >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY symbol
ORDER BY avg_daily_volume DESC;

-- Days with unusually high volume (> 2x average)
WITH volume_stats AS (
    SELECT 
        symbol,
        AVG(total_volume) as avg_volume,
        STDDEV(total_volume) as stddev_volume
    FROM iceberg.stock_analytics.daily_aggregates
    WHERE date >= CURRENT_DATE - INTERVAL '30' DAY
    GROUP BY symbol
)
SELECT 
    d.date,
    d.symbol,
    d.total_volume,
    v.avg_volume,
    d.total_volume / v.avg_volume as volume_multiplier
FROM iceberg.stock_analytics.daily_aggregates d
JOIN volume_stats v ON d.symbol = v.symbol
WHERE d.date >= CURRENT_DATE - INTERVAL '7' DAY
    AND d.total_volume > v.avg_volume * 2
ORDER BY volume_multiplier DESC;

-- ============================================================================
-- Time Travel Queries (Iceberg-specific)
-- ============================================================================

-- Query historical data at a specific timestamp
SELECT 
    symbol,
    price,
    timestamp
FROM iceberg.stock_analytics.stock_prices FOR TIMESTAMP AS OF TIMESTAMP '2025-11-30 10:00:00'
WHERE symbol = 'AAPL'
ORDER BY timestamp DESC
LIMIT 100;

-- Compare current vs historical snapshot
SELECT 
    current.symbol,
    current.avg_price as current_price,
    historical.avg_price as historical_price,
    current.avg_price - historical.avg_price as price_change
FROM iceberg.stock_analytics.daily_aggregates current
JOIN iceberg.stock_analytics.daily_aggregates FOR TIMESTAMP AS OF TIMESTAMP '2025-11-29 00:00:00' historical
    ON current.symbol = historical.symbol 
    AND current.date = historical.date
WHERE current.date = CURRENT_DATE;

-- ============================================================================
-- Performance Optimization Examples
-- ============================================================================

-- Partition pruning: Query uses partition filters for optimal performance
SELECT 
    symbol,
    date,
    avg_price,
    total_volume
FROM iceberg.stock_analytics.daily_aggregates
WHERE symbol = 'AAPL'  -- Partition filter
    AND date >= CURRENT_DATE - INTERVAL '7' DAY  -- Partition filter
ORDER BY date DESC;

-- Aggregation pushdown: Aggregations computed at storage level
SELECT 
    symbol,
    COUNT(*) as record_count,
    AVG(avg_price) as overall_avg_price,
    SUM(total_volume) as total_volume_all_time
FROM iceberg.stock_analytics.daily_aggregates
WHERE date >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY symbol
ORDER BY total_volume_all_time DESC;
