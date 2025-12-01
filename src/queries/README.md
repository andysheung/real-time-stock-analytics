# Trino Query Examples

This directory contains SQL query examples for Trino to query the Iceberg data lakehouse.

## Accessing Trino

### Web UI
- **URL**: http://localhost:8081
- **Default user**: `admin` (no password required for local setup)

### CLI Client
```bash
# Using Trino CLI (if installed)
trino --server http://localhost:8081 --user admin

# Or using Docker
docker exec -it trino-coordinator trino --server http://localhost:8081 --user admin
```

### Python Client
```python
from trino.dbapi import connect

conn = connect(
    host='localhost',
    port=8081,
    user='admin',
    catalog='iceberg',
    schema='stock_analytics'
)

cur = conn.cursor()
cur.execute("SELECT * FROM stock_prices LIMIT 10")
rows = cur.fetchall()
```

## Query Examples

See `stock_analytics_queries.sql` for comprehensive query examples including:

### Basic Queries
- Latest prices
- Daily aggregates
- Symbol-specific queries

### Aggregation Queries
- Average prices by symbol
- Top symbols by volume
- Statistical aggregations

### Time Series Analysis
- Price trends
- Moving averages
- Historical comparisons

### Comparative Analysis
- Cross-symbol comparisons
- Correlation analysis
- Volatility metrics

### Volume Analysis
- Volume distributions
- Unusual volume detection
- Volume trends

### Time Travel Queries
- Historical snapshots
- Point-in-time comparisons
- Version queries

## Catalog Configuration

Trino is configured with the following catalogs:

### `iceberg` Catalog
- **Type**: Iceberg
- **Warehouse**: `s3a://stock-data-lakehouse/iceberg-warehouse`
- **Tables**: `stock_prices`, `stock_volumes`, `daily_aggregates`

### `hive` Catalog (Optional)
- **Type**: Hive Metastore
- **Use**: For legacy Hive tables if needed

## Query Optimization Tips

1. **Use Partition Filters**: Always filter by partition columns (`symbol`, `date`) when possible
2. **Limit Result Sets**: Use `LIMIT` for exploratory queries
3. **Aggregate Early**: Push aggregations down when possible
4. **Use Time Travel Sparingly**: Time travel queries can be slower
5. **Monitor Query Plans**: Use `EXPLAIN` to understand query execution

## Example Usage

```sql
-- Connect to iceberg catalog
USE iceberg.stock_analytics;

-- Query latest prices
SELECT * FROM stock_prices 
WHERE symbol = 'AAPL' 
    AND date = CURRENT_DATE
ORDER BY timestamp DESC
LIMIT 10;

-- Get daily aggregates
SELECT * FROM daily_aggregates
WHERE symbol = 'AAPL'
    AND date >= CURRENT_DATE - INTERVAL '7' DAY
ORDER BY date DESC;
```

## Performance Tuning

### Memory Configuration
- Query max memory: 5GB
- Query max memory per node: 2GB
- Adjust in `config/trino/config.properties` if needed

### Partitioning Strategy
Tables are partitioned by:
- `symbol` (identity partition)
- `date` (day partition)

Always include partition filters in WHERE clauses for optimal performance.

### Query Monitoring
Monitor queries via:
- Trino Web UI: http://localhost:8081
- Query history and statistics
- Resource usage metrics
