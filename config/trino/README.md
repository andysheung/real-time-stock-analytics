# Trino Configuration

This directory contains Trino configuration files for the query engine.

## Files

### `config.properties`
Main Trino configuration:
- Coordinator settings
- Memory limits
- Discovery URI

### `node.properties`
Node-specific configuration:
- Environment name
- Data directory

### `jvm.config`
JVM settings for Trino:
- Memory allocation
- GC settings
- Heap dump configuration

### `catalog/iceberg.properties`
Iceberg catalog configuration:
- Connector: Iceberg
- Catalog type: Hadoop
- Warehouse location: S3/MinIO
- S3 endpoint and credentials

### `catalog/hive.properties`
Hive catalog configuration (optional):
- Connector: Hive
- Metastore URI
- S3 configuration

## Catalog Usage

### Iceberg Catalog
```sql
-- Use Iceberg catalog
USE iceberg.stock_analytics;

-- Query Iceberg tables
SELECT * FROM stock_prices LIMIT 10;
```

### Catalog Structure
```
iceberg
└── stock_analytics
    ├── stock_prices
    ├── stock_volumes
    └── daily_aggregates
```

## Configuration Updates

To modify Trino configuration:
1. Edit the appropriate `.properties` file
2. Restart Trino services:
   ```bash
   docker-compose restart trino-coordinator trino-worker
   ```

## Troubleshooting

### Connection Issues
- Verify Trino is running: `docker ps | grep trino`
- Check logs: `docker logs trino-coordinator`
- Verify MinIO is accessible from Trino containers

### Catalog Issues
- Ensure catalog properties files are in `catalog/` directory
- Check S3/MinIO credentials match MinIO configuration
- Verify warehouse path exists in MinIO

### Performance Issues
- Adjust memory settings in `config.properties`
- Monitor query execution in Trino Web UI
- Check partition filters are being used
