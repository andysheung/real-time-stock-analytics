# Performance Tuning Guide

This guide provides recommendations for optimizing the performance of the Real-Time Stock Analytics Platform.

## Overview

Performance optimization involves tuning multiple layers:
- Data ingestion
- Stream processing
- Storage systems
- Query engines
- Network and infrastructure

## Kafka Performance Tuning

### Producer Configuration

```python
# Optimize for throughput
producer_config = {
    'batch_size': 32768,  # Increase batch size
    'linger_ms': 10,      # Wait for batching
    'compression_type': 'snappy',  # Enable compression
    'acks': 1,  # Faster writes (use 'all' for durability)
    'buffer_memory': 67108864,  # 64MB buffer
}
```

### Consumer Configuration

```python
# Optimize for throughput
consumer_config = {
    'fetch_min_bytes': 1048576,  # 1MB minimum fetch
    'fetch_max_wait_ms': 500,
    'max_partition_fetch_bytes': 10485760,  # 10MB per partition
    'enable_auto_commit': False,  # Manual commit for better control
}
```

### Topic Configuration

```bash
# Create topics with optimized settings
kafka-topics --create \
  --topic stock-prices \
  --partitions 6 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config segment.bytes=1073741824
```

## Spark Performance Tuning

### Spark Configuration

```yaml
# spark_config.yaml (optimized)
spark:
  session:
    sql:
      adaptive:
        enabled: true
        coalescePartitions:
          enabled: true
      shuffle:
        partitions: 200
    streaming:
      minBatchesToRetain: 100
      checkpointLocation: "s3a://checkpoints/"
  
  # Memory settings
  executor:
    memory: "4g"
    memoryFraction: 0.8
    cores: 2
  
  # Serialization
  serializer: "org.apache.spark.serializer.KryoSerializer"
  
  # Dynamic allocation
  dynamicAllocation:
    enabled: true
    minExecutors: 2
    maxExecutors: 10
```

### Streaming Optimization

1. **Checkpointing**: Use reliable checkpoint locations (S3/HDFS)
2. **Windowing**: Choose appropriate window sizes
3. **Partitioning**: Partition data by key columns
4. **Caching**: Cache frequently accessed DataFrames
5. **Broadcast Variables**: Use for small lookup tables

### Batch Job Optimization

1. **Partition Pruning**: Always filter by partition columns
2. **Predicate Pushdown**: Use WHERE clauses effectively
3. **Column Pruning**: Select only needed columns
4. **Join Optimization**: Use broadcast joins for small tables
5. **Skew Handling**: Handle data skew in joins

## MinIO/S3 Performance

### Client Configuration

```python
# Optimize S3 client
s3_config = {
    'multipart_threshold': 8388608,  # 8MB
    'multipart_chunksize': 8388608,  # 8MB chunks
    'max_concurrency': 10,
    'use_threads': True,
}
```

### Bucket Configuration

- Enable versioning for data protection
- Configure lifecycle policies
- Use appropriate storage classes
- Enable transfer acceleration if available

## HDFS Performance

### Configuration Tuning

```xml
<!-- hdfs-site.xml -->
<property>
  <name>dfs.block.size</name>
  <value>134217728</value>  <!-- 128MB -->
</property>

<property>
  <name>dfs.replication</name>
  <value>3</value>
</property>

<property>
  <name>dfs.namenode.handler.count</name>
  <value>100</value>
</property>
```

### Optimization Tips

1. **Block Size**: Use 128MB or larger for large files
2. **Replication**: Balance between durability and storage
3. **Rack Awareness**: Configure for data locality
4. **Compression**: Use Snappy or LZ4 for fast compression

## Trino Query Optimization

### Query Best Practices

1. **Partition Filters**: Always include partition columns in WHERE
2. **Limit Results**: Use LIMIT for exploratory queries
3. **Aggregations**: Push aggregations down when possible
4. **Join Order**: Join smaller tables first
5. **Avoid SELECT ***: Select only needed columns

### Configuration Tuning

```properties
# config/trino/config.properties
query.max-memory=10GB
query.max-memory-per-node=4GB
query.max-run-time=1h
task.concurrency=4
task.max-worker-threads=8
```

## Iceberg Table Optimization

### Regular Maintenance

```bash
# Compact tables weekly
python src/lakehouse/table_optimization.py --table all --operation compact

# Expire old snapshots daily
python src/lakehouse/table_optimization.py --table all --operation expire
```

### Partitioning Strategy

- Partition by frequently filtered columns
- Use appropriate partition granularity (day vs hour)
- Avoid over-partitioning (too many small files)

## Monitoring Performance

### Key Metrics to Monitor

**Kafka:**
- Message throughput (messages/sec)
- Consumer lag
- Broker CPU and memory
- Disk I/O

**Spark:**
- Processing rate (records/sec)
- Batch processing time
- Executor utilization
- Shuffle read/write

**Storage:**
- Read/write throughput
- Storage utilization
- Latency percentiles

**Trino:**
- Query execution time
- Memory usage
- CPU utilization
- Query queue length

## Capacity Planning

### Resource Estimation

**Kafka:**
- Disk: ~1GB per 1M messages (with replication)
- Memory: 4-8GB per broker
- CPU: 2-4 cores per broker

**Spark:**
- Memory: 4-8GB per executor
- CPU: 2-4 cores per executor
- Disk: 10-20GB for shuffle

**Storage:**
- Estimate data growth rate
- Plan for 3x replication (HDFS)
- Reserve 20% overhead

## Performance Testing

### Load Testing

```bash
# Kafka producer performance test
kafka-producer-perf-test \
  --topic stock-prices \
  --num-records 1000000 \
  --record-size 1024 \
  --throughput 10000

# Spark streaming benchmark
spark-submit \
  --class StreamingBenchmark \
  --master yarn \
  streaming-benchmark.jar
```

### Benchmarking Queries

```sql
-- Measure query performance
EXPLAIN ANALYZE
SELECT * FROM iceberg.stock_analytics.stock_prices
WHERE symbol = 'AAPL' AND date = CURRENT_DATE;
```

## Troubleshooting Performance Issues

### Common Issues

1. **High Latency**: Check network, increase parallelism
2. **Memory Issues**: Tune memory settings, check for leaks
3. **Disk I/O**: Optimize storage, use SSDs
4. **CPU Bottleneck**: Scale horizontally, optimize queries
5. **Data Skew**: Repartition data, use broadcast joins

### Performance Profiling

- Use Spark UI for streaming jobs
- Use Trino query plan analysis
- Monitor Prometheus metrics
- Analyze Grafana dashboards

## Best Practices

1. **Start Small**: Begin with conservative settings
2. **Monitor First**: Establish baseline metrics
3. **Iterate**: Tune based on actual workload
4. **Test Changes**: Benchmark before production
5. **Document**: Keep track of configuration changes
