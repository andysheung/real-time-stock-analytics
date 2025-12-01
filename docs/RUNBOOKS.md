# Operational Runbooks

This document contains operational procedures for the Real-Time Stock Analytics Platform.

## Table of Contents

1. [Service Restart Procedures](#service-restart-procedures)
2. [Troubleshooting Common Issues](#troubleshooting-common-issues)
3. [Data Recovery Procedures](#data-recovery-procedures)
4. [Scaling Operations](#scaling-operations)
5. [Maintenance Windows](#maintenance-windows)

## Service Restart Procedures

### Restart All Services

```bash
# Graceful shutdown
docker-compose down --timeout 30

# Start services
docker-compose up -d

# Verify health
./scripts/health_check.sh
```

### Restart Individual Service

```bash
# Restart Kafka
docker-compose restart kafka

# Restart Spark streaming (if running)
# Stop and restart the streaming application
pkill -f streaming_app.py
python src/streaming/streaming_app.py &
```

## Troubleshooting Common Issues

### Kafka Issues

**Problem**: Kafka broker not responding

**Solution**:
```bash
# Check Kafka logs
docker logs kafka

# Restart Kafka
docker-compose restart kafka

# Verify topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

**Problem**: High consumer lag

**Solution**:
1. Check consumer group status
2. Scale consumers if needed
3. Check for processing bottlenecks
4. Review Spark streaming performance

### Spark Streaming Issues

**Problem**: Streaming job stopped

**Solution**:
```bash
# Check Spark logs
tail -f logs/spark-streaming.log

# Check checkpoint directory
ls -la checkpoints/

# Restart streaming job
python src/streaming/streaming_app.py
```

**Problem**: Checkpoint corruption

**Solution**:
```bash
# Backup corrupted checkpoint
mv checkpoints/prices checkpoints/prices.backup

# Restart with fresh checkpoint
# Note: Will reprocess from latest offset
python src/streaming/streaming_app.py
```

### HDFS Issues

**Problem**: NameNode in safe mode

**Solution**:
```bash
# Leave safe mode
docker exec namenode hdfs dfsadmin -safemode leave

# Check DataNode status
docker exec namenode hdfs dfsadmin -report
```

**Problem**: DataNode not connecting

**Solution**:
```bash
# Check DataNode logs
docker logs datanode1
docker logs datanode2

# Restart DataNode
docker-compose restart datanode1
```

### Airflow Issues

**Problem**: DAG not running

**Solution**:
1. Check DAG status in Airflow UI
2. Review task logs
3. Check scheduler logs: `docker logs airflow-scheduler`
4. Verify DAG file syntax
5. Unpause DAG if paused

**Problem**: Task failures

**Solution**:
1. Check task logs in Airflow UI
2. Review error messages
3. Check data availability
4. Verify dependencies
5. Retry failed tasks

### Trino Issues

**Problem**: Query timeout

**Solution**:
1. Check query complexity
2. Review query plan: `EXPLAIN SELECT ...`
3. Optimize query (add filters, limit results)
4. Increase query timeout in config

**Problem**: Cannot connect to Iceberg tables

**Solution**:
1. Verify MinIO is accessible
2. Check catalog configuration
3. Verify table exists
4. Check permissions

### Storage Issues

**Problem**: MinIO disk full

**Solution**:
```bash
# Check disk usage
docker exec minio df -h

# Clean up old data (if applicable)
# Use lifecycle policies for automatic cleanup

# Scale storage if needed
```

**Problem**: HDFS storage full

**Solution**:
```bash
# Check HDFS usage
docker exec namenode hdfs dfs -df -h

# Clean up old data
docker exec namenode hdfs dfs -rm -r /old-data

# Add more DataNodes if needed
```

## Data Recovery Procedures

### Restore from Backup

```bash
# List available backups
ls -la backups/

# Restore MinIO data
docker exec minio mc mirror backups/YYYYMMDD_HHMMSS/minio /data

# Restore HDFS data
docker exec namenode hdfs dfs -put backups/YYYYMMDD_HHMMSS/hdfs/* /stock-data/

# Restore Airflow database
docker exec -i postgres psql -U airflow airflow < backups/YYYYMMDD_HHMMSS/airflow_db.sql
```

### Replay Kafka Messages

```bash
# Reset consumer group offset to earliest
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group stock-analytics-group \
  --reset-offsets \
  --to-earliest \
  --topic stock-prices \
  --execute
```

### Recover Spark Checkpoint

```bash
# If checkpoint is corrupted, remove it
rm -rf checkpoints/prices

# Restart streaming (will start from latest Kafka offset)
python src/streaming/streaming_app.py
```

## Scaling Operations

### Scale Kafka

```bash
# Add more Kafka brokers to docker-compose.yml
# Update broker IDs and ports
# Restart services
docker-compose up -d

# Rebalance partitions
docker exec kafka kafka-reassign-partitions \
  --bootstrap-server localhost:9092 \
  --reassignment-json-file reassign.json \
  --execute
```

### Scale Spark

```yaml
# Update spark_config.yaml
spark:
  executor:
    instances: 5  # Increase executors
    cores: 4      # Increase cores per executor
    memory: "8g"  # Increase memory
```

### Scale HDFS

```bash
# Add more DataNodes to docker-compose.yml
# Start new DataNodes
docker-compose up -d datanode3

# Verify DataNodes are registered
docker exec namenode hdfs dfsadmin -report
```

### Scale Trino

```bash
# Add more Trino workers to docker-compose.yml
docker-compose up -d trino-worker2

# Verify workers are registered
curl http://localhost:8081/v1/node
```

## Maintenance Windows

### Planned Maintenance Procedure

1. **Notify Users**: Send maintenance notification
2. **Backup Data**: Run backup script
3. **Stop Services**: Graceful shutdown
4. **Perform Maintenance**: Updates, patches, etc.
5. **Start Services**: Bring services back online
6. **Verify Health**: Run health checks
7. **Notify Completion**: Send completion notification

### Zero-Downtime Updates

For zero-downtime updates:

1. **Rolling Updates**: Update services one at a time
2. **Health Checks**: Verify each service before proceeding
3. **Load Balancing**: Use multiple instances
4. **Blue-Green Deployment**: Deploy new version alongside old

## Monitoring and Alerts

### Key Metrics to Monitor

- Service uptime
- Data ingestion rate
- Processing latency
- Storage utilization
- Error rates
- Consumer lag

### Alert Response

1. **Acknowledge Alert**: Confirm receipt
2. **Investigate**: Check logs and metrics
3. **Assess Impact**: Determine severity
4. **Take Action**: Apply fix or escalate
5. **Document**: Record incident and resolution

## Emergency Procedures

### Complete System Failure

1. **Assess Damage**: Check all services
2. **Restore from Backup**: Use latest backup
3. **Restart Services**: Bring services online
4. **Verify Data Integrity**: Check data consistency
5. **Resume Operations**: Start data ingestion

### Data Corruption

1. **Stop Processing**: Halt data ingestion
2. **Identify Corruption**: Locate affected data
3. **Restore Backup**: Restore from known good backup
4. **Replay Data**: Reprocess from Kafka if needed
5. **Resume Operations**: Restart processing

## Contact Information

- **On-Call Engineer**: Check rotation schedule
- **Escalation**: Contact team lead
- **Emergency**: Use incident response channel

## Appendix

### Useful Commands

```bash
# View all logs
docker-compose logs -f

# View specific service logs
docker logs kafka -f

# Check service status
docker-compose ps

# Execute command in container
docker exec -it kafka bash

# Check resource usage
docker stats
```
