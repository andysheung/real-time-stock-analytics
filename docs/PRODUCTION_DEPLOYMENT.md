# Production Deployment Guide

This guide provides step-by-step instructions for deploying the Real-Time Stock Analytics Platform to production.

## Prerequisites

### Infrastructure Requirements

- **Compute**: 
  - Minimum: 8 CPU cores, 32GB RAM
  - Recommended: 16+ CPU cores, 64GB+ RAM
- **Storage**: 
  - Minimum: 500GB
  - Recommended: 2TB+ with expansion capability
- **Network**: 
  - High bandwidth (1Gbps+)
  - Low latency between services

### Software Requirements

- Docker 20.10+
- Docker Compose 2.0+
- Python 3.9+
- Git

### Security Requirements

- SSL/TLS certificates
- Secrets management system
- Firewall rules configured
- VPN access (if remote)

## Pre-Deployment Checklist

- [ ] Infrastructure provisioned and tested
- [ ] Security configurations reviewed
- [ ] SSL certificates obtained
- [ ] Secrets configured in secrets manager
- [ ] Network access configured
- [ ] Backup strategy defined
- [ ] Monitoring and alerting configured
- [ ] Runbooks reviewed
- [ ] Team trained on operations

## Deployment Steps

### 1. Environment Setup

```bash
# Clone repository
git clone <repository-url>
cd real-time-stock-analytics

# Checkout production branch/tag
git checkout production

# Create environment file
cp .env.example .env.production
# Edit .env.production with production values
```

### 2. Configuration Updates

Update configuration files for production:

**docker-compose.yml:**
- Update resource limits
- Configure production networks
- Set up SSL/TLS
- Configure secrets

**config/spark_config.yaml:**
- Set production Spark settings
- Configure production storage paths
- Update memory and CPU settings

**config/kafka_config.yaml:**
- Enable SASL authentication
- Configure SSL/TLS
- Set production retention policies

### 3. Secrets Management

```bash
# Store secrets in secrets manager (example with environment variables)
export KAFKA_PASSWORD=$(vault kv get -field=password secret/kafka)
export MINIO_ACCESS_KEY=$(vault kv get -field=access_key secret/minio)
export MINIO_SECRET_KEY=$(vault kv get -field=secret_key secret/minio)
export POSTGRES_PASSWORD=$(vault kv get -field=password secret/postgres)
```

### 4. Deploy Infrastructure

```bash
# Run deployment script
chmod +x scripts/deploy.sh
ENVIRONMENT=production ./scripts/deploy.sh
```

### 5. Initialize Services

```bash
# Wait for services to be ready
sleep 60

# Verify all services are healthy
./scripts/health_check.sh

# Initialize Kafka topics (if not auto-created)
docker exec kafka kafka-topics --create \
  --topic stock-prices \
  --bootstrap-server localhost:9092 \
  --partitions 6 \
  --replication-factor 3

# Initialize MinIO buckets (if not auto-created)
docker exec minio-setup mc mb myminio/stock-data-raw
docker exec minio-setup mc mb myminio/stock-data-lakehouse

# Initialize HDFS directories
docker exec namenode hdfs dfs -mkdir -p /stock-data/raw
docker exec namenode hdfs dfs -mkdir -p /stock-data/lakehouse

# Initialize Iceberg tables
python src/lakehouse/iceberg_tables.py --table all
```

### 6. Start Data Processing

```bash
# Start Kafka producer
nohup python src/ingestion/kafka_producer.py > logs/producer.log 2>&1 &

# Start Spark streaming
nohup python src/streaming/streaming_app.py > logs/streaming.log 2>&1 &

# Verify Airflow DAGs are running
# Check Airflow UI: http://localhost:8080
```

### 7. Verify Deployment

```bash
# Run comprehensive health checks
./scripts/health_check.sh

# Check data flow
# 1. Verify Kafka messages
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic stock-prices \
  --from-beginning \
  --max-messages 10

# 2. Verify MinIO data
docker exec minio mc ls myminio/stock-data-raw/

# 3. Verify HDFS data
docker exec namenode hdfs dfs -ls /stock-data/raw/

# 4. Verify Trino queries
docker exec trino-coordinator trino \
  --server http://localhost:8081 \
  --execute "SELECT COUNT(*) FROM iceberg.stock_analytics.stock_prices"
```

## Post-Deployment

### Monitoring Setup

1. **Verify Prometheus**: http://localhost:9090
2. **Verify Grafana**: http://localhost:3000
3. **Check Dashboards**: Verify all dashboards are loading
4. **Test Alerts**: Verify alerting is working

### Documentation

1. **Update Runbooks**: Document any environment-specific procedures
2. **Update Contact Info**: Ensure on-call information is current
3. **Document Configurations**: Record all production settings

## Scaling for Production

### Horizontal Scaling

**Kafka:**
- Add more brokers
- Increase partition count
- Distribute load

**Spark:**
- Increase executor count
- Use dynamic allocation
- Scale based on workload

**Trino:**
- Add more workers
- Distribute queries
- Monitor query queue

### Vertical Scaling

- Increase memory for services
- Add CPU cores
- Upgrade storage (SSD)

## Backup Strategy

### Automated Backups

```bash
# Set up cron job for daily backups
0 2 * * * /path/to/scripts/backup.sh

# Backup retention: 30 days
# Backup location: Off-site storage
```

### Backup Verification

- Test restore procedures monthly
- Verify backup integrity
- Document restore procedures

## Disaster Recovery

### Recovery Time Objective (RTO)
- Target: < 4 hours
- Critical services: < 1 hour

### Recovery Point Objective (RPO)
- Target: < 1 hour data loss
- Critical data: < 15 minutes

### DR Procedures

1. **Failover**: Switch to backup site
2. **Restore**: Restore from backups
3. **Verify**: Validate data integrity
4. **Resume**: Restart operations

## Maintenance Windows

### Scheduled Maintenance

- **Frequency**: Monthly
- **Duration**: 2-4 hours
- **Notification**: 1 week advance notice

### Emergency Maintenance

- **Process**: Follow incident response procedure
- **Communication**: Notify stakeholders immediately
- **Documentation**: Document all changes

## Performance Tuning

See [PERFORMANCE_TUNING.md](PERFORMANCE_TUNING.md) for detailed performance optimization guidelines.

## Security Hardening

See [SECURITY.md](SECURITY.md) for security configuration and best practices.

## Troubleshooting

See [RUNBOOKS.md](RUNBOOKS.md) for operational procedures and troubleshooting guides.

## Support

- **Documentation**: Check docs/ directory
- **Runbooks**: See RUNBOOKS.md
- **On-Call**: Check rotation schedule
- **Escalation**: Contact team lead
