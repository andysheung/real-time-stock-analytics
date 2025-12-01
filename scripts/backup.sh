#!/bin/bash
# Backup Script for Production Data

set -e

BACKUP_DIR="backups/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"

echo "Creating backup in $BACKUP_DIR"

# Backup MinIO data
echo "Backing up MinIO data..."
docker exec minio mc mirror /data "$BACKUP_DIR/minio" || echo "MinIO backup skipped"

# Backup HDFS data
echo "Backing up HDFS data..."
docker exec namenode hdfs dfs -get /stock-data "$BACKUP_DIR/hdfs" || echo "HDFS backup skipped"

# Backup PostgreSQL (Airflow metadata)
echo "Backing up PostgreSQL..."
docker exec postgres pg_dump -U airflow airflow > "$BACKUP_DIR/airflow_db.sql" || echo "PostgreSQL backup skipped"

# Backup configuration files
echo "Backing up configuration..."
cp -r config "$BACKUP_DIR/" || true

# Backup checkpoints
echo "Backing up Spark checkpoints..."
cp -r checkpoints "$BACKUP_DIR/" 2>/dev/null || echo "No checkpoints to backup"

# Create backup manifest
cat > "$BACKUP_DIR/manifest.txt" << EOF
Backup created: $(date)
Environment: ${ENVIRONMENT:-production}
Services:
  - Kafka
  - MinIO
  - HDFS
  - Airflow
  - Trino
  - Prometheus
  - Grafana
EOF

echo "Backup completed: $BACKUP_DIR"
