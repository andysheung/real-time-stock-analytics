#!/bin/bash
# Comprehensive Health Check Script

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "Running comprehensive health checks..."

# Check Docker services
check_service() {
    local service=$1
    local check_cmd=$2
    
    if eval "$check_cmd" &> /dev/null; then
        echo -e "${GREEN}✓ $service is healthy${NC}"
        return 0
    else
        echo -e "${RED}✗ $service health check failed${NC}"
        return 1
    fi
}

# Kafka
check_service "Kafka" "docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092"

# MinIO
check_service "MinIO" "curl -f http://localhost:9000/minio/health/live"

# HDFS NameNode
check_service "HDFS NameNode" "curl -f http://localhost:9870"

# HDFS DataNodes
DATANODE_COUNT=$(docker ps | grep datanode | wc -l)
if [ "$DATANODE_COUNT" -ge 2 ]; then
    echo -e "${GREEN}✓ HDFS DataNodes: $DATANODE_COUNT running${NC}"
else
    echo -e "${RED}✗ HDFS DataNodes: Only $DATANODE_COUNT running (expected at least 2)${NC}"
fi

# PostgreSQL
check_service "PostgreSQL" "docker exec postgres pg_isready -U airflow"

# Airflow
check_service "Airflow" "curl -f http://localhost:8080/health"

# Trino
check_service "Trino" "curl -f http://localhost:8081/v1/info"

# Prometheus
check_service "Prometheus" "curl -f http://localhost:9090/-/healthy"

# Grafana
check_service "Grafana" "curl -f http://localhost:3000/api/health"

# Check disk space
DISK_USAGE=$(df -h . | awk 'NR==2 {print $5}' | sed 's/%//')
if [ "$DISK_USAGE" -lt 80 ]; then
    echo -e "${GREEN}✓ Disk usage: ${DISK_USAGE}%${NC}"
else
    echo -e "${RED}✗ Disk usage: ${DISK_USAGE}% (warning: >80%)${NC}"
fi

# Check memory
MEM_USAGE=$(free | awk 'NR==2{printf "%.0f", $3*100/$2}')
if [ "$MEM_USAGE" -lt 90 ]; then
    echo -e "${GREEN}✓ Memory usage: ${MEM_USAGE}%${NC}"
else
    echo -e "${RED}✗ Memory usage: ${MEM_USAGE}% (warning: >90%)${NC}"
fi

echo ""
echo "Health check completed"
