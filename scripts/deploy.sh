#!/bin/bash
# Production Deployment Script

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
ENVIRONMENT=${ENVIRONMENT:-production}
DOCKER_COMPOSE_FILE="docker-compose.yml"

echo -e "${GREEN}Starting deployment for environment: ${ENVIRONMENT}${NC}"

# Pre-deployment checks
echo -e "${YELLOW}Running pre-deployment checks...${NC}"

# Check Docker
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Docker is not installed${NC}"
    exit 1
fi

# Check Docker Compose
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}Docker Compose is not installed${NC}"
    exit 1
fi

# Check Python dependencies
if ! python3 -c "import pyspark" &> /dev/null; then
    echo -e "${YELLOW}Installing Python dependencies...${NC}"
    pip install -r requirements.txt
fi

# Backup existing data (if applicable)
if [ -d "data" ]; then
    echo -e "${YELLOW}Creating backup...${NC}"
    BACKUP_DIR="backups/$(date +%Y%m%d_%H%M%S)"
    mkdir -p "$BACKUP_DIR"
    cp -r data "$BACKUP_DIR/" || true
fi

# Pull latest images
echo -e "${YELLOW}Pulling latest Docker images...${NC}"
docker-compose pull

# Stop existing services gracefully
echo -e "${YELLOW}Stopping existing services...${NC}"
docker-compose down --timeout 30

# Start services
echo -e "${YELLOW}Starting services...${NC}"
docker-compose up -d

# Wait for services to be healthy
echo -e "${YELLOW}Waiting for services to be healthy...${NC}"
sleep 30

# Health checks
echo -e "${YELLOW}Running health checks...${NC}"

# Check Kafka
if docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &> /dev/null; then
    echo -e "${GREEN}✓ Kafka is healthy${NC}"
else
    echo -e "${RED}✗ Kafka health check failed${NC}"
    exit 1
fi

# Check MinIO
if curl -f http://localhost:9000/minio/health/live &> /dev/null; then
    echo -e "${GREEN}✓ MinIO is healthy${NC}"
else
    echo -e "${RED}✗ MinIO health check failed${NC}"
    exit 1
fi

# Check HDFS
if curl -f http://localhost:9870 &> /dev/null; then
    echo -e "${GREEN}✓ HDFS NameNode is healthy${NC}"
else
    echo -e "${RED}✗ HDFS health check failed${NC}"
    exit 1
fi

# Check Airflow
if curl -f http://localhost:8080/health &> /dev/null; then
    echo -e "${GREEN}✓ Airflow is healthy${NC}"
else
    echo -e "${RED}✗ Airflow health check failed${NC}"
    exit 1
fi

# Check Trino
if curl -f http://localhost:8081/v1/info &> /dev/null; then
    echo -e "${GREEN}✓ Trino is healthy${NC}"
else
    echo -e "${RED}✗ Trino health check failed${NC}"
    exit 1
fi

# Check Prometheus
if curl -f http://localhost:9090/-/healthy &> /dev/null; then
    echo -e "${GREEN}✓ Prometheus is healthy${NC}"
else
    echo -e "${RED}✗ Prometheus health check failed${NC}"
    exit 1
fi

# Check Grafana
if curl -f http://localhost:3000/api/health &> /dev/null; then
    echo -e "${GREEN}✓ Grafana is healthy${NC}"
else
    echo -e "${RED}✗ Grafana health check failed${NC}"
    exit 1
fi

echo -e "${GREEN}Deployment completed successfully!${NC}"
echo -e "${GREEN}All services are healthy and running.${NC}"

# Display service URLs
echo -e "\n${GREEN}Service URLs:${NC}"
echo "  - Kafka: localhost:9092"
echo "  - MinIO Console: http://localhost:9001"
echo "  - HDFS NameNode: http://localhost:9870"
echo "  - Airflow: http://localhost:8080"
echo "  - Trino: http://localhost:8081"
echo "  - Prometheus: http://localhost:9090"
echo "  - Grafana: http://localhost:3000"
