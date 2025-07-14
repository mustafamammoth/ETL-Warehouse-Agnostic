#!/bin/bash

# Simple warehouse switching script

if [ $# -eq 0 ]; then
    echo "Usage: $0 <warehouse_type>"
    echo "Available warehouses: postgres, snowflake, clickhouse"
    exit 1
fi

WAREHOUSE=$1

echo "🔄 Switching to $WAREHOUSE warehouse..."

# 1. Set environment variable
export ACTIVE_WAREHOUSE=$WAREHOUSE
echo "export ACTIVE_WAREHOUSE=$WAREHOUSE" > .env.warehouse

# 2. Generate configurations
echo "📝 Generating dbt profiles..."
python scripts/generate_dbt_profiles.py

echo "🐳 Generating docker-compose..."
python scripts/generate_docker_compose.py

# 3. Stop current services
echo "⏹️  Stopping current services..."
docker-compose down

# 4. Start new services
echo "🚀 Starting services for $WAREHOUSE..."
source .env.warehouse
docker-compose up -d

# 5. Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 30

# 6. Check service health
echo "🔍 Checking service health..."
docker-compose ps

echo "✅ Switched to $WAREHOUSE warehouse!"
echo "🌐 Airflow UI: http://localhost:8080"
echo "📊 Active Warehouse: $WAREHOUSE"