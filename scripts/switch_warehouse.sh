#!/bin/bash

# Warehouse switching script with environment variable support

if [ $# -eq 0 ]; then
    echo "Usage: $0 <warehouse_type>"
    echo "Available warehouses: postgres, snowflake, clickhouse"
    exit 1
fi

WAREHOUSE=$1

echo "🔄 Switching to $WAREHOUSE warehouse..."

# 1. Check if .env file exists
if [ ! -f ".env" ]; then
    echo "❌ .env file not found. Please create one with your configuration."
    echo "   See .env.example for reference."
    exit 1
fi

# 2. Update ACTIVE_WAREHOUSE in .env file
echo "📝 Updating .env file..."
if grep -q "^ACTIVE_WAREHOUSE=" .env; then
    # Replace existing line - Fixed for macOS compatibility
    sed -i '' "s/^ACTIVE_WAREHOUSE=.*/ACTIVE_WAREHOUSE=${WAREHOUSE}/" .env
else
    # Add new line
    echo "ACTIVE_WAREHOUSE=${WAREHOUSE}" >> .env
fi

# 3. Source the .env file to load variables
echo "🔧 Loading environment variables..."
export $(grep -v '^#' .env | xargs)

# 4. Generate configurations
echo "📝 Generating dbt profiles..."
python scripts/generate_dbt_profiles.py

echo "🐳 Generating docker-compose..."
python scripts/generate_docker_compose.py

# 5. Stop current services
echo "⏹️  Stopping current services..."
docker-compose down

# 6. Start new services
echo "🚀 Starting services for $WAREHOUSE..."
docker-compose up -d

# 7. Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 30

# 8. Check service health
echo "🔍 Checking service health..."
docker-compose ps

echo "✅ Switched to $WAREHOUSE warehouse!"
echo "🌐 Airflow UI: http://localhost:8080"
echo "📊 Active Warehouse: $WAREHOUSE"
echo "🔐 Using secure environment variables from .env file"

# 9. Show what services are running
echo ""
echo "📋 Running services:"
docker-compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"