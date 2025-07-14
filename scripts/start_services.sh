#!/bin/bash
# Start all services for local development

echo "🚀 Starting Data Platform Services..."

# Start PostgreSQL and Airflow
docker-compose up -d

echo "✅ Services starting..."
echo ""
echo "🌐 Service URLs:"
echo "   Airflow UI: http://localhost:8080"
echo "   PostgreSQL: localhost:5432"
echo ""
echo "📊 Default Credentials:"
echo "   Airflow: admin / admin"
echo "   PostgreSQL: postgres / postgres"
echo ""
echo "⏳ Services may take 30-60 seconds to fully start"
echo "   Check status: docker-compose ps"