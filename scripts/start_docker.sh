#!/bin/bash
# Check Docker and start services

echo "🔧 Checking Docker status..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running!"
    echo ""
    echo "Please start Docker Desktop:"
    echo "📱 Mac: Open Docker Desktop app from Applications"
    echo "🪟 Windows: Open Docker Desktop from Start menu"
    echo "🐧 Linux: sudo systemctl start docker"
    echo ""
    echo "Then run this script again."
    exit 1
fi

echo "✅ Docker is running"

# Start services
echo "🚀 Starting Data Platform Services..."
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