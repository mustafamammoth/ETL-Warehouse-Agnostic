#!/bin/bash
# Setup script for the data platform

echo "🔧 Setting up Data Platform Environment..."

# Create necessary directories
mkdir -p data/raw/acumatica
mkdir -p airflow/logs
mkdir -p airflow/plugins

echo "✅ Created directory structure"

# Initialize Airflow database (updated command)
export AIRFLOW_HOME=$(pwd)/airflow
airflow db migrate

echo "✅ Initialized Airflow database"

# Create Airflow admin user (updated command structure)
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

echo "✅ Created Airflow admin user"
echo "   Username: admin"
echo "   Password: admin"

echo "🎉 Environment setup complete!"
echo ""
echo "Next steps:"
echo "1. Copy .env.example to .env and fill in your credentials"
echo "2. Run: python scripts/test_acumatica_connection.py"
echo "3. Start services: docker-compose up -d"
echo "4. Access Airflow UI: http://localhost:8080"


# #!/bin/bash
# # Setup script for the data platform

# echo "🔧 Setting up Data Platform Environment..."

# # Create necessary directories
# mkdir -p data/raw/acumatica
# mkdir -p airflow/logs
# mkdir -p airflow/plugins

# echo "✅ Created directory structure"

# # Initialize Airflow database
# export AIRFLOW_HOME=$(pwd)/airflow
# airflow db init

# echo "✅ Initialized Airflow database"

# # Create Airflow admin user
# airflow users create \
#     --username admin \
#     --firstname Admin \
#     --lastname User \
#     --role Admin \
#     --email admin@example.com \
#     --password admin

# echo "✅ Created Airflow admin user"
# echo "   Username: admin"
# echo "   Password: admin"

# echo "🎉 Environment setup complete!"
# echo ""
# echo "Next steps:"
# echo "1. Copy .env.example to .env and fill in your credentials"
# echo "2. Run: python scripts/test_acumatica_connection.py"
# echo "3. Start services: docker-compose up -d"
# echo "4. Access Airflow UI: http://localhost:8080"