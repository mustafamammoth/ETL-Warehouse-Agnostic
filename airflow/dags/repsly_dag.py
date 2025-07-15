# repsly_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import os
import requests
import pandas as pd
import time
import json
import subprocess
import sys
from requests.auth import HTTPBasicAuth

# Default arguments for the DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Create the DAG
dag = DAG(
    'repsly_extract_transform',
    default_args=default_args,
    description='Extract from Repsly API and transform with dbt',
    schedule=None,  # Manual trigger for now
    max_active_runs=1,
    tags=['repsly', 'extract', 'transform', 'dbt']
)

# SECURE: Get credentials from environment variables
REPSLY_BASE_URL = os.getenv('REPSLY_BASE_URL', 'https://api.repsly.com/v3')
REPSLY_USERNAME = os.getenv('REPSLY_USERNAME')
REPSLY_PASSWORD = os.getenv('REPSLY_PASSWORD')

# TESTING vs PRODUCTION configuration
TESTING_MODE = True  # Set to False for production (full data extraction)
MAX_RECORDS_PER_ENDPOINT = 50 if TESTING_MODE else None  # Remove limit for production

# Validate that required environment variables are set
if not all([REPSLY_USERNAME, REPSLY_PASSWORD]):
    missing_vars = []
    if not REPSLY_USERNAME: missing_vars.append('REPSLY_USERNAME')
    if not REPSLY_PASSWORD: missing_vars.append('REPSLY_PASSWORD')
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Define all endpoints you want to extract (just like Acumatica ENDPOINTS)
ENDPOINTS = {
    'clients': {
        'path': 'export/clients',
        'pagination_type': 'timestamp',
        'limit': 50,
        'timestamp_field': 'LastTimeStamp',
        'data_field': 'Clients',
        'total_count_field': 'TotalCount'
    },
    'client_notes': {
        'path': 'export/clientnotes',
        'pagination_type': 'id',
        'limit': 50,
        'id_field': 'LastClientNoteID',
        'data_field': 'ClientNotes',
        'total_count_field': 'TotalCount'
    },
    'visits': {
        'path': 'export/visits',
        'pagination_type': 'timestamp',
        'limit': 50,
        'timestamp_field': 'LastTimeStamp',
        'data_field': 'Visits',
        'total_count_field': 'TotalCount'
    },
    'daily_working_time': {
        'path': 'export/dailyworkingtime',
        'pagination_type': 'id',
        'limit': 50,
        'id_field': 'LastDailyWorkingTimeID',
        'data_field': 'DailyWorkingTime',
        'total_count_field': 'TotalCount'
    },
    'products': {
        'path': 'export/products',
        'pagination_type': 'id',
        'limit': 50,
        'id_field': 'LastProductID',
        'data_field': 'Products',
        'total_count_field': 'TotalCount'
    },
    'users': {
        'path': 'export/users',
        'pagination_type': 'id',
        'limit': 50,
        'id_field': 'LastProductID',
        'data_field': 'Users',
        'total_count_field': 'TotalCount'
    },
    'representatives': {
        'path': 'export/representatives',
        'pagination_type': 'static',
        'data_field': 'Representatives'
    }
    # Add more endpoints here as needed
}

def create_authenticated_session():
    """Create authenticated session for Repsly"""
    session = requests.Session()
    session.auth = HTTPBasicAuth(REPSLY_USERNAME, REPSLY_PASSWORD)
    session.headers.update({'Accept': 'application/json'})
    
    # Test authentication
    test_url = f"{REPSLY_BASE_URL}/export/clients/0"
    response = session.get(test_url)
    response.raise_for_status()
    print("✅ Authentication successful")
    
    return session

def flatten_repsly_record(record):
    """Flatten Repsly record structure"""
    flattened = {}
    
    if isinstance(record, dict):
        for key, value in record.items():
            if isinstance(value, dict):
                # Flatten nested objects
                for nested_key, nested_value in value.items():
                    flattened[f"{key}_{nested_key}"] = nested_value
            elif isinstance(value, list):
                # Convert arrays to JSON strings
                flattened[key] = json.dumps(value) if value else None
            else:
                flattened[key] = value
    else:
        flattened['value'] = record
    
    return flattened

def get_paginated_data(session, endpoint_config, endpoint_name):
    """Get data from Repsly endpoint with appropriate pagination (just like Acumatica)"""
    all_data = []
    
    if endpoint_config['pagination_type'] == 'static':
        # Static endpoints - single request
        endpoint_url = f"{REPSLY_BASE_URL}/{endpoint_config['path']}"
        print(f"   Fetching data from: {endpoint_url}")
        
        try:
            response = session.get(endpoint_url)
            response.raise_for_status()
            data = response.json()
            
            if endpoint_config.get('data_field'):
                records = data.get(endpoint_config['data_field'], [])
            else:
                records = data if isinstance(data, list) else [data]
            
            all_data.extend(records)
            
        except Exception as e:
            print(f"   Error fetching static data: {e}")
            raise
            
    else:
        # Paginated endpoints
        last_value = 0
        page_count = 0
        max_pages = 1000
        
        while page_count < max_pages:
            page_count += 1
            endpoint_url = f"{REPSLY_BASE_URL}/{endpoint_config['path']}/{last_value}"
            
            print(f"   Page {page_count}: Fetching from {endpoint_url}")
            
            try:
                response = session.get(endpoint_url)
                response.raise_for_status()
                data = response.json()
                
                meta = data.get('MetaCollectionResult', {})
                total_count = meta.get(endpoint_config['total_count_field'], 0)
                
                if endpoint_config['pagination_type'] == 'timestamp':
                    new_value = meta.get(endpoint_config['timestamp_field'], 0)
                else:
                    new_value = meta.get(endpoint_config['id_field'], 0)
                
                records = data.get(endpoint_config['data_field'], [])
                
                print(f"   Retrieved {len(records)} records (total: {len(all_data)})")
                
                if not records or total_count == 0:
                    break
                
                all_data.extend(records)
                
                # Check if we've hit testing limit
                if TESTING_MODE and MAX_RECORDS_PER_ENDPOINT and len(all_data) >= MAX_RECORDS_PER_ENDPOINT:
                    all_data = all_data[:MAX_RECORDS_PER_ENDPOINT]
                    break
                
                if new_value > last_value:
                    last_value = new_value
                else:
                    break
                    
                if len(records) < endpoint_config['limit']:
                    break
                
                time.sleep(0.1)
                
            except Exception as e:
                print(f"   Error on page {page_count}: {e}")
                if page_count == 1:
                    raise
                else:
                    break
    
    # Trim to exact limit (just like Acumatica)
    if TESTING_MODE and MAX_RECORDS_PER_ENDPOINT:
        all_data = all_data[:MAX_RECORDS_PER_ENDPOINT]
    
    print(f"✅ {endpoint_name}: Collected {len(all_data)} total records")
    return all_data

def extract_repsly_endpoint(endpoint_name, **context):
    """Extract data from any Repsly endpoint (same pattern as Acumatica)"""
    
    if endpoint_name not in ENDPOINTS:
        raise ValueError(f"Unknown endpoint: {endpoint_name}")
    
    endpoint_config = ENDPOINTS[endpoint_name]
    print(f"🔄 Extracting {endpoint_name} from {endpoint_config['path']}")
    
    try:
        # Create authenticated session
        session = create_authenticated_session()
        
        # Get all data with pagination
        raw_data = get_paginated_data(session, endpoint_config, endpoint_name)
        
        if not raw_data:
            print(f"⚠️  No data found for {endpoint_name}")
            return 0
        
        # Flatten the data structure
        print(f"   Flattening {len(raw_data)} records...")
        flattened_data = [flatten_repsly_record(record) for record in raw_data]
        
        # Convert to DataFrame
        df = pd.DataFrame(flattened_data)
        df['_extracted_at'] = datetime.now()
        df['_source_system'] = 'repsly'
        df['_endpoint'] = endpoint_name
        
        # Save to CSV with Docker path (same as Acumatica)
        os.makedirs('/opt/airflow/data/raw/repsly', exist_ok=True)
        filename = f'/opt/airflow/data/raw/repsly/{endpoint_name}.csv'
        df.to_csv(filename, index=False)
        
        print(f"✅ {endpoint_name}: Saved {len(df)} records to {filename}")
        print(f"   Sample columns: {list(df.columns)[:10]}")
        
        return len(df)
        
    except Exception as e:
        print(f"❌ Failed to extract {endpoint_name}: {e}")
        raise

def load_csv_to_warehouse(**context):
    """Load CSV files to any warehouse (dynamic) - SAME as Acumatica"""
    
    print("📊 Loading CSV files to warehouse...")
    
    try:
        import sys
        sys.path.append('/opt/airflow/config')
        from warehouse_config import load_warehouse_config, get_active_warehouse, get_connection_string
        
        # Get warehouse configuration
        warehouse_type = get_active_warehouse()
        config = load_warehouse_config(warehouse_type)
        
        print(f"Loading to: {config['warehouse']['name']} ({warehouse_type})")
        
        # Use warehouse-specific loading logic
        if warehouse_type == 'postgres':
            return load_to_postgres_warehouse(config)
        elif warehouse_type == 'snowflake':
            return load_to_snowflake_warehouse(config)
        elif warehouse_type == 'clickhouse':
            return load_to_clickhouse_warehouse(config)
        else:
            raise ValueError(f"Unsupported warehouse: {warehouse_type}")
        
    except Exception as e:
        print(f"❌ Failed to load data to warehouse: {e}")
        import traceback
        print(f"Full error: {traceback.format_exc()}")
        raise

def load_to_postgres_warehouse(config):
    """PostgreSQL-specific loading - DYNAMIC like Acumatica"""
    from sqlalchemy import create_engine, text
    import pandas as pd
    
    # Get connection string
    sys.path.append('/opt/airflow/config')
    from warehouse_config import get_connection_string
    
    connection_string = get_connection_string('postgres')
    engine = create_engine(connection_string)
    
    # Create repsly schema if it doesn't exist
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS repsly"))
        print("✅ Created/verified repsly schema")
    
    # DYNAMIC CSV file mapping (discovers files automatically)
    csv_directory = '/opt/airflow/data/raw/repsly'
    csv_files = {}
    
    for endpoint_name in ENDPOINTS.keys():
        csv_path = f'{csv_directory}/{endpoint_name}.csv'
        if os.path.exists(csv_path):
            table_name = f'repsly.raw_{endpoint_name}'
            csv_files[table_name] = csv_path
    
    total_records = 0
    for table_name, csv_path in csv_files.items():
        if os.path.exists(csv_path):
            print(f"   Loading {csv_path} to PostgreSQL...")
            df = pd.read_csv(csv_path)
            
            # Extract schema and table name
            schema_name, table_name_only = table_name.split('.')
            
            # Same logic as Acumatica
            with engine.begin() as conn:
                # Check if table exists
                table_exists_query = text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = :schema_name 
                        AND table_name = :table_name
                    )
                """)
                
                result = conn.execute(table_exists_query, {
                    'schema_name': schema_name, 
                    'table_name': table_name_only
                })
                table_exists = result.scalar()
                
                if table_exists:
                    conn.execute(text(f"TRUNCATE TABLE {table_name}"))
                    print(f"   Truncated existing table {table_name}")
                    
                    df.to_sql(
                        name=table_name_only,
                        schema=schema_name,
                        con=conn,
                        if_exists='append',
                        index=False,
                        method='multi'
                    )
                else:
                    print(f"   Table {table_name} doesn't exist, creating...")
                    df.to_sql(
                        name=table_name_only,
                        schema=schema_name,
                        con=conn,
                        if_exists='replace',
                        index=False,
                        method='multi'
                    )
            
            total_records += len(df)
            print(f"✅ Loaded {len(df)} records to PostgreSQL.{table_name}")
        else:
            print(f"⚠️  File not found: {csv_path}")
    
    engine.dispose()
    print(f"✅ PostgreSQL: Total {total_records} records loaded")
    return total_records

def load_to_snowflake_warehouse(config):
    """Snowflake-specific loading"""
    print("⚠️  Snowflake loading implementation needed")
    return 0

def load_to_clickhouse_warehouse(config):
    """ClickHouse-specific loading"""
    print("⚠️  ClickHouse loading implementation needed")
    return 0

def debug_database_tables(**context):
    """Debug function to see what tables exist"""
    
    print("🔍 Checking what tables exist in database...")
    
    try:
        from sqlalchemy import create_engine
        
        connection_string = "postgresql://postgres:postgres@postgres:5432/airflow"
        engine = create_engine(connection_string)
        
        with engine.connect() as conn:
            # Check all tables in both schemas
            result = conn.execute("""
                SELECT schemaname, tablename, tableowner 
                FROM pg_tables 
                WHERE schemaname IN ('public', 'repsly') 
                ORDER BY schemaname, tablename;
            """)
            
            print("📊 Tables in database:")
            for row in result.fetchall():
                print(f"   {row[0]}.{row[1]} (owner: {row[2]})")
            
            # Check all views
            result = conn.execute("""
                SELECT schemaname, viewname, viewowner 
                FROM pg_views 
                WHERE schemaname IN ('public', 'repsly') 
                ORDER BY schemaname, viewname;
            """)
            
            print("👁️ Views in database:")
            for row in result.fetchall():
                print(f"   {row[0]}.{row[1]} (owner: {row[2]})")
        
        engine.dispose()
        
    except Exception as e:
        print(f"❌ Debug failed: {e}")
        raise

def run_dbt_transformations(**context):
    """Run dbt transformations with debugging - SAME as Acumatica"""
    
    print("🔧 Running dbt transformations...")
    
    try:
        dbt_dir = '/opt/airflow/dbt'
        print(f"✅ Found dbt directory: {dbt_dir}")
        
        # First, debug what exists
        debug_database_tables()
        
        # MODULAR: Just add commands here as you create models
        commands = [
            'dbt run --select clients_raw',
            'dbt run --select clients',

            'dbt run --select client_notes_raw',
            'dbt run --select client_notes',

            'dbt run --select daily_working_time_raw',
            'dbt run --select daily_working_time',
            # Add more as you create them:
            'dbt run --select visits_raw',
            'dbt run --select visits',

            'dbt run --select representatives_raw',      # ✅ ADD THIS
            'dbt run --select representatives',          # ✅ ADD THIS

            'dbt run --select users_raw',      # ✅ ADD THIS
            'dbt run --select users',          # ✅ ADD THIS
        ]
        
        for cmd in commands:
            print(f"Running: {cmd}")
            result = subprocess.run(
                cmd.split(), 
                cwd=dbt_dir,
                capture_output=True, 
                text=True,
                check=False  # Don't fail immediately
            )
            
            if result.returncode == 0:
                print(f"✅ {cmd} completed successfully")
                print(result.stdout)
                
                # Debug after each successful run
                debug_database_tables()
            else:
                print(f"❌ {cmd} failed:")
                print(f"Error: {result.stderr}")
                print(f"Output: {result.stdout}")
                raise Exception(f"dbt command failed: {cmd}")
        
        print("✅ All dbt transformations completed")
        
    except Exception as e:
        print(f"❌ Failed to run dbt: {e}")
        raise

def check_transformed_data(**context):
    """Check the results of dbt transformations - SAME pattern as Acumatica"""
    
    print("🔍 Checking transformed data quality...")
    
    try:
        from sqlalchemy import create_engine
        
        # Use SQLAlchemy engine
        connection_string = "postgresql://postgres:postgres@postgres:5432/airflow"
        engine = create_engine(connection_string)
        
        # Check clients table (equivalent to customers in Acumatica)
        print("📊 Checking clients data...")
        with engine.connect() as conn:
            result = conn.execute("""
                SELECT 
                    COUNT(*) as total_clients,
                    COUNT(DISTINCT client_id) as unique_clients,
                    COUNT(email_clean) as clients_with_email,
                    COUNT(CASE WHEN is_active = true THEN 1 END) as active_clients
                FROM public.clients
            """)
            
            row = result.fetchone()
            print(f"📊 Client Data Quality:")
            print(f"   Total clients: {row[0]}")
            print(f"   Unique client IDs: {row[1]}")
            print(f"   Clients with email: {row[2]}")
            print(f"   Active clients: {row[3]}")
        
        # Check client_notes table (equivalent to sales_invoices in Acumatica)
        print("\n📊 Checking client notes data...")
        with engine.connect() as conn:
            result = conn.execute("""
                SELECT 
                    COUNT(*) as total_notes,
                    COUNT(DISTINCT client_note_id) as unique_notes,
                    COUNT(DISTINCT client_code) as clients_with_notes,
                    AVG(note_length) as avg_note_length,
                    COUNT(CASE WHEN note_category = 'Ordering' THEN 1 END) as ordering_notes,
                    COUNT(CASE WHEN note_sentiment = 'Positive' THEN 1 END) as positive_notes
                FROM public.client_notes
            """)
            
            row = result.fetchone()
            print(f"📊 Client Notes Data Quality:")
            print(f"   Total notes: {row[0]}")
            print(f"   Unique note IDs: {row[1]}")
            print(f"   Clients with notes: {row[2]}")
            print(f"   Average note length: {row[3]:.1f}" if row[3] else "N/A")
            print(f"   Ordering notes: {row[4]}")
            print(f"   Positive sentiment: {row[5]}")
            
            if row[0] > 0:
                print("✅ Data transformation successful!")
            else:
                raise Exception("No data found in client_notes table")

        # Check daily working time data
        print("\n📊 Checking daily working time data...")
        with engine.connect() as conn:
            result = conn.execute("""
                SELECT 
                    COUNT(*) as total_work_days,
                    COUNT(DISTINCT representative_code) as unique_reps,
                    AVG(actual_work_duration_minutes) as avg_work_duration,
                    AVG(number_of_visits) as avg_visits_per_day,
                    AVG(client_time_percentage) as avg_client_time_pct,
                    COUNT(CASE WHEN efficiency_rating = 'High Efficiency' THEN 1 END) as high_efficiency_days,
                    AVG(mileage_total) as avg_daily_mileage
                FROM public.daily_working_time
            """)
            
            row = result.fetchone()
            print(f"📊 Daily Working Time Data Quality:")
            print(f"   Total work days: {row[0]}")
            print(f"   Unique representatives: {row[1]}")
            print(f"   Average work duration: {row[2]:.1f} minutes" if row[2] else "N/A")
            print(f"   Average visits per day: {row[3]:.1f}" if row[3] else "N/A")
            print(f"   Average client time: {row[4]:.1f}%" if row[4] else "N/A")
            print(f"   High efficiency days: {row[5]}")
            print(f"   Average daily mileage: {row[6]:.1f}" if row[6] else "N/A")

        # ✅ ADD THIS - Check representatives data
        print("\n📊 Checking representatives data...")
        with engine.connect() as conn:
            result = conn.execute("""
                SELECT 
                    COUNT(*) as total_representatives,
                    COUNT(DISTINCT representative_code) as unique_rep_codes,
                    COUNT(email_clean) as reps_with_email,
                    COUNT(CASE WHEN is_active = true THEN 1 END) as active_reps,
                    COUNT(CASE WHEN company_affiliation = 'Mammoth Distribution' THEN 1 END) as mammoth_reps,
                    COUNT(CASE WHEN company_affiliation = '710 Labs' THEN 1 END) as labs_reps,
                    AVG(territory_count) as avg_territories_per_rep,
                    COUNT(CASE WHEN representative_tier = 'Internal - Primary' THEN 1 END) as internal_primary,
                    AVG(data_completeness_score) as avg_completeness_score
                FROM public.representatives
            """)
            
            row = result.fetchone()
            print(f"📊 Representatives Data Quality:")
            print(f"   Total representatives: {row[0]}")
            print(f"   Unique rep codes: {row[1]}")
            print(f"   Reps with email: {row[2]}")
            print(f"   Active representatives: {row[3]}")
            print(f"   Mammoth reps: {row[4]}")
            print(f"   710 Labs reps: {row[5]}")
            print(f"   Avg territories per rep: {row[6]:.1f}" if row[6] else "N/A")
            print(f"   Internal primary reps: {row[7]}")
            print(f"   Avg completeness score: {row[8]:.1f}" if row[8] else "N/A")

        # Add more data quality checks here as you add models...
        
        engine.dispose()
        print("✅ All data quality checks completed successfully!")
        
    except Exception as e:
        print(f"❌ Data quality check failed: {e}")
        # Don't fail the pipeline if this is just a check issue
        print("⚠️  Continuing pipeline despite quality check failure")

# ========================
# TASK DEFINITIONS - SAME as Acumatica
# ========================

start_task = EmptyOperator(
    task_id='start_pipeline',
    dag=dag
)

# Test connection task
def test_connection(**context):
    """Test connection to Repsly"""
    try:
        session = create_authenticated_session()
        print("✅ Connection test successful")
        return True
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        raise

test_task = PythonOperator(
    task_id='test_repsly_connection',
    python_callable=test_connection,
    dag=dag
)

# Create extraction tasks for each endpoint
extraction_tasks = []
for endpoint_name in ENDPOINTS.keys():
    task = PythonOperator(
        task_id=f'extract_{endpoint_name}',
        python_callable=extract_repsly_endpoint,
        op_args=[endpoint_name],
        dag=dag
    )
    extraction_tasks.append(task)

# Load data to warehouse
load_task = PythonOperator(
    task_id='load_to_warehouse',
    python_callable=load_csv_to_warehouse,
    dag=dag
)

# Run dbt transformations
transform_task = PythonOperator(
    task_id='run_dbt_transformations',
    python_callable=run_dbt_transformations,
    dag=dag
)

quality_check_task = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_transformed_data,
    dag=dag
)

end_task = EmptyOperator(
    task_id='end_pipeline',
    dag=dag
)

# ========================
# TASK DEPENDENCIES - SAME as Acumatica
# ========================

# Linear flow: Extract -> Load -> Transform -> Quality Check
start_task >> test_task >> extraction_tasks >> load_task >> transform_task >> quality_check_task >> end_task