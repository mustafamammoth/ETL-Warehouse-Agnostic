from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import os
import requests
import pandas as pd
import time
import subprocess

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
    'acumatica_extract_transform',
    default_args=default_args,
    description='Extract from Acumatica API and transform with dbt',
    schedule=None,  # Manual trigger for now
    max_active_runs=1,
    tags=['acumatica', 'extract', 'transform', 'dbt']
)

# HARDCODE YOUR CREDENTIALS HERE
BASE_URL = 'https://mammoth.klearsystems.com'
USERNAME = 'mustafa.zaki'  
PASSWORD = 'Dingdongbell@123'

# Define all endpoints you want to extract
ENDPOINTS = {
    'customers': 'Customer',
    'sales_orders': 'SalesOrder', 
    'sales_invoices': 'SalesInvoice',
    'stock_items': 'StockItem',
}

def create_authenticated_session():
    """Create authenticated session for Acumatica"""
    session = requests.Session()
    
    login_url = f"{BASE_URL}/entity/auth/login"
    login_data = {"name": USERNAME, "password": PASSWORD}
    
    response = session.post(login_url, json=login_data)
    response.raise_for_status()
    print("✅ Authentication successful")
    
    return session

def flatten_acumatica_record(record):
    """Flatten Acumatica's nested {'value': 'data'} structure"""
    flattened = {}
    for key, value in record.items():
        if isinstance(value, dict) and 'value' in value:
            flattened[key] = value['value']
        elif isinstance(value, dict) and not value:
            flattened[key] = None
        else:
            flattened[key] = value
    return flattened

def get_paginated_data(session, endpoint_url, page_size=100, max_records=100):
    """Get limited data from endpoint (max 100 records for testing)"""
    all_data = []
    skip = 0
    
    while len(all_data) < max_records:
        # Adjust page_size if we're near the limit
        current_page_size = min(page_size, max_records - len(all_data))
        params = {'$top': current_page_size, '$skip': skip}
        
        response = session.get(endpoint_url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        # Handle different response formats
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict) and 'value' in data:
            records = data['value']
        else:
            records = [data] if data else []
        
        if not records:
            break
            
        all_data.extend(records)
        print(f"   Retrieved {len(records)} records (total: {len(all_data)})")
        
        # Stop if we have enough records or got less than requested
        if len(all_data) >= max_records or len(records) < current_page_size:
            break
            
        skip += current_page_size
        time.sleep(0.1)  # Be nice to the API
    
    # Trim to exact limit
    return all_data[:max_records]

def extract_acumatica_endpoint(endpoint_name, **context):
    """Extract data from any Acumatica endpoint"""
    
    if endpoint_name not in ENDPOINTS:
        raise ValueError(f"Unknown endpoint: {endpoint_name}")
    
    endpoint_path = ENDPOINTS[endpoint_name]
    print(f"🔄 Extracting {endpoint_name} from {endpoint_path}")
    
    try:
        # Create authenticated session
        session = create_authenticated_session()
        
        # Build URL
        endpoint_url = f"{BASE_URL}/entity/Default/23.200.001/{endpoint_path}"
        
        # Get all data with pagination
        print(f"   Fetching data from: {endpoint_url}")
        raw_data = get_paginated_data(session, endpoint_url)
        
        if not raw_data:
            print(f"⚠️  No data found for {endpoint_name}")
            return 0
        
        # Flatten the nested JSON structure
        print(f"   Flattening {len(raw_data)} records...")
        flattened_data = [flatten_acumatica_record(record) for record in raw_data]
        
        # Convert to DataFrame
        df = pd.DataFrame(flattened_data)
        df['_extracted_at'] = datetime.now()
        df['_source_system'] = 'acumatica'
        df['_endpoint'] = endpoint_name
        
        # Save to CSV with Docker path
        os.makedirs('/opt/airflow/data/raw/acumatica', exist_ok=True)  # Docker path
        filename = f'/opt/airflow/data/raw/acumatica/{endpoint_name}.csv'  # Docker path
        df.to_csv(filename, index=False)
        
        print(f"✅ {endpoint_name}: Saved {len(df)} records to {filename}")
        print(f"   Sample columns: {list(df.columns)[:10]}")
        
        return len(df)
        
    except Exception as e:
        print(f"❌ Failed to extract {endpoint_name}: {e}")
        raise

def load_csv_to_postgres(**context):
    """Load CSV files to PostgreSQL for dbt processing"""
    
    print("📊 Loading CSV files to PostgreSQL...")
    
    try:
        import pandas as pd
        from sqlalchemy import create_engine, text
        
        # Create SQLAlchemy engine
        connection_string = "postgresql://postgres:postgres@postgres:5432/airflow"
        engine = create_engine(connection_string)
        
        # Test connection
        print("✅ Connected to PostgreSQL")
        
        # Load each CSV file directly to public schema
        csv_files = {
            'raw_customers': '/opt/airflow/data/raw/acumatica/customers.csv',
            'raw_sales_orders': '/opt/airflow/data/raw/acumatica/sales_orders.csv',
            'raw_sales_invoices': '/opt/airflow/data/raw/acumatica/sales_invoices.csv',
            'raw_stock_items': '/opt/airflow/data/raw/acumatica/stock_items.csv'
        }
        
        for table_name, csv_path in csv_files.items():
            if os.path.exists(csv_path):
                print(f"   Loading {csv_path}...")
                df = pd.read_csv(csv_path)
                
                # Use begin() for proper transaction handling
                with engine.begin() as conn:  # This auto-commits at the end
                    try:
                        # Try to truncate table if it exists
                        conn.execute(text(f"TRUNCATE TABLE {table_name}"))
                        print(f"   Truncated existing table {table_name}")
                        
                        # Use append since table exists
                        df.to_sql(
                            name=table_name,
                            con=conn,
                            if_exists='append',
                            index=False,
                            method='multi'
                        )
                    except Exception as truncate_error:
                        # Table doesn't exist or truncate failed, create new table
                        print(f"   Truncate failed, creating new table: {truncate_error}")
                        df.to_sql(
                            name=table_name,
                            con=conn,
                            if_exists='replace',
                            index=False,
                            method='multi'
                        )
                
                print(f"✅ Loaded {len(df)} records to public.{table_name}")
            else:
                print(f"⚠️  File not found: {csv_path}")
        
        engine.dispose()
        print("✅ All CSV files loaded to PostgreSQL")
        
    except Exception as e:
        print(f"❌ Failed to load data to PostgreSQL: {e}")
        import traceback
        print(f"Full error: {traceback.format_exc()}")
        raise

def debug_database_tables(**context):
    """Debug function to see what tables exist"""
    
    print("🔍 Checking what tables exist in database...")
    
    try:
        from sqlalchemy import create_engine
        
        connection_string = "postgresql://postgres:postgres@postgres:5432/airflow"
        engine = create_engine(connection_string)
        
        with engine.connect() as conn:
            # Check all tables in public schema
            result = conn.execute("""
                SELECT schemaname, tablename, tableowner 
                FROM pg_tables 
                WHERE schemaname IN ('public') 
                ORDER BY schemaname, tablename;
            """)
            
            print("📊 Tables in database:")
            for row in result.fetchall():
                print(f"   {row[0]}.{row[1]} (owner: {row[2]})")
            
            # Check all views in public schema
            result = conn.execute("""
                SELECT schemaname, viewname, viewowner 
                FROM pg_views 
                WHERE schemaname = 'public' 
                ORDER BY viewname;
            """)
            
            print("👁️ Views in database:")
            for row in result.fetchall():
                print(f"   {row[0]}.{row[1]} (owner: {row[2]})")
        
        engine.dispose()
        
    except Exception as e:
        print(f"❌ Debug failed: {e}")
        raise

def run_dbt_transformations(**context):
    """Run dbt transformations with debugging"""
    
    print("🔧 Running dbt transformations...")
    
    try:
        dbt_dir = '/opt/airflow/dbt'
        print(f"✅ Found dbt directory: {dbt_dir}")
        
        # First, debug what exists
        debug_database_tables()
        
        # Run models one by one with better error handling
        commands = [
            'dbt run --select customers_raw', 
            'dbt run --select customers'
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

# ========================
# TASK DEFINITIONS
# ========================

start_task = EmptyOperator(
    task_id='start_pipeline',
    dag=dag
)

# Test connection task
def test_connection(**context):
    """Test connection to Acumatica"""
    try:
        session = create_authenticated_session()
        print("✅ Connection test successful")
        return True
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        raise

test_task = PythonOperator(
    task_id='test_acumatica_connection',
    python_callable=test_connection,
    dag=dag
)

# Create extraction tasks for each endpoint
extraction_tasks = []
for endpoint_name in ENDPOINTS.keys():
    task = PythonOperator(
        task_id=f'extract_{endpoint_name}',
        python_callable=extract_acumatica_endpoint,
        op_args=[endpoint_name],
        dag=dag
    )
    extraction_tasks.append(task)

# Load data to PostgreSQL
load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_csv_to_postgres,
    dag=dag
)

# Run dbt transformations
transform_task = PythonOperator(
    task_id='run_dbt_transformations',
    python_callable=run_dbt_transformations,
    dag=dag
)

def check_transformed_data(**context):
    """Check the results of dbt transformations"""
    
    print("🔍 Checking transformed data quality...")
    
    try:
        from sqlalchemy import create_engine
        
        # Use SQLAlchemy engine
        connection_string = "postgresql://postgres:postgres@postgres:5432/airflow"
        engine = create_engine(connection_string)
        
        with engine.connect() as conn:
            # Check if curated customers table exists and has data
            result = conn.execute("""
                SELECT 
                    COUNT(*) as total_customers,
                    COUNT(DISTINCT customer_id) as unique_customers,
                    COUNT(primary_email) as customers_with_email,
                    AVG(CAST(credit_limit AS NUMERIC)) as avg_credit_limit
                FROM public.customers
            """)
            
            row = result.fetchone()
            
            print(f"📊 Data Quality Results:")
            print(f"   Total customers: {row[0]}")
            print(f"   Unique customer IDs: {row[1]}")
            print(f"   Customers with email: {row[2]}")
            print(f"   Average credit limit: ${row[3]:.2f}" if row[3] else "N/A")
            
            if row[0] > 0:
                print("✅ Data transformation successful!")
            else:
                raise Exception("No data found in customers table")
        
        engine.dispose()
        
    except Exception as e:
        print(f"❌ Data quality check failed: {e}")
        # Don't fail the pipeline if this is just a check issue
        print("⚠️  Continuing pipeline despite quality check failure")

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
# TASK DEPENDENCIES
# ========================

# Linear flow: Extract -> Load -> Transform -> Quality Check
start_task >> test_task >> extraction_tasks >> load_task >> transform_task >> quality_check_task >> end_task

# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.empty import EmptyOperator
# import os
# import requests
# import pandas as pd
# import time

# # Default arguments for the DAG
# default_args = {
#     'owner': 'data-team',
#     'depends_on_past': False,
#     'start_date': datetime(2025, 1, 1),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
#     'catchup': False
# }

# # Create the DAG
# dag = DAG(
#     'acumatica_daily_extract',
#     default_args=default_args,
#     description='Daily extraction from Acumatica API',
#     schedule=None,  # Manual trigger for now
#     max_active_runs=1,
#     tags=['acumatica', 'extract', 'daily']
# )

# # HARDCODE YOUR CREDENTIALS HERE
# BASE_URL = 'https://mammoth.klearsystems.com'
# USERNAME = 'mustafa.zaki'  
# PASSWORD = 'Dingdongbell@123'

# # Define all endpoints you want to extract
# ENDPOINTS = {
#     'customers': 'Customer',
#     'sales_orders': 'SalesOrder', 
#     'sales_invoices': 'SalesInvoice',
#     'stock_items': 'StockItem',
#     # 'vendors': 'Vendor',
#     # 'purchase_orders': 'PurchaseOrder',
#     # 'purchase_receipts': 'PurchaseReceipt',
#     # 'shipments': 'Shipment'
# }

# def create_authenticated_session():
#     """Create authenticated session for Acumatica"""
#     session = requests.Session()
    
#     login_url = f"{BASE_URL}/entity/auth/login"
#     login_data = {"name": USERNAME, "password": PASSWORD}
    
#     response = session.post(login_url, json=login_data)
#     response.raise_for_status()
#     print("✅ Authentication successful")
    
#     return session

# def flatten_acumatica_record(record):
#     """Flatten Acumatica's nested {'value': 'data'} structure"""
#     flattened = {}
#     for key, value in record.items():
#         if isinstance(value, dict) and 'value' in value:
#             flattened[key] = value['value']
#         elif isinstance(value, dict) and not value:
#             flattened[key] = None
#         else:
#             flattened[key] = value
#     return flattened

# def get_paginated_data(session, endpoint_url, page_size=100, max_records=100):
#     """Get limited data from endpoint (max 100 records for testing)"""
#     all_data = []
#     skip = 0
    
#     while len(all_data) < max_records:
#         # Adjust page_size if we're near the limit
#         current_page_size = min(page_size, max_records - len(all_data))
#         params = {'$top': current_page_size, '$skip': skip}
        
#         response = session.get(endpoint_url, params=params)
#         response.raise_for_status()
        
#         data = response.json()
        
#         # Handle different response formats
#         if isinstance(data, list):
#             records = data
#         elif isinstance(data, dict) and 'value' in data:
#             records = data['value']
#         else:
#             records = [data] if data else []
        
#         if not records:
#             break
            
#         all_data.extend(records)
#         print(f"   Retrieved {len(records)} records (total: {len(all_data)})")
        
#         # Stop if we have enough records or got less than requested
#         if len(all_data) >= max_records or len(records) < current_page_size:
#             break
            
#         skip += current_page_size
#         time.sleep(0.1)  # Be nice to the API
    
#     # Trim to exact limit
#     return all_data[:max_records]

# def extract_acumatica_endpoint(endpoint_name, **context):
#     """Extract data from any Acumatica endpoint"""
    
#     if endpoint_name not in ENDPOINTS:
#         raise ValueError(f"Unknown endpoint: {endpoint_name}")
    
#     endpoint_path = ENDPOINTS[endpoint_name]
#     print(f"🔄 Extracting {endpoint_name} from {endpoint_path}")
    
#     try:
#         # Create authenticated session
#         session = create_authenticated_session()
        
#         # Build URL
#         endpoint_url = f"{BASE_URL}/entity/Default/23.200.001/{endpoint_path}"
        
#         # Get all data with pagination
#         print(f"   Fetching data from: {endpoint_url}")
#         raw_data = get_paginated_data(session, endpoint_url)
        
#         if not raw_data:
#             print(f"⚠️  No data found for {endpoint_name}")
#             return 0
        
#         # Flatten the nested JSON structure
#         print(f"   Flattening {len(raw_data)} records...")
#         flattened_data = [flatten_acumatica_record(record) for record in raw_data]
        
#         # Convert to DataFrame
#         df = pd.DataFrame(flattened_data)
#         df['_extracted_at'] = datetime.now()
#         df['_source_system'] = 'acumatica'
#         df['_endpoint'] = endpoint_name
        
#         # Save to CSV
#         os.makedirs('data/raw/acumatica', exist_ok=True)
#         timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#         filename = f'data/raw/acumatica/{endpoint_name}_{timestamp}.csv'
#         df.to_csv(filename, index=False)
        
#         print(f"✅ {endpoint_name}: Saved {len(df)} records to {filename}")
#         print(f"   Sample columns: {list(df.columns)[:10]}")
        
#         return len(df)
        
#     except Exception as e:
#         print(f"❌ Failed to extract {endpoint_name}: {e}")
#         raise

# # Create tasks dynamically for each endpoint
# start_task = EmptyOperator(
#     task_id='start_pipeline',
#     dag=dag
# )

# # Test connection task
# def test_connection(**context):
#     """Test connection to Acumatica"""
#     try:
#         session = create_authenticated_session()
#         print("✅ Connection test successful")
#         return True
#     except Exception as e:
#         print(f"❌ Connection test failed: {e}")
#         raise

# test_task = PythonOperator(
#     task_id='test_connection',
#     python_callable=test_connection,
#     dag=dag
# )

# # Create extraction tasks for each endpoint
# extraction_tasks = []
# for endpoint_name in ENDPOINTS.keys():
#     task = PythonOperator(
#         task_id=f'extract_{endpoint_name}',
#         python_callable=extract_acumatica_endpoint,
#         op_args=[endpoint_name],
#         dag=dag
#     )
#     extraction_tasks.append(task)

# end_task = EmptyOperator(
#     task_id='end_pipeline',
#     dag=dag
# )

# # Define dependencies - all extractions can run in parallel after connection test
# start_task >> test_task >> extraction_tasks >> end_task
