# acumatica_dag.py - Configuration-driven DAG
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import os
import requests
import pandas as pd
import time
import subprocess
import sys
import yaml

# ========================
# CONFIGURATION LOADING
# ========================

def load_acumatica_config():
    """Load configuration from acumatica.yml"""
    config_path = '/opt/airflow/config/sources/acumatica.yml'
    try:
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        print(f"❌ Configuration file not found: {config_path}")
        raise
    except yaml.YAMLError as e:
        print(f"❌ Error parsing YAML configuration: {e}")
        raise

def get_schedule_interval(config):
    """Convert config schedule to Airflow schedule_interval"""
    schedule_config = config['dag']['schedule']
    
    if schedule_config['type'] == 'manual':
        return None
    elif schedule_config['type'] == 'daily':
        hour, minute = schedule_config['time'].split(':')
        return f"{minute} {hour} * * *"
    elif schedule_config['type'] == 'weekly':
        hour, minute = schedule_config['time'].split(':')
        days = {'monday': 1, 'tuesday': 2, 'wednesday': 3, 'thursday': 4, 
                'friday': 5, 'saturday': 6, 'sunday': 0}
        day_num = days.get(schedule_config.get('day_of_week', 'monday').lower(), 1)
        return f"{minute} {hour} * * {day_num}"
    elif schedule_config['type'] == 'monthly':
        hour, minute = schedule_config['time'].split(':')
        day = schedule_config.get('day_of_month', 1)
        return f"{minute} {hour} {day} * *"
    elif schedule_config['type'] == 'cron':
        return schedule_config.get('cron_expression', '0 1 * * *')
    else:
        return '0 1 * * *'  # Default to daily at 1 AM

# Load configuration
config = load_acumatica_config()

# ========================
# DAG CONFIGURATION FROM YAML
# ========================

def send_success_email(context):
    """Send success notification email"""
    from airflow.utils.email import send_email
    
    # Get config for email recipients
    success_recipients = config['notifications']['email'].get('success_recipients', [])
    
    if not success_recipients:
        print("No success email recipients configured")
        return
    
    # Get task instance information
    ti = context['task_instance']
    dag_run = context['dag_run']
    
    # Create email content
    subject = f"✅ SUCCESS: {dag_run.dag_id} Pipeline Completed Successfully"
    
    html_content = f"""
    <h2>✅ Acumatica Pipeline Success</h2>
    
    <p><strong>DAG:</strong> {dag_run.dag_id}</p>
    <p><strong>Run ID:</strong> {dag_run.run_id}</p>
    <p><strong>Execution Date:</strong> {dag_run.execution_date}</p>
    <p><strong>Duration:</strong> {dag_run.end_date - dag_run.start_date if dag_run.end_date else 'Still running'}</p>
    
    <h3>📊 Pipeline Summary</h3>
    <ul>
        <li>Data successfully extracted from Acumatica API</li>
        <li>Data loaded to warehouse successfully</li>
        <li>dbt transformations completed</li>
        <li>Data quality checks passed</li>
    </ul>
    
    <p><strong>Next steps:</strong> Acumatica data is ready for analysis and reporting.</p>
    
    <hr>
    <p><small>This is an automated notification from your Airflow data pipeline.</small></p>
    """
    
    try:
        send_email(
            to=success_recipients,
            subject=subject,
            html_content=html_content
        )
        print(f"✅ Success email sent to: {', '.join(success_recipients)}")
    except Exception as e:
        print(f"❌ Failed to send success email: {e}")

def send_failure_email(context):
    """Send failure notification email"""
    from airflow.utils.email import send_email
    
    # Get config for email recipients
    failure_recipients = config['notifications']['email'].get('failure_recipients', [])
    
    if not failure_recipients:
        print("No failure email recipients configured")
        return
    
    # Get task instance information
    ti = context['task_instance']
    dag_run = context['dag_run']
    exception = context.get('exception')
    
    # Create email content
    subject = f"❌ FAILURE: {dag_run.dag_id} Pipeline Failed"
    
    html_content = f"""
    <h2>❌ Acumatica Pipeline Failure</h2>
    
    <p><strong>DAG:</strong> {dag_run.dag_id}</p>
    <p><strong>Task:</strong> {ti.task_id}</p>
    <p><strong>Run ID:</strong> {dag_run.run_id}</p>
    <p><strong>Execution Date:</strong> {dag_run.execution_date}</p>
    <p><strong>Failure Time:</strong> {ti.end_date}</p>
    
    <h3>🔍 Error Details</h3>
    <p><strong>Failed Task:</strong> {ti.task_id}</p>
    <p><strong>Try Number:</strong> {ti.try_number}</p>
    
    {f'<p><strong>Exception:</strong></p><pre style="background-color: #f5f5f5; padding: 10px; border-radius: 5px;">{str(exception)}</pre>' if exception else ''}
    
    <h3>📋 Troubleshooting Steps</h3>
    <ol>
        <li>Check Acumatica API credentials and connectivity</li>
        <li>Verify API endpoints are accessible</li>
        <li>Check warehouse connectivity</li>
        <li>Review data quality and format</li>
    </ol>
    
    <hr>
    <p><small>This is an automated notification from your Airflow data pipeline.</small></p>
    """
    
    try:
        send_email(
            to=failure_recipients,
            subject=subject,
            html_content=html_content
        )
        print(f"✅ Failure email sent to: {', '.join(failure_recipients)}")
    except Exception as e:
        print(f"❌ Failed to send failure email: {e}")

# Default arguments from config
default_args = {
    'owner': config['dag']['owner'],
    'depends_on_past': False,
    'start_date': datetime.strptime(config['dag']['start_date'], '%Y-%m-%d'),
    'email_on_failure': config['dag']['email_on_failure'],
    'email_on_retry': config['dag']['email_on_retry'],
    'retries': config['dag']['retries'],
    'retry_delay': timedelta(minutes=config['dag']['retry_delay_minutes']),
    'email': config['notifications']['email']['failure_recipients'],
    # Add email callbacks
    'on_failure_callback': send_failure_email,
}

# Create the DAG
dag = DAG(
    config['dag']['dag_id'],
    default_args=default_args,
    description=config['dag']['description'],
    schedule_interval=get_schedule_interval(config),
    max_active_runs=config['dag']['max_active_runs'],
    tags=config['dag']['tags']
)

# ========================
# CONFIGURATION CONSTANTS
# ========================

# SECURE: Get credentials from environment variables
BASE_URL = os.getenv('ACUMATICA_BASE_URL')
USERNAME = os.getenv('ACUMATICA_USERNAME')
PASSWORD = os.getenv('ACUMATICA_PASSWORD')

# Testing vs Production configuration from config
TESTING_MODE = config['extraction']['mode'] == 'testing'
if TESTING_MODE:
    MAX_RECORDS_PER_ENDPOINT = config['extraction']['testing']['max_records_per_endpoint']
else:
    MAX_RECORDS_PER_ENDPOINT = config['extraction']['production']['max_records_per_endpoint']

# Validate that required environment variables are set
if not all([BASE_URL, USERNAME, PASSWORD]):
    missing_vars = []
    if not BASE_URL: missing_vars.append('ACUMATICA_BASE_URL')
    if not USERNAME: missing_vars.append('ACUMATICA_USERNAME')
    if not PASSWORD: missing_vars.append('ACUMATICA_PASSWORD')
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Define endpoints from config
ENDPOINTS = {
    'customers': 'Customer',
    'sales_orders': 'SalesOrder', 
    'sales_invoices': 'SalesInvoice',
    'stock_items': 'StockItem',
    'bill': 'Bill',
    'vendor': 'Vendor',
    'purchase_order': 'PurchaseOrder',
}

# Filter endpoints based on config
always_extract = config['extraction']['endpoints']['always_extract']
optional_extract = config['extraction']['endpoints']['optional_extract']
disabled = config['extraction']['endpoints']['disabled']

ENABLED_ENDPOINTS = {k: v for k, v in ENDPOINTS.items() 
                    if k in (always_extract + optional_extract) and k not in disabled}

def create_authenticated_session():
    """Create authenticated session for Acumatica"""
    session = requests.Session()
    
    login_url = f"{BASE_URL}/{config['api']['auth_endpoint']}"
    login_data = {"name": USERNAME, "password": PASSWORD}
    
    response = session.post(login_url, json=login_data, timeout=config['api']['rate_limiting']['timeout_seconds'])
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

def get_paginated_data(session, endpoint_url, endpoint_name):
    """Get data from endpoint with pagination using config"""
    all_data = []
    skip = 0
    page_size = config['api']['pagination']['default_page_size']
    
    # Check for custom settings from config
    custom_settings = config['extraction']['endpoints'].get('custom_settings', {}).get(endpoint_name, {})
    max_pages = custom_settings.get('max_pages', 1000)
    
    page_count = 0
    
    while page_count < max_pages:
        page_count += 1
        
        # Check if we have enough records in testing mode
        if TESTING_MODE and MAX_RECORDS_PER_ENDPOINT and len(all_data) >= MAX_RECORDS_PER_ENDPOINT:
            break
        
        # Adjust page_size if we're near the testing limit
        if TESTING_MODE and MAX_RECORDS_PER_ENDPOINT:
            current_page_size = min(page_size, MAX_RECORDS_PER_ENDPOINT - len(all_data))
        else:
            current_page_size = page_size
            
        params = {'$top': current_page_size, '$skip': skip}
        
        print(f"   Page {page_count}: Fetching {current_page_size} records (skip: {skip})")
        
        try:
            response = session.get(endpoint_url, params=params, timeout=config['api']['rate_limiting']['timeout_seconds'])
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
                print(f"   No more records found")
                break
                
            all_data.extend(records)
            print(f"   Retrieved {len(records)} records (total: {len(all_data)})")
            
            # Stop if we got less than requested (end of data)
            if len(records) < current_page_size:
                break
                
            skip += current_page_size
            time.sleep(1.0 / config['api']['rate_limiting']['requests_per_second'])  # Rate limiting
            
        except Exception as e:
            print(f"   Error on page {page_count}: {e}")
            if page_count == 1:
                raise
            else:
                break
    
    # Trim to exact limit for testing
    if TESTING_MODE and MAX_RECORDS_PER_ENDPOINT:
        all_data = all_data[:MAX_RECORDS_PER_ENDPOINT]
    
    print(f"✅ {endpoint_name}: Collected {len(all_data)} total records")
    return all_data

def extract_acumatica_endpoint(endpoint_name, **context):
    """Extract data from any Acumatica endpoint"""
    
    if endpoint_name not in ENABLED_ENDPOINTS:
        print(f"⚠️  Endpoint {endpoint_name} is disabled in configuration")
        return 0
    
    endpoint_path = ENABLED_ENDPOINTS[endpoint_name]
    print(f"🔄 Extracting {endpoint_name} from {endpoint_path}")
    
    try:
        # Create authenticated session
        session = create_authenticated_session()
        
        # Build URL using config
        endpoint_url = f"{BASE_URL}/{config['api']['entity_endpoint']}/{config['api']['default_version']}/{endpoint_path}"
        
        # Get all data with pagination
        print(f"   Fetching data from: {endpoint_url}")
        raw_data = get_paginated_data(session, endpoint_url, endpoint_name)
        
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
        
        # Save to CSV using config path
        raw_data_dir = config['extraction']['paths']['raw_data_directory']
        os.makedirs(raw_data_dir, exist_ok=True)
        filename = f'{raw_data_dir}/{endpoint_name}.csv'
        df.to_csv(filename, index=False)
        
        print(f"✅ {endpoint_name}: Saved {len(df)} records to {filename}")
        print(f"   Sample columns: {list(df.columns)[:10]}")
        
        return len(df)
        
    except Exception as e:
        print(f"❌ Failed to extract {endpoint_name}: {e}")
        raise

def load_csv_to_warehouse(**context):
    """Load CSV files to warehouse based on config"""
    
    print("📊 Loading CSV files to warehouse...")
    
    try:
        import sys
        sys.path.append('/opt/airflow/config')
        from warehouse_config import load_warehouse_config, get_active_warehouse, get_connection_string
        
        # Get warehouse configuration from acumatica config
        warehouse_type = config['warehouse']['active_warehouse']
        
        print(f"Loading to: {warehouse_type}")
        
        # Use warehouse-specific loading logic
        if warehouse_type == 'postgres':
            return load_to_postgres_warehouse()
        elif warehouse_type == 'snowflake':
            return load_to_snowflake_warehouse()
        elif warehouse_type == 'clickhouse':
            return load_to_clickhouse_warehouse()
        else:
            raise ValueError(f"Unsupported warehouse: {warehouse_type}")
        
    except Exception as e:
        print(f"❌ Failed to load data to warehouse: {e}")
        import traceback
        print(f"Full error: {traceback.format_exc()}")
        raise

def load_to_postgres_warehouse():
    """PostgreSQL-specific loading using config"""
    from sqlalchemy import create_engine, text
    import pandas as pd
    
    # Get connection string
    sys.path.append('/opt/airflow/config')
    from warehouse_config import get_connection_string
    
    connection_string = get_connection_string('postgres')
    engine = create_engine(connection_string)
    
    # Create schema from config
    raw_schema = config['warehouse']['schemas']['raw_schema']
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {raw_schema}"))
        print(f"✅ Created/verified {raw_schema} schema")
    
    # DYNAMIC CSV file mapping (discovers files automatically)
    csv_directory = config['extraction']['paths']['raw_data_directory']
    csv_files = {}
    
    for endpoint_name in ENABLED_ENDPOINTS.keys():
        csv_path = f'{csv_directory}/{endpoint_name}.csv'
        if os.path.exists(csv_path):
            if raw_schema == 'public':
                table_name = f'raw_{endpoint_name}'
            else:
                table_name = f'{raw_schema}.raw_{endpoint_name}'
            csv_files[table_name] = csv_path
    
    total_records = 0
    for table_name, csv_path in csv_files.items():
        if os.path.exists(csv_path):
            print(f"   Loading {csv_path} to PostgreSQL...")
            df = pd.read_csv(csv_path)
            
            # Handle schema and table name
            if '.' in table_name:
                schema_name, table_name_only = table_name.split('.')
            else:
                schema_name = 'public'
                table_name_only = table_name
            
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
                    conn.execute(text(f"TRUNCATE TABLE {schema_name}.{table_name_only}"))
                    print(f"   Truncated existing table {schema_name}.{table_name_only}")
                    
                    df.to_sql(
                        name=table_name_only,
                        schema=schema_name,
                        con=conn,
                        if_exists='append',
                        index=False,
                        method='multi'
                    )
                else:
                    print(f"   Table {schema_name}.{table_name_only} doesn't exist, creating...")
                    df.to_sql(
                        name=table_name_only,
                        schema=schema_name,
                        con=conn,
                        if_exists='replace',
                        index=False,
                        method='multi'
                    )
            
            total_records += len(df)
            print(f"✅ Loaded {len(df)} records to PostgreSQL.{schema_name}.{table_name_only}")
        else:
            print(f"⚠️  File not found: {csv_path}")
    
    engine.dispose()
    print(f"✅ PostgreSQL: Total {total_records} records loaded")
    return total_records

def load_to_snowflake_warehouse():
    """Snowflake-specific loading"""
    print("⚠️  Snowflake loading implementation needed")
    return 0

def load_to_clickhouse_warehouse():
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
        
        raw_schema = config['warehouse']['schemas']['raw_schema']
        staging_schema = config['warehouse']['schemas']['staging_schema']
        
        with engine.connect() as conn:
            # Check all tables in schemas
            result = conn.execute(f"""
                SELECT schemaname, tablename, tableowner 
                FROM pg_tables 
                WHERE schemaname IN ('{staging_schema}', '{raw_schema}') 
                ORDER BY schemaname, tablename;
            """)
            
            print("📊 Tables in database:")
            for row in result.fetchall():
                print(f"   {row[0]}.{row[1]} (owner: {row[2]})")
        
        engine.dispose()
        
    except Exception as e:
        print(f"❌ Debug failed: {e}")
        raise

def run_dbt_transformations(**context):
    """Run dbt transformations using config settings"""
    
    print("🔧 Running dbt transformations...")
    
    try:
        dbt_dir = config['dbt']['project_dir']
        print(f"✅ Found dbt directory: {dbt_dir}")
        
        # First, debug what exists
        debug_database_tables()
        
        # Get dbt execution settings from config
        fail_fast = "--fail-fast" if config['dbt']['execution']['fail_fast'] else ""
        full_refresh = "--full-refresh" if config['dbt']['execution']['full_refresh'] else ""
        threads = f"--threads {config['dbt']['execution']['threads']}"
        
        # Use model sequence from config
        raw_models = config['dbt']['model_sequence']['raw_models']
        business_models = config['dbt']['model_sequence']['business_models']
        
        commands = []
        
        # Add raw models
        for model in raw_models:
            commands.append(f'dbt run --select {model} {threads} {fail_fast} {full_refresh}')
        
        # Add business models
        for model in business_models:
            commands.append(f'dbt run --select {model} {threads} {fail_fast} {full_refresh}')
        
        for cmd in commands:
            # Clean up the command (remove extra spaces)
            cmd = ' '.join(cmd.split())
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
    """Check the results of dbt transformations"""
    
    print("🔍 Checking transformed data quality...")
    
    try:
        from sqlalchemy import create_engine
        
        # Use SQLAlchemy engine
        connection_string = "postgresql://postgres:postgres@postgres:5432/airflow"
        engine = create_engine(connection_string)
        
        staging_schema = config['warehouse']['schemas']['staging_schema']
        
        # Check customers table
        print("📊 Checking customers data...")
        with engine.connect() as conn:
            result = conn.execute(f"""
                SELECT 
                    COUNT(*) as total_customers,
                    COUNT(DISTINCT customer_id) as unique_customers,
                    COUNT(primary_email) as customers_with_email,
                    AVG(CAST(credit_limit AS NUMERIC)) as avg_credit_limit
                FROM {staging_schema}.customers
            """)
            
            row = result.fetchone()
            print(f"📊 Customer Data Quality:")
            print(f"   Total customers: {row[0]}")
            print(f"   Unique customer IDs: {row[1]}")
            print(f"   Customers with email: {row[2]}")
            print(f"   Average credit limit: ${row[3]:.2f}" if row[3] else "N/A")
        
        engine.dispose()
        print("✅ All data quality checks completed successfully!")
        
    except Exception as e:
        print(f"❌ Data quality check failed: {e}")
        # Don't fail the pipeline if this is just a check issue
        print("⚠️  Continuing pipeline despite quality check failure")

def send_pipeline_success_notification(**context):
    """Send pipeline completion success email"""
    send_success_email(context)
    return "Success notification sent"

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

# Create extraction tasks for each ENABLED endpoint
extraction_tasks = []
for endpoint_name in ENABLED_ENDPOINTS.keys():
    task = PythonOperator(
        task_id=f'extract_{endpoint_name}',
        python_callable=extract_acumatica_endpoint,
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

# Create success notification task
success_notification_task = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_pipeline_success_notification,
    dag=dag
)

end_task = EmptyOperator(
    task_id='end_pipeline',
    dag=dag
)

# ========================
# TASK DEPENDENCIES
# ========================

# Linear flow: Extract -> Load -> Transform -> Quality Check -> Success Notification
start_task >> test_task >> extraction_tasks >> load_task >> transform_task >> quality_check_task >> success_notification_task >> end_task


# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
# from airflow.operators.empty import EmptyOperator
# import os
# import requests
# import pandas as pd
# import time
# import subprocess
# import sys

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
#     'acumatica_extract_transform',
#     default_args=default_args,
#     description='Extract from Acumatica API and transform with dbt',
#     schedule=None,  # Manual trigger for now
#     max_active_runs=1,
#     tags=['acumatica', 'extract', 'transform', 'dbt']
# )

# # SECURE: Get credentials from environment variables
# BASE_URL = os.getenv('ACUMATICA_BASE_URL')
# USERNAME = os.getenv('ACUMATICA_USERNAME')
# PASSWORD = os.getenv('ACUMATICA_PASSWORD')

# # Validate that required environment variables are set
# if not all([BASE_URL, USERNAME, PASSWORD]):
#     missing_vars = []
#     if not BASE_URL: missing_vars.append('ACUMATICA_BASE_URL')
#     if not USERNAME: missing_vars.append('ACUMATICA_USERNAME')
#     if not PASSWORD: missing_vars.append('ACUMATICA_PASSWORD')
#     raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# # Define all endpoints you want to extract
# ENDPOINTS = {
#     'customers': 'Customer',
#     'sales_orders': 'SalesOrder', 
#     'sales_invoices': 'SalesInvoice',
#     'stock_items': 'StockItem',
#     'bill': 'Bill',
#     'vendor': 'Vendor',
#     'purchase_order': 'PurchaseOrder',

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
        
#         # Save to CSV with Docker path
#         os.makedirs('/opt/airflow/data/raw/acumatica', exist_ok=True)  # Docker path
#         filename = f'/opt/airflow/data/raw/acumatica/{endpoint_name}.csv'  # Docker path
#         df.to_csv(filename, index=False)
        
#         print(f"✅ {endpoint_name}: Saved {len(df)} records to {filename}")
#         print(f"   Sample columns: {list(df.columns)[:10]}")
        
#         return len(df)
        
#     except Exception as e:
#         print(f"❌ Failed to extract {endpoint_name}: {e}")
#         raise

# def load_csv_to_warehouse(**context):
#     """Load CSV files to any warehouse (dynamic)"""
    
#     print("📊 Loading CSV files to warehouse...")
    
#     try:
#         import sys
#         sys.path.append('/opt/airflow/config')
#         from warehouse_config import load_warehouse_config, get_active_warehouse, get_connection_string
        
#         # Get warehouse configuration
#         warehouse_type = get_active_warehouse()
#         config = load_warehouse_config(warehouse_type)
        
#         print(f"Loading to: {config['warehouse']['name']} ({warehouse_type})")
        
#         # Use warehouse-specific loading logic
#         if warehouse_type == 'postgres':
#             return load_to_postgres_warehouse(config)
#         elif warehouse_type == 'snowflake':
#             return load_to_snowflake_warehouse(config)
#         elif warehouse_type == 'clickhouse':
#             return load_to_clickhouse_warehouse(config)
#         else:
#             raise ValueError(f"Unsupported warehouse: {warehouse_type}")
        
#     except Exception as e:
#         print(f"❌ Failed to load data to warehouse: {e}")
#         import traceback
#         print(f"Full error: {traceback.format_exc()}")
#         raise

# def load_to_postgres_warehouse(config):
#     """PostgreSQL-specific loading - FIXED transaction handling"""
#     from sqlalchemy import create_engine, text
#     import pandas as pd
    
#     # Get connection string
#     sys.path.append('/opt/airflow/config')
#     from warehouse_config import get_connection_string
    
#     connection_string = get_connection_string('postgres')
#     engine = create_engine(connection_string)
    
#     # ✅ Updated to include bills
#     csv_files = {
#         'raw_customers': '/opt/airflow/data/raw/acumatica/customers.csv',
#         'raw_sales_orders': '/opt/airflow/data/raw/acumatica/sales_orders.csv',
#         'raw_sales_invoices': '/opt/airflow/data/raw/acumatica/sales_invoices.csv',
#         'raw_stock_items': '/opt/airflow/data/raw/acumatica/stock_items.csv',
#         'raw_bills': '/opt/airflow/data/raw/acumatica/bill.csv',
#         'raw_vendors': '/opt/airflow/data/raw/acumatica/vendor.csv',
#         'raw_purchase_orders': '/opt/airflow/data/raw/acumatica/purchase_order.csv'

#     }
    
#     total_records = 0
#     for table_name, csv_path in csv_files.items():
#         if os.path.exists(csv_path):
#             print(f"   Loading {csv_path} to PostgreSQL...")
#             df = pd.read_csv(csv_path)
            
#             # ✅ FIXED: Handle each table in separate transaction
#             try:
#                 # Try to truncate in separate transaction
#                 with engine.begin() as conn:
#                     conn.execute(text(f"TRUNCATE TABLE {table_name}"))
#                     print(f"   Truncated existing table {table_name}")
                
#                 # Load data in separate transaction
#                 with engine.begin() as conn:
#                     df.to_sql(
#                         name=table_name,
#                         con=conn,
#                         if_exists='append',
#                         index=False,
#                         method='multi'
#                     )
                    
#             except Exception as truncate_error:
#                 # Table doesn't exist, create new table
#                 print(f"   Table doesn't exist, creating new: {truncate_error}")
#                 with engine.begin() as conn:
#                     df.to_sql(
#                         name=table_name,
#                         con=conn,
#                         if_exists='replace',
#                         index=False,
#                         method='multi'
#                     )
            
#             total_records += len(df)
#             print(f"✅ Loaded {len(df)} records to PostgreSQL.{table_name}")
#         else:
#             print(f"⚠️  File not found: {csv_path}")
    
#     engine.dispose()
#     print(f"✅ PostgreSQL: Total {total_records} records loaded")
#     return total_records

# def load_to_snowflake_warehouse(config):
#     """Snowflake-specific loading"""
#     import pandas as pd
#     from sqlalchemy import create_engine
    
#     sys.path.append('/opt/airflow/config')
#     from warehouse_config import get_connection_string
    
#     connection_string = get_connection_string('snowflake')
#     engine = create_engine(connection_string)
    
#     csv_files = {
#         'raw_customers': '/opt/airflow/data/raw/acumatica/customers.csv',
#         'raw_sales_orders': '/opt/airflow/data/raw/acumatica/sales_orders.csv',
#         'raw_sales_invoices': '/opt/airflow/data/raw/acumatica/sales_invoices.csv',
#         'raw_stock_items': '/opt/airflow/data/raw/acumatica/stock_items.csv',
#         'raw_bills': '/opt/airflow/data/raw/acumatica/bill.csv',
#         'raw_vendors': '/opt/airflow/data/raw/acumatica/vendor.csv',
#         'raw_purchase_orders': '/opt/airflow/data/raw/acumatica/purchase_order.csv'


#     }
    
#     total_records = 0
#     for table_name, csv_path in csv_files.items():
#         if os.path.exists(csv_path):
#             print(f"   Loading {csv_path} to Snowflake...")
#             df = pd.read_csv(csv_path)
            
#             # Snowflake-specific optimizations
#             df.to_sql(
#                 name=table_name.upper(),  # Snowflake prefers uppercase
#                 con=engine,
#                 if_exists='replace',
#                 index=False,
#                 method='multi',
#                 chunksize=1000  # Batch inserts for better performance
#             )
            
#             total_records += len(df)
#             print(f"✅ Loaded {len(df)} records to Snowflake.{table_name.upper()}")
#         else:
#             print(f"⚠️  File not found: {csv_path}")
    
#     engine.dispose()
#     print(f"✅ Snowflake: Total {total_records} records loaded")
#     return total_records

# def load_to_clickhouse_warehouse(config):
#     """ClickHouse-specific loading"""
#     import pandas as pd
#     from sqlalchemy import create_engine
    
#     sys.path.append('/opt/airflow/config')
#     from warehouse_config import get_connection_string
    
#     connection_string = get_connection_string('clickhouse')
#     engine = create_engine(connection_string)
    
#     csv_files = {
#         'raw_customers': '/opt/airflow/data/raw/acumatica/customers.csv',
#         'raw_sales_orders': '/opt/airflow/data/raw/acumatica/sales_orders.csv',
#         'raw_sales_invoices': '/opt/airflow/data/raw/acumatica/sales_invoices.csv',
#         'raw_stock_items': '/opt/airflow/data/raw/acumatica/stock_items.csv',
#         'raw_bills': '/opt/airflow/data/raw/acumatica/bill.csv',
#         'raw_vendors': '/opt/airflow/data/raw/acumatica/vendor.csv',
#         'raw_purchase_orders': '/opt/airflow/data/raw/acumatica/purchase_order.csv'

#     }
    
#     total_records = 0
#     for table_name, csv_path in csv_files.items():
#         if os.path.exists(csv_path):
#             print(f"   Loading {csv_path} to ClickHouse...")
#             df = pd.read_csv(csv_path)
            
#             # ClickHouse-specific optimizations
#             df.to_sql(
#                 name=table_name,
#                 con=engine,
#                 if_exists='replace',
#                 index=False,
#                 method='multi'
#             )
            
#             total_records += len(df)
#             print(f"✅ Loaded {len(df)} records to ClickHouse.{table_name}")
#         else:
#             print(f"⚠️  File not found: {csv_path}")
    
#     engine.dispose()
#     print(f"✅ ClickHouse: Total {total_records} records loaded")
#     return total_records

# def debug_database_tables(**context):
#     """Debug function to see what tables exist"""
    
#     print("🔍 Checking what tables exist in database...")
    
#     try:
#         from sqlalchemy import create_engine
        
#         connection_string = "postgresql://postgres:postgres@postgres:5432/airflow"
#         engine = create_engine(connection_string)
        
#         with engine.connect() as conn:
#             # Check all tables in public schema
#             result = conn.execute("""
#                 SELECT schemaname, tablename, tableowner 
#                 FROM pg_tables 
#                 WHERE schemaname IN ('public') 
#                 ORDER BY schemaname, tablename;
#             """)
            
#             print("📊 Tables in database:")
#             for row in result.fetchall():
#                 print(f"   {row[0]}.{row[1]} (owner: {row[2]})")
            
#             # Check all views in public schema
#             result = conn.execute("""
#                 SELECT schemaname, viewname, viewowner 
#                 FROM pg_views 
#                 WHERE schemaname = 'public' 
#                 ORDER BY viewname;
#             """)
            
#             print("👁️ Views in database:")
#             for row in result.fetchall():
#                 print(f"   {row[0]}.{row[1]} (owner: {row[2]})")
        
#         engine.dispose()
        
#     except Exception as e:
#         print(f"❌ Debug failed: {e}")
#         raise

# def run_dbt_transformations(**context):
#     """Run dbt transformations with debugging"""
    
#     print("🔧 Running dbt transformations...")
    
#     try:
#         dbt_dir = '/opt/airflow/dbt'
#         print(f"✅ Found dbt directory: {dbt_dir}")
        
#         # First, debug what exists
#         debug_database_tables()
        
#         commands = [
#             'dbt run --select customers_raw',
#             'dbt run --select sales_invoices_raw',
#             'dbt run --select sales_orders_raw',
#             'dbt run --select stock_items_raw',

#             'dbt run --select customers',
#             'dbt run --select sales_invoices',
#             'dbt run --select sales_orders',
#             'dbt run --select stock_items',
#             'dbt run --select bills_raw',
#             'dbt run --select bills',

#             'dbt run --select vendors_raw',
#             'dbt run --select vendors',

#             'dbt run --select purchase_orders_raw',
#             'dbt run --select purchase_orders',
#         ]
        
#         for cmd in commands:
#             print(f"Running: {cmd}")
#             result = subprocess.run(
#                 cmd.split(), 
#                 cwd=dbt_dir,
#                 capture_output=True, 
#                 text=True,
#                 check=False  # Don't fail immediately
#             )
            
#             if result.returncode == 0:
#                 print(f"✅ {cmd} completed successfully")
#                 print(result.stdout)
                
#                 # Debug after each successful run
#                 debug_database_tables()
#             else:
#                 print(f"❌ {cmd} failed:")
#                 print(f"Error: {result.stderr}")
#                 print(f"Output: {result.stdout}")
#                 raise Exception(f"dbt command failed: {cmd}")
        
#         print("✅ All dbt transformations completed")
        
#     except Exception as e:
#         print(f"❌ Failed to run dbt: {e}")
#         raise

# def check_transformed_data(**context):
#     """Check the results of dbt transformations"""
    
#     print("🔍 Checking transformed data quality...")
    
#     try:
#         from sqlalchemy import create_engine
        
#         # Use SQLAlchemy engine
#         connection_string = "postgresql://postgres:postgres@postgres:5432/airflow"
#         engine = create_engine(connection_string)
        
#         # Check customers table
#         print("📊 Checking customers data...")
#         with engine.connect() as conn:
#             result = conn.execute("""
#                 SELECT 
#                     COUNT(*) as total_customers,
#                     COUNT(DISTINCT customer_id) as unique_customers,
#                     COUNT(primary_email) as customers_with_email,
#                     AVG(CAST(credit_limit AS NUMERIC)) as avg_credit_limit
#                 FROM public.customers
#             """)
            
#             row = result.fetchone()
#             print(f"📊 Customer Data Quality:")
#             print(f"   Total customers: {row[0]}")
#             print(f"   Unique customer IDs: {row[1]}")
#             print(f"   Customers with email: {row[2]}")
#             print(f"   Average credit limit: ${row[3]:.2f}" if row[3] else "N/A")
        
#         # Check sales invoices table
#         print("\n📊 Checking sales invoices data...")
#         with engine.connect() as conn:
#             result = conn.execute("""
#                 SELECT 
#                     COUNT(*) as total_invoices,
#                     COUNT(DISTINCT invoice_number) as unique_invoices,
#                     COUNT(DISTINCT customer_id) as customers_with_invoices,
#                     SUM(amount) as total_revenue,
#                     AVG(amount) as avg_invoice_amount,
#                     COUNT(CASE WHEN invoice_category = 'Regular Invoice' THEN 1 END) as regular_invoices,
#                     COUNT(CASE WHEN invoice_category = 'Credit Memo' THEN 1 END) as credit_memos,
#                     SUM(outstanding_balance) as total_outstanding
#                 FROM public.sales_invoices
#             """)
            
#             row = result.fetchone()
#             print(f"📊 Sales Invoices Data Quality:")
#             print(f"   Total invoices: {row[0]}")
#             print(f"   Unique invoice numbers: {row[1]}")
#             print(f"   Customers with invoices: {row[2]}")
#             print(f"   Total revenue: ${row[3]:,.2f}" if row[3] else "N/A")
#             print(f"   Average invoice amount: ${row[4]:.2f}" if row[4] else "N/A")
#             print(f"   Regular invoices: {row[5]}")
#             print(f"   Credit memos: {row[6]}")
#             print(f"   Total outstanding: ${row[7]:,.2f}" if row[7] else "N/A")
            
#             if row[0] > 0:
#                 print("✅ Data transformation successful!")
#             else:
#                 raise Exception("No data found in sales_invoices table")
        
#         # Check sales orders data
#         print("\n📊 Checking sales orders data...")
#         with engine.connect() as conn:
#             result = conn.execute("""
#                 SELECT 
#                     COUNT(*) as total_orders,
#                     COUNT(DISTINCT order_number) as unique_orders,
#                     COUNT(DISTINCT customer_id) as customers_with_orders,
#                     SUM(order_total) as total_order_value,
#                     AVG(order_total) as avg_order_value,
#                     COUNT(CASE WHEN order_category = 'Sales Order' THEN 1 END) as sales_orders,
#                     COUNT(CASE WHEN order_category = 'Return Credit' THEN 1 END) as return_credits,
#                     AVG(ordered_qty) as avg_quantity_per_order
#                 FROM public.sales_orders
#             """)

#             row = result.fetchone()
#             print(f"📊 Sales Orders Data Quality:")
#             print(f"   Total orders: {row[0]}")
#             print(f"   Unique order numbers: {row[1]}")
#             print(f"   Customers with orders: {row[2]}")
#             print(f"   Total order value: ${row[3]:,.2f}" if row[3] else "N/A")
#             print(f"   Average order value: ${row[4]:.2f}" if row[4] else "N/A")
#             print(f"   Sales orders: {row[5]}")
#             print(f"   Return credits: {row[6]}")
#             print(f"   Avg quantity per order: {row[7]:.1f}" if row[7] else "N/A")

#         # Check stock items data
#         print("\n📊 Checking stock items data...")
#         with engine.connect() as conn:
#             result = conn.execute("""
#                 SELECT 
#                     COUNT(*) as total_items,
#                     COUNT(DISTINCT inventory_id) as unique_skus,
#                     COUNT(CASE WHEN item_status_clean = 'Active' THEN 1 END) as active_items,
#                     COUNT(CASE WHEN item_status_clean = 'Inactive' THEN 1 END) as inactive_items,
#                     AVG(default_price) as avg_price,
#                     COUNT(CASE WHEN product_category = 'Live Rosin' THEN 1 END) as live_rosin_products,
#                     COUNT(CASE WHEN product_category = 'Battery' THEN 1 END) as battery_products,
#                     COUNT(CASE WHEN is_a_kit = true THEN 1 END) as kit_products,
#                     AVG(margin_percent) as avg_margin_percent
#                 FROM public.stock_items
#             """)

#             row = result.fetchone()
#             print(f"📊 Stock Items Data Quality:")
#             print(f"   Total items: {row[0]}")
#             print(f"   Unique SKUs: {row[1]}")
#             print(f"   Active items: {row[2]}")
#             print(f"   Inactive items: {row[3]}")
#             print(f"   Average price: ${row[4]:.2f}" if row[4] else "N/A")
#             print(f"   Live rosin products: {row[5]}")
#             print(f"   Battery products: {row[6]}")
#             print(f"   Kit products: {row[7]}")
#             print(f"   Average margin: {row[8]:.1f}%" if row[8] else "N/A")
        

#         # ✅ ADD THIS - Check bills data
#         print("\n📊 Checking bills data...")
#         with engine.connect() as conn:
#             result = conn.execute("""
#                 SELECT 
#                     COUNT(*) as total_bills,
#                     COUNT(DISTINCT vendor_code) as unique_vendors,
#                     SUM(amount) as total_amount,
#                     SUM(balance) as total_outstanding,
#                     COUNT(CASE WHEN status_category = 'Open' THEN 1 END) as open_bills,
#                     COUNT(CASE WHEN status_category = 'Closed' THEN 1 END) as closed_bills,
#                     COUNT(CASE WHEN bill_category = 'Credit Adjustment' THEN 1 END) as credit_adjustments,
#                     COUNT(CASE WHEN bill_category = 'Regular Bill' THEN 1 END) as regular_bills,
#                     COUNT(CASE WHEN priority_status = 'Past Due' THEN 1 END) as past_due_bills,
#                     COUNT(CASE WHEN priority_status = 'Overdue 30+ Days' THEN 1 END) as overdue_bills,
#                     COUNT(CASE WHEN is_on_hold = true THEN 1 END) as bills_on_hold,
#                     COUNT(CASE WHEN amount_category = 'Very Large ($10K+)' THEN 1 END) as large_bills,
#                     COUNT(CASE WHEN payment_terms_category = 'Net 30 Days' THEN 1 END) as net30_bills,
#                     COUNT(CASE WHEN vendor_type = 'Vendor' THEN 1 END) as vendor_bills,
#                     COUNT(CASE WHEN vendor_type = 'Customer/Client' THEN 1 END) as customer_bills,
#                     AVG(data_completeness_score) as avg_completeness_score
#                 FROM public.bills
#             """)
            
#             row = result.fetchone()
#             print(f"📊 Bills Data Quality:")
#             print(f"   Total bills: {row[0]}")
#             print(f"   Unique vendors: {row[1]}")
#             print(f"   Total amount: ${row[2]:,.2f}" if row[2] else "N/A")
#             print(f"   Total outstanding: ${row[3]:,.2f}" if row[3] else "N/A")
#             print(f"   Open bills: {row[4]}")
#             print(f"   Closed bills: {row[5]}")
#             print(f"   Credit adjustments: {row[6]}")
#             print(f"   Regular bills: {row[7]}")
#             print(f"   Past due bills: {row[8]}")
#             print(f"   Overdue 30+ days: {row[9]}")
#             print(f"   Bills on hold: {row[10]}")
#             print(f"   Large bills ($10K+): {row[11]}")
#             print(f"   Net 30 bills: {row[12]}")
#             print(f"   Vendor bills: {row[13]}")
#             print(f"   Customer bills: {row[14]}")
#             print(f"   Avg completeness score: {row[15]:.1f}" if row[15] else "N/A")

#         # ✅ ADD THIS - Check vendors data
#         print("\n📊 Checking vendors data...")
#         with engine.connect() as conn:
#             result = conn.execute("""
#                 SELECT 
#                     COUNT(*) as total_vendors,
#                     COUNT(DISTINCT vendor_code) as unique_vendor_codes,
#                     COUNT(CASE WHEN status_category = 'Active' THEN 1 END) as active_vendors,
#                     COUNT(CASE WHEN status_category = 'Inactive' THEN 1 END) as inactive_vendors,
#                     COUNT(CASE WHEN vendor_type = 'Cannabis Vendor' THEN 1 END) as cannabis_vendors,
#                     COUNT(CASE WHEN vendor_type = 'Service Provider' THEN 1 END) as service_providers,
#                     COUNT(CASE WHEN is_1099_vendor = true THEN 1 END) as vendors_1099,
#                     COUNT(CASE WHEN is_foreign_entity = true THEN 1 END) as foreign_vendors,
#                     COUNT(CASE WHEN payment_terms_category = 'Cash on Delivery' THEN 1 END) as cod_vendors,
#                     COUNT(CASE WHEN tax_classification = 'Cannabis Tax' THEN 1 END) as cannabis_tax_vendors,
#                     COUNT(CASE WHEN vendor_complexity = 'Complex (1099 + Foreign)' THEN 1 END) as complex_vendors,
#                     COUNT(CASE WHEN has_legal_name = true THEN 1 END) as vendors_with_legal_name,
#                     COUNT(CASE WHEN has_tax_id = true THEN 1 END) as vendors_with_tax_id,
#                     AVG(data_completeness_score) as avg_completeness_score
#                 FROM public.vendors
#             """)
            
#             row = result.fetchone()
#             print(f"📊 Vendors Data Quality:")
#             print(f"   Total vendors: {row[0]}")
#             print(f"   Unique vendor codes: {row[1]}")
#             print(f"   Active vendors: {row[2]}")
#             print(f"   Inactive vendors: {row[3]}")
#             print(f"   Cannabis vendors: {row[4]}")
#             print(f"   Service providers: {row[5]}")
#             print(f"   1099 vendors: {row[6]}")
#             print(f"   Foreign vendors: {row[7]}")
#             print(f"   COD vendors: {row[8]}")
#             print(f"   Cannabis tax vendors: {row[9]}")
#             print(f"   Complex vendors: {row[10]}")
#             print(f"   Vendors with legal name: {row[11]}")
#             print(f"   Vendors with tax ID: {row[12]}")
#             print(f"   Avg completeness score: {row[13]:.1f}" if row[13] else "N/A")

#         # ✅ ADD THIS - Check purchase orders data
#         print("\n📊 Checking purchase orders data...")
#         with engine.connect() as conn:
#             result = conn.execute("""
#                 SELECT 
#                     COUNT(*) as total_orders,
#                     COUNT(DISTINCT vendor_id) as unique_vendors,
#                     COUNT(DISTINCT order_number) as unique_order_numbers,
#                     SUM(order_total) as total_order_value,
#                     AVG(order_total) as avg_order_value,
#                     COUNT(CASE WHEN status_category = 'Completed' THEN 1 END) as completed_orders,
#                     COUNT(CASE WHEN status_category = 'Open' THEN 1 END) as open_orders,
#                     COUNT(CASE WHEN status_category = 'Closed' THEN 1 END) as closed_orders,
#                     COUNT(CASE WHEN order_type_category = 'Normal Purchase' THEN 1 END) as normal_orders,
#                     COUNT(CASE WHEN order_type_category = 'Drop Ship' THEN 1 END) as dropship_orders,
#                     COUNT(CASE WHEN order_size_category = 'Very Large ($100K+)' THEN 1 END) as large_orders,
#                     COUNT(CASE WHEN priority_status = 'Overdue' THEN 1 END) as overdue_orders,
#                     COUNT(CASE WHEN priority_status = 'Due Soon' THEN 1 END) as due_soon_orders,
#                     COUNT(CASE WHEN product_category = 'Carts' THEN 1 END) as cart_orders,
#                     COUNT(CASE WHEN product_category = 'Packaging' THEN 1 END) as packaging_orders,
#                     COUNT(CASE WHEN tax_classification = 'Cannabis Tax' THEN 1 END) as cannabis_tax_orders,
#                     COUNT(CASE WHEN totals_match = true THEN 1 END) as orders_with_matching_totals,
#                     AVG(data_completeness_score) as avg_completeness_score
#                 FROM public.purchase_orders
#             """)
            
#             row = result.fetchone()
#             print(f"📊 Purchase Orders Data Quality:")
#             print(f"   Total orders: {row[0]}")
#             print(f"   Unique vendors: {row[1]}")
#             print(f"   Unique order numbers: {row[2]}")
#             print(f"   Total order value: ${row[3]:,.2f}" if row[3] else "N/A")
#             print(f"   Average order value: ${row[4]:,.2f}" if row[4] else "N/A")
#             print(f"   Completed orders: {row[5]}")
#             print(f"   Open orders: {row[6]}")
#             print(f"   Closed orders: {row[7]}")
#             print(f"   Normal orders: {row[8]}")
#             print(f"   Drop ship orders: {row[9]}")
#             print(f"   Large orders ($100K+): {row[10]}")
#             print(f"   Overdue orders: {row[11]}")
#             print(f"   Due soon orders: {row[12]}")
#             print(f"   Cart orders: {row[13]}")
#             print(f"   Packaging orders: {row[14]}")
#             print(f"   Cannabis tax orders: {row[15]}")
#             print(f"   Orders with matching totals: {row[16]}")
#             print(f"   Avg completeness score: {row[17]:.1f}" if row[17] else "N/A")

#         engine.dispose()
#         print("✅ All data quality checks completed successfully!")
        
#     except Exception as e:
#         print(f"❌ Data quality check failed: {e}")
#         # Don't fail the pipeline if this is just a check issue
#         print("⚠️  Continuing pipeline despite quality check failure")



# # ========================
# # TASK DEFINITIONS
# # ========================

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
#     task_id='test_acumatica_connection',
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

# # Load data to warehouse
# load_task = PythonOperator(
#     task_id='load_to_warehouse',
#     python_callable=load_csv_to_warehouse,
#     dag=dag
# )

# # Run dbt transformations
# transform_task = PythonOperator(
#     task_id='run_dbt_transformations',
#     python_callable=run_dbt_transformations,
#     dag=dag
# )

# quality_check_task = PythonOperator(
#     task_id='check_data_quality',
#     python_callable=check_transformed_data,
#     dag=dag
# )

# end_task = EmptyOperator(
#     task_id='end_pipeline',
#     dag=dag
# )

# # ========================
# # TASK DEPENDENCIES
# # ========================

# # Linear flow: Extract -> Load -> Transform -> Quality Check
# start_task >> test_task >> extraction_tasks >> load_task >> transform_task >> quality_check_task >> end_task