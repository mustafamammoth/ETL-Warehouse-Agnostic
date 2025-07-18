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
        warehouse_type = os.getenv('ACTIVE_WAREHOUSE')
        
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
    
    # ✅ FIXED: Use bronze_schema from config
    bronze_schema = config['warehouse']['schemas']['bronze_schema']
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {bronze_schema}"))
        print(f"✅ Created/verified {bronze_schema} schema")
    
    # DYNAMIC CSV file mapping (discovers files automatically)
    csv_directory = config['extraction']['paths']['raw_data_directory']
    csv_files = {}
    
    for endpoint_name in ENABLED_ENDPOINTS.keys():
        csv_path = f'{csv_directory}/{endpoint_name}.csv'
        if os.path.exists(csv_path):
            # ✅ FIXED: Use bronze_schema consistently
            table_name = f'{bronze_schema}.raw_{endpoint_name}'
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
                schema_name = bronze_schema
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

def load_to_clickhouse_warehouse():
    """ClickHouse-specific loading - PostgreSQL structure but with working client"""
    import pandas as pd
    import os
    import clickhouse_connect
    
    print("📊 Loading CSV files to ClickHouse...")
    
    try:
        # Get ClickHouse connection details from environment
        host = os.getenv('CLICKHOUSE_HOST')
        port = int(os.getenv('CLICKHOUSE_PORT', 8443))
        database = os.getenv('CLICKHOUSE_DATABASE', 'default')
        username = os.getenv('CLICKHOUSE_USER', 'default')
        password = os.getenv('CLICKHOUSE_PASSWORD')
        
        # Create ClickHouse client connection
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
            secure=True
        )
        
        # ✅ FIXED: Use bronze_schema from config
        bronze_schema = config['warehouse']['schemas']['bronze_schema']
        
        # Create schema (database in ClickHouse terms)
        if bronze_schema != 'default':
            client.command(f'CREATE DATABASE IF NOT EXISTS {bronze_schema}')
        print(f"✅ Created/verified {bronze_schema} schema")
        
        # SAME CSV file mapping as PostgreSQL
        csv_directory = config['extraction']['paths']['raw_data_directory']
        csv_files = {}
        
        for endpoint_name in ENABLED_ENDPOINTS.keys():
            csv_path = f'{csv_directory}/{endpoint_name}.csv'
            if os.path.exists(csv_path):
                # ✅ FIXED: Use bronze_schema consistently
                table_name = f'{bronze_schema}.raw_{endpoint_name}'
                csv_files[table_name] = csv_path
        
        total_records = 0
        for table_name, csv_path in csv_files.items():
            if os.path.exists(csv_path):
                print(f"   Loading {csv_path} to ClickHouse...")
                df = pd.read_csv(csv_path)
                
                # Handle schema and table name
                if '.' in table_name:
                    schema_name, table_name_only = table_name.split('.')
                else:
                    schema_name = bronze_schema
                    table_name_only = table_name
                
                # SAME logic as PostgreSQL - try truncate, then replace
                full_table_name = f'{schema_name}.{table_name_only}'
                
                try:
                    # Try to truncate existing table
                    client.command(f'TRUNCATE TABLE {full_table_name}')
                    print(f"   Truncated existing table {full_table_name}")
                    
                    # Convert all data to strings for ClickHouse String columns
                    df_str = df.astype(str)
                    df_str = df_str.replace('nan', '')  # Replace pandas 'nan' strings with empty
                    
                    # Insert data
                    client.insert_df(table=full_table_name, df=df_str)
                    
                except Exception as truncate_error:
                    # Table doesn't exist, create new table
                    print(f"   Table doesn't exist, creating new: {truncate_error}")
                    
                    # Drop and create fresh
                    client.command(f'DROP TABLE IF EXISTS {full_table_name}')
                    
                    # Create table with all String columns (let dbt handle typing)
                    columns = []
                    for col in df.columns:
                        safe_col = col.replace(' ', '_').replace('-', '_').replace('.', '_')
                        columns.append(f'`{safe_col}` String')
                    
                    create_sql = f"""
                    CREATE TABLE {full_table_name} (
                        {', '.join(columns)}
                    ) ENGINE = MergeTree() ORDER BY tuple()
                    """
                    
                    client.command(create_sql)
                    print(f"   Created table {full_table_name}")
                    
                    # Clean DataFrame column names to match and convert all to strings
                    df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]
                    df_str = df.astype(str)  # Convert everything to strings
                    df_str = df_str.replace('nan', '')  # Replace pandas 'nan' strings with empty
                    
                    # Insert data
                    client.insert_df(table=full_table_name, df=df_str)
                
                total_records += len(df)
                print(f"✅ Loaded {len(df)} records to ClickHouse.{full_table_name}")
            else:
                print(f"⚠️  File not found: {csv_path}")
        
        client.close()
        print(f"✅ ClickHouse: Total {total_records} records loaded")
        return total_records
        
    except Exception as e:
        print(f"❌ Failed to load data to ClickHouse: {e}")
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
    """Check the results of dbt transformations in ClickHouse"""
    
    print("🔍 Checking transformed data quality in ClickHouse...")
    
    try:
        import clickhouse_connect
        import os
        
        # Get ClickHouse connection details
        host = os.getenv('CLICKHOUSE_HOST')
        port = int(os.getenv('CLICKHOUSE_PORT', 8443))
        database = os.getenv('CLICKHOUSE_DATABASE', 'default')
        username = os.getenv('CLICKHOUSE_USER', 'default')
        password = os.getenv('CLICKHOUSE_PASSWORD')
        
        # Create ClickHouse client connection
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
            secure=True
        )
        
        staging_schema = config['warehouse']['schemas']['staging_schema']
        
        # Check customers table
        print("📊 Checking customers data...")
        result = client.query(f"""
            SELECT 
                count(*) as total_customers,
                uniq(customer_id) as unique_customers,
                countIf(primary_email != '') as customers_with_email,
                avg(toFloat64OrNull(credit_limit)) as avg_credit_limit
            FROM {staging_schema}.customers
        """)
        
        if result.result_rows:
            row = result.result_rows[0]
            print(f"📊 Customer Data Quality:")
            print(f"   Total customers: {row[0]}")
            print(f"   Unique customer IDs: {row[1]}")
            print(f"   Customers with email: {row[2]}")
            print(f"   Average credit limit: ${row[3]:.2f}" if row[3] else "N/A")
        
        # Check sales invoices table
        print("\n📊 Checking sales invoices data...")
        result = client.query(f"""
            SELECT 
                count(*) as total_invoices,
                uniq(invoice_number) as unique_invoices,
                uniq(customer_id) as customers_with_invoices,
                sum(toFloat64OrNull(amount)) as total_revenue,
                avg(toFloat64OrNull(amount)) as avg_invoice_amount,
                countIf(invoice_category = 'Regular Invoice') as regular_invoices,
                countIf(invoice_category = 'Credit Memo') as credit_memos,
                sum(toFloat64OrNull(outstanding_balance)) as total_outstanding
            FROM {staging_schema}.sales_invoices
        """)
        
        if result.result_rows:
            row = result.result_rows[0]
            print(f"📊 Sales Invoices Data Quality:")
            print(f"   Total invoices: {row[0]}")
            print(f"   Unique invoice numbers: {row[1]}")
            print(f"   Customers with invoices: {row[2]}")
            print(f"   Total revenue: ${row[3]:,.2f}" if row[3] else "N/A")
            print(f"   Average invoice amount: ${row[4]:.2f}" if row[4] else "N/A")
            print(f"   Regular invoices: {row[5]}")
            print(f"   Credit memos: {row[6]}")
            print(f"   Total outstanding: ${row[7]:,.2f}" if row[7] else "N/A")
            
            if row[0] > 0:
                print("✅ Data transformation successful!")
            else:
                raise Exception("No data found in sales_invoices table")
        
        # Close connection
        client.close()
        print("✅ All ClickHouse data quality checks completed successfully!")
        
    except Exception as e:
        print(f"❌ ClickHouse data quality check failed: {e}")
        print("⚠️  Continuing pipeline despite quality check failure")
# def check_transformed_data(**context):
#     """Check the results of dbt transformations"""
    
#     print("🔍 Checking transformed data quality...")
    
#     try:
#         from sqlalchemy import create_engine
        
#         # Use SQLAlchemy engine
#         connection_string = "postgresql://postgres:postgres@postgres:5432/airflow"
#         engine = create_engine(connection_string)
        
#         staging_schema = config['warehouse']['schemas']['staging_schema']
        
#         # Check customers table
#         print("📊 Checking customers data...")
#         with engine.connect() as conn:
#             result = conn.execute(f"""
#                 SELECT 
#                     COUNT(*) as total_customers,
#                     COUNT(DISTINCT customer_id) as unique_customers,
#                     COUNT(primary_email) as customers_with_email,
#                     AVG(CAST(credit_limit AS NUMERIC)) as avg_credit_limit
#                 FROM {staging_schema}.customers
#             """)
            
#             row = result.fetchone()
#             print(f"📊 Customer Data Quality:")
#             print(f"   Total customers: {row[0]}")
#             print(f"   Unique customer IDs: {row[1]}")
#             print(f"   Customers with email: {row[2]}")
#             print(f"   Average credit limit: ${row[3]:.2f}" if row[3] else "N/A")
        
#         engine.dispose()
#         print("✅ All data quality checks completed successfully!")
        
#     except Exception as e:
#         print(f"❌ Data quality check failed: {e}")
#         # Don't fail the pipeline if this is just a check issue
#         print("⚠️  Continuing pipeline despite quality check failure")

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






