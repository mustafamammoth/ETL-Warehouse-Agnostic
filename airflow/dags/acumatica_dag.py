# acumatica_dag.py - Dynamic, incremental-aware pipeline (FIXED)
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import os
import subprocess
import sys
import yaml
import re
from glob import glob
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Allow importing extractor
sys.path.append('/opt/airflow')
sys.path.append('/opt/airflow/config')

from extractors.acumatica import extractor as acx

ENV_PATTERN = re.compile(r'^\$\{([A-Z0-9_]+)\}$')

def _sub_env(obj):
    """Substitute environment variables in config."""
    if isinstance(obj, dict):
        return {k: _sub_env(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sub_env(v) for v in obj]
    if isinstance(obj, str):
        m = ENV_PATTERN.match(obj.strip())
        if m:
            return os.getenv(m.group(1), obj)
    return obj

def load_acumatica_config():
    """Load and validate Acumatica configuration."""
    config_path = '/opt/airflow/config/sources/acumatica.yml'
    try:
        with open(config_path, 'r') as f:
            raw = yaml.safe_load(f)
        config = _sub_env(raw)
        
        # Validate essential configuration
        required_keys = ['dag', 'api', 'extraction', 'warehouse']
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required config section: {key}")
        
        logger.info(f"✅ Configuration loaded from {config_path}")
        return config
    except FileNotFoundError:
        logger.error(f"❌ Configuration file not found: {config_path}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"❌ YAML parse error: {e}")
        raise

def get_schedule_interval(config):
    """Convert schedule config to cron expression."""
    schedule_config = config['dag']['schedule']
    stype = schedule_config.get('type')
    
    if stype == 'manual': 
        return None
    if stype == 'hourly': 
        return "0 * * * *"
    if stype == 'daily':
        t = schedule_config.get('time', "01:00")
        h, m = t.split(':')
        return f"{m} {h} * * *"
    if stype == 'weekly':
        t = schedule_config.get('time', "01:00")
        h, m = t.split(':')
        days = {'monday': 1, 'tuesday': 2, 'wednesday': 3, 'thursday': 4, 
                'friday': 5, 'saturday': 6, 'sunday': 0}
        dnum = days.get(schedule_config.get('day_of_week', 'monday').lower(), 1)
        return f"{m} {h} * * {dnum}"
    if stype == 'monthly':
        t = schedule_config.get('time', "01:00")
        h, m = t.split(':')
        day = schedule_config.get('day_of_month', 1)
        return f"{m} {h} {day} * *"
    if stype == 'cron':
        return schedule_config.get('cron_expression', '0 1 * * *')
    
    return '0 1 * * *'  # Default daily at 1 AM

# Load configuration and initialize extractor
config = load_acumatica_config()
enabled_endpoints = acx.init_extractor(config)
logger.info(f"✅ Enabled endpoints: {list(enabled_endpoints.keys())}")

# ---------------- EMAIL CALLBACKS ---------------- #
def send_success_email(context):
    """Send success notification email."""
    try:
        from airflow.utils.email import send_email
        recipients = config['notifications']['email'].get('success_recipients', [])
        if not recipients: 
            return
            
        dag_run = context['dag_run']
        subject = f"✅ SUCCESS: {dag_run.dag_id}"
        html = f"""
        <h3>Acumatica Pipeline Success</h3>
        <p><strong>DAG:</strong> {dag_run.dag_id}</p>
        <p><strong>Run ID:</strong> {dag_run.run_id}</p>
        <p><strong>Execution Date:</strong> {dag_run.execution_date}</p>
        <p><strong>Start Date:</strong> {dag_run.start_date}</p>
        """
        
        send_email(to=recipients, subject=subject, html_content=html)
        logger.info(f"✅ Success email sent to {recipients}")
    except Exception as e:
        logger.error(f"❌ Failed to send success email: {e}")

def send_failure_email(context):
    """Send failure notification email."""
    try:
        from airflow.utils.email import send_email
        recipients = config['notifications']['email'].get('failure_recipients', [])
        if not recipients:
            return
            
        ti = context['task_instance']
        ex = context.get('exception', 'Unknown error')
        subject = f"❌ FAILURE: {ti.dag_id}.{ti.task_id}"
        html = f"""
        <h3>Acumatica Pipeline Failure</h3>
        <p><strong>DAG:</strong> {ti.dag_id}</p>
        <p><strong>Task:</strong> {ti.task_id}</p>
        <p><strong>Execution Date:</strong> {ti.execution_date}</p>
        <p><strong>Error:</strong></p>
        <pre>{ex}</pre>
        """
        
        send_email(to=recipients, subject=subject, html_content=html)
        logger.info(f"✅ Failure email sent to {recipients}")
    except Exception as e:
        logger.error(f"❌ Failed to send failure email: {e}")

# DAG default arguments
default_args = {
    'owner': config['dag']['owner'],
    'depends_on_past': False,
    'start_date': datetime.strptime(config['dag']['start_date'], '%Y-%m-%d'),
    'email_on_failure': config['dag']['email_on_failure'],
    'email_on_retry': config['dag']['email_on_retry'],
    'retries': config['dag']['retries'],
    'retry_delay': timedelta(minutes=config['dag']['retry_delay_minutes']),
    'email': config['notifications']['email']['failure_recipients'],
    'on_failure_callback': send_failure_email,
}

# Create DAG
dag = DAG(
    config['dag']['dag_id'],
    default_args=default_args,
    description=config['dag']['description'],
    schedule_interval=get_schedule_interval(config),
    max_active_runs=config['dag']['max_active_runs'],
    tags=config['dag']['tags'],
    catchup=False  # Important for incremental workflows
)

# ---------------- CONNECTION TEST ---------------- #
def test_connection(**context):
    """Test Acumatica connection."""
    try:
        session = acx.create_authenticated_session()
        logger.info("✅ Connection test successful")
        return True
    except Exception as e:
        logger.error(f"❌ Connection test failed: {e}")
        raise

# ---------------- WAREHOUSE LOADING (ClickHouse specific) ---------------- #
def load_csv_to_warehouse(**context):
    """Load CSV files to ClickHouse warehouse."""
    logger.info("📊 Loading CSV files to ClickHouse warehouse...")
    
    wh = os.getenv('ACTIVE_WAREHOUSE', config['warehouse']['active_warehouse'])
    if wh != 'clickhouse':
        raise ValueError(f"Only ClickHouse warehouse is supported, got: {wh}")
    
    return load_to_clickhouse_warehouse()

def load_to_clickhouse_warehouse():
    """Load data to ClickHouse with improved error handling."""
    import pandas as pd
    import clickhouse_connect
    
    # Configuration
    bronze_schema = config['warehouse']['schemas']['bronze_schema']
    csv_dir = config['extraction']['paths']['raw_data_directory']
    
    logger.info(f"📊 Loading CSV files to ClickHouse schema: {bronze_schema}")
    
    # Connection parameters
    host = os.getenv('CLICKHOUSE_HOST')
    port = int(os.getenv('CLICKHOUSE_PORT', 8443))
    database = os.getenv('CLICKHOUSE_DATABASE', 'default')
    username = os.getenv('CLICKHOUSE_USER', 'default')
    password = os.getenv('CLICKHOUSE_PASSWORD')
    
    if not all([host, password]):
        raise ValueError("ClickHouse connection parameters missing")
    
    # Connect to ClickHouse
    try:
        client = clickhouse_connect.get_client(
            host=host, port=port, username=username, password=password,
            database=database, secure=True
        )
        logger.info("✅ Connected to ClickHouse")
    except Exception as e:
        logger.error(f"❌ Failed to connect to ClickHouse: {e}")
        raise
    
    # Create schema if needed
    if bronze_schema != 'default':
        try:
            client.command(f"CREATE DATABASE IF NOT EXISTS {bronze_schema}")
            logger.info(f"✅ Ensured schema exists: {bronze_schema}")
        except Exception as e:
            logger.error(f"❌ Failed to create schema: {e}")
            raise
    
    # Process CSV files
    csv_files = glob(os.path.join(csv_dir, '*.csv'))
    if not csv_files:
        logger.warning(f"⚠️ No CSV files found in {csv_dir}")
        return 0
    
    total_records = 0
    
    for csv_path in csv_files:
        endpoint_key = os.path.splitext(os.path.basename(csv_path))[0]
        table_name = f"raw_{endpoint_key}"
        full_table = f"{bronze_schema}.{table_name}"
        
        try:
            # Load CSV
            df = pd.read_csv(csv_path)
            if df.empty:
                logger.warning(f"⚠️ Empty CSV file: {csv_path}")
                continue
            
            logger.info(f"📄 Processing {len(df)} records from {csv_path}")
            
            # Determine primary key and version columns
            pk_candidates = [c for c in df.columns if c.lower() in ('id', 'guid', 'bill_guid', 'reference_number', 'referencenbr')]
            pk_col = pk_candidates[0] if pk_candidates else None
            
            version_candidates = [c for c in df.columns if 'last_modified' in c.lower() or c == '_extracted_at']
            version_col = version_candidates[0] if version_candidates else None
            
            # Clean column names for ClickHouse
            def safe_column_name(name):
                return name.replace(' ', '_').replace('-', '_').replace('.', '_').replace('(', '').replace(')', '')
            
            df.columns = [safe_column_name(col) for col in df.columns]
            if pk_col:
                pk_col = safe_column_name(pk_col)
            if version_col:
                version_col = safe_column_name(version_col)
            
            # Check if table exists
            table_exists = False
            try:
                client.query(f"DESCRIBE TABLE {full_table}")
                table_exists = True
                logger.info(f"📋 Table {full_table} already exists")
            except Exception:
                table_exists = False
            
            # Create table if it doesn't exist
            if not table_exists:
                columns = []
                for col in df.columns:
                    columns.append(f"`{col}` String")
                
                # Determine table engine
                order_by = f"`{pk_col}`" if pk_col else "tuple()"
                if version_col:
                    engine = f"ReplacingMergeTree(`{version_col}`)"
                else:
                    engine = "MergeTree()"
                
                create_ddl = f"""
                CREATE TABLE {full_table} (
                    {', '.join(columns)}
                ) ENGINE = {engine}
                ORDER BY {order_by}
                """
                
                client.command(create_ddl)
                logger.info(f"🆕 Created table {full_table} with engine {engine}")
            
            # Convert DataFrame to string type for ClickHouse
            df_str = df.astype(str).replace('nan', '').replace('None', '')
            
            # Insert data
            client.insert_df(table=full_table, df=df_str)
            total_records += len(df)
            
            logger.info(f"✅ Loaded {len(df)} records into {full_table}")
            
        except Exception as e:
            logger.error(f"❌ Failed to process {csv_path}: {e}")
            # Continue with other files rather than failing completely
            continue
    
    # Close connection
    client.close()
    logger.info(f"✅ ClickHouse loading complete. Total records processed: {total_records}")
    return total_records

# ---------------- DBT TRANSFORMATIONS ---------------- #
def list_dbt_models():
    """List available dbt models."""
    dbt_dir = config['dbt']['project_dir']
    try:
        cmd = ["dbt", "ls", "--resource-type", "model", "--output", "name"]
        result = subprocess.run(cmd, cwd=dbt_dir, capture_output=True, text=True, check=True)
        models = {line.strip() for line in result.stdout.splitlines() if line.strip()}
        logger.info(f"🧾 Discovered {len(models)} dbt models")
        return models
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Failed to list dbt models: {e.stderr}")
        return set()
    except Exception as e:
        logger.error(f"❌ Error listing dbt models: {e}")
        return set()

def derive_raw_model_name(endpoint_key, project_models):
    """Derive raw model name from endpoint key."""
    candidates = [
        f"{endpoint_key}_raw",
        f"raw_{endpoint_key}",
    ]
    
    # Try singular/plural variations
    if endpoint_key.endswith('s'):
        singular = endpoint_key[:-1]
        candidates.extend([f"{singular}_raw", f"raw_{singular}"])
    else:
        candidates.extend([f"{endpoint_key}s_raw", f"raw_{endpoint_key}s"])
    
    for candidate in candidates:
        if candidate in project_models:
            return candidate
    return None

def run_dbt_transformations(**context):
    """Run dbt transformations dynamically based on extracted data."""
    logger.info("🔧 Running dbt transformations...")
    
    dbt_dir = config['dbt']['project_dir']
    dbt_config = config['dbt']['execution']
    
    # Build dbt command options
    options = []
    if dbt_config.get('fail_fast'):
        options.append("--fail-fast")
    if dbt_config.get('full_refresh'):
        options.append("--full-refresh")
    if dbt_config.get('threads'):
        options.append(f"--threads {dbt_config['threads']}")
    
    options_str = ' '.join(options)
    
    # Get available models
    project_models = list_dbt_models()
    if not project_models:
        logger.warning("⚠️ No dbt models found, skipping transformations")
        return
    
    # Determine which endpoints have data
    csv_dir = config['extraction']['paths']['raw_data_directory']
    csv_files = glob(os.path.join(csv_dir, '*.csv'))
    endpoints_with_data = [os.path.splitext(os.path.basename(f))[0] for f in csv_files]
    
    # Filter to only enabled endpoints
    enabled_endpoints_set = set(enabled_endpoints.keys())
    endpoints_to_process = [ep for ep in endpoints_with_data if ep in enabled_endpoints_set]
    
    if not endpoints_to_process:
        logger.warning("⚠️ No enabled endpoints with data found")
        return
    
    logger.info(f"📊 Processing dbt models for endpoints: {endpoints_to_process}")
    
    # Process raw models first
    raw_models_executed = []
    for endpoint_key in endpoints_to_process:
        raw_model = derive_raw_model_name(endpoint_key, project_models)
        if raw_model:
            try:
                cmd = f"dbt run --select {raw_model} {options_str}"
                logger.info(f"▶️ Running raw model: {cmd}")
                
                result = subprocess.run(
                    cmd.split(), 
                    cwd=dbt_dir, 
                    capture_output=True, 
                    text=True, 
                    check=True
                )
                
                raw_models_executed.append(raw_model)
                logger.info(f"✅ Raw model {raw_model} completed successfully")
                
            except subprocess.CalledProcessError as e:
                logger.error(f"❌ Raw model {raw_model} failed:")
                logger.error(f"   stdout: {e.stdout}")
                logger.error(f"   stderr: {e.stderr}")
                raise
        else:
            logger.warning(f"⏭ No raw model found for endpoint {endpoint_key}")
    
    # Process business/silver models
    business_models_executed = []
    for raw_model in raw_models_executed:
        # Derive business model name (remove _raw suffix)
        base_name = raw_model.replace('_raw', '').replace('raw_', '')
        business_candidates = [base_name]
        
        # Try singular/plural variations
        if base_name.endswith('s'):
            business_candidates.append(base_name[:-1])
        else:
            business_candidates.append(f"{base_name}s")
        
        business_model = None
        for candidate in business_candidates:
            if candidate in project_models:
                business_model = candidate
                break
        
        if business_model:
            try:
                cmd = f"dbt run --select {business_model} {options_str}"
                logger.info(f"▶️ Running business model: {cmd}")
                
                result = subprocess.run(
                    cmd.split(), 
                    cwd=dbt_dir, 
                    capture_output=True, 
                    text=True, 
                    check=True
                )
                
                business_models_executed.append(business_model)
                logger.info(f"✅ Business model {business_model} completed successfully")
                
            except subprocess.CalledProcessError as e:
                logger.error(f"❌ Business model {business_model} failed:")
                logger.error(f"   stdout: {e.stdout}")
                logger.error(f"   stderr: {e.stderr}")
                raise
        else:
            logger.info(f"🔍 No business model found for {raw_model}")
    
    logger.info(f"✅ dbt transformations complete. Raw models: {len(raw_models_executed)}, Business models: {len(business_models_executed)}")

def check_transformed_data(**context):
    """Perform basic data quality checks."""
    logger.info("🔍 Performing data quality checks...")
    
    try:
        import clickhouse_connect
        
        # Connection parameters
        host = os.getenv('CLICKHOUSE_HOST')
        port = int(os.getenv('CLICKHOUSE_PORT', 8443))
        database = os.getenv('CLICKHOUSE_DATABASE', 'default')
        username = os.getenv('CLICKHOUSE_USER', 'default')
        password = os.getenv('CLICKHOUSE_PASSWORD')
        
        client = clickhouse_connect.get_client(
            host=host, port=port, username=username, password=password,
            database=database, secure=True
        )
        
        # Check bronze layer
        bronze_schema = config['warehouse']['schemas']['bronze_schema']
        try:
            bronze_tables = client.query(f"SHOW TABLES FROM {bronze_schema}")
            bronze_table_names = [row[0] for row in bronze_tables.result_rows]
            logger.info(f"📊 Bronze tables ({bronze_schema}): {bronze_table_names}")
            
            # Basic row counts
            for table_name in bronze_table_names:
                try:
                    count_result = client.query(f"SELECT count() FROM {bronze_schema}.{table_name}")
                    row_count = count_result.result_rows[0][0]
                    logger.info(f"   {table_name}: {row_count:,} rows")
                except Exception as e:
                    logger.warning(f"   Failed to count {table_name}: {e}")
        except Exception as e:
            logger.warning(f"⚠️ Could not check bronze schema: {e}")
        
        # Check silver layer
        silver_schema = config['warehouse']['schemas']['silver_schema']
        try:
            silver_tables = client.query(f"SHOW TABLES FROM {silver_schema}")
            silver_table_names = [row[0] for row in silver_tables.result_rows]
            logger.info(f"📊 Silver tables ({silver_schema}): {silver_table_names}")
            
            # Basic row counts
            for table_name in silver_table_names:
                try:
                    count_result = client.query(f"SELECT count() FROM {silver_schema}.{table_name}")
                    row_count = count_result.result_rows[0][0]
                    logger.info(f"   {table_name}: {row_count:,} rows")
                except Exception as e:
                    logger.warning(f"   Failed to count {table_name}: {e}")
        except Exception as e:
            logger.warning(f"⚠️ Could not check silver schema: {e}")
        
        client.close()
        logger.info("✅ Data quality checks completed")
        
    except Exception as e:
        logger.warning(f"⚠️ Quality checks skipped due to error: {e}")

def send_pipeline_success_notification(**context):
    """Send pipeline success notification."""
    send_success_email(context)
    logger.info("✅ Pipeline success notification sent")
    return "Success notification sent"

# ---------------- DAG DEFINITION ---------------- #
with dag:
    # Start task
    start_task = EmptyOperator(task_id='start_pipeline')
    
    # Connection test
    test_task = PythonOperator(
        task_id='test_acumatica_connection',
        python_callable=test_connection
    )
    
    # Dynamic extraction tasks
    extraction_tasks = []
    for endpoint_key in enabled_endpoints.keys():
        extraction_task = PythonOperator(
            task_id=f"extract_{endpoint_key}",
            python_callable=acx.extract_acumatica_endpoint,
            op_kwargs={'endpoint_key': endpoint_key},
            provide_context=True
        )
        extraction_tasks.append(extraction_task)
    
    # Loading task
    load_task = PythonOperator(
        task_id='load_to_warehouse',
        python_callable=load_csv_to_warehouse
    )
    
    # Transformation task
    transform_task = PythonOperator(
        task_id='run_dbt_transformations',
        python_callable=run_dbt_transformations
    )
    
    # Quality check task
    quality_check_task = PythonOperator(
        task_id='check_data_quality',
        python_callable=check_transformed_data
    )
    
    # Success notification task
    success_notification_task = PythonOperator(
        task_id='send_success_notification',
        python_callable=send_pipeline_success_notification
    )
    
    # End task
    end_task = EmptyOperator(task_id='end_pipeline')
    
    # Define task dependencies
    start_task >> test_task >> extraction_tasks >> load_task >> transform_task >> quality_check_task >> success_notification_task >> end_task