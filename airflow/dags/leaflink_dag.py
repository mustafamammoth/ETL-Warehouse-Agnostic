# leaflink_dag.py - LeafLink ETL pipeline with incremental extraction
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
import os
import subprocess
import sys
import yaml
import re
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Allow importing extractor
sys.path.append('/opt/airflow')
sys.path.append('/opt/airflow/config')

from extractors.leaflink import extractor as leaflink

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

def validate_extraction_integrity(**context):
    """Enhanced validation with comprehensive data quality checks."""
    logger.info("🔍 Validating LeafLink extraction data integrity...")
    
    try:
        import clickhouse_connect
        
        # Get extraction results
        extraction_results = context['task_instance'].xcom_pull(
            task_ids='run_coordinated_extraction', 
            key='extraction_results'
        )
        
        if not extraction_results:
            logger.warning("⚠️ No extraction results to validate")
            return {'status': 'no_data', 'issues': ['No extraction results found']}
        
        # Filter only successful extractions
        successful_extractions = {
            endpoint: count for endpoint, count in extraction_results.items()
            if isinstance(count, int) and count > 0
        }
        
        if not successful_extractions:
            logger.warning("⚠️ No successful extractions to validate")
            return {'status': 'no_successful_extractions', 'issues': ['No successful extractions found']}
        
        logger.info(f"🔍 Validating {len(successful_extractions)} successful extractions")
        
        # Connection
        host = os.getenv('CLICKHOUSE_HOST')
        port = int(os.getenv('CLICKHOUSE_PORT', 8443))
        database = os.getenv('CLICKHOUSE_DATABASE', 'default')
        username = os.getenv('CLICKHOUSE_USER', 'default')
        password = os.getenv('CLICKHOUSE_PASSWORD')
        
        client = clickhouse_connect.get_client(
            host=host, port=port, username=username, password=password,
            database=database, secure=True
        )
        
        raw_schema = config['warehouse']['schemas']['raw_schema']
        validation_results = {}
        critical_issues = []
        warnings = []
        
        for endpoint, expected_records in extraction_results.items():
            if isinstance(expected_records, int) and expected_records > 0:
                table_name = f"raw_{endpoint}"
                full_table = f"`{raw_schema}`.`{table_name}`"
                
                try:
                    # Get latest extraction timestamp
                    latest_extraction_query = f"""
                        SELECT max(_extracted_at) 
                        FROM {full_table}
                    """
                    latest_extraction = client.query(latest_extraction_query).result_rows[0][0]
                    
                    if not latest_extraction:
                        critical_issues.append(f"{endpoint}: No extraction timestamp found")
                        continue
                    
                    # Count records from latest extraction
                    actual_records_query = f"""
                        SELECT count() 
                        FROM {full_table} 
                        WHERE _extracted_at = '{latest_extraction}'
                    """
                    actual_records = client.query(actual_records_query).result_rows[0][0]
                    
                    # Enhanced: LeafLink-specific data quality checks
                    quality_checks = {}
                    
                    if endpoint == 'orders_received':
                        # Check for null/empty critical fields
                        null_check = client.query(f"""
                            SELECT count() 
                            FROM {full_table} 
                            WHERE _extracted_at = '{latest_extraction}' 
                            AND (number IS NULL OR number = '' OR buyer IS NULL OR seller IS NULL)
                        """).result_rows[0][0]
                        quality_checks['null_critical_fields'] = null_check
                        
                        # Check for duplicate order numbers
                        duplicate_check = client.query(f"""
                            SELECT count(*) - count(DISTINCT number) as duplicates
                            FROM {full_table} 
                            WHERE _extracted_at = '{latest_extraction}'
                            AND number IS NOT NULL AND number != ''
                        """).result_rows[0][0]
                        quality_checks['duplicates'] = duplicate_check
                        
                        # Check for invalid order statuses
                        valid_statuses = ['draft', 'submitted', 'accepted', 'backorder', 'fulfilled', 'shipped', 'complete', 'rejected', 'combined', 'cancelled']
                        invalid_status_check = client.query(f"""
                            SELECT count() 
                            FROM {full_table} 
                            WHERE _extracted_at = '{latest_extraction}'
                            AND lower(status) NOT IN ({','.join([f"'{s}'" for s in valid_statuses])})
                        """).result_rows[0][0]
                        quality_checks['invalid_statuses'] = invalid_status_check
                        
                        # Check for orders with zero or negative totals (more lenient)
                        invalid_totals = client.query(f"""
                            SELECT count() 
                            FROM {full_table} 
                            WHERE _extracted_at = '{latest_extraction}'
                            AND (total_amount IS NULL 
                                 OR (total_amount != '' AND total_amount != 'None' AND toFloat64OrNull(total_amount) < 0))
                        """).result_rows[0][0]
                        quality_checks['invalid_totals'] = invalid_totals
                        
                        # Check for zero-value orders separately (these might be legitimate)
                        zero_totals = client.query(f"""
                            SELECT count() 
                            FROM {full_table} 
                            WHERE _extracted_at = '{latest_extraction}'
                            AND (total_amount = '' OR total_amount = '0' OR total_amount = '0.0' 
                                 OR toFloat64OrNull(total_amount) = 0)
                        """).result_rows[0][0]
                        quality_checks['zero_totals'] = zero_totals
                        
                        # Debug: Sample total_amount values to understand the data
                        sample_totals = client.query(f"""
                            SELECT total_amount, count() as cnt
                            FROM {full_table} 
                            WHERE _extracted_at = '{latest_extraction}'
                            GROUP BY total_amount
                            ORDER BY cnt DESC
                            LIMIT 10
                        """).result_rows
                        
                        logger.info(f"   📊 Sample total_amount values: {sample_totals}")
                        
                    elif endpoint == 'order_payments':
                        # Check for null/empty critical fields in payments
                        null_check = client.query(f"""
                            SELECT count() 
                            FROM {full_table} 
                            WHERE _extracted_at = '{latest_extraction}' 
                            AND (id IS NULL OR id = '' OR "order" IS NULL OR "order" = '')
                        """).result_rows[0][0]
                        quality_checks['null_critical_fields'] = null_check
                        
                        # Check for duplicate payment IDs
                        duplicate_check = client.query(f"""
                            SELECT count(*) - count(DISTINCT id) as duplicates
                            FROM {full_table} 
                            WHERE _extracted_at = '{latest_extraction}'
                            AND id IS NOT NULL AND id != ''
                        """).result_rows[0][0]
                        quality_checks['duplicates'] = duplicate_check
                        
                        # Check for invalid payment types
                        valid_payment_types = ['other', 'cash', 'check', 'credit', 'trade', 'ach', 'wire', 'cashier']
                        invalid_payment_types = client.query(f"""
                            SELECT count() 
                            FROM {full_table} 
                            WHERE _extracted_at = '{latest_extraction}'
                            AND lower(payment_type) NOT IN ({','.join([f"'{pt}'" for pt in valid_payment_types])})
                        """).result_rows[0][0]
                        quality_checks['invalid_payment_types'] = invalid_payment_types
                        
                        # Check for invalid payment amounts
                        invalid_amounts = client.query(f"""
                            SELECT count() 
                            FROM {full_table} 
                            WHERE _extracted_at = '{latest_extraction}'
                            AND (total_amount IS NULL OR toFloat64OrNull(total_amount) <= 0)
                        """).result_rows[0][0]
                        quality_checks['invalid_amounts'] = invalid_amounts
                        
                        # Set other fields to 0 for consistency
                        quality_checks['invalid_statuses'] = 0
                        quality_checks['invalid_totals'] = 0
                        quality_checks['zero_totals'] = 0
                        
                    else:
                        # Generic checks for other endpoints
                        quality_checks['null_critical_fields'] = 0
                        quality_checks['duplicates'] = 0
                        quality_checks['invalid_statuses'] = 0
                        quality_checks['invalid_totals'] = 0
                        quality_checks['zero_totals'] = 0
                        quality_checks['invalid_payment_types'] = 0
                        quality_checks['invalid_amounts'] = 0
                    
                    # Validation status determination
                    status = "✅ VALID"
                    issues = []
                    
                    if actual_records != expected_records:
                        status = "❌ RECORD MISMATCH"
                        issues.append(f"Expected {expected_records}, got {actual_records}")
                        critical_issues.append(f"{endpoint}: Record count mismatch")
                    
                    if quality_checks.get('duplicates', 0) > 0:
                        status = "❌ DUPLICATES FOUND"
                        issues.append(f"{quality_checks['duplicates']} duplicate records")
                        critical_issues.append(f"{endpoint}: {quality_checks['duplicates']} duplicates")
                    
                    if quality_checks.get('null_critical_fields', 0) > 0:
                        if quality_checks['null_critical_fields'] > expected_records * 0.1:  # >10% null
                            status = "❌ DATA QUALITY ISSUES"
                            issues.append(f"{quality_checks['null_critical_fields']} records with null critical fields")
                            critical_issues.append(f"{endpoint}: High null rate in critical fields")
                        else:
                            warnings.append(f"{endpoint}: {quality_checks['null_critical_fields']} records with null fields")
                    
                    if quality_checks.get('invalid_statuses', 0) > 0:
                        status = "❌ INVALID DATA"
                        issues.append(f"{quality_checks['invalid_statuses']} records with invalid order status")
                        critical_issues.append(f"{endpoint}: Invalid order statuses detected")
                    
                    if quality_checks.get('invalid_payment_types', 0) > 0:
                        status = "❌ INVALID DATA"
                        issues.append(f"{quality_checks['invalid_payment_types']} records with invalid payment type")
                        critical_issues.append(f"{endpoint}: Invalid payment types detected")
                    
                    if quality_checks.get('invalid_amounts', 0) > 0:
                        if quality_checks['invalid_amounts'] > expected_records * 0.05:  # >5% invalid amounts
                            status = "❌ FINANCIAL DATA ISSUES"
                            issues.append(f"{quality_checks['invalid_amounts']} records with invalid payment amounts")
                            critical_issues.append(f"{endpoint}: High rate of invalid payment amounts")
                        else:
                            warnings.append(f"{endpoint}: {quality_checks['invalid_amounts']} invalid payment amounts")
                    
                    if quality_checks.get('invalid_totals', 0) > 0:
                        # Only flag as critical if more than 10% have truly invalid totals (negative or null)
                        if quality_checks['invalid_totals'] > expected_records * 0.10:  # >10% invalid totals
                            status = "❌ FINANCIAL DATA ISSUES"
                            issues.append(f"{quality_checks['invalid_totals']} records with invalid order totals")
                            critical_issues.append(f"{endpoint}: High rate of invalid order totals")
                        else:
                            warnings.append(f"{endpoint}: {quality_checks['invalid_totals']} invalid order totals")
                    
                    # Report zero totals as info, not critical (these might be legitimate samples, etc.)
                    if quality_checks.get('zero_totals', 0) > 0:
                        zero_percentage = (quality_checks['zero_totals'] / expected_records) * 100
                        warnings.append(f"{endpoint}: {quality_checks['zero_totals']} zero-value orders ({zero_percentage:.1f}%)")
                    
                    validation_results[endpoint] = {
                        'expected': expected_records,
                        'actual': actual_records,
                        'quality_checks': quality_checks,
                        'status': status,
                        'issues': issues,
                        'latest_extraction': latest_extraction
                    }
                    
                    logger.info(f"📊 {endpoint}: {status}")
                    logger.info(f"   Expected: {expected_records}, Actual: {actual_records}")
                    logger.info(f"   Quality checks: {quality_checks}")
                    
                except Exception as e:
                    logger.error(f"❌ Failed to validate {endpoint}: {e}")
                    validation_results[endpoint] = {'status': f'❌ VALIDATION ERROR: {e}'}
                    critical_issues.append(f"{endpoint}: Validation failed - {e}")
        
        client.close()
        
        # Overall validation status
        all_valid = len(critical_issues) == 0
        
        validation_summary = {
            'overall_status': 'PASS' if all_valid else 'FAIL',
            'critical_issues': critical_issues,
            'warnings': warnings,
            'endpoint_results': validation_results,
            'total_endpoints_validated': len(validation_results)
        }
        
        if all_valid:
            logger.info("✅ All extractions passed validation")
            if warnings:
                logger.warning(f"⚠️ {len(warnings)} warnings detected:")
                for warning in warnings:
                    logger.warning(f"   {warning}")
        else:
            logger.error("❌ CRITICAL VALIDATION FAILURES DETECTED!")
            for issue in critical_issues:
                logger.error(f"   {issue}")
            
            # CRITICAL: Raise exception to fail the pipeline on data quality issues
            raise ValueError(f"Data validation failed with {len(critical_issues)} critical issues")
        
        # Store validation results
        context['task_instance'].xcom_push(key='validation_results', value=validation_summary)
        return validation_summary
        
    except Exception as e:
        logger.error(f"❌ Validation process failed: {e}")
        raise

def load_leaflink_config():
    """Load and validate LeafLink configuration."""
    config_path = '/opt/airflow/config/sources/leaflink.yml'
    try:
        with open(config_path, 'r') as f:
            raw = yaml.safe_load(f)
        config = _sub_env(raw)
        
        # Enhanced validation
        required_keys = ['dag', 'api', 'extraction', 'warehouse']
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required config section: {key}")
        
        # Validate critical nested keys
        if 'incremental' not in config['extraction']:
            raise ValueError("Missing extraction.incremental configuration")
        
        if 'state_path' not in config['extraction']['incremental']:
            raise ValueError("Missing extraction.incremental.state_path")
        
        # Ensure state directory exists
        state_path = config['extraction']['incremental']['state_path']
        state_dir = os.path.dirname(state_path)
        if state_dir:
            os.makedirs(state_dir, exist_ok=True)
            logger.info(f"✅ State directory ensured: {state_dir}")
        
        logger.info(f"✅ Configuration loaded and validated from {config_path}")
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
        t = schedule_config.get('time', "04:00")
        h, m = t.split(':')
        return f"{m} {h} * * *"
    if stype == 'weekly':
        t = schedule_config.get('time', "04:00")
        h, m = t.split(':')
        days = {'monday': 1, 'tuesday': 2, 'wednesday': 3, 'thursday': 4, 
                'friday': 5, 'saturday': 6, 'sunday': 0}
        dnum = days.get(schedule_config.get('day_of_week', 'monday').lower(), 1)
        return f"{m} {h} * * {dnum}"
    if stype == 'monthly':
        t = schedule_config.get('time', "04:00")
        h, m = t.split(':')
        day = schedule_config.get('day_of_month', 1)
        return f"{m} {h} {day} * *"
    if stype == 'cron':
        return schedule_config.get('cron_expression', '0 4 * * *')
    
    return '0 4 * * *'  # Default daily at 4 AM

# Load configuration and initialize extractor
config = load_leaflink_config()
enabled_endpoints = leaflink.init_extractor(config)
logger.info(f"✅ Enabled endpoints: {list(enabled_endpoints.keys())}")

# ---------------- EMAIL CALLBACKS (Enhanced) ---------------- #
def send_success_email(context):
    """Send enhanced extraction success notification email."""
    try:
        from airflow.utils.email import send_email
        recipients = config['notifications']['email'].get('success_recipients', [])
        if not recipients: 
            return
            
        dag_run = context['dag_run']
        
        # Get validation results
        validation_results = context['task_instance'].xcom_pull(
            task_ids='validate_extraction_integrity', 
            key='validation_results'
        )
        
        # Get extraction results for detailed summary
        extraction_results = context['task_instance'].xcom_pull(
            task_ids='run_coordinated_extraction', 
            key='extraction_results'
        ) or {}
        
        total_records = context['task_instance'].xcom_pull(
            task_ids='run_coordinated_extraction', 
            key='total_records'
        ) or 0
        
        subject = f"✅ EXTRACTION SUCCESS: {dag_run.dag_id} - {total_records:,} records"
        
        html = f"""
        <h3>✅ LeafLink Extraction Pipeline Success</h3>
        <p><strong>DAG:</strong> {dag_run.dag_id}</p>
        <p><strong>Run ID:</strong> {dag_run.run_id}</p>
        <p><strong>Execution Date:</strong> {dag_run.execution_date}</p>
        <p><strong>Total Records Extracted:</strong> {total_records:,}</p>
        
        <h4>📊 Extraction Summary by Endpoint</h4>
        <table border="1" style="border-collapse: collapse;">
        <tr><th>Endpoint</th><th>Records</th><th>Status</th></tr>
        """
        
        for endpoint, result in extraction_results.items():
            if isinstance(result, int):
                status = "✅ Success"
                records = f"{result:,}"
            else:
                status = "❌ Failed" if "Failed" in str(result) else "⚠️ Warning"
                records = str(result)
            html += f"<tr><td>{endpoint}</td><td>{records}</td><td>{status}</td></tr>"
        
        html += "</table>"
        
        if validation_results:
            html += f"""
            <h4>🔍 Data Validation Summary</h4>
            <p><strong>Overall Status:</strong> {validation_results.get('overall_status', 'Unknown')}</p>
            <p><strong>Endpoints Validated:</strong> {validation_results.get('total_endpoints_validated', 0)}</p>
            """
            
            if validation_results.get('warnings'):
                html += f"<p><strong>Warnings:</strong> {len(validation_results['warnings'])}</p>"
                html += "<ul>"
                for warning in validation_results['warnings'][:3]:  # Show first 3
                    html += f"<li>{warning}</li>"
                html += "</ul>"
        
        html += "<p><em>💡 Silver transformations will be handled by the unified silver DAG</em></p>"
        
        send_email(to=recipients, subject=subject, html_content=html)
        logger.info(f"✅ Extraction success email sent to {recipients}")
    except Exception as e:
        logger.error(f"❌ Failed to send success email: {e}")

def send_failure_email(context):
    """Send enhanced failure notification email."""
    try:
        from airflow.utils.email import send_email
        recipients = config['notifications']['email'].get('failure_recipients', [])
        if not recipients:
            return
            
        ti = context['task_instance']
        ex = context.get('exception', 'Unknown error')
        
        # Get validation results if available
        validation_results = None
        try:
            validation_results = context['task_instance'].xcom_pull(
                task_ids='validate_extraction_integrity', 
                key='validation_results'
            )
        except:
            pass
        
        subject = f"❌ FAILURE: {ti.dag_id}.{ti.task_id}"
        
        html = f"""
        <h3>❌ LeafLink Pipeline Failure</h3>
        <p><strong>DAG:</strong> {ti.dag_id}</p>
        <p><strong>Task:</strong> {ti.task_id}</p>
        <p><strong>Execution Date:</strong> {ti.execution_date}</p>
        <p><strong>Error:</strong></p>
        <pre>{ex}</pre>
        """
        
        if validation_results and validation_results.get('critical_issues'):
            html += f"""
            <h4>🚨 Critical Data Issues Detected</h4>
            <ul>
            """
            for issue in validation_results['critical_issues']:
                html += f"<li>{issue}</li>"
            html += "</ul>"
        
        send_email(to=recipients, subject=subject, html_content=html)
        logger.info(f"✅ Failure email sent to {recipients}")
    except Exception as e:
        logger.error(f"❌ Failed to send failure email: {e}")

# DAG default arguments (Enhanced)
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
    'max_active_tis_per_dag': 10,
}

# Create DAG
dag = DAG(
    config['dag']['dag_id'],
    default_args=default_args,
    description=config['dag']['description'],
    schedule_interval=get_schedule_interval(config),
    max_active_runs=config['dag']['max_active_runs'],
    tags=config['dag']['tags'],
    catchup=False,
    max_active_tasks=5
)

# ---------------- CONNECTION TEST ---------------- #
def test_connection(**context):
    """Test LeafLink connection and validate prerequisites."""
    try:
        # Check required environment variables
        api_key = os.getenv('LEAFLINK_API_KEY')
        company_id = os.getenv('LEAFLINK_COMPANY_ID')
        
        if not api_key:
            raise ValueError("Missing required environment variable: LEAFLINK_API_KEY")
        
        if not company_id:
            logger.warning("⚠️ LEAFLINK_COMPANY_ID not set - using global endpoints only")
            logger.warning("   For company-specific data, set LEAFLINK_COMPANY_ID environment variable")
        else:
            logger.info(f"✅ Company ID configured: {company_id}")
        
        # Test LeafLink API connection
        session = leaflink.create_authenticated_session()
        logger.info("✅ LeafLink API connection successful")
        
        # Test ClickHouse connection
        import clickhouse_connect
        
        host = os.getenv('CLICKHOUSE_HOST')
        port = int(os.getenv('CLICKHOUSE_PORT', 8443))
        database = os.getenv('CLICKHOUSE_DATABASE', 'default')
        username = os.getenv('CLICKHOUSE_USER', 'default')
        password = os.getenv('CLICKHOUSE_PASSWORD')
        
        if not all([host, password]):
            raise ValueError("ClickHouse connection parameters missing")
        
        client = clickhouse_connect.get_client(
            host=host, port=port, username=username, password=password,
            database=database, secure=True
        )
        
        # Test basic query
        result = client.query("SELECT 1 as test")
        if result.result_rows[0][0] == 1:
            logger.info("✅ ClickHouse connection successful")
        else:
            raise ValueError("ClickHouse test query failed")
        
        client.close()
        
        # Test state file access
        state_path = config['extraction']['incremental']['state_path']
        state_dir = os.path.dirname(state_path)
        
        if not os.access(state_dir, os.W_OK):
            raise ValueError(f"State directory not writable: {state_dir}")
        
        logger.info(f"✅ State directory writable: {state_dir}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Connection test failed: {e}")
        raise

# ---------------- COORDINATED EXTRACTION ---------------- #
def run_coordinated_extraction(**context):
    """Run extraction with proper dependency handling and atomic state management."""
    logger.info("🚀 Starting coordinated LeafLink extraction...")
    
    try:
        # Use the coordinated extraction function
        results = leaflink.extract_all_endpoints_with_dependencies(**context)
        
        # Store results in context for downstream tasks
        context['task_instance'].xcom_push(key='extraction_results', value=results['extraction_results'])
        context['task_instance'].xcom_push(key='total_records', value=results['total_records'])
        context['task_instance'].xcom_push(key='completed_endpoints', value=results['completed_endpoints'])
        
        # Check if any critical endpoints failed
        critical_endpoints = ['orders_received']  # Define critical endpoints
        failed_critical = []
        
        for endpoint in critical_endpoints:
            if endpoint in results['extraction_results']:
                result = results['extraction_results'][endpoint]
                if isinstance(result, str) and 'Failed' in result:
                    failed_critical.append(endpoint)
        
        if failed_critical:
            raise ValueError(f"Critical endpoints failed: {failed_critical}")
        
        logger.info(f"✅ Coordinated extraction completed: {results['total_records']} total records")
        return results['total_records']
        
    except Exception as e:
        logger.error(f"❌ Coordinated extraction failed: {e}")
        raise

# ---------------- STATE MANAGEMENT ---------------- #
def finalize_extraction_state(**context):
    """Finalize state after successful extraction and validation."""
    try:
        # Get extraction results from previous task
        extraction_results = context['task_instance'].xcom_pull(
            task_ids='run_coordinated_extraction', 
            key='extraction_results'
        )
        
        # Get validation results
        validation_results = context['task_instance'].xcom_pull(
            task_ids='validate_extraction_integrity', 
            key='validation_results'
        )
        
        if validation_results and validation_results.get('overall_status') != 'PASS':
            logger.error("❌ Cannot finalize state - validation failed")
            raise ValueError("Validation failed - state not finalized")
        
        if extraction_results:
            successful_extractions = [
                endpoint for endpoint, result in extraction_results.items() 
                if isinstance(result, int) and result > 0
            ]
            logger.info(f"✅ Finalizing state for {len(successful_extractions)} successful extractions")
        
        # Call extractor finalization
        leaflink.finalize_state_after_warehouse_load(context)
        
        logger.info("✅ Extraction state finalized successfully")
        return "State finalized successfully"
        
    except Exception as e:
        logger.error(f"❌ Failed to finalize extraction state: {e}")
        raise

# ---------------- DBT TRANSFORMATIONS ---------------- #
def list_dbt_models():
    """Return a set of model names using dbt ls with explicit project/profiles paths."""
    dbt_dir = config['dbt']['project_dir']
    profiles_dir = config['dbt'].get('profiles_dir', os.path.join(dbt_dir, 'profiles'))
    cmd = [
        "dbt", "ls",
        "--resource-type", "model",
        "--output", "name",
        "--project-dir", dbt_dir,
        "--profiles-dir", profiles_dir
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return {ln.strip() for ln in result.stdout.splitlines() if ln.strip()}
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ dbt ls failed:\nSTDOUT:\n{e.stdout}\nSTDERR:\n{e.stderr}")
        raise



def skip_dbt_transformations(**context):
    """Skip dbt transformations - handled by separate unified silver DAG."""
    logger.info("⏭️ Skipping dbt transformations (handled by unified silver DAG)")
    return {"status": "skipped", "reason": "Silver processing moved to unified DAG"}


# def run_dbt_transformations(**context):
#     """
#     Build a dbt project with ONLY the LeafLink silver/curated models for endpoints that were actually extracted.
#     Only searches in LeafLink-specific folders to avoid conflicts with other data sources.
#     """
#     import tempfile, shutil, re, subprocess, yaml, json, os
#     from pathlib import Path

#     logger.info("🔧 Running LeafLink dbt transformations (silver models only)...")

#     ti = context["task_instance"]
#     extraction_results = ti.xcom_pull(task_ids="run_coordinated_extraction", key="extraction_results") or {}

#     always_extract = config["extraction"]["endpoints"].get("always_extract", []) or []
#     optional_extract = config["extraction"]["endpoints"].get("optional_extract", []) or []
#     disabled = set(config["extraction"]["endpoints"].get("disabled", []) or [])
#     allowed = set(always_extract + optional_extract) - disabled

#     loaded_endpoints = [e for e, v in extraction_results.items() if isinstance(v, int) and v > 0]
#     endpoints_to_transform = [e for e in loaded_endpoints if e in allowed]

#     if not endpoints_to_transform:
#         logger.info("⚠️ No endpoints need transformation, skipping dbt.")
#         return {"silver_models": []}

#     logger.info("📊 Endpoints to transform: %s", endpoints_to_transform)

#     project_dir = Path(config["dbt"]["project_dir"])
#     profiles_dir = Path(config["dbt"]["profiles_dir"])
#     models_dir = project_dir / "models"
    
#     # ============== CRITICAL FIX ==============
#     # Search ONLY in LeafLink-specific folders (skip if they don't exist)
#     leaflink_folders = ["curated/leaflink", "raw/leaflink", "staging/leaflink"]
#     all_sql_files = {}
    
#     for folder in leaflink_folders:
#         folder_path = models_dir / folder
#         if folder_path.exists():
#             logger.info(f"📁 Searching for models in: {folder_path}")
#             for f in folder_path.rglob("*.sql"):
#                 all_sql_files[f.stem] = f
#         else:
#             logger.info(f"⏭️ Skipping non-existent folder: {folder_path}")
    
#     if not all_sql_files:
#         logger.info("⚠️ No LeafLink model folders found, skipping dbt transformations.")
#         return {"silver_models": []}
    
#     logger.info(f"📦 Found {len(all_sql_files)} LeafLink models: {list(all_sql_files.keys())}")
#     # ==========================================

#     def silver_candidates(ep):
#         """Silver model candidates for LeafLink."""
#         candidates = []
        
#         # Basic patterns first
#         candidates.extend([ep, f"leaflink_{ep}"])
        
#         if ep.endswith("s"):
#             singular = ep[:-1]
#             candidates.extend([singular, f"leaflink_{singular}"])
            
#             # Business patterns
#             candidates.extend([
#                 f"{ep}_staging",
#                 f"{singular}_staging",
#                 f"{ep}_business",
#                 f"{singular}_business"
#             ])
#         else:
#             plural = f"{ep}s"
#             candidates.extend([
#                 plural,
#                 f"leaflink_{plural}",
#                 f"{ep}_staging",
#                 f"{plural}_staging"
#             ])
        
#         return candidates

#     # Find all models and their dependencies
#     def find_model_dependencies(model_name, all_sql_files, visited=None):
#         """Find all dependencies for a model by parsing its SQL."""
#         if visited is None:
#             visited = set()
        
#         if model_name in visited or model_name not in all_sql_files:
#             return set()
        
#         visited.add(model_name)
#         dependencies = set()
        
#         try:
#             sql_content = all_sql_files[model_name].read_text()
            
#             # Find ref() calls
#             ref_pattern = re.compile(r"ref\(\s*['\"]([^'\"]+)['\"]\s*\)")
#             refs = ref_pattern.findall(sql_content)
            
#             for ref_model in refs:
#                 if ref_model in all_sql_files:
#                     dependencies.add(ref_model)
#                     dependencies.update(find_model_dependencies(ref_model, all_sql_files, visited.copy()))
            
#         except Exception as e:
#             logger.warning(f"⚠️ Could not parse dependencies for {model_name}: {e}")
        
#         return dependencies

#     silver_models = []
#     all_needed_models = set()
    
#     for ep in endpoints_to_transform:
#         # Find silver model only (no raw model search)
#         s = next((c for c in silver_candidates(ep) if c in all_sql_files), None)
#         if s: 
#             silver_models.append(s)
#             all_needed_models.add(s)
#             logger.info(f"✅ Found silver model for {ep}: {s}")
            
#             # Find all dependencies for this silver model
#             deps = find_model_dependencies(s, all_sql_files)
#             if deps:
#                 logger.info(f"📋 {s} depends on: {sorted(deps)}")
#                 all_needed_models.update(deps)
#                 for dep in deps:
#                     if dep not in silver_models:
#                         silver_models.append(dep)
#         else: 
#             logger.warning(f"⏭ No silver model for {ep} in LeafLink folders")

#     if not silver_models:
#         logger.info("⚠️ No matching LeafLink silver models found.")
#         return {"silver_models": []}

#     logger.info("📦 Silver models to copy: %s", sorted(all_needed_models))

#     tmp_path = Path(tempfile.mkdtemp(prefix="dbt_filtered_leaflink_"))
#     logger.info("📁 Temp dbt project: %s", tmp_path)

#     # Create dirs
#     for d in ["models", "macros", "seeds", "snapshots", "tests", "analyses", "target", "logs"]:
#         (tmp_path / d).mkdir(parents=True, exist_ok=True)

#     # Copy macros if they exist
#     macros_src = project_dir / "macros"
#     if macros_src.exists():
#         macros_dest = tmp_path / "macros"
#         shutil.copytree(macros_src, macros_dest, dirs_exist_ok=True)
#         logger.info("✅ Copied macros to temp project")

#     # Copy only needed models
#     def copy_model(stem):
#         src = all_sql_files[stem]
#         rel = src.relative_to(models_dir)
#         dest = tmp_path / "models" / rel
#         dest.parent.mkdir(parents=True, exist_ok=True)
#         shutil.copy2(src, dest)

#     for m in sorted(all_needed_models):
#         copy_model(m)

#     # dbt_project.yml
#     orig_proj = yaml.safe_load((project_dir / "dbt_project.yml").read_text())

#     base = {
#         "name": orig_proj["name"],
#         "version": "1.0.0",
#         "config-version": 2,
#         "profile": orig_proj.get("profile", "data_platform"),
#         "model-paths": ["models"],
#         "macro-paths": ["macros"],
#         "seed-paths": ["seeds"],
#         "snapshot-paths": ["snapshots"],
#         "test-paths": ["tests"],
#         "analysis-paths": ["analyses"],
#         "clean-targets": ["target"],
#         "models": orig_proj.get("models", {}),
#         "vars": orig_proj.get("vars", {}),
#     }
#     (tmp_path / "dbt_project.yml").write_text(yaml.safe_dump(base, sort_keys=False))

#     # Sources file for LeafLink (silver models will reference raw tables via sources)
#     raw_schema = config["warehouse"]["schemas"]["raw_schema"]
#     src_regex = re.compile(r"source\(\s*['\"]bronze_leaflink['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)")

#     source_tables = set()
#     for m in all_needed_models:
#         rel = all_sql_files[m].relative_to(models_dir)
#         sql_txt = (tmp_path / "models" / rel).read_text()
#         matches = src_regex.findall(sql_txt)
#         if matches:
#             source_tables.update(matches)

#     # Add common LeafLink source tables that silver models typically need
#     for ep in endpoints_to_transform:
#         source_tables.add(f"raw_{ep}")

#     src_yaml = [
#         "version: 2",
#         "sources:",
#         "  - name: bronze_leaflink",
#         f"    schema: {raw_schema}",
#         "    tables:"
#     ] + [f"      - name: {t}" for t in sorted(source_tables)]
#     (tmp_path / "models" / "_leaflink_sources.yml").write_text("\n".join(src_yaml) + "\n")
#     logger.info("🧾 Wrote sources file with tables: %s", sorted(source_tables))

#     # Helper to run dbt commands
#     def run_cmd(cmd_list, prefix):
#         logger.info("▶️ %s: %s", prefix, " ".join(cmd_list))
#         env = os.environ.copy()
#         env["DBT_LOG_FORMAT"] = "json"
#         try:
#             res = subprocess.run(cmd_list, cwd=str(tmp_path), capture_output=True, text=True, check=True, env=env)
#             logger.info("✅ %s succeeded", prefix)
#             return res.stdout, res.stderr
#         except subprocess.CalledProcessError as e:
#             logger.error("❌ %s FAILED", prefix)
#             logger.error("STDOUT:\n%s", e.stdout)
#             logger.error("STDERR:\n%s", e.stderr)
#             raise

#     exec_conf = config["dbt"]["execution"]
#     select_list = sorted(all_needed_models)

#     # 1) compile
#     compile_cmd = [
#         "dbt", "compile",
#         "--project-dir", str(tmp_path),
#         "--profiles-dir", str(profiles_dir),
#         "--no-partial-parse",
#         "--select", *select_list
#     ]
#     run_cmd(compile_cmd, "dbt compile")

#     # 2) run
#     run_cmd_list = [
#         "dbt", "run",
#         "--project-dir", str(tmp_path),
#         "--profiles-dir", str(profiles_dir),
#         "--no-partial-parse",
#         "--select", *select_list
#     ]
#     if exec_conf.get("fail_fast"):
#         run_cmd_list.append("--fail-fast")
#     if exec_conf.get("threads"):
#         run_cmd_list += ["--threads", str(exec_conf["threads"])]

#     try:
#         out, err = run_cmd(run_cmd_list, "dbt run")
#     except subprocess.CalledProcessError as e:
#         logger.error("❌ dbt run FAILED - analyzing errors...")
        
#         # Detailed error analysis
#         rr = tmp_path / "target" / "run_results.json"
#         mf = tmp_path / "target" / "manifest.json"
        
#         if rr.exists() and mf.exists():
#             try:
#                 run_results = json.loads(rr.read_text())
#                 manifest = json.loads(mf.read_text())
                
#                 failed_nodes = [r for r in run_results.get("results", []) if r.get("status") == "error"]
                
#                 if failed_nodes:
#                     logger.error(f"🚨 {len(failed_nodes)} dbt models failed:")
                    
#                     for result in failed_nodes:
#                         node_id = result["unique_id"]
#                         node = manifest["nodes"].get(node_id, {})
#                         model_name = node.get("name", node_id)
#                         error_msg = result.get("message", "Unknown error")
                        
#                         logger.error(f"   ❌ {model_name}: {error_msg}")
                        
#                         # Show compiled SQL for debugging
#                         compiled_path = node.get("compiled_path")
#                         if compiled_path:
#                             compiled_file = tmp_path / compiled_path
#                             if compiled_file.exists():
#                                 sql_content = compiled_file.read_text()
#                                 sql_preview = sql_content[:500] + "..." if len(sql_content) > 500 else sql_content
#                                 logger.error(f"   📄 Compiled SQL preview:\n{sql_preview}")
                
#                 logger.error(f"🔍 dbt project preserved for debugging: {tmp_path}")
                
#             except Exception as parse_error:
#                 logger.error(f"❌ Failed to parse dbt results: {parse_error}")
        
#         else:
#             logger.error("❌ dbt results files not found")
#             logger.error(f"🔍 Temp project: {tmp_path}")
        
#         raise RuntimeError(f"dbt transformations failed: {str(e)}")
#     finally:
#         # Only clean on success
#         if (tmp_path / "target" / "run_results.json").exists():
#             rr = json.loads((tmp_path / "target" / "run_results.json").read_text())
#             statuses = {r["status"] for r in rr.get("results", [])}
#             if statuses == {"success"}:
#                 shutil.rmtree(tmp_path, ignore_errors=True)
#             else:
#                 logger.warning("❗ Leaving temp project at %s for debugging", tmp_path)

#     return {"silver_models": silver_models}


# ---------------- DATA QUALITY CHECKS ---------------- #
# CHANGE 2: Replace the data quality check function 
def skip_data_quality_checks(**context):
    """Skip data quality checks - handled by unified silver DAG."""
    logger.info("⏭️ Skipping data quality checks (handled by unified silver DAG)")
    return {"status": "skipped", "reason": "Quality checks moved to unified DAG"}

def check_clickhouse_data_quality_enhanced():
    """Enhanced ClickHouse data quality checks for LeafLink."""
    import clickhouse_connect
    
    logger.info("🔍 Running enhanced ClickHouse data quality checks for LeafLink...")
    
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
    
    try:
        raw_schema = config['warehouse']['schemas']['raw_schema']
        silver_schema = config['warehouse']['schemas']['silver_schema']
        
        quality_results = {
            'raw_tables': {},
            'silver_tables': {},
            'overall_status': 'PASS',
            'issues': []
        }
        
        # Enhanced raw table checks for LeafLink
        try:
            tables = client.query(f"SHOW TABLES FROM `{raw_schema}`")
            table_names = [row[0] for row in tables.result_rows]
            
            logger.info(f"📊 Enhanced raw table analysis ({raw_schema}):")
            for table_name in table_names:
                try:
                    # Basic row count
                    count_result = client.query(f"SELECT count() FROM `{raw_schema}`.`{table_name}`")
                    row_count = count_result.result_rows[0][0]
                    
                    # Check data freshness (last 24 hours)
                    freshness_result = client.query(f"""
                        SELECT count() FROM `{raw_schema}`.`{table_name}` 
                        WHERE parseDateTimeBestEffort(_extracted_at) >= now() - INTERVAL 24 HOUR
                    """)
                    recent_count = freshness_result.result_rows[0][0]
                    
                    table_quality = {
                        'total_rows': row_count,
                        'recent_rows': recent_count,
                        'freshness_ok': recent_count > 0,
                        'status': 'PASS'
                    }
                    
                    if recent_count == 0 and row_count > 0:
                        table_quality['status'] = 'STALE'
                        quality_results['issues'].append(f"{table_name}: No recent data (24h)")
                    
                    quality_results['raw_tables'][table_name] = table_quality
                    
                    logger.info(f"   {table_name}: {row_count:,} rows, {recent_count:,} recent")
                    
                except Exception as e:
                    logger.warning(f"   ❌ Failed to check {table_name}: {e}")
                    quality_results['raw_tables'][table_name] = {'status': 'ERROR', 'error': str(e)}
                    quality_results['issues'].append(f"{table_name}: Quality check failed")
                    
        except Exception as e:
            logger.warning(f"⚠️ Could not check raw schema: {e}")
            quality_results['issues'].append(f"Raw schema check failed: {e}")
        
        # Silver table checks (similar pattern)
        try:
            silver_tables = client.query(f"SHOW TABLES FROM `{silver_schema}`")
            silver_table_names = [row[0] for row in silver_tables.result_rows]
            
            if silver_table_names:
                logger.info(f"📊 Enhanced silver table analysis ({silver_schema}):")
                for table_name in silver_table_names:
                    try:
                        count_result = client.query(f"SELECT count() FROM `{silver_schema}`.`{table_name}`")
                        row_count = count_result.result_rows[0][0]
                        
                        table_quality = {
                            'total_rows': row_count,
                            'status': 'PASS'
                        }
                        
                        quality_results['silver_tables'][table_name] = table_quality
                        logger.info(f"   {table_name}: {row_count:,} rows")
                        
                    except Exception as e:
                        logger.warning(f"   ❌ Failed to check {table_name}: {e}")
                        quality_results['silver_tables'][table_name] = {'status': 'ERROR', 'error': str(e)}
            else:
                logger.info(f"📊 No silver tables found in {silver_schema}")
                
        except Exception as e:
            logger.info(f"ℹ️ Silver schema {silver_schema} not accessible or doesn't exist yet: {e}")
        
        # Determine overall status
        if quality_results['issues']:
            quality_results['overall_status'] = 'ISSUES_DETECTED'
            logger.warning(f"⚠️ {len(quality_results['issues'])} data quality issues detected")
            for issue in quality_results['issues']:
                logger.warning(f"   {issue}")
        else:
            logger.info("✅ All data quality checks passed")
        
        client.close()
        return quality_results
        
    except Exception as e:
        logger.error(f"❌ Failed to connect to ClickHouse for quality checks: {e}")
        raise

def monitor_warehouse_health(**context):
    """Monitor warehouse health with enhanced metrics for LeafLink."""
    logger.info("🏥 Monitoring warehouse health for LeafLink...")
    
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
        
        raw_schema = config['warehouse']['schemas']['raw_schema']
        health_status = {
            'overall_status': 'HEALTHY',
            'database_accessible': False,
            'tables_analyzed': 0,
            'partitioned_tables': 0,
            'issues': [],
            'recommendations': []
        }
        
        # Check database accessibility and create if needed
        try:
            databases = client.query("SHOW DATABASES")
            db_names = [row[0] for row in databases.result_rows]
            
            if raw_schema not in db_names:
                if raw_schema != 'default':
                    logger.info(f"🆕 Creating database {raw_schema}")
                    client.command(f"CREATE DATABASE IF NOT EXISTS `{raw_schema}`")
                    health_status['database_accessible'] = True
                    health_status['recommendations'].append(f"Created missing database {raw_schema}")
                    logger.info(f"✅ Created database {raw_schema}")
                else:
                    health_status['database_accessible'] = True
            else:
                health_status['database_accessible'] = True
                logger.info(f"✅ Database {raw_schema} is accessible")
                
                # Get table information
                tables = client.query(f"SHOW TABLES FROM `{raw_schema}`")
                table_names = [row[0] for row in tables.result_rows]
                health_status['tables_analyzed'] = len(table_names)
                
                for table_name in table_names:
                    try:
                        # Check table engine and structure
                        table_info = client.query(f"SHOW CREATE TABLE `{raw_schema}`.`{table_name}`")
                        create_statement = table_info.result_rows[0][0]
                        
                        # Check partitioning
                        if "PARTITION BY" in create_statement:
                            health_status['partitioned_tables'] += 1
                            
                            # Get basic row count for partitioned tables
                            try:
                                count_result = client.query(f"SELECT count() FROM `{raw_schema}`.`{table_name}`")
                                row_count = count_result.result_rows[0][0]
                                logger.info(f"   📊 {table_name}: {row_count:,} total rows (partitioned)")
                            except Exception as count_error:
                                logger.warning(f"   ❌ {table_name}: Could not get row count: {count_error}")
                                health_status['issues'].append(f"{table_name}: Cannot access table data")
                        else:
                            # Non-partitioned table
                            logger.warning(f"   ⚠️ {table_name}: NOT PARTITIONED")
                            health_status['issues'].append(f"{table_name}: Table is not partitioned")
                            health_status['recommendations'].append(f"Recreate {table_name} with partitioning for better performance")
                            
                            try:
                                count_result = client.query(f"SELECT count() FROM `{raw_schema}`.`{table_name}`")
                                row_count = count_result.result_rows[0][0]
                                logger.warning(f"   📊 {table_name}: {row_count:,} rows (NOT PARTITIONED)")
                            except Exception as e:
                                logger.error(f"   ❌ {table_name}: Could not access table: {e}")
                                health_status['issues'].append(f"{table_name}: Table inaccessible")
                                
                    except Exception as e:
                        logger.error(f"   ❌ {table_name}: Could not analyze table structure: {e}")
                        health_status['issues'].append(f"{table_name}: Structure analysis failed")
        
        except Exception as e:
            # Try to create the database
            try:
                if raw_schema != 'default':
                    logger.info(f"🆕 Attempting to create missing database {raw_schema}")
                    client.command(f"CREATE DATABASE IF NOT EXISTS `{raw_schema}`")
                    health_status['database_accessible'] = True
                    health_status['recommendations'].append(f"Auto-created missing database {raw_schema}")
                    logger.info(f"✅ Successfully created database {raw_schema}")
                else:
                    health_status['database_accessible'] = True
            except Exception as create_error:
                health_status['database_accessible'] = False
                health_status['overall_status'] = 'CRITICAL'
                health_status['issues'].append(f"Cannot create database {raw_schema}: {create_error}")
                logger.error(f"❌ Failed to create database {raw_schema}: {create_error}")
        
        # Determine final health status
        if health_status['issues']:
            if health_status['database_accessible']:
                health_status['overall_status'] = 'DEGRADED'
            else:
                health_status['overall_status'] = 'CRITICAL'
        
        # Summary
        partitioned_ratio = 0
        if health_status['tables_analyzed'] > 0:
            partitioned_ratio = health_status['partitioned_tables'] / health_status['tables_analyzed']
        
        logger.info(f"🏥 Warehouse Health Summary:")
        logger.info(f"   Status: {health_status['overall_status']}")
        logger.info(f"   Tables analyzed: {health_status['tables_analyzed']}")
        logger.info(f"   Partitioned tables: {health_status['partitioned_tables']} ({partitioned_ratio:.1%})")
        logger.info(f"   Issues: {len(health_status['issues'])}")
        logger.info(f"   Recommendations: {len(health_status['recommendations'])}")
        
        if health_status['issues']:
            logger.warning("⚠️ Issues detected:")
            for issue in health_status['issues']:
                logger.warning(f"   {issue}")
        
        if health_status['recommendations']:
            logger.info("💡 Recommendations:")
            for rec in health_status['recommendations']:
                logger.info(f"   {rec}")
        
        client.close()
        
        # Store health status for monitoring
        context['task_instance'].xcom_push(key='warehouse_health', value=health_status)
        
        # Only raise alert for truly critical issues
        critical_issues = [
            issue for issue in health_status['issues'] 
            if not (issue.startswith("Database") and "not found" in issue)
            and not (issue.startswith("Cannot create database") and "already exists" in str(issue))
        ]
        
        if critical_issues:
            health_status['overall_status'] = 'CRITICAL'
            raise ValueError(f"Critical warehouse health issues detected: {critical_issues}")
        elif health_status['issues']:
            health_status['overall_status'] = 'DEGRADED'
            logger.warning(f"⚠️ Non-critical health issues detected: {health_status['issues']}")
        else:
            health_status['overall_status'] = 'HEALTHY'
        
        return health_status
        
    except Exception as e:
        logger.error(f"❌ Warehouse health check failed: {e}")
        raise

# ---------------- PIPELINE SUMMARY ---------------- #
def send_pipeline_success_notification(**context):
    """Send extraction-only pipeline success notification."""
    try:
        # Get results from previous tasks
        total_records = context['task_instance'].xcom_pull(
            task_ids='run_coordinated_extraction', 
            key='total_records'
        ) or 0
        
        extraction_results = context['task_instance'].xcom_pull(
            task_ids='run_coordinated_extraction', 
            key='extraction_results'
        ) or {}
        
        validation_results = context['task_instance'].xcom_pull(
            task_ids='validate_extraction_integrity', 
            key='validation_results'
        ) or {}
        
        warehouse_health = context['task_instance'].xcom_pull(
            task_ids='monitor_warehouse_health', 
            key='warehouse_health'
        ) or {}
        
        # Create extraction-focused summary
        summary = f"""
        🎉 LEAFLINK EXTRACTION SUCCESS SUMMARY 🎉
        
        📊 Data Extraction:
        - Total records loaded: {total_records:,}
        - Endpoints processed: {len(extraction_results)}
        - Validation status: {validation_results.get('overall_status', 'Unknown')}
        
        🏥 Warehouse Health:
        - Status: {warehouse_health.get('overall_status', 'Unknown')}
        - Tables analyzed: {warehouse_health.get('tables_analyzed', 0)}
        - Partitioned tables: {warehouse_health.get('partitioned_tables', 0)}
        
        📈 Endpoint Details:
        """
        
        for endpoint, result in extraction_results.items():
            if isinstance(result, int):
                summary += f"        - {endpoint}: {result:,} records\n"
            else:
                summary += f"        - {endpoint}: {result}\n"
        
        if validation_results.get('warnings'):
            summary += f"\n⚠️ Warnings ({len(validation_results['warnings'])}):\n"
            for warning in validation_results['warnings'][:5]:  # Show first 5
                summary += f"        - {warning}\n"
        
        summary += "\n💡 Silver transformations handled by unified silver DAG"
        
        logger.info(summary)
        
        # Send email notification
        send_success_email(context)
        
        logger.info("✅ Extraction pipeline success notification sent")
        return "Extraction success notification sent"
        
    except Exception as e:
        logger.error(f"❌ Failed to send pipeline notification: {e}")
        return f"Notification failed: {e}"

# ---------------- DAG DEFINITION WITH PROPER DEPENDENCIES ---------------- #
with dag:
    # Start task
    start_task = EmptyOperator(task_id='start_pipeline')
    
    # Connection test (critical - fails pipeline if connections don't work)
    test_task = PythonOperator(
        task_id='test_connections',
        python_callable=test_connection,
        retries=2,
        retry_delay=timedelta(minutes=2)
    )
    
    # Coordinated extraction
    extraction_task = PythonOperator(
        task_id='run_coordinated_extraction',
        python_callable=run_coordinated_extraction,
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=5)
    )
    
    # Critical validation task
    validation_task = PythonOperator(
        task_id='validate_extraction_integrity',
        python_callable=validate_extraction_integrity,
        provide_context=True,
        retries=0,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    # State finalization
    finalize_state_task = PythonOperator(
        task_id='finalize_extraction_state',
        python_callable=finalize_extraction_state,
        provide_context=True,
        retries=0,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    # Warehouse health monitoring
    monitor_task = PythonOperator(
        task_id='monitor_warehouse_health',
        python_callable=monitor_warehouse_health,
        provide_context=True,
        retries=1,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    # Transformation task
    transform_task = PythonOperator(
        task_id='skip_dbt_transformations',  # Renamed for clarity
        python_callable=skip_dbt_transformations,  # Use skip function
        provide_context=True,
        retries=0,  # No retries needed for skip
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    # Quality check task
    quality_check_task = PythonOperator(
        task_id='skip_data_quality_checks',  # Renamed for clarity
        python_callable=skip_data_quality_checks,  # Use skip function
        provide_context=True,
        retries=0,  # No retries needed for skip
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    # Success notification task
    success_notification_task = PythonOperator(
        task_id='send_success_notification',
        python_callable=send_pipeline_success_notification,
        provide_context=True,
        retries=2,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    # End task
    end_task = EmptyOperator(
        task_id='end_pipeline',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

start_task >> test_task >> extraction_task >> validation_task >> finalize_state_task >> monitor_task >> transform_task >> quality_check_task >> success_notification_task >> end_task
# # Define task dependencies with proper error handling
# start_task >> test_task >> extraction_task >> validation_task >> finalize_state_task

# # Parallel execution after state finalization
# finalize_state_task >> [monitor_task, transform_task]

# # Quality checks depend on transformations
# transform_task >> quality_check_task

# # Success notification waits for all tasks to complete
# [monitor_task, quality_check_task] >> success_notification_task >> end_task