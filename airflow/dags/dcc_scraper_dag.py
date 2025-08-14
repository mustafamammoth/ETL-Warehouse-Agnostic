# dcc_scraper_dag.py - DCC Cannabis License Scraper pipeline
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
import os
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

from extractors.dcc_scraper import extractor as dcc_scraper

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
    """Validate DCC scraper extraction data integrity."""
    logger.info("🔍 Validating DCC scraper extraction data integrity...")
    
    try:
        import clickhouse_connect
        
        # Get extraction results
        extraction_results = context['task_instance'].xcom_pull(
            task_ids='run_dcc_extraction', 
            key='extraction_results'
        )
        
        if not extraction_results:
            logger.warning("⚠️ No extraction results to validate")
            return {'status': 'no_data', 'issues': ['No extraction results found']}
        
        # Filter only successful extractions
        successful_extractions = {
            prefix: count for prefix, count in extraction_results.items()
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
        
        table_name = "raw_dcc_licenses"
        full_table = f"`{raw_schema}`.`{table_name}`"
        
        try:
            # Get latest extraction timestamp
            latest_extraction_query = f"""
                SELECT max(_extracted_at) 
                FROM {full_table}
            """
            latest_extraction = client.query(latest_extraction_query).result_rows[0][0]
            
            if not latest_extraction:
                critical_issues.append(f"DCC: No extraction timestamp found")
            else:
                # Count records from latest extraction
                actual_records_query = f"""
                    SELECT count() 
                    FROM {full_table} 
                    WHERE _extracted_at = '{latest_extraction}'
                """
                actual_records = client.query(actual_records_query).result_rows[0][0]
                
                # Basic data quality checks - simplified
                quality_checks = {}
                
                # Check for empty license numbers
                empty_license_check = client.query(f"""
                    SELECT count(*) 
                    FROM {full_table} 
                    WHERE _extracted_at = '{latest_extraction}'
                    AND (license_number IS NULL OR trim(license_number) = '')
                """).result_rows[0][0]
                quality_checks['empty_license_numbers'] = empty_license_check
                
                # Validation status determination
                status = "✅ VALID"
                issues = []
                
                total_expected = sum(successful_extractions.values())
                if actual_records != total_expected:
                    status = "❌ RECORD MISMATCH"
                    issues.append(f"Expected {total_expected}, got {actual_records}")
                    critical_issues.append(f"DCC: Record count mismatch")
                
                if quality_checks.get('empty_license_numbers', 0) > total_expected * 0.1:  # >10% empty
                    status = "❌ DATA QUALITY ISSUES"
                    issues.append(f"{quality_checks['empty_license_numbers']} empty license numbers")
                    critical_issues.append(f"DCC: High empty license number rate")
                
                validation_results['dcc_licenses'] = {
                    'expected': total_expected,
                    'actual': actual_records,
                    'quality_checks': quality_checks,
                    'status': status,
                    'issues': issues,
                    'latest_extraction': latest_extraction
                }
                
                logger.info(f"📊 DCC Licenses: {status}")
                logger.info(f"   Expected: {total_expected}, Actual: {actual_records}")
                logger.info(f"   Quality checks: {quality_checks}")
                
        except Exception as e:
            logger.error(f"❌ Failed to validate DCC data: {e}")
            validation_results['dcc_licenses'] = {'status': f'❌ VALIDATION ERROR: {e}'}
            critical_issues.append(f"DCC: Validation failed - {e}")
        
        client.close()
        
        # Overall validation status
        all_valid = len(critical_issues) == 0
        
        validation_summary = {
            'overall_status': 'PASS' if all_valid else 'FAIL',
            'critical_issues': critical_issues,
            'validation_results': validation_results,
            'total_validated': len(validation_results)
        }
        
        if all_valid:
            logger.info("✅ DCC extraction passed validation")
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

def load_dcc_config():
    """Load and validate DCC scraper configuration."""
    config_path = '/opt/airflow/config/sources/dcc_scraper.yml'
    try:
        with open(config_path, 'r') as f:
            raw = yaml.safe_load(f)
        config = _sub_env(raw)
        
        # Enhanced validation
        required_keys = ['dag', 'scraper', 'extraction', 'warehouse']
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required config section: {key}")
        
        logger.info(f"✅ Configuration loaded and validated from {config_path}")
        return config
    except FileNotFoundError:
        logger.error(f"❌ Configuration file not found: {config_path}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"❌ YAML parse error: {e}")
        raise

# ---------------- EMAIL CALLBACKS ---------------- #
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
        
        # Get extraction results
        extraction_results = context['task_instance'].xcom_pull(
            task_ids='run_dcc_extraction', 
            key='extraction_results'
        ) or {}
        
        total_records = context['task_instance'].xcom_pull(
            task_ids='run_dcc_extraction', 
            key='total_records'
        ) or 0
        
        subject = f"✅ DCC SCRAPER SUCCESS: {dag_run.dag_id} - {total_records:,} records"
        
        html = f"""
        <h3>✅ DCC Cannabis License Scraper Success</h3>
        <p><strong>DAG:</strong> {dag_run.dag_id}</p>
        <p><strong>Run ID:</strong> {dag_run.run_id}</p>
        <p><strong>Execution Date:</strong> {dag_run.execution_date}</p>
        <p><strong>Total Records Extracted:</strong> {total_records:,}</p>
        
        <h4>📊 Extraction Summary by License Prefix</h4>
        <table border="1" style="border-collapse: collapse;">
        <tr><th>License Prefix</th><th>Records</th><th>Status</th></tr>
        """
        
        for prefix, result in extraction_results.items():
            if isinstance(result, int):
                status = "✅ Success"
                records = f"{result:,}"
            else:
                status = "❌ Failed" if "Failed" in str(result) else "⚠️ Warning"
                records = str(result)
            html += f"<tr><td>{prefix}</td><td>{records}</td><td>{status}</td></tr>"
        
        html += "</table>"
        
        if validation_results:
            html += f"""
            <h4>🔍 Data Validation Summary</h4>
            <p><strong>Overall Status:</strong> {validation_results.get('overall_status', 'Unknown')}</p>
            <p><strong>Records Validated:</strong> {validation_results.get('total_validated', 0)}</p>
            """
        
        send_email(to=recipients, subject=subject, html_content=html)
        logger.info(f"✅ DCC success email sent to {recipients}")
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
        
        subject = f"❌ FAILURE: {ti.dag_id}.{ti.task_id}"
        
        html = f"""
        <h3>❌ DCC Scraper Pipeline Failure</h3>
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

# Load configuration and initialize extractor
config = load_dcc_config()
enabled_prefixes = dcc_scraper.init_extractor(config)
logger.info(f"✅ Enabled license prefixes: {enabled_prefixes}")

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
    'max_active_tis_per_dag': 5,
}

# Create DAG
dag = DAG(
    config['dag']['dag_id'],
    default_args=default_args,
    description=config['dag']['description'],
    schedule_interval=config['dag']['schedule_interval'],
    max_active_runs=config['dag']['max_active_runs'],
    tags=config['dag']['tags'],
    catchup=False,
    max_active_tasks=3
)

# ---------------- CONNECTION TEST ---------------- #
def test_connection(**context):
    """Test selenium and ClickHouse connections."""
    try:
        # Test selenium/Chrome setup
        dcc_scraper.test_selenium_connection()
        logger.info("✅ Selenium connection successful")
        
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
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Connection test failed: {e}")
        raise

# ---------------- DCC EXTRACTION ---------------- #
def run_dcc_extraction(**context):
    """Run DCC license data extraction."""
    logger.info("🚀 Starting DCC Cannabis License data extraction...")
    
    try:
        execution_date = context.get('logical_date') or context.get('execution_date')
        if execution_date is None:
            execution_date = datetime.utcnow()
        
        logger.info(f"📅 Execution date: {execution_date}")
        
        # Run the extraction
        results = dcc_scraper.extract_all_licenses(**context)
        
        # Store results in context for downstream tasks
        context['task_instance'].xcom_push(key='extraction_results', value=results['extraction_results'])
        context['task_instance'].xcom_push(key='total_records', value=results['total_records'])
        
        logger.info(f"✅ DCC extraction completed: {results['total_records']} total records")
        return results['total_records']
        
    except Exception as e:
        logger.error(f"❌ DCC extraction failed: {e}")
        raise

# ---------------- WAREHOUSE HEALTH MONITORING ---------------- #
def monitor_warehouse_health(**context):
    """Monitor warehouse health for DCC data."""
    logger.info("🏥 Monitoring warehouse health for DCC data...")
    
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
            'issues': [],
            'recommendations': []
        }
        
        # Check database accessibility
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
                
                # Check DCC table
                table_name = "raw_dcc_licenses"
                full_table = f"`{raw_schema}`.`{table_name}`"
                
                try:
                    # Check if table exists
                    tables = client.query(f"SHOW TABLES FROM `{raw_schema}`")
                    table_names = [row[0] for row in tables.result_rows]
                    
                    if table_name in table_names:
                        health_status['tables_analyzed'] = 1
                        
                        # Check table structure
                        table_info = client.query(f"SHOW CREATE TABLE {full_table}")
                        create_statement = table_info.result_rows[0][0]
                        
                        if "PARTITION BY" in create_statement:
                            logger.info(f"✅ {table_name} is properly partitioned")
                            
                            # Get row count
                            count_result = client.query(f"SELECT count() FROM {full_table}")
                            row_count = count_result.result_rows[0][0]
                            logger.info(f"   📊 {table_name}: {row_count:,} total rows")
                        else:
                            logger.warning(f"   ⚠️ {table_name}: NOT PARTITIONED")
                            health_status['issues'].append(f"{table_name}: Table is not partitioned")
                    else:
                        logger.info(f"ℹ️ DCC table {table_name} does not exist yet")
                        
                except Exception as e:
                    logger.error(f"   ❌ {table_name}: Could not analyze table: {e}")
                    health_status['issues'].append(f"{table_name}: Analysis failed")
        
        except Exception as e:
            logger.error(f"❌ Database check failed: {e}")
            health_status['database_accessible'] = False
            health_status['overall_status'] = 'CRITICAL'
            health_status['issues'].append(f"Database access failed: {e}")
        
        # Determine final health status
        if health_status['issues']:
            if health_status['database_accessible']:
                health_status['overall_status'] = 'DEGRADED'
            else:
                health_status['overall_status'] = 'CRITICAL'
        
        logger.info(f"🏥 Warehouse Health Summary:")
        logger.info(f"   Status: {health_status['overall_status']}")
        logger.info(f"   Tables analyzed: {health_status['tables_analyzed']}")
        logger.info(f"   Issues: {len(health_status['issues'])}")
        
        client.close()
        
        context['task_instance'].xcom_push(key='warehouse_health', value=health_status)
        return health_status
        
    except Exception as e:
        logger.error(f"❌ Warehouse health check failed: {e}")
        raise

# ---------------- PIPELINE SUCCESS NOTIFICATION ---------------- #
def send_pipeline_success_notification(**context):
    """Send DCC pipeline success notification."""
    try:
        # Get results from previous tasks
        total_records = context['task_instance'].xcom_pull(
            task_ids='run_dcc_extraction', 
            key='total_records'
        ) or 0
        
        extraction_results = context['task_instance'].xcom_pull(
            task_ids='run_dcc_extraction', 
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
        
        summary = f"""
        🎉 DCC CANNABIS LICENSE SCRAPER SUCCESS 🎉
        
        📊 Data Extraction:
        - Total records loaded: {total_records:,}
        - License prefixes processed: {len(extraction_results)}
        - Validation status: {validation_results.get('overall_status', 'Unknown')}
        
        🏥 Warehouse Health:
        - Status: {warehouse_health.get('overall_status', 'Unknown')}
        - Tables analyzed: {warehouse_health.get('tables_analyzed', 0)}
        
        📈 License Prefix Details:
        """
        
        for prefix, result in extraction_results.items():
            if isinstance(result, int):
                summary += f"        - {prefix}: {result:,} records\n"
            else:
                summary += f"        - {prefix}: {result}\n"
        
        logger.info(summary)
        
        # Send email notification
        send_success_email(context)
        
        logger.info("✅ DCC pipeline success notification sent")
        return "DCC success notification sent"
        
    except Exception as e:
        logger.error(f"❌ Failed to send pipeline notification: {e}")
        return f"Notification failed: {e}"

# ---------------- DAG DEFINITION ---------------- #
with dag:
    # Start task
    start_task = EmptyOperator(task_id='start_pipeline')
    
    # Connection test
    test_task = PythonOperator(
        task_id='test_connections',
        python_callable=test_connection,
        retries=2,
        retry_delay=timedelta(minutes=2)
    )
    
    # DCC extraction
    extraction_task = PythonOperator(
        task_id='run_dcc_extraction',
        python_callable=run_dcc_extraction,
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=5)
    )
    
    # Validation task
    validation_task = PythonOperator(
        task_id='validate_extraction_integrity',
        python_callable=validate_extraction_integrity,
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

# Define task dependencies
start_task >> test_task >> extraction_task >> validation_task >> monitor_task >> success_notification_task >> end_task