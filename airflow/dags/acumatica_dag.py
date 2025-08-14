# acumatica_dag.py - Acumatica ERP pipeline with incremental extraction (single schema)
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
import copy

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Allow importing extractor
sys.path.append('/opt/airflow')
sys.path.append('/opt/airflow/config')

from extractors.acumatica import extractor as acumatica

ENV_PATTERN = re.compile(r'^\$\{([A-Z0-9_]+)\}$')

# ---- DAG-run type shim (works on 2.3 → 2.9) -----------------------------
try:
    from airflow.utils.types import DagRunType          # Airflow ≥2.6 / 2.7
except ImportError:                                     # older / some 2.8 builds
    from airflow.models.dagrun import DagRunType
# -------------------------------------------------------------------------

def run_coordinated_extraction(**context):
    """
    Extract all endpoints into single schema without branch filtering.
    """
    logical_dt = context['logical_date']
    dag_run = context['dag_run']
    
    cfg = copy.deepcopy(config)
    cfg['extraction']['source_system'] = "acumatica"
    cfg['warehouse']['schemas']['raw_schema'] = config['warehouse']['schemas']['raw_schema']
    cfg['extraction']['incremental']['state_path'] = "/opt/airflow/state/acumatica_watermarks.json"
    
    acumatica.init_extractor(cfg)
    result = acumatica.extract_all_endpoints_with_dependencies(
        raw_schema=config['warehouse']['schemas']['raw_schema'],
        **context
    )
    
    total_rows = result['total_records']
    extraction_results = result['extraction_results']

    # push results to XCom for downstream tasks
    ti = context['task_instance']
    ti.xcom_push(key='extraction_results', value=extraction_results)
    ti.xcom_push(key='total_records', value=total_rows)

    logger.info(f"✅ Extraction complete – {total_rows:,} rows total")
    return total_rows

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
    """
    Validates each successful extraction in the single schema.
    """
    import clickhouse_connect
    logger.info("🔍 Validating Acumatica extraction data integrity...")

    # pull results produced by run_coordinated_extraction
    extraction_results = context['task_instance'].xcom_pull(
        task_ids='run_coordinated_extraction', key='extraction_results'
    ) or {}

    raw_schema = config['warehouse']['schemas']['raw_schema']

    # keep only successful loads
    successful = {
        k: v for k, v in extraction_results.items() if isinstance(v, int) and v > 0
    }
    if not successful:
        logger.warning("⚠️ No successful extractions to validate")
        return {'status': 'no_successful_extractions'}

    logger.info(f"🔍 Validating {len(successful)} successful extractions")

    # ClickHouse connection
    client = clickhouse_connect.get_client(
        host=os.getenv('CLICKHOUSE_HOST'),
        port=int(os.getenv('CLICKHOUSE_PORT', 8443)),
        username=os.getenv('CLICKHOUSE_USER', 'default'),
        password=os.getenv('CLICKHOUSE_PASSWORD'),
        database=os.getenv('CLICKHOUSE_DATABASE', 'default'),
        secure=True
    )

    validation = {'endpoint_results': {}, 'critical_issues': [], 'warnings': []}

    for endpoint_key, expected_rows in successful.items():
        table = f"`{raw_schema}`.`raw_{endpoint_key}`"
        try:
            latest_ts = client.query(
                f"SELECT max(_extracted_at) FROM {table}"
            ).result_rows[0][0]
            actual_rows = client.query(
                f"SELECT count() FROM {table} WHERE _extracted_at = '{latest_ts}'"
            ).result_rows[0][0]

            status = "PASS" if actual_rows == expected_rows else "COUNT_MISMATCH"
            if status != "PASS":
                validation['critical_issues'].append(
                    f"{endpoint_key}: expected {expected_rows}, got {actual_rows}"
                )

            validation['endpoint_results'][endpoint_key] = {
                'expected': expected_rows,
                'actual': actual_rows,
                'status': status,
                'latest_extraction': latest_ts
            }
            logger.info(f"📊 {endpoint_key}: {status}")

        except Exception as e:
            logger.error(f"❌ Validation failed for {endpoint_key}: {e}")
            validation['critical_issues'].append(f"{endpoint_key}: {e}")
            validation['endpoint_results'][endpoint_key] = {
                'status': 'ERROR', 'error': str(e)
            }

    client.close()

    validation['overall_status'] = (
        'PASS' if not validation['critical_issues'] else 'FAIL'
    )

    if validation['overall_status'] == 'FAIL':
        raise ValueError(
            f"Data validation failed – {len(validation['critical_issues'])} critical issues"
        )

    context['task_instance'].xcom_push(key='validation_results', value=validation)
    return validation

def load_acumatica_config():
    """Load and validate Acumatica configuration."""
    config_path = '/opt/airflow/config/sources/acumatica.yml'
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
        t = schedule_config.get('time', "06:00")
        h, m = t.split(':')
        return f"{m} {h} * * *"
    if stype == 'weekly':
        t = schedule_config.get('time', "06:00")
        h, m = t.split(':')
        days = {'monday': 1, 'tuesday': 2, 'wednesday': 3, 'thursday': 4, 
                'friday': 5, 'saturday': 6, 'sunday': 0}
        dnum = days.get(schedule_config.get('day_of_week', 'monday').lower(), 1)
        return f"{m} {h} * * {dnum}"
    if stype == 'monthly':
        t = schedule_config.get('time', "06:00")
        h, m = t.split(':')
        day = schedule_config.get('day_of_month', 1)
        return f"{m} {h} {day} * *"
    if stype == 'cron':
        return schedule_config.get('cron_expression', '0 6 * * *')
    
    return '0 6 * * *'  # Default daily at 6 AM

# Load configuration and initialize extractor
config = load_acumatica_config()
enabled_endpoints = acumatica.init_extractor(config)
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
        <h3>✅ Acumatica Extraction Pipeline Success</h3>
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
        <h3>❌ Acumatica Pipeline Failure</h3>
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
    """Test Acumatica connection and validate prerequisites."""
    try:
        # Check required environment variables
        username = os.getenv('ACUMATICA_USERNAME')
        password = os.getenv('ACUMATICA_PASSWORD')
        
        if not username:
            raise ValueError("Missing required environment variable: ACUMATICA_USERNAME")
        
        if not password:
            raise ValueError("Missing required environment variable: ACUMATICA_PASSWORD")
        
        logger.info(f"✅ Username configured: {username}")
        
        # Test Acumatica API connection
        session = acumatica.create_authenticated_session()
        logger.info("✅ Acumatica API connection successful")
        
        # Test ClickHouse connection
        import clickhouse_connect
        
        host = os.getenv('CLICKHOUSE_HOST')
        port = int(os.getenv('CLICKHOUSE_PORT', 8443))
        database = os.getenv('CLICKHOUSE_DATABASE', 'default')
        username_ch = os.getenv('CLICKHOUSE_USER', 'default')
        password_ch = os.getenv('CLICKHOUSE_PASSWORD')
        
        if not all([host, password_ch]):
            raise ValueError("ClickHouse connection parameters missing")
        
        client = clickhouse_connect.get_client(
            host=host, port=port, username=username_ch, password=password_ch,
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
        acumatica.finalize_state_after_warehouse_load(context)
        
        logger.info("✅ Extraction state finalized successfully")
        return "State finalized successfully"
        
    except Exception as e:
        logger.error(f"❌ Failed to finalize extraction state: {e}")
        raise

# ---------------- SKIP TRANSFORMATIONS ---------------- #
def skip_dbt_transformations(**context):
    """Skip dbt transformations - handled by separate unified silver DAG."""
    logger.info("⏭️ Skipping dbt transformations (handled by unified silver DAG)")
    return {"status": "skipped", "reason": "Silver processing moved to unified DAG"}

def skip_data_quality_checks(**context):
    """Skip data quality checks - handled by unified silver DAG."""
    logger.info("⏭️ Skipping data quality checks (handled by unified silver DAG)")
    return {"status": "skipped", "reason": "Quality checks moved to unified DAG"}

def monitor_warehouse_health(**context):
    """
    Check ClickHouse accessibility and basic table health for the single schema.
    """
    import clickhouse_connect
    logger.info("🏥 Monitoring warehouse health for Acumatica (single schema) …")

    raw_schema = config['warehouse']['schemas']['raw_schema']

    # ------ connect once ------
    client = clickhouse_connect.get_client(
        host=os.getenv('CLICKHOUSE_HOST'),
        port=int(os.getenv('CLICKHOUSE_PORT', 8443)),
        username=os.getenv('CLICKHOUSE_USER', 'default'),
        password=os.getenv('CLICKHOUSE_PASSWORD'),
        database=os.getenv('CLICKHOUSE_DATABASE', 'default'),
        secure=True
    )

    overall = {
        'overall_status': 'HEALTHY',
        'schema': raw_schema,
        'tables_analyzed': 0,
        'partitioned_tables': 0,
        'issues': [],
        'recommendations': []
    }

    logger.info(f"🔎 Analysing schema {raw_schema}")

    try:
        # create in case it doesn't exist
        client.command(f"CREATE DATABASE IF NOT EXISTS `{raw_schema}`")
        tables = client.query(f"SHOW TABLES FROM `{raw_schema}`").result_rows
        table_names = [t[0] for t in tables]
        overall['tables_analyzed'] = len(table_names)

        for t in table_names:
            ddl = client.query(
                f"SHOW CREATE TABLE `{raw_schema}`.`{t}`"
            ).result_rows[0][0]

            if "PARTITION BY" in ddl:
                overall['partitioned_tables'] += 1
            else:
                overall['issues'].append(f"{t}: not partitioned")
                overall['recommendations'].append(
                    f"{t}@{raw_schema}: recreate with partitioning"
                )

    except Exception as e:
        msg = f"{raw_schema}: cannot analyse – {e}"
        overall['issues'].append(msg)

    client.close()

    # decide global status
    if overall['issues']:
        overall['overall_status'] = 'DEGRADED'
        logger.warning(f"⚠️ Warehouse issues: {overall['issues']}")

    context['task_instance'].xcom_push(key='warehouse_health', value=overall)
    logger.info("✅ Warehouse health check complete")
    return overall

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
        🎉 ACUMATICA EXTRACTION SUCCESS SUMMARY 🎉
        
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
        task_id='skip_dbt_transformations',
        python_callable=skip_dbt_transformations,
        provide_context=True,
        retries=0,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    # Quality check task
    quality_check_task = PythonOperator(
        task_id='skip_data_quality_checks',
        python_callable=skip_data_quality_checks,
        provide_context=True,
        retries=0,
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
start_task >> test_task >> extraction_task >> validation_task >> finalize_state_task >> monitor_task >> transform_task >> quality_check_task >> success_notification_task >> end_task