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
from croniter import croniter
import copy
# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Allow importing extractor
sys.path.append('/opt/airflow')
sys.path.append('/opt/airflow/config')

from extractors.leaflink import extractor as leaflink

ENV_PATTERN = re.compile(r'^\$\{([A-Z0-9_]+)\}$')




# ---------- DROP-IN ---------- #
from croniter import croniter
import copy
# ---- DAG-run type shim (works on 2.3 → 2.9) -----------------------------
try:
    from airflow.utils.types import DagRunType          # Airflow ≥2.6 / 2.7
except ImportError:                                     # older / some 2.8 builds
    from airflow.models.dagrun import DagRunType
# -------------------------------------------------------------------------


def _due_this_run(cron_expr: str, logical_dt: datetime) -> bool:
    """Return True when cron fires exactly at logical_dt (UTC)."""
    return croniter(cron_expr, logical_dt - timedelta(seconds=1)).\
           get_next(datetime) == logical_dt

def _run_company(company_cfg, **context):
    cfg = copy.deepcopy(config)

    # NEW ─ identify the state rows for this run
    cfg['extraction']['source_system'] = f"leaflink_{company_cfg['region']}"

    cfg['warehouse']['schemas']['raw_schema'] = company_cfg['raw_schema']
    cfg['extraction']['incremental']['state_path'] = (
        f"/opt/airflow/state/leaflink_{company_cfg['region']}_watermarks.json"
    )
    leaflink.init_extractor(cfg)
    return leaflink.extract_all_endpoints_with_dependencies(
        company_id=company_cfg['company_id'],
        raw_schema=company_cfg['raw_schema'],
        **context
    )


def run_coordinated_extraction(**context):
    """
    * Scheduled runs * → obey each company's schedule_cron.
    * Manual runs     → run **all** companies, ignoring their cron.
    """
    logical_dt = context['logical_date']
    dag_run     = context['dag_run']
    manual      = (dag_run.external_trigger or
                   dag_run.run_type == DagRunType.MANUAL)

    total_rows, combined_results = 0, {}

    for comp in config['companies']:
        # Skip only when scheduled and not yet due
        if not manual and not _due_this_run(comp['schedule_cron'], logical_dt):
            logger.info(f"⏭️  {comp['region'].upper()} not scheduled this tick")
            continue

        out = _run_company(comp, **context)
        total_rows += out['total_records']
        # prefix results with region so validation can pick the right schema
        combined_results.update({
            f"{comp['region']}_{k}": v for k, v in out['extraction_results'].items()
        })

    # push results to XCom for downstream tasks
    ti = context['task_instance']
    ti.xcom_push(key='extraction_results', value=combined_results)
    ti.xcom_push(key='total_records',      value=total_rows)

    logger.info(f"✅ Extraction complete – {total_rows:,} rows total")
    return total_rows
# ---------- END DROP-IN ---------- #



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
    Validates each successful extraction. Works with multi-company schemas by
    inferring the correct ClickHouse database from the endpoint key’s region
    prefix (e.g.  nj_orders_received  →  bronze_leaflink_nj).
    """
    import clickhouse_connect
    logger.info("🔍 Validating LeafLink extraction data integrity...")

    # pull results produced by run_coordinated_extraction
    extraction_results = context['task_instance'].xcom_pull(
        task_ids='run_coordinated_extraction', key='extraction_results'
    ) or {}

    # map region → raw_schema from leaflink.yml
    region_to_schema = {
        c['region']: c['raw_schema'] for c in config.get('companies', [])
    }

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

    for compound_key, expected_rows in successful.items():
        # split   nj_orders_received  →  region = nj, ep = orders_received
        region, endpoint = compound_key.split('_', 1)
        raw_schema = region_to_schema.get(region)
        if not raw_schema:
            validation['warnings'].append(f"{compound_key}: unknown region")
            continue

        table = f"`{raw_schema}`.`raw_{endpoint}`"
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
                    f"{compound_key}: expected {expected_rows}, got {actual_rows}"
                )

            validation['endpoint_results'][compound_key] = {
                'expected': expected_rows,
                'actual': actual_rows,
                'status': status,
                'latest_extraction': latest_ts
            }
            logger.info(f"📊 {compound_key}: {status}")

        except Exception as e:
            logger.error(f"❌ Validation failed for {compound_key}: {e}")
            validation['critical_issues'].append(f"{compound_key}: {e}")
            validation['endpoint_results'][compound_key] = {
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
# def run_coordinated_extraction(**context):
#     """Loop through all companies; honour their individual cron schedules."""
#     logical_dt = context['logical_date']      # passed by Airflow
#     total, combined = 0, {}

#     for comp in config['companies']:
#         if not _due_this_run(comp['schedule_cron'], logical_dt):
#             logger.info(f"⏭️  {comp['region'].upper()} not scheduled this tick")
#             continue

#         out = _run_company(comp, **context)
#         total += out['total_records']
#         combined.update({f"{comp['region']}_{k}": v for k, v in out['extraction_results'].items()})

#     context['task_instance'].xcom_push(key='extraction_results', value=combined)
#     context['task_instance'].xcom_push(key='total_records', value=total)
#     logger.info(f"✅ All companies complete: {total} rows")
#     return total


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
    """
    Check ClickHouse accessibility and basic table health for **each company
    schema** listed in leaflink.yml → companies[].raw_schema.
    Aggregates the per-schema results into one report.
    """
    import clickhouse_connect
    logger.info("🏥 Monitoring warehouse health for LeafLink (multi-schema) …")

    # Map region → raw_schema from config
    region_to_schema = {
        c['region']: c['raw_schema'] for c in config.get('companies', [])
    }

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
        'schemas': {},
        'issues': [],
        'recommendations': []
    }

    for region, raw_schema in region_to_schema.items():
        schema_report = {
            'tables_analyzed': 0,
            'partitioned_tables': 0,
            'issues': []
        }
        logger.info(f"🔎 Analysing schema {raw_schema} ({region.upper()})")

        try:
            # create in case it doesn't exist
            client.command(f"CREATE DATABASE IF NOT EXISTS `{raw_schema}`")
            tables = client.query(f"SHOW TABLES FROM `{raw_schema}`").result_rows
            table_names = [t[0] for t in tables]
            schema_report['tables_analyzed'] = len(table_names)

            for t in table_names:
                ddl = client.query(
                    f"SHOW CREATE TABLE `{raw_schema}`.`{t}`"
                ).result_rows[0][0]

                if "PARTITION BY" in ddl:
                    schema_report['partitioned_tables'] += 1
                else:
                    schema_report['issues'].append(f"{t}: not partitioned")
                    overall['recommendations'].append(
                        f"{t}@{raw_schema}: recreate with partitioning"
                    )

        except Exception as e:
            msg = f"{raw_schema}: cannot analyse – {e}"
            schema_report['issues'].append(msg)
            overall['issues'].append(msg)

        overall['schemas'][raw_schema] = schema_report

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