# google_sheets_dag.py - Google Sheets ETL pipeline with full refresh and incremental loading
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

from extractors.google_sheets import extractor as google_sheets

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
    logger.info("🔍 Validating Google Sheets extraction data integrity...")
    
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
            sheet: count for sheet, count in extraction_results.items()
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
        
        for sheet_name, expected_records in extraction_results.items():
            if isinstance(expected_records, int) and expected_records > 0:
                table_name = f"raw_{sheet_name}"
                full_table = f"`{raw_schema}`.`{table_name}`"
                
                try:
                    # Get latest extraction timestamp
                    latest_extraction_query = f"""
                        SELECT max(_extracted_at) 
                        FROM {full_table}
                    """
                    latest_extraction = client.query(latest_extraction_query).result_rows[0][0]
                    
                    if not latest_extraction:
                        critical_issues.append(f"{sheet_name}: No extraction timestamp found")
                        continue
                    
                    # Count records from latest extraction
                    actual_records_query = f"""
                        SELECT count() 
                        FROM {full_table} 
                        WHERE _extracted_at = '{latest_extraction}'
                    """
                    actual_records = client.query(actual_records_query).result_rows[0][0]
                    
                    # Basic data quality checks
                    quality_checks = {}
                    
                    # Check for completely empty rows
                    empty_rows_check = client.query(f"""
                        SELECT count(*) 
                        FROM {full_table} 
                        WHERE _extracted_at = '{latest_extraction}'
                        AND length(concat(ifNull(toString(*),' '))) <= 10
                    """).result_rows[0][0]
                    quality_checks['empty_rows'] = empty_rows_check
                    
                    # Check for duplicate rows (if not full refresh)
                    sheet_config = config['extraction']['sheets'].get(sheet_name, {})
                    if not sheet_config.get('full_refresh', False):
                        duplicate_check = client.query(f"""
                            SELECT count(*) - count(DISTINCT *) as duplicates
                            FROM {full_table} 
                            WHERE _extracted_at = '{latest_extraction}'
                        """).result_rows[0][0]
                        quality_checks['duplicates'] = duplicate_check
                    else:
                        quality_checks['duplicates'] = 0
                    
                    # Validation status determination
                    status = "✅ VALID"
                    issues = []
                    
                    if actual_records != expected_records:
                        status = "❌ RECORD MISMATCH"
                        issues.append(f"Expected {expected_records}, got {actual_records}")
                        critical_issues.append(f"{sheet_name}: Record count mismatch")
                    
                    if quality_checks.get('duplicates', 0) > 0:
                        status = "❌ DUPLICATES FOUND"
                        issues.append(f"{quality_checks['duplicates']} duplicate records")
                        critical_issues.append(f"{sheet_name}: {quality_checks['duplicates']} duplicates")
                    
                    if quality_checks.get('empty_rows', 0) > expected_records * 0.1:  # >10% empty
                        status = "❌ DATA QUALITY ISSUES"
                        issues.append(f"{quality_checks['empty_rows']} empty rows")
                        critical_issues.append(f"{sheet_name}: High empty row rate")
                    
                    validation_results[sheet_name] = {
                        'expected': expected_records,
                        'actual': actual_records,
                        'quality_checks': quality_checks,
                        'status': status,
                        'issues': issues,
                        'latest_extraction': latest_extraction
                    }
                    
                    logger.info(f"📊 {sheet_name}: {status}")
                    logger.info(f"   Expected: {expected_records}, Actual: {actual_records}")
                    logger.info(f"   Quality checks: {quality_checks}")
                    
                except Exception as e:
                    logger.error(f"❌ Failed to validate {sheet_name}: {e}")
                    validation_results[sheet_name] = {'status': f'❌ VALIDATION ERROR: {e}'}
                    critical_issues.append(f"{sheet_name}: Validation failed - {e}")
        
        client.close()
        
        # Overall validation status
        all_valid = len(critical_issues) == 0
        
        validation_summary = {
            'overall_status': 'PASS' if all_valid else 'FAIL',
            'critical_issues': critical_issues,
            'warnings': warnings,
            'sheet_results': validation_results,
            'total_sheets_validated': len(validation_results)
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

def load_google_sheets_config():
    """Load and validate Google Sheets configuration."""
    config_path = '/opt/airflow/config/sources/google_sheets.yml'
    try:
        with open(config_path, 'r') as f:
            raw = yaml.safe_load(f)
        config = _sub_env(raw)
        
        # Enhanced validation
        required_keys = ['dag', 'google_sheets', 'extraction', 'warehouse']
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required config section: {key}")
        
        # Validate critical nested keys
        if 'spreadsheet_id' not in config['google_sheets']:
            raise ValueError("Missing google_sheets.spreadsheet_id configuration")
        
        logger.info(f"✅ Configuration loaded and validated from {config_path}")
        return config
    except FileNotFoundError:
        logger.error(f"❌ Configuration file not found: {config_path}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"❌ YAML parse error: {e}")
        raise

def get_schedule_interval(config):
    """Get the most frequent schedule from all enabled sheets."""
    # Find the most frequent schedule needed across all sheets
    sheet_configs = config['extraction']['sheets']
    
    # Default fallback
    if not sheet_configs:
        return "0 5 * * *"  # Daily at 5 AM
    
    # Get all cron schedules from enabled sheets
    cron_schedules = []
    for sheet_name, sheet_config in sheet_configs.items():
        if not sheet_config.get('enabled', True):
            continue
        
        schedule_config = sheet_config.get('schedule', {})
        if 'cron' in schedule_config:
            cron_schedules.append(schedule_config['cron'])
    
    if not cron_schedules:
        return "0 5 * * *"  # Default to daily at 5 AM
    
    # For simplicity, use the first cron schedule found
    # In production, you might want to calculate the GCD of all schedules
    # or run at the most frequent interval (e.g., every hour if any sheet needs hourly)
    return cron_schedules[0]

# Load configuration and initialize extractor
config = load_google_sheets_config()
enabled_sheets = google_sheets.init_extractor(config)
logger.info(f"✅ Enabled sheets: {list(enabled_sheets.keys())}")

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
        <h3>✅ Google Sheets Extraction Pipeline Success</h3>
        <p><strong>DAG:</strong> {dag_run.dag_id}</p>
        <p><strong>Run ID:</strong> {dag_run.run_id}</p>
        <p><strong>Execution Date:</strong> {dag_run.execution_date}</p>
        <p><strong>Total Records Extracted:</strong> {total_records:,}</p>
        
        <h4>📊 Extraction Summary by Sheet</h4>
        <table border="1" style="border-collapse: collapse;">
        <tr><th>Sheet</th><th>Records</th><th>Status</th><th>Type</th></tr>
        """
        
        for sheet, result in extraction_results.items():
            if isinstance(result, int):
                status = "✅ Success"
                records = f"{result:,}"
                sheet_config = config['extraction']['sheets'].get(sheet, {})
                refresh_type = "Full Refresh" if sheet_config.get('full_refresh', False) else "Incremental"
            else:
                status = "❌ Failed" if "Failed" in str(result) else "⚠️ Warning"
                records = str(result)
                refresh_type = "N/A"
            html += f"<tr><td>{sheet}</td><td>{records}</td><td>{status}</td><td>{refresh_type}</td></tr>"
        
        html += "</table>"
        
        if validation_results:
            html += f"""
            <h4>🔍 Data Validation Summary</h4>
            <p><strong>Overall Status:</strong> {validation_results.get('overall_status', 'Unknown')}</p>
            <p><strong>Sheets Validated:</strong> {validation_results.get('total_sheets_validated', 0)}</p>
            """
            
            if validation_results.get('warnings'):
                html += f"<p><strong>Warnings:</strong> {len(validation_results['warnings'])}</p>"
                html += "<ul>"
                for warning in validation_results['warnings'][:3]:  # Show first 3
                    html += f"<li>{warning}</li>"
                html += "</ul>"
        
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
        <h3>❌ Google Sheets Pipeline Failure</h3>
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
    """Test Google Sheets connection and validate prerequisites."""
    try:
        # Test Google Sheets API connection
        google_sheets.test_google_sheets_connection()
        logger.info("✅ Google Sheets API connection successful")
        
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

# ---------------- SHEET SCHEDULING LOGIC ---------------- #
def should_extract_sheet(sheet_name, sheet_config, execution_date):
    """Check if a sheet should be extracted based on its schedule."""
    from croniter import croniter
    
    if not sheet_config.get('enabled', True):
        logger.info(f"⏭️ Skipping {sheet_name}: Sheet is disabled")
        return False
    
    schedule_config = sheet_config.get('schedule', {})
    
    # If no schedule is specified, extract every time
    if not schedule_config:
        logger.info(f"✅ {sheet_name}: No schedule specified, extracting")
        return True
    
    # Handle cron-based scheduling
    if 'cron' in schedule_config:
        cron_expr = schedule_config['cron']
        try:
            cron = croniter(cron_expr, execution_date)
            # Check if the execution_date matches the cron schedule
            # We look back a bit to account for slight timing differences
            prev_run = cron.get_prev(datetime)
            next_run = cron.get_next(datetime)
            
            # If execution time is within 5 minutes of a scheduled time, consider it a match
            time_diff_prev = abs((execution_date - prev_run).total_seconds())
            time_diff_next = abs((next_run - execution_date).total_seconds())
            
            if min(time_diff_prev, time_diff_next) <= 300:  # 5 minutes tolerance
                logger.info(f"✅ {sheet_name}: Cron schedule matched ({cron_expr})")
                return True
            else:
                logger.info(f"⏭️ Skipping {sheet_name}: Cron schedule not matched ({cron_expr})")
                logger.info(f"   Current time: {execution_date}")
                logger.info(f"   Previous scheduled: {prev_run}")
                logger.info(f"   Next scheduled: {next_run}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Invalid cron expression for {sheet_name}: {cron_expr} - {e}")
            # If cron is invalid, extract anyway to be safe
            return True
    
    # Default: extract if we don't understand the schedule
    logger.warning(f"⚠️ {sheet_name}: Unknown schedule format, extracting anyway")
    return True

def is_manual_run(dag_run):
    """Check if this is a manual DAG run."""
    if not dag_run:
        return False
    
    # Check multiple indicators of manual runs
    is_manual = (
        dag_run.run_type == 'manual' or 
        'manual__' in dag_run.run_id or
        dag_run.external_trigger or
        dag_run.run_id.startswith('manual')
    )
    
    return is_manual
# ---------------- COORDINATED EXTRACTION ---------------- #
def run_coordinated_extraction(**context):
    """Run extraction with individual sheet scheduling logic."""
    logger.info("🚀 Starting coordinated Google Sheets extraction with individual scheduling...")
    
    try:
        execution_date = context.get('logical_date') or context.get('execution_date')
        if execution_date is None:
            execution_date = datetime.utcnow()
        
        dag_run = context.get('dag_run')
        manual_run = is_manual_run(dag_run)
        force_extract = config['extraction'].get('force_extract_all', False)
        
        logger.info(f"📅 Execution date: {execution_date}")
        logger.info(f"🔍 DAG run info: {dag_run.run_id if dag_run else 'None'}, type: {dag_run.run_type if dag_run else 'None'}")
        logger.info(f"🔍 Manual run detected: {manual_run}")
        logger.info(f"🔍 Force extract all: {force_extract}")
        
        if manual_run or force_extract:
            logger.info("🖱️ Manual run or force extract - extracting all enabled sheets")
        
        # Check which sheets should be extracted based on their individual schedules
        sheets_to_extract = []
        skipped_sheets = []
        
        for sheet_name, sheet_config in config['extraction']['sheets'].items():
            # For manual runs or force extract, extract all enabled sheets regardless of schedule
            if manual_run or force_extract:
                if sheet_config.get('enabled', True):
                    sheets_to_extract.append(sheet_name)
                    logger.info(f"✅ {sheet_name}: Manual/forced run - extracting")
                else:
                    skipped_sheets.append(sheet_name)
                    logger.info(f"⏭️ {sheet_name}: Disabled")
            else:
                # For scheduled runs, check the schedule
                if should_extract_sheet(sheet_name, sheet_config, execution_date):
                    sheets_to_extract.append(sheet_name)
                else:
                    skipped_sheets.append(sheet_name)
        
        logger.info(f"📊 Sheets to extract: {sheets_to_extract}")
        if skipped_sheets:
            logger.info(f"⏭️ Sheets skipped: {skipped_sheets}")
        
        if not sheets_to_extract:
            if manual_run:
                logger.warning("⚠️ Manual run but no enabled sheets found!")
            else:
                logger.info("ℹ️ No sheets scheduled for extraction at this time")
            return {
                'total_records': 0,
                'extraction_results': {},
                'completed_sheets': [],
                'skipped_sheets': skipped_sheets
            }
        
        # Use the coordinated extraction function with filtered sheets
        results = google_sheets.extract_filtered_sheets(sheets_to_extract, **context)
        
        # Add skipped sheets to results
        results['skipped_sheets'] = skipped_sheets
        
        # Store results in context for downstream tasks
        context['task_instance'].xcom_push(key='extraction_results', value=results['extraction_results'])
        context['task_instance'].xcom_push(key='total_records', value=results['total_records'])
        context['task_instance'].xcom_push(key='completed_sheets', value=results['completed_sheets'])
        context['task_instance'].xcom_push(key='skipped_sheets', value=results['skipped_sheets'])
        
        logger.info(f"✅ Coordinated extraction completed: {results['total_records']} total records from {len(results['completed_sheets'])} sheets")
        return results['total_records']
        
    except Exception as e:
        logger.error(f"❌ Coordinated extraction failed: {e}")
        raise

# ---------------- WAREHOUSE HEALTH MONITORING ---------------- #
def monitor_warehouse_health(**context):
    """Monitor warehouse health with enhanced metrics for Google Sheets."""
    logger.info("🏥 Monitoring warehouse health for Google Sheets...")
    
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
        
        client.close()
        
        # Store health status for monitoring
        context['task_instance'].xcom_push(key='warehouse_health', value=health_status)
        
        return health_status
        
    except Exception as e:
        logger.error(f"❌ Warehouse health check failed: {e}")
        raise

# ---------------- PIPELINE SUMMARY ---------------- #
def send_pipeline_success_notification(**context):
    """Send extraction pipeline success notification."""
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
        🎉 GOOGLE SHEETS EXTRACTION SUCCESS SUMMARY 🎉
        
        📊 Data Extraction:
        - Total records loaded: {total_records:,}
        - Sheets processed: {len(extraction_results)}
        - Validation status: {validation_results.get('overall_status', 'Unknown')}
        
        🏥 Warehouse Health:
        - Status: {warehouse_health.get('overall_status', 'Unknown')}
        - Tables analyzed: {warehouse_health.get('tables_analyzed', 0)}
        - Partitioned tables: {warehouse_health.get('partitioned_tables', 0)}
        
        📈 Sheet Details:
        """
        
        for sheet, result in extraction_results.items():
            if isinstance(result, int):
                sheet_config = config['extraction']['sheets'].get(sheet, {})
                refresh_type = "Full Refresh" if sheet_config.get('full_refresh', False) else "Incremental"
                summary += f"        - {sheet} ({refresh_type}): {result:,} records\n"
            else:
                summary += f"        - {sheet}: {result}\n"
        
        if validation_results.get('warnings'):
            summary += f"\n⚠️ Warnings ({len(validation_results['warnings'])}):\n"
            for warning in validation_results['warnings'][:5]:  # Show first 5
                summary += f"        - {warning}\n"
        
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