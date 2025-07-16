# repsly_dag.py - Configuration-driven DAG
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
import yaml
from requests.auth import HTTPBasicAuth

# ========================
# CONFIGURATION LOADING
# ========================

def load_repsly_config():
    """Load configuration from repsly.yml"""
    config_path = '/opt/airflow/config/sources/repsly.yml'
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
        return schedule_config.get('cron_expression', '0 2 * * *')
    else:
        return '0 2 * * *'  # Default to daily at 2 AM

# Load configuration
config = load_repsly_config()

# ========================
# DAG CONFIGURATION FROM YAML
# ========================

# Default arguments from config
default_args = {
    'owner': config['dag']['owner'],
    'depends_on_past': False,
    'start_date': datetime.strptime(config['dag']['start_date'], '%Y-%m-%d'),
    'email_on_failure': config['dag']['email_on_failure'],
    'email_on_retry': config['dag']['email_on_retry'],
    'retries': config['dag']['retries'],
    'retry_delay': timedelta(minutes=config['dag']['retry_delay_minutes']),
    'catchup': config['dag']['catchup'],
    'email': config['notifications']['email']['failure_recipients']
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
    <h2>✅ Pipeline Success Notification</h2>
    
    <p><strong>DAG:</strong> {dag_run.dag_id}</p>
    <p><strong>Run ID:</strong> {dag_run.run_id}</p>
    <p><strong>Execution Date:</strong> {dag_run.execution_date}</p>
    <p><strong>Start Date:</strong> {dag_run.start_date}</p>
    <p><strong>End Date:</strong> {dag_run.end_date}</p>
    <p><strong>Duration:</strong> {dag_run.end_date - dag_run.start_date if dag_run.end_date else 'Still running'}</p>
    
    <h3>📊 Pipeline Summary</h3>
    <ul>
        <li>Data successfully extracted from Repsly API</li>
        <li>Data loaded to warehouse successfully</li>
        <li>dbt transformations completed</li>
        <li>Data quality checks passed</li>
    </ul>
    
    <p><strong>Next steps:</strong> Data is ready for analysis and reporting.</p>
    
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
    <h2>❌ Pipeline Failure Notification</h2>
    
    <p><strong>DAG:</strong> {dag_run.dag_id}</p>
    <p><strong>Task:</strong> {ti.task_id}</p>
    <p><strong>Run ID:</strong> {dag_run.run_id}</p>
    <p><strong>Execution Date:</strong> {dag_run.execution_date}</p>
    <p><strong>Start Date:</strong> {dag_run.start_date}</p>
    <p><strong>Failure Time:</strong> {ti.end_date}</p>
    
    <h3>🔍 Error Details</h3>
    <p><strong>Failed Task:</strong> {ti.task_id}</p>
    <p><strong>Try Number:</strong> {ti.try_number}</p>
    
    {f'<p><strong>Exception:</strong></p><pre style="background-color: #f5f5f5; padding: 10px; border-radius: 5px;">{str(exception)}</pre>' if exception else ''}
    
    <h3>📋 Troubleshooting Steps</h3>
    <ol>
        <li>Check the Airflow logs for detailed error information</li>
        <li>Verify API credentials and connectivity</li>
        <li>Check warehouse connectivity</li>
        <li>Review data quality and format</li>
    </ol>
    
    <p><strong>Action Required:</strong> Please investigate and resolve the issue.</p>
    
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

# Update your default_args to include email callbacks
default_args = {
    'owner': config['dag']['owner'],
    'depends_on_past': False,
    'start_date': datetime.strptime(config['dag']['start_date'], '%Y-%m-%d'),
    'email_on_failure': config['dag']['email_on_failure'],
    'email_on_retry': config['dag']['email_on_retry'],
    'retries': config['dag']['retries'],
    'retry_delay': timedelta(minutes=config['dag']['retry_delay_minutes']),
    'catchup': config['dag']['catchup'],
    'email': config['notifications']['email']['failure_recipients'],
    # Add email callbacks
    'on_failure_callback': send_failure_email,
    'on_success_callback': None,  # We'll add this to specific tasks
}

# Add success notification task at the end
def send_pipeline_success_notification(**context):
    """Send pipeline completion success email"""
    send_success_email(context)
    return "Success notification sent"

# Create success notification task
success_notification_task = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_pipeline_success_notification,
    dag=dag
)



# ========================
# CONFIGURATION CONSTANTS
# ========================

# SECURE: Get credentials from environment variables (set in Docker)
REPSLY_BASE_URL = config['api']['base_url']
REPSLY_USERNAME = os.getenv('REPSLY_USERNAME')
REPSLY_PASSWORD = os.getenv('REPSLY_PASSWORD')

# Testing vs Production configuration from config
TESTING_MODE = config['extraction']['mode'] == 'testing'
if TESTING_MODE:
    MAX_RECORDS_PER_ENDPOINT = config['extraction']['testing']['max_records_per_endpoint']
    DATE_RANGE_DAYS = config['extraction']['testing']['date_range_days']
else:
    MAX_RECORDS_PER_ENDPOINT = config['extraction']['production']['max_records_per_endpoint']
    DATE_RANGE_DAYS = config['extraction']['production']['date_range_days']

# Validate that required environment variables are set
if not all([REPSLY_USERNAME, REPSLY_PASSWORD]):
    missing_vars = []
    if not REPSLY_USERNAME: missing_vars.append('REPSLY_USERNAME')
    if not REPSLY_PASSWORD: missing_vars.append('REPSLY_PASSWORD')
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Define endpoints to extract based on config
ENDPOINTS = {
    # ✅ Core endpoints from config
    'clients': {
        'path': 'export/clients',
        'pagination_type': 'timestamp',
        'limit': 50,
        'timestamp_field': 'LastTimeStamp',
        'data_field': 'Clients',
        'total_count_field': 'TotalCount',
        'enabled': 'clients' in config['extraction']['endpoints']['always_extract'] + config['extraction']['endpoints']['optional_extract']
    },
    'client_notes': {
        'path': 'export/clientnotes',
        'pagination_type': 'id',
        'limit': 50,
        'id_field': 'LastClientNoteID',
        'data_field': 'ClientNotes',
        'total_count_field': 'TotalCount',
        'enabled': 'client_notes' in config['extraction']['endpoints']['always_extract'] + config['extraction']['endpoints']['optional_extract']
    },
    'visits': {
        'path': 'export/visits',
        'pagination_type': 'timestamp',
        'limit': 50,
        'timestamp_field': 'LastTimeStamp',
        'data_field': 'Visits',
        'total_count_field': 'TotalCount',
        'enabled': 'visits' in config['extraction']['endpoints']['always_extract'] + config['extraction']['endpoints']['optional_extract']
    },
    'daily_working_time': {
        'path': 'export/dailyworkingtime',
        'pagination_type': 'id',
        'limit': 50,
        'id_field': 'LastDailyWorkingTimeID',
        'data_field': 'DailyWorkingTime',
        'total_count_field': 'TotalCount',
        'enabled': 'daily_working_time' in config['extraction']['endpoints']['always_extract'] + config['extraction']['endpoints']['optional_extract']
    },
    'representatives': {
        'path': 'export/representatives',
        'pagination_type': 'static',
        'data_field': 'Representatives',
        'enabled': 'representatives' in config['extraction']['endpoints']['always_extract'] + config['extraction']['endpoints']['optional_extract']
    },
    'users': {
        'path': 'export/users',
        'pagination_type': 'id',
        'limit': 50,
        'id_field': 'LastUserID',
        'data_field': 'Users',
        'total_count_field': 'TotalCount',
        'enabled': 'users' in config['extraction']['endpoints']['always_extract'] + config['extraction']['endpoints']['optional_extract']
    },
    'products': {
        'path': 'export/products',
        'pagination_type': 'id',
        'limit': 50,
        'id_field': 'LastProductID',
        'data_field': 'Products',
        'total_count_field': 'TotalCount',
        'enabled': 'products' in config['extraction']['endpoints']['always_extract'] + config['extraction']['endpoints']['optional_extract']
    },
    'retail_audits': {
        'path': 'export/retailaudits',
        'pagination_type': 'id',
        'limit': 50,
        'id_field': 'LastRetailAuditID',
        'data_field': 'RetailAudits',
        'total_count_field': 'TotalCount',
        'enabled': 'retail_audits' in config['extraction']['endpoints']['always_extract'] + config['extraction']['endpoints']['optional_extract']
    },
    'purchase_orders': {
        'path': 'export/purchaseorders',
        'pagination_type': 'id',
        'limit': 50,
        'id_field': 'LastPurchaseOrderID',
        'data_field': 'PurchaseOrders',
        'total_count_field': 'TotalCount',
        'enabled': 'purchase_orders' in config['extraction']['endpoints']['always_extract'] + config['extraction']['endpoints']['optional_extract']
    },
    'document_types': {
        'path': 'export/documenttypes',
        'pagination_type': 'static',
        'data_field': 'DocumentTypes',
        'enabled': 'document_types' in config['extraction']['endpoints']['always_extract'] + config['extraction']['endpoints']['optional_extract']
    },
    'pricelists': {
        'path': 'export/pricelists',
        'pagination_type': 'static',
        'data_field': 'PriceLists',
        'enabled': 'pricelists' in config['extraction']['endpoints']['always_extract'] + config['extraction']['endpoints']['optional_extract']
    },
    'pricelist_items': {
        'path': 'export/pricelistsItems',
        'pagination_type': 'id',
        'limit': 50,
        'id_field': 'LastPriceListItemID',
        'data_field': 'PriceListItems',
        'total_count_field': 'TotalCount',
        'enabled': 'pricelist_items' in config['extraction']['endpoints']['always_extract'] + config['extraction']['endpoints']['optional_extract']
    },
    'forms': {
        'path': 'export/forms',
        'pagination_type': 'id',
        'limit': 50,
        'id_field': 'LastFormID',
        'data_field': 'Forms',
        'total_count_field': 'TotalCount',
        'enabled': 'forms' in config['extraction']['endpoints']['always_extract'] + config['extraction']['endpoints']['optional_extract']
    },
    'photos': {
        'path': 'export/photos',
        'pagination_type': 'id',
        'limit': 50,
        'id_field': 'LastPhotoID',
        'data_field': 'Photos',
        'total_count_field': 'TotalCount',
        'enabled': 'photos' in config['extraction']['endpoints']['always_extract'] + config['extraction']['endpoints']['optional_extract']
    },
    'visit_schedules': {
        'path': 'export/visitschedules',
        'pagination_type': 'datetime_range',
        'data_field': 'VisitSchedules',
        'url_pattern': 'export/visitschedules/{start_date}/{end_date}',
        'enabled': 'visit_schedules' in config['extraction']['endpoints']['always_extract'] + config['extraction']['endpoints']['optional_extract']
    },
    'visit_schedules_extended': {
        'path': 'export/schedules',
        'pagination_type': 'datetime_range', 
        'data_field': 'Schedules',
        'url_pattern': 'export/schedules/{start_date}/{end_date}',
        'enabled': 'visit_schedules_extended' in config['extraction']['endpoints']['always_extract'] + config['extraction']['endpoints']['optional_extract']
    },
    'visit_schedule_realizations': {
        'path': 'export/visitrealizations',
        'pagination_type': 'query_params',
        'data_field': 'VisitRealizations',
        'url_pattern': 'export/visitrealizations',
        'query_params': {
            'modified_field': 'modified',
            'skip_field': 'skip'
        },
        'limit': 50,
        'total_count_field': 'TotalCount',
        'enabled': 'visit_schedule_realizations' in config['extraction']['endpoints']['always_extract'] + config['extraction']['endpoints']['optional_extract']
    }
}

# Filter endpoints based on config
ENABLED_ENDPOINTS = {k: v for k, v in ENDPOINTS.items() if v.get('enabled', True) and k not in config['extraction']['endpoints']['disabled']}

def create_authenticated_session():
    """Create authenticated session for Repsly"""
    session = requests.Session()
    session.auth = HTTPBasicAuth(REPSLY_USERNAME, REPSLY_PASSWORD)
    session.headers.update({'Accept': 'application/json'})
    
    # Test authentication using config
    test_url = f"{REPSLY_BASE_URL}/{config['api']['test_endpoint']}"
    response = session.get(test_url, timeout=config['api']['rate_limiting']['timeout_seconds'])
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
    """Get data from Repsly endpoint with appropriate pagination (handles multiple patterns)"""
    all_data = []
    
    if endpoint_config['pagination_type'] == 'static':
        # Static endpoints - single request
        endpoint_url = f"{REPSLY_BASE_URL}/{endpoint_config['path']}"
        print(f"   Fetching data from: {endpoint_url}")
        
        try:
            response = session.get(endpoint_url, timeout=config['api']['rate_limiting']['timeout_seconds'])
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
    
    elif endpoint_config['pagination_type'] == 'datetime_range':
        # Date range endpoints (visit schedules)
        from datetime import datetime, timedelta
        
        # Use date range from config
        end_date = datetime.now()
        start_date = end_date - timedelta(days=DATE_RANGE_DAYS)
        
        # Format dates as required by API (usually YYYY-MM-DD or ISO format)
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        endpoint_url = f"{REPSLY_BASE_URL}/{endpoint_config['url_pattern'].format(start_date=start_date_str, end_date=end_date_str)}"
        print(f"   Fetching date range data from: {endpoint_url}")
        
        try:
            response = session.get(endpoint_url, timeout=config['api']['rate_limiting']['timeout_seconds'])
            response.raise_for_status()
            data = response.json()
            
            if endpoint_config.get('data_field'):
                records = data.get(endpoint_config['data_field'], [])
            else:
                records = data if isinstance(data, list) else [data]
            
            all_data.extend(records)
            print(f"   Retrieved {len(records)} records for date range {start_date_str} to {end_date_str}")
            
        except Exception as e:
            print(f"   Error fetching date range data: {e}")
            raise
    
    elif endpoint_config['pagination_type'] == 'query_params':
        # Query parameter based pagination (visit realizations)
        from datetime import datetime, timedelta
        
        # Start with a reasonable date - use date range from config
        start_date = datetime.now() - timedelta(days=DATE_RANGE_DAYS)
        
        # Format date as ISO string
        modified_date = start_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        
        skip = 0
        page_size = 50  # API limit
        max_pages = 190  # Respect 9,500 skip limit (9500/50 = 190 pages)
        page_count = 0
        
        print(f"   Starting pagination with modified date: {modified_date}")
        
        while page_count < max_pages:
            page_count += 1
            
            # Check skip limit (API limitation)
            if skip >= 9500:
                print(f"   Reached skip limit (9500), stopping pagination")
                break
            
            # Build the URL with proper parameters
            endpoint_url = f"{REPSLY_BASE_URL}/{endpoint_config['url_pattern']}?modified={modified_date}&skip={skip}"
            print(f"   Page {page_count}: Fetching from {endpoint_url}")
            
            try:
                response = session.get(endpoint_url, timeout=config['api']['rate_limiting']['timeout_seconds'])
                
                # Check for specific error responses
                if response.status_code == 400:
                    print(f"   400 Bad Request. Response: {response.text[:200]}")
                    # Try moving the date forward by a day and reset skip
                    if skip == 0:
                        # If this is the first attempt with this date, try a more recent date
                        start_date = start_date + timedelta(days=1)
                        modified_date = start_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                        print(f"   Trying newer date: {modified_date}")
                        continue
                    else:
                        # If we were paginating successfully but hit an error, 
                        # move date forward and reset skip
                        start_date = start_date + timedelta(days=30)
                        modified_date = start_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                        skip = 0
                        print(f"   Moving to newer date: {modified_date}, resetting skip to 0")
                        continue
                
                response.raise_for_status()
                data = response.json()
                
                # Extract records using the correct field name
                records = data.get(endpoint_config['data_field'], [])
                print(f"   Retrieved {len(records)} records (total so far: {len(all_data)})")
                
                # Check if we got any records
                if not records:
                    print(f"   No records found for date {modified_date}, trying newer date")
                    # Move forward by 30 days and reset skip
                    start_date = start_date + timedelta(days=30)
                    modified_date = start_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                    skip = 0
                    
                    # If we've moved too far into the future, stop
                    if start_date > datetime.now():
                        print(f"   Reached current date, stopping pagination")
                        break
                    continue
                
                all_data.extend(records)
                
                # Check if we've hit testing limit
                if TESTING_MODE and MAX_RECORDS_PER_ENDPOINT and len(all_data) >= MAX_RECORDS_PER_ENDPOINT:
                    all_data = all_data[:MAX_RECORDS_PER_ENDPOINT]
                    print(f"   Reached testing limit of {MAX_RECORDS_PER_ENDPOINT} records")
                    break
                
                # If we got fewer records than page size, we might be at the end
                if len(records) < page_size:
                    print(f"   Got {len(records)} records (less than page size {page_size})")
                    # Try moving forward in time to see if there's more recent data
                    start_date = start_date + timedelta(days=30)
                    modified_date = start_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                    skip = 0
                    
                    # If we've moved too far into the future, stop
                    if start_date > datetime.now():
                        print(f"   Reached current date, stopping pagination")
                        break
                    continue
                
                # Move to next page
                skip += page_size
                time.sleep(1.0 / config['api']['rate_limiting']['requests_per_second'])  # Rate limiting from config
                
            except requests.exceptions.RequestException as e:
                print(f"   Request error on page {page_count}: {e}")
                if page_count == 1:
                    # If first request fails, try a different date
                    start_date = start_date + timedelta(days=30)
                    modified_date = start_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                    skip = 0
                    page_count = 0  # Reset page count to retry
                    continue
                else:
                    # If we were paginating successfully but hit an error, 
                    # try moving forward in time
                    start_date = start_date + timedelta(days=30)
                    modified_date = start_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                    skip = 0
                    continue
            
            except Exception as e:
                print(f"   Unexpected error on page {page_count}: {e}")
                break
            
    else:
        # Standard paginated endpoints (existing logic)
        last_value = 0
        page_count = 0
        max_pages = 1000
        
        while page_count < max_pages:
            page_count += 1
            endpoint_url = f"{REPSLY_BASE_URL}/{endpoint_config['path']}/{last_value}"
            
            print(f"   Page {page_count}: Fetching from {endpoint_url}")
            
            try:
                response = session.get(endpoint_url, timeout=config['api']['rate_limiting']['timeout_seconds'])
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
                
                time.sleep(1.0 / config['api']['rate_limiting']['requests_per_second'])  # Rate limiting from config
                
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
    """Extract data from any Repsly endpoint"""
    
    if endpoint_name not in ENABLED_ENDPOINTS:
        print(f"⚠️  Endpoint {endpoint_name} is disabled in configuration")
        return 0
    
    endpoint_config = ENABLED_ENDPOINTS[endpoint_name]
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
        
        # Save to CSV with path from config
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
        
        # Get warehouse configuration from repsly config
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
            table_name = f'{raw_schema}.raw_{endpoint_name}'
            csv_files[table_name] = csv_path
    
    total_records = 0
    for table_name, csv_path in csv_files.items():
        if os.path.exists(csv_path):
            print(f"   Loading {csv_path} to PostgreSQL...")
            df = pd.read_csv(csv_path)
            
            # Extract schema and table name
            schema_name, table_name_only = table_name.split('.')
            
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
            # Check all tables in both schemas
            result = conn.execute(f"""
                SELECT schemaname, tablename, tableowner 
                FROM pg_tables 
                WHERE schemaname IN ('{staging_schema}', '{raw_schema}') 
                ORDER BY schemaname, tablename;
            """)
            
            print("📊 Tables in database:")
            for row in result.fetchall():
                print(f"   {row[0]}.{row[1]} (owner: {row[2]})")
            
            # Check all views
            result = conn.execute(f"""
                SELECT schemaname, viewname, viewowner 
                FROM pg_views 
                WHERE schemaname IN ('{staging_schema}', '{raw_schema}') 
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
        
        # Define dbt commands - you can make this configurable too
        commands = [
            f'dbt run --select clients_raw {threads} {fail_fast} {full_refresh}',
            f'dbt run --select clients {threads} {fail_fast} {full_refresh}',
            f'dbt run --select client_notes_raw {threads} {fail_fast} {full_refresh}',
            f'dbt run --select client_notes {threads} {fail_fast} {full_refresh}',
            f'dbt run --select daily_working_time_raw {threads} {fail_fast} {full_refresh}',
            f'dbt run --select daily_working_time {threads} {fail_fast} {full_refresh}',
            f'dbt run --select visits_raw {threads} {fail_fast} {full_refresh}',
            f'dbt run --select visits {threads} {fail_fast} {full_refresh}',
            f'dbt run --select representatives_raw {threads} {fail_fast} {full_refresh}',
            f'dbt run --select representatives {threads} {fail_fast} {full_refresh}',
            f'dbt run --select users_raw {threads} {fail_fast} {full_refresh}',
            f'dbt run --select users {threads} {fail_fast} {full_refresh}',
            f'dbt run --select visit_schedule_realizations_raw {threads} {fail_fast} {full_refresh}',
            f'dbt run --select visit_schedule_realizations {threads} {fail_fast} {full_refresh}',
            f'dbt run --select visit_schedules_extended_raw {threads} {fail_fast} {full_refresh}',
            f'dbt run --select visit_schedules_extended {threads} {fail_fast} {full_refresh}',
            f'dbt run --select forms_raw {threads} {fail_fast} {full_refresh}',
            f'dbt run --select forms_staging {threads} {fail_fast} {full_refresh}',
            f'dbt run --select form_items {threads} {fail_fast} {full_refresh}',
            f'dbt run --select forms_business {threads} {fail_fast} {full_refresh}',
            f'dbt run --select photos_raw {threads} {fail_fast} {full_refresh}',
            f'dbt run --select photos {threads} {fail_fast} {full_refresh}',
            f'dbt run --select visit_schedules_raw {threads} {fail_fast} {full_refresh}',
            f'dbt run --select visit_schedules {threads} {fail_fast} {full_refresh}',
        ]
        
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
        
        # Check clients table
        print("📊 Checking clients data...")
        with engine.connect() as conn:
            result = conn.execute(f"""
                SELECT 
                    COUNT(*) as total_clients,
                    COUNT(DISTINCT client_id) as unique_clients,
                    COUNT(email_clean) as clients_with_email,
                    COUNT(CASE WHEN is_active = true THEN 1 END) as active_clients
                FROM {staging_schema}.clients
            """)
            
            row = result.fetchone()
            print(f"📊 Client Data Quality:")
            print(f"   Total clients: {row[0]}")
            print(f"   Unique client IDs: {row[1]}")
            print(f"   Clients with email: {row[2]}")
            print(f"   Active clients: {row[3]}")
        
        # Check other tables...
        print("\n📊 Checking client notes data...")
        with engine.connect() as conn:
            result = conn.execute(f"""
                SELECT 
                    COUNT(*) as total_notes,
                    COUNT(DISTINCT client_note_id) as unique_notes,
                    COUNT(DISTINCT client_code) as clients_with_notes,
                    AVG(note_length) as avg_note_length
                FROM {staging_schema}.client_notes
            """)
            
            row = result.fetchone()
            print(f"📊 Client Notes Data Quality:")
            print(f"   Total notes: {row[0]}")
            print(f"   Unique note IDs: {row[1]}")
            print(f"   Clients with notes: {row[2]}")
            print(f"   Average note length: {row[3]:.1f}" if row[3] else "N/A")
        
        engine.dispose()
        print("✅ All data quality checks completed successfully!")
        
    except Exception as e:
        print(f"❌ Data quality check failed: {e}")
        # Don't fail the pipeline if this is just a check issue
        print("⚠️  Continuing pipeline despite quality check failure")

# ========================
# TASK DEFINITIONS
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

# Create extraction tasks for each ENABLED endpoint
extraction_tasks = []
for endpoint_name in ENABLED_ENDPOINTS.keys():
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
# TASK DEPENDENCIES
# ========================

# Linear flow: Extract -> Load -> Transform -> Quality Check
start_task >> test_task >> extraction_tasks >> load_task >> transform_task >> quality_check_task >> success_notification_task >> end_task
# start_task >> test_task >> extraction_tasks >> load_task >> transform_task >> quality_check_task >> end_task