# extractor.py - Fixed critical issues: state management, incremental logic, error handling
# Path: root/extractors/repsly/extractor.py

from datetime import datetime, timedelta, timezone
import os
import time
import json
import requests
import pandas as pd
from typing import Dict, Any, List, Optional
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
import threading
import hashlib

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------- GLOBALS (initialized via init_extractor) ---------------- #
CONFIG: Dict[str, Any] = {}
TESTING_MODE: bool = False
MAX_RECORDS_PER_ENDPOINT: Optional[int] = None
ENDPOINTS_CONFIG: Dict[str, Dict[str, Any]] = {}
ENABLED_ENDPOINTS: Dict[str, str] = {}

# ---------------- THREAD-SAFE STATE MANAGEMENT ---------------- #
_STATE_CACHE: Dict[str, str] = {}
_STATE_LOCK = threading.Lock()
_STATE_DIR_CREATED = False

# Global session cache for connection pooling
_SESSION_CACHE = None

def _utc_now():
    return datetime.utcnow().replace(tzinfo=timezone.utc)

# ---------------- ENDPOINT DEFINITIONS ---------------- #
REPSLY_ENDPOINTS = {
    'clients': {
        'path': 'export/clients',
        'pagination_type': 'timestamp',
        'limit': 50,
        'timestamp_field': 'LastTimeStamp',
        'data_field': 'Clients',
        'total_count_field': 'TotalCount',
        'incremental_field': 'TimeStamp',
        'priority': 'high'
    },
    'client_notes': {
        'path': 'export/clientnotes',
        'pagination_type': 'id',
        'limit': 50,
        'id_field': 'LastClientNoteID',
        'data_field': 'ClientNotes',
        'total_count_field': 'TotalCount',
        'incremental_field': 'DateAndTime',
        'depends_on': ['clients']  # NEW: dependency handling
    },
    'visits': {
        'path': 'export/visits',
        'pagination_type': 'timestamp',
        'limit': 50,
        'timestamp_field': 'LastTimeStamp',
        'data_field': 'Visits',
        'total_count_field': 'TotalCount',
        'incremental_field': 'LastTimeStamp',
        'depends_on': ['clients']
    },
    'daily_working_time': {
        'path': 'export/dailyworkingtime',
        'pagination_type': 'id',
        'limit': 50,
        'id_field': 'LastDailyWorkingTimeID',
        'data_field': 'DailyWorkingTime',
        'total_count_field': 'TotalCount',
        'incremental_field': 'DateAndTime'
    },
    'representatives': {
        'path': 'export/representatives',
        'pagination_type': 'static',
        'data_field': 'Representatives',
        'incremental_field': 'ModifiedDate'  # Still has incremental for watermark
    },
    'users': {
        'path': 'export/users',
        'pagination_type': 'static',  # CORRECTED: This is static, not ID-based!
        'data_field': 'Users',
        'incremental_field': 'ModifiedDate'  # Still has incremental for watermark
    },
    'products': {
        'path': 'export/products',
        'pagination_type': 'id',
        'limit': 50,
        'id_field': 'LastProductID',
        'data_field': 'Products',
        'total_count_field': 'TotalCount',
        'incremental_field': 'ModifiedDate'
    },
    'retail_audits': {
        'path': 'export/retailaudits',
        'pagination_type': 'id',
        'limit': 50,
        'id_field': 'LastRetailAuditID',
        'data_field': 'RetailAudits',
        'total_count_field': 'TotalCount',
        'incremental_field': 'Date'
    },
    'purchase_orders': {
        'path': 'export/purchaseorders',
        'pagination_type': 'id',
        'limit': 50,
        'id_field': 'LastPurchaseOrderID',
        'data_field': 'PurchaseOrders',
        'total_count_field': 'TotalCount',
        'incremental_field': 'Date'
    },
    'document_types': {
        'path': 'export/documenttypes',
        'pagination_type': 'static',
        'data_field': 'DocumentTypes',
        'incremental_field': None
    },
    'pricelists': {
        'path': 'export/pricelists',
        'pagination_type': 'static',
        'data_field': 'PriceLists',
        'incremental_field': None
    },
    'pricelist_items': {
        'path': 'export/pricelistsItems',
        'pagination_type': 'id',
        'limit': 50,
        'id_field': 'LastPriceListItemID',
        'data_field': 'PriceListItems',
        'total_count_field': 'TotalCount',
        'incremental_field': 'ModifiedDate'
    },
    'forms': {
        'path': 'export/forms',
        'pagination_type': 'id',
        'limit': 50,
        'id_field': 'LastFormID',
        'data_field': 'Forms',
        'total_count_field': 'TotalCount',
        'incremental_field': 'Date'
    },
    'photos': {
        'path': 'export/photos',
        'pagination_type': 'id',
        'limit': 50,
        'id_field': 'LastPhotoID',
        'data_field': 'Photos',
        'total_count_field': 'TotalCount',
        'incremental_field': 'Date'
    },
    
    'visit_schedules': {
        'path': 'export/visitschedules',
        'pagination_type': 'datetime_range',
        'data_field': 'VisitSchedules',
        'url_pattern': 'export/visitschedules/{start_date}/{end_date}',
        'incremental_field': 'Date',
        'datetime_format': '%Y-%m-%d',  # API expects YYYY-MM-DD format
        'max_range_days': 30,  # Smaller chunks to prevent timeouts
        'requires_auth': True,
        'priority': 'medium'
    },

    'visit_schedules_extended': {
        'path': 'export/schedules',
        'pagination_type': 'datetime_range', 
        'data_field': 'Schedules',
        'url_pattern': 'export/schedules/{start_date}/{end_date}',
        'incremental_field': 'Date',
        'datetime_format': '%Y-%m-%d',
        'max_range_days': 30,
        'requires_auth': True,
        'priority': 'medium'
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
        'incremental_field': 'Modified',  # Changed from 'ModifiedDate' to 'Modified'
        'requires_auth': True,
        'priority': 'medium'
    }
}

# ---------------- INIT ---------------- #
def init_extractor(config):
    """Initialize extractor module with loaded YAML config."""
    global CONFIG, TESTING_MODE, MAX_RECORDS_PER_ENDPOINT, ENDPOINTS_CONFIG, ENABLED_ENDPOINTS
    CONFIG = config
    
    TESTING_MODE = config['extraction']['mode'] == 'testing'
    if TESTING_MODE:
        MAX_RECORDS_PER_ENDPOINT = config['extraction']['testing']['max_records_per_endpoint']
    else:
        MAX_RECORDS_PER_ENDPOINT = config['extraction']['production']['max_records_per_endpoint']
    
    # Build enabled endpoints from config
    always_extract = config['extraction']['endpoints'].get('always_extract', []) or []
    optional_extract = config['extraction']['endpoints'].get('optional_extract', []) or []
    disabled = config['extraction']['endpoints'].get('disabled', []) or []
    
    all_allowed = always_extract + optional_extract
    
    ENABLED_ENDPOINTS = {
        k: v['path']
        for k, v in REPSLY_ENDPOINTS.items()
        if k in all_allowed and k not in disabled
    }
    
    _load_state()
    
    logger.info("=" * 80)
    logger.info("🚀 REPSLY EXTRACTOR INITIALIZATION")
    logger.info("=" * 80)
    logger.info(f"📋 Enabled Endpoints: {len(ENABLED_ENDPOINTS)} endpoints")
    base_url = CONFIG['api']['base_url']
    
    for endpoint_key in sorted(ENABLED_ENDPOINTS.keys()):
        endpoint_config = REPSLY_ENDPOINTS[endpoint_key]
        pagination_type = endpoint_config.get('pagination_type', 'unknown').upper()
        incremental_field = endpoint_config.get('incremental_field', 'None')
        dependencies = endpoint_config.get('depends_on', [])
        path = endpoint_config['path']
        
        # Build URL pattern based on pagination type
        if pagination_type == 'STATIC':
            url_pattern = f"{base_url}/{path}"
        elif pagination_type == 'DATETIME_RANGE':
            url_pattern = f"{base_url}/{endpoint_config.get('url_pattern', path)}"
        elif pagination_type == 'QUERY_PARAMS':
            url_pattern = f"{base_url}/{endpoint_config.get('url_pattern', path)}?modified={{date}}&skip={{offset}}"
        else:  # ID or TIMESTAMP
            url_pattern = f"{base_url}/{path}/{{value}}"
        
        logger.info(f"   ├─ {endpoint_key:<25} [{pagination_type}]")
        logger.info(f"   │  ├─ URL: {url_pattern}")
        logger.info(f"   │  ├─ Incremental Field: {incremental_field}")
        
        if dependencies:
            logger.info(f"   │  ├─ Dependencies: {', '.join(dependencies)}")
        else:
            logger.info(f"   │  ├─ Dependencies: None")
            
        if pagination_type in ['ID', 'TIMESTAMP']:
            limit = endpoint_config.get('limit', 'N/A')
            logger.info(f"   │  └─ Page Limit: {limit}")
        elif pagination_type == 'DATETIME_RANGE':
            max_days = endpoint_config.get('max_range_days', 'N/A')
            logger.info(f"   │  └─ Max Range Days: {max_days}")
        else:
            logger.info(f"   │  └─ Single Request")
    
    logger.info("=" * 80)
    return ENABLED_ENDPOINTS

# ---------------- AUTH WITH CONNECTION POOLING ---------------- #
def create_authenticated_session():
    """Create authenticated session with connection pooling and retry logic."""
    global _SESSION_CACHE
    
    # Return cached session if available
    if _SESSION_CACHE is not None:
        try:
            # Test if session is still valid
            test_url = f"{CONFIG['api']['base_url']}/{CONFIG['api']['test_endpoint']}"
            response = _SESSION_CACHE.get(test_url, timeout=5)
            if response.status_code == 200:
                logger.info("🔄 Session: Reusing existing authenticated session")
                return _SESSION_CACHE
        except Exception:
            logger.info("🔄 Session: Cached session invalid, creating new one")
            _SESSION_CACHE = None
    
    username = os.getenv('REPSLY_USERNAME')
    password = os.getenv('REPSLY_PASSWORD')
    base_url = CONFIG['api']['base_url']
    
    if not all([username, password]):
        missing = []
        if not username: missing.append('REPSLY_USERNAME')
        if not password: missing.append('REPSLY_PASSWORD')
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    # Create session with connection pooling
    session = requests.Session()
    session.auth = HTTPBasicAuth(username, password)
    session.headers.update({'Accept': 'application/json'})
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=CONFIG['api']['rate_limiting']['max_retries'],
        backoff_factor=CONFIG['api']['rate_limiting']['backoff_factor'],
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    
    # Configure connection pooling
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,
        pool_maxsize=20,
        pool_block=False
    )
    
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Test authentication
    test_url = f"{base_url}/{CONFIG['api']['test_endpoint']}"
    try:
        response = session.get(test_url, timeout=CONFIG['api']['rate_limiting']['timeout_seconds'])
        response.raise_for_status()
        logger.info("✅ Authentication: Successfully authenticated with pooled connection")
        
        # Cache the session
        _SESSION_CACHE = session
        return session
    except Exception as e:
        logger.error(f"❌ Authentication: Failed to authenticate - {e}")
        raise

# ---------------- UTILITIES ---------------- #
def flatten_repsly_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten Repsly record structure."""
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

def smart_rate_limit(response, config):
    """Smart rate limiting based on response headers and status."""
    # Check for rate limit headers
    remaining = response.headers.get('X-RateLimit-Remaining')
    reset_time = response.headers.get('X-RateLimit-Reset')
    
    if remaining and int(remaining) < 5:
        # Near rate limit, slow down
        time.sleep(2.0)
        logger.info("⏳ Rate Limit: Near limit, slowing down (remaining: <5)")
    elif response.status_code == 429:
        # Hit rate limit, wait longer
        wait_time = int(reset_time) if reset_time else 60
        logger.warning(f"⏸️ Rate Limit: Hit rate limit, waiting {wait_time}s")
        time.sleep(wait_time)
    else:
        # Normal rate limiting
        time.sleep(1.0 / config['api']['rate_limiting']['requests_per_second'])

# ---------------- THREAD-SAFE INCREMENTAL STATE MANAGEMENT ---------------- #
def _state_path():
    return CONFIG['extraction']['incremental']['state_path']

def _ensure_state_dir():
    """Ensure state directory exists."""
    global _STATE_DIR_CREATED
    if _STATE_DIR_CREATED:
        return
    path = _state_path()
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    _STATE_DIR_CREATED = True

def _load_state():
    """Load watermark state JSON (endpoint -> ISO timestamp) - THREAD SAFE."""
    global _STATE_CACHE
    
    with _STATE_LOCK:
        _ensure_state_dir()
        path = _state_path()
        
        if not os.path.exists(path):
            _STATE_CACHE = {}
            logger.info("📁 State: No existing state file found, starting fresh")
            return
        
        try:
            # Check if file is empty
            if os.path.getsize(path) == 0:
                logger.warning("📁 State: File is empty, starting fresh")
                _STATE_CACHE = {}
                return
                
            with open(path, 'r') as f:
                content = f.read().strip()
                if not content:
                    logger.warning("📁 State: File content is empty, starting fresh")
                    _STATE_CACHE = {}
                    return
                _STATE_CACHE = json.loads(content)
            logger.info(f"📁 State: Loaded from {path}")
            for endpoint, watermark in _STATE_CACHE.items():
                if endpoint.endswith('_last_id') or endpoint.endswith('_last_timestamp'):
                    logger.info(f"   ├─ {endpoint:<25} = {watermark}")
                else:
                    logger.info(f"   ├─ {endpoint:<25} = {watermark}")
        except json.JSONDecodeError as e:
            logger.warning(f"⚠️ State: Invalid JSON in file {path}: {e}. Starting fresh.")
            _STATE_CACHE = {}
        except Exception as e:
            logger.warning(f"⚠️ State: Failed to load file {path}: {e}")
            _STATE_CACHE = {}

def _save_state():
    """Save watermark state to file - THREAD SAFE with atomic write."""
    with _STATE_LOCK:
        _ensure_state_dir()
        path = _state_path()
        temp_path = f"{path}.tmp"
        
        try:
            # Create checksum for verification
            state_json = json.dumps(_STATE_CACHE, indent=2, sort_keys=True)
            checksum = hashlib.md5(state_json.encode()).hexdigest()
            
            logger.info(f"💾 State: Saving to {path}")
            for endpoint, watermark in _STATE_CACHE.items():
                if endpoint.endswith('_last_id') or endpoint.endswith('_last_timestamp'):
                    logger.info(f"   ├─ {endpoint:<25} = {watermark}")
                else:
                    logger.info(f"   ├─ {endpoint:<25} = {watermark}")
            
            # Atomic write: write to temp file first
            with open(temp_path, 'w') as f:
                f.write(state_json)
                f.flush()
                os.fsync(f.fileno())  # Force write to disk
            
            # Verify temp file
            with open(temp_path, 'r') as f:
                verify_content = f.read()
                verify_checksum = hashlib.md5(verify_content.encode()).hexdigest()
                
            if verify_checksum != checksum:
                raise ValueError("State file verification failed - checksum mismatch")
            
            # Atomic move
            if os.name == 'nt':  # Windows
                if os.path.exists(path):
                    os.remove(path)
            os.rename(temp_path, path)
            
            logger.info(f"💾 State: Successfully saved (checksum: {checksum[:8]})")
            
        except Exception as e:
            logger.error(f"❌ State: Failed to save file {path}: {e}")
            # Clean up temp file
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass
            raise

def _get_last_watermark(endpoint_key: str) -> Optional[datetime]:
    """Get last watermark for endpoint - THREAD SAFE."""
    with _STATE_LOCK:
        iso = _STATE_CACHE.get(endpoint_key)
        if not iso:
            return None
        try:
            # Handle both Z and +00:00 formats
            if iso.endswith('Z'):
                iso = iso.replace('Z', '+00:00')
            return datetime.fromisoformat(iso)
        except Exception as e:
            logger.warning(f"⚠️ State: Invalid watermark format for {endpoint_key}: {iso}")
            return None

def _update_watermark(endpoint_key: str, new_ts: datetime):
    """Update watermark for endpoint - THREAD SAFE, NOT SAVED YET."""
    with _STATE_LOCK:
        if new_ts.tzinfo is None:
            new_ts = new_ts.replace(tzinfo=timezone.utc)
        # Always store in UTC ISO format with Z
        _STATE_CACHE[endpoint_key] = new_ts.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
        logger.info(f"🕒 Watermark: Updated in memory {endpoint_key} → {_STATE_CACHE[endpoint_key]}")

# ---------------- IMPROVED INCREMENTAL HELPERS ---------------- #
def _utc_now():
    """Always return timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)

def _parse_microsoft_json_date(date_str: str) -> Optional[datetime]:
    """Parse Microsoft JSON date format with robust error handling and timezone awareness."""
    if not date_str or date_str == '' or date_str is None:
        return None
    
    try:
        date_str = str(date_str).strip()
        
        # Handle Microsoft JSON date format: /Date(1665059530000+0000)/ or /Date(1665059530000)/
        if date_str.startswith('/Date(') and date_str.endswith(')/'):
            import re
            # More robust regex to handle timezone offsets
            match = re.search(r'/Date\((\d+)([\+\-]\d{4})?\)/', date_str)
            if match:
                timestamp_ms = int(match.group(1))
                # Convert milliseconds to seconds
                timestamp_s = timestamp_ms / 1000
                return datetime.fromtimestamp(timestamp_s, tz=timezone.utc)
            else:
                logger.warning(f"Could not parse Microsoft JSON date: {date_str}")
                return None
        
        # Try standard ISO format
        if 'T' in date_str:
            if date_str.endswith('Z'):
                date_str = date_str[:-1] + '+00:00'
            elif '+' not in date_str and '-' not in date_str[-6:]:
                # No timezone info, assume UTC
                date_str += '+00:00'
            return datetime.fromisoformat(date_str)
        
        # Date only format - assume UTC
        return datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        
    except Exception as e:
        logger.warning(f"Failed to parse timestamp: {date_str} - {e}")
        return None

def _parse_timestamp_column(df: pd.DataFrame, timestamp_field: str) -> pd.Series:
    """Parse timestamp column with robust Microsoft JSON date handling."""
    if timestamp_field not in df.columns:
        return pd.Series([None] * len(df))
    
    return df[timestamp_field].apply(_parse_microsoft_json_date)

def _should_use_incremental(endpoint_key: str) -> bool:
    """
    ENHANCED: Check if endpoint should use incremental extraction.
    """
    if not CONFIG['extraction']['incremental'].get('enabled', False):
        return False
    
    endpoint_config = REPSLY_ENDPOINTS.get(endpoint_key, {})
    pagination_type = endpoint_config.get('pagination_type')
    incremental_field = endpoint_config.get('incremental_field')
    
    # Static endpoints don't support incremental
    if pagination_type == 'static':
        return False
    
    # ENHANCED: ID-based endpoints can use incremental if they have an incremental field
    if pagination_type == 'id':
        # ID endpoints can still be incremental based on ModifiedDate, etc.
        return incremental_field is not None
    
    # Timestamp and other types can use incremental if they have an incremental field
    return incremental_field is not None

def _get_incremental_date_range(endpoint_key: str):
    """
    FIXED: Get date range for incremental extraction with proper timezone handling.
    """
    endpoint_config = REPSLY_ENDPOINTS.get(endpoint_key, {})
    pagination_type = endpoint_config.get('pagination_type')
    
    if not _should_use_incremental(endpoint_key):
        # Default date range for full extraction
        end_date = _utc_now()  # Always UTC
        if TESTING_MODE:
            start_date = end_date - timedelta(days=CONFIG['extraction']['testing']['date_range_days'])
        else:
            start_date = end_date - timedelta(days=CONFIG['extraction']['production']['date_range_days'])
        logger.info(f"🔄 Full Refresh: {endpoint_key} using {(end_date - start_date).days} day range")
        return start_date, end_date
    
    last_wm = _get_last_watermark(endpoint_key)
    end_date = _utc_now()  # Always UTC
    
    if last_wm:
        # Ensure last_wm is timezone-aware
        if last_wm.tzinfo is None:
            last_wm = last_wm.replace(tzinfo=timezone.utc)
        
        lookback = CONFIG['extraction']['incremental'].get('lookback_minutes', 10)
        start_date = last_wm - timedelta(minutes=lookback)
        logger.info(f"🔁 Incremental: {endpoint_key} from {start_date.strftime('%Y-%m-%d %H:%M:%S')} UTC (lookback: {lookback}m)")
    else:
        # First run - use default range
        if TESTING_MODE:
            start_date = end_date - timedelta(days=CONFIG['extraction']['testing']['date_range_days'])
        else:
            # For ID-based endpoints, use shorter initial range
            if pagination_type == 'id':
                start_date = end_date - timedelta(days=30)  # Shorter range for ID endpoints
                logger.info(f"🔄 First Run: ID-based {endpoint_key} using 30-day range")
            else:
                start_date = end_date - timedelta(days=CONFIG['extraction']['production']['date_range_days'])
                logger.info(f"🔄 First Run: {endpoint_key} using default range ({CONFIG['extraction']['production']['date_range_days']} days)")
    
    return start_date, end_date


# ---------------- SAFE WAREHOUSE LOADING FUNCTIONS ---------------- #
def table_exists(client, full_table):
    """Check if table exists in ClickHouse."""
    try:
        client.query(f"DESCRIBE TABLE {full_table}")
        return True
    except Exception:
        return False

def create_table_from_dataframe(client, full_table, df):
    """Create ClickHouse table from DataFrame structure with proper partitioning."""
    columns = []
    for col in df.columns:
        columns.append(f"`{col}` String")
    
    # Use MergeTree with partitioning by extraction date
    create_ddl = f"""
    CREATE TABLE {full_table} (
        {', '.join(columns)}
    ) ENGINE = MergeTree()
    PARTITION BY toDate(parseDateTimeBestEffort(_extracted_at))
    ORDER BY (_extracted_at, {df.columns[0] if len(df.columns) > 0 else '_extracted_at'})
    SETTINGS index_granularity = 8192
    """
    
    try:
        client.command(create_ddl)
        logger.info(f"🆕 Table: Created partitioned table {full_table}")
        
        # Verify the table was created with partitions
        verify_query = f"SHOW CREATE TABLE {full_table}"
        result = client.query(verify_query)
        create_statement = result.result_rows[0][0]
        
        if "PARTITION BY" in create_statement:
            logger.info(f"✅ Table: Verified {full_table} has partitioning")
        else:
            logger.error(f"❌ CRITICAL: {full_table} was created WITHOUT partitioning!")
            raise Exception(f"Table {full_table} created without partitioning")
            
    except Exception as e:
        logger.error(f"❌ Table: Failed to create table {full_table}: {e}")
        raise

def load_dataframe_to_warehouse_verified(df, endpoint_key, extracted_at):
    """Load DataFrame with comprehensive verification and duplicate prevention."""
    import clickhouse_connect
    
    raw_schema = CONFIG['warehouse']['schemas']['raw_schema']
    
    # Connection setup
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
    
    try:
        # Ensure schema exists with proper error handling
        if raw_schema != 'default':
            try:
                client.command(f"CREATE DATABASE IF NOT EXISTS `{raw_schema}`")
                logger.info(f"✅ Database: {raw_schema} ready")
            except Exception as db_error:
                logger.error(f"❌ Database: Failed to create {raw_schema}: {db_error}")
                raise ValueError(f"Cannot access or create database {raw_schema}: {db_error}")
        
        table_name = f"raw_{endpoint_key}"
        full_table = f"`{raw_schema}`.`{table_name}`"
        extraction_timestamp = extracted_at.isoformat()
        
        # CRITICAL: Duplicate prevention check
        if table_exists(client, full_table):
            duplicate_check = client.query(f"""
                SELECT count() 
                FROM {full_table} 
                WHERE _extracted_at = '{extraction_timestamp}'
            """)
            
            existing_count = duplicate_check.result_rows[0][0]
            if existing_count > 0:
                logger.warning(f"⚠️ Warehouse: Duplicates detected - skipping load")
                logger.warning(f"   ├─ Existing records: {existing_count}")
                logger.warning(f"   └─ Timestamp: {extraction_timestamp}")
                return 0  # Success but no new records
            
            logger.info(f"✅ Warehouse: No duplicates found for {extraction_timestamp}")
        else:
            # Create table if doesn't exist
            create_table_from_dataframe(client, full_table, df)
        
        # Data validation before load
        unique_timestamps = df['_extracted_at'].nunique()
        if unique_timestamps != 1:
            raise ValueError(f"Data integrity error: {unique_timestamps} different timestamps")
        
        actual_timestamp = df['_extracted_at'].iloc[0]
        if actual_timestamp != extraction_timestamp:
            raise ValueError(f"Timestamp mismatch: expected {extraction_timestamp}, got {actual_timestamp}")
        
        # Prepare data
        df_clean = df.copy()
        df_clean.columns = [col.replace(' ', '_').replace('-', '_') for col in df_clean.columns]
        df_str = df_clean.astype(str).replace(['nan', 'None', 'null', '<NA>'], '')
        
        # Load data
        client.insert_df(table=full_table, df=df_str)
        
        # CRITICAL: Verify the load
        verification_check = client.query(f"""
            SELECT count() 
            FROM {full_table} 
            WHERE _extracted_at = '{extraction_timestamp}'
        """)
        
        actual_loaded = verification_check.result_rows[0][0]
        
        if actual_loaded != len(df):
            raise ValueError(f"Warehouse: Load verification failed - expected {len(df)}, loaded {actual_loaded}")
        
        logger.info(f"✅ Warehouse: Loaded {actual_loaded} rows into {full_table}")
        return actual_loaded
        
    except Exception as e:
        logger.error(f"❌ Warehouse: Load failed - {e}")
        raise
    finally:
        client.close()

def load_dataframe_to_warehouse(df, endpoint_key, extracted_at):
    """Load DataFrame directly to ClickHouse warehouse with improved safety."""
    import clickhouse_connect
    
    # Get warehouse config
    raw_schema = CONFIG['warehouse']['schemas']['raw_schema']
    
    # Connect to ClickHouse
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
    
    try:
        # Create schema if needed
        if raw_schema != 'default':
            client.command(f"CREATE DATABASE IF NOT EXISTS `{raw_schema}`")
        
        table_name = f"raw_{endpoint_key}"
        full_table = f"`{raw_schema}`.`{table_name}`"
        
        # CRITICAL: Check for idempotency - prevent duplicate loads
        extraction_batch_id = f"{extracted_at.isoformat()}_{endpoint_key}"
        if table_exists(client, full_table):
            existing_check = client.query(
                f"SELECT count() FROM {full_table} WHERE _extracted_at = '{extracted_at.isoformat()}'"
            )
            
            if existing_check.result_rows[0][0] > 0:
                logger.warning(f"⚠️ Data for {extracted_at.isoformat()} already exists in {full_table}")
                logger.info(f"   This indicates a pipeline retry or duplicate run")
                logger.info(f"   Skipping load to prevent duplicates")
                return 0
            
            # Check table structure
            table_info = client.query(f"SHOW CREATE TABLE {full_table}")
            create_statement = table_info.result_rows[0][0]
            
            if "PARTITION BY" not in create_statement:
                logger.warning(f"⚠️ Table {full_table} is NOT partitioned!")
                logger.warning(f"   Performance will be impacted but continuing...")
            else:
                logger.info(f"✅ Table {full_table} has proper partitioning")
        else:
            # Create new table with partitioning
            logger.info(f"🆕 Creating new partitioned table {full_table}")
            create_table_from_dataframe(client, full_table, df)
        
        # Data validation before insertion
        logger.info(f"🔍 Validating data before insertion...")
        logger.info(f"   DataFrame shape: {df.shape}")
        logger.info(f"   Unique _extracted_at values: {df['_extracted_at'].nunique()}")
        
        expected_extracted_at = extracted_at.isoformat()
        actual_extracted_at_values = df['_extracted_at'].unique()
        
        if len(actual_extracted_at_values) != 1 or actual_extracted_at_values[0] != expected_extracted_at:
            logger.error(f"❌ CRITICAL: Inconsistent _extracted_at values!")
            logger.error(f"   Expected: {expected_extracted_at}")
            logger.error(f"   Found: {actual_extracted_at_values}")
            raise ValueError("Data integrity check failed: inconsistent extraction timestamps")
        
        # Clean column names and prepare data
        df_clean = df.copy()
        df_clean.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df_clean.columns]
        
        # Convert to string but handle nulls properly
        df_str = df_clean.astype(str).replace(['nan', 'None', 'null'], '')
        
        # CHUNKED INSERTION for large datasets
        chunk_size = 1000
        total_inserted = 0
        
        if len(df_str) <= chunk_size:
            # Small dataset - single insert
            client.insert_df(table=full_table, df=df_str)
            total_inserted = len(df_str)
            logger.info(f"✅ Loaded {total_inserted} records to {full_table} (single chunk)")
        else:
            # Large dataset - chunked insert
            logger.info(f"📦 Loading {len(df_str)} records in chunks of {chunk_size}")
            
            for i in range(0, len(df_str), chunk_size):
                chunk_df = df_str.iloc[i:i+chunk_size]
                client.insert_df(table=full_table, df=chunk_df)
                total_inserted += len(chunk_df)
                logger.info(f"   Loaded chunk {i//chunk_size + 1}: {len(chunk_df)} records (total: {total_inserted})")
        
        # Verify insertion
        verification_check = client.query(
            f"SELECT count() FROM {full_table} WHERE _extracted_at = '{expected_extracted_at}'"
        )
        actual_inserted = verification_check.result_rows[0][0]
        
        if actual_inserted != len(df):
            logger.error(f"❌ CRITICAL: Insertion verification failed!")
            logger.error(f"   Expected: {len(df)} records")
            logger.error(f"   Actually inserted: {actual_inserted} records")
            raise ValueError("Data insertion verification failed")
        else:
            logger.info(f"✅ Insertion verified: {actual_inserted} records")
        
        return total_inserted
        
    except Exception as e:
        logger.error(f"❌ Failed to insert data into {full_table}: {e}")
        raise
    finally:
        client.close()

# ---------------- UPDATED STATE MANAGEMENT - ONLY AFTER SUCCESSFUL LOAD ---------------- #
def update_extraction_state_after_successful_load(endpoint_key, endpoint_config, df, raw_data):
    """Update extraction state ONLY after successful warehouse load - prevents data loss."""
    try:
        logger.info(f"📝 Updating state after successful load for {endpoint_key}")
        
        # Update watermark ONLY from successfully loaded data
        incremental_field = endpoint_config.get('incremental_field')
        if incremental_field and not df.empty:
            # Find the incremental field in the dataframe
            incremental_columns = [col for col in df.columns if incremental_field.lower() in col.lower()]
            if incremental_columns:
                actual_field = incremental_columns[0]
                timestamps = _parse_timestamp_column(df, actual_field)
                valid_timestamps = [ts for ts in timestamps if ts is not None]
                
                if valid_timestamps:
                    max_ts = max(valid_timestamps)
                    # Store the maximum timestamp from LOADED data
                    _update_watermark(endpoint_key, max_ts)
                    logger.info(f"🕒 Updated watermark from loaded data: {max_ts.isoformat()}")
                else:
                    logger.warning(f"⚠️ No valid timestamps found in loaded data for {endpoint_key}")
            else:
                logger.warning(f"⚠️ Incremental field {incremental_field} not found in loaded data for {endpoint_key}")
        
        # Update pagination state for next run
        if endpoint_config['pagination_type'] == 'id' and raw_data:
            id_field_name = endpoint_config.get('id_field', 'LastClientNoteID').replace('Last', '')
            if id_field_name in raw_data[-1]:
                last_id_key = f"{endpoint_key}_last_id"
                with _STATE_LOCK:
                    _STATE_CACHE[last_id_key] = raw_data[-1][id_field_name]
                logger.info(f"   📝 Updated ID state: {raw_data[-1][id_field_name]}")
        
        elif endpoint_config['pagination_type'] == 'timestamp' and raw_data:
            timestamp_key = f"{endpoint_key}_last_timestamp"
            with _STATE_LOCK:
                if timestamp_key in _STATE_CACHE:
                    logger.info(f"   📝 Timestamp state current: {_STATE_CACHE[timestamp_key]}")
        
        # CRITICAL: Save state only after successful processing
        _save_state()
        logger.info("✅ Extraction state updated and saved after successful load")
        
    except Exception as e:
        logger.error(f"❌ Failed to update extraction state: {e}")
        # Don't raise - we don't want to fail the pipeline for state issues
        # But log it as critical since it affects next run
        logger.error("❌ CRITICAL: State not updated - next run may have data gaps!")

# ---------------- IMPROVED PAGINATION + FETCH WITH ERROR HANDLING ---------------- #
def get_paginated_data_fixed_query_params(session, endpoint_config, endpoint_name):
    """
    FIXED: Query parameter pagination for visit_schedule_realizations with proper datetime handling.
    """
    all_data = []
    base_url = CONFIG['api']['base_url']
    max_retries = 3
    
    start_date, end_date = _get_incremental_date_range(endpoint_name)
    
    # Ensure both dates are timezone-aware
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=timezone.utc)
    if end_date.tzinfo is None:
        end_date = end_date.replace(tzinfo=timezone.utc)
    
    # Format date for API (ensure UTC)
    modified_date = start_date.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z')
    skip = 0
    page_size = 50
    max_pages = 50  # Reduced to prevent infinite loops
    page_count = 0
    
    logger.info(f"📡 API: Query params pagination starting")
    logger.info(f"   ├─ Modified Date: {modified_date}")
    logger.info(f"   ├─ Page Size: {page_size}")
    logger.info(f"   └─ Max Pages: {max_pages}")
    
    while page_count < max_pages:
        page_count += 1
        
        if skip >= 9500:
            logger.info(f"   📄 Page {page_count}: Skip limit reached (9500), advancing date by 90 days")
            start_date = start_date + timedelta(days=90)  # Jump forward 90 days
            if start_date >= _utc_now():
                logger.info(f"   📄 Page {page_count}: Reached current date, stopping")
                break
            modified_date = start_date.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z')
            skip = 0
            page_count = 0
            continue
        
        endpoint_url = f"{base_url}/{endpoint_config['url_pattern']}?modified={modified_date}&skip={skip}"
        logger.info(f"   📄 Page {page_count:2d} | Skip: {skip:5d} | Fetching...")
        
        success = False
        for attempt in range(max_retries):
            try:
                response = session.get(endpoint_url, timeout=CONFIG['api']['rate_limiting']['timeout_seconds'])
                
                if response.status_code == 400:
                    logger.info(f"   📄 Page {page_count:2d} | 400 Bad Request - advancing date by 90 days")
                    start_date = start_date + timedelta(days=90)
                    if start_date >= _utc_now():
                        logger.info(f"   📄 Page {page_count:2d} | Reached current date, stopping")
                        return all_data
                    modified_date = start_date.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z')
                    skip = 0
                    page_count = 0
                    success = True
                    break
                
                response.raise_for_status()
                data = response.json()
                
                records = data.get(endpoint_config['data_field'], [])
                logger.info(f"   📄 Page {page_count:2d} | Got: {len(records):3d} records | Total: {len(all_data):5d}")
                
                if not records:
                    logger.info(f"   📄 Page {page_count:2d} | No records - advancing date by 90 days")
                    start_date = start_date + timedelta(days=90)
                    if start_date >= _utc_now():
                        logger.info(f"   📄 Page {page_count:2d} | Reached current date, stopping")
                        return all_data
                    modified_date = start_date.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z')
                    skip = 0
                    page_count = 0
                    success = True
                    break
                
                all_data.extend(records)
                
                # Check testing limit
                if TESTING_MODE and MAX_RECORDS_PER_ENDPOINT and len(all_data) >= MAX_RECORDS_PER_ENDPOINT:
                    all_data = all_data[:MAX_RECORDS_PER_ENDPOINT]
                    logger.info(f"   📄 Page {page_count:2d} | Testing limit reached ({MAX_RECORDS_PER_ENDPOINT})")
                    return all_data
                
                if len(records) < page_size:
                    logger.info(f"   📄 Page {page_count:2d} | Got fewer than page size - advancing date")
                    start_date = start_date + timedelta(days=90)
                    if start_date >= _utc_now():
                        return all_data
                    modified_date = start_date.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z')
                    skip = 0
                    page_count = 0
                    success = True
                    break
                
                skip += page_size
                smart_rate_limit(response, CONFIG)
                success = True
                break
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"   📄 Page {page_count:2d} | Request error (attempt {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    logger.warning(f"   📄 Page {page_count:2d} | Max retries reached - advancing date")
                    start_date = start_date + timedelta(days=90)
                    if start_date >= _utc_now():
                        return all_data
                    modified_date = start_date.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z')
                    skip = 0
                    page_count = 0
                    success = True
                    break
                time.sleep(2 ** attempt)
            
            except Exception as e:
                logger.error(f"   📄 Page {page_count:2d} | Unexpected error (attempt {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    logger.error(f"   📄 Page {page_count:2d} | Giving up - advancing date")
                    start_date = start_date + timedelta(days=90)
                    if start_date >= _utc_now():
                        return all_data
                    modified_date = start_date.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z')
                    skip = 0
                    page_count = 0
                    success = True
                    break
                time.sleep(2 ** attempt)
        
        if not success:
            logger.error(f"   📄 Page {page_count:2d} | Failed completely, stopping")
            break
    
    logger.info(f"✅ Extraction: Collected {len(all_data)} total records from {page_count} pages")
    return all_data

def get_paginated_data(session, endpoint_config, endpoint_name):
    """
    UPDATED: Get data from Repsly endpoint with fixed datetime handling for query_params.
    """
    all_data = []
    base_url = CONFIG['api']['base_url']
    max_retries = 3
    consecutive_failures = 0
    pagination_type = endpoint_config.get('pagination_type')
    
    logger.info(f"📡 API: Starting {pagination_type.upper()} pagination")
    
    if pagination_type == 'static':
        # Static endpoints - single request (unchanged)
        endpoint_url = f"{base_url}/{endpoint_config['path']}"
        logger.info(f"📡 API: Fetching static data from {endpoint_url}")
        
        for attempt in range(max_retries):
            try:
                response = session.get(endpoint_url, timeout=CONFIG['api']['rate_limiting']['timeout_seconds'])
                response.raise_for_status()
                data = response.json()
                
                if endpoint_config.get('data_field'):
                    records = data.get(endpoint_config['data_field'], [])
                else:
                    records = data if isinstance(data, list) else [data]
                
                all_data.extend(records)
                logger.info(f"✅ API: Retrieved {len(records)} records from static endpoint")
                break
                
            except Exception as e:
                consecutive_failures += 1
                logger.warning(f"   📡 API: Static attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
    
    elif pagination_type == 'query_params':
        # FIXED: Use the new query params handler with proper datetime handling
        return get_paginated_data_fixed_query_params(session, endpoint_config, endpoint_name)
    
    elif pagination_type == 'datetime_range':
        # Date range endpoints (keep existing logic but ensure timezone awareness)
        start_date, end_date = _get_incremental_date_range(endpoint_name)
        
        # Ensure timezone awareness
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)
        
        # Split large date ranges to avoid timeouts
        max_range_days = endpoint_config.get('max_range_days', 30)
        current_start = start_date
        
        logger.info(f"📡 API: Date range pagination")
        logger.info(f"   ├─ Total Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        logger.info(f"   └─ Chunk Size: {max_range_days} days")
        
        chunk_count = 0
        while current_start < end_date:
            chunk_count += 1
            current_end = min(current_start + timedelta(days=max_range_days), end_date)
            
            start_date_str = current_start.strftime('%Y-%m-%d')
            end_date_str = current_end.strftime('%Y-%m-%d')
            
            endpoint_url = f"{base_url}/{endpoint_config['url_pattern'].format(start_date=start_date_str, end_date=end_date_str)}"
            logger.info(f"   📄 Chunk {chunk_count}: {start_date_str} to {end_date_str}")
            
            for attempt in range(max_retries):
                try:
                    response = session.get(endpoint_url, timeout=CONFIG['api']['rate_limiting']['timeout_seconds'])
                    response.raise_for_status()
                    data = response.json()
                    
                    if endpoint_config.get('data_field'):
                        records = data.get(endpoint_config['data_field'], [])
                    else:
                        records = data if isinstance(data, list) else [data]
                    
                    all_data.extend(records)
                    logger.info(f"   📄 Chunk {chunk_count}: Got {len(records)} records | Total: {len(all_data)}")
                    break
                    
                except Exception as e:
                    logger.warning(f"   📄 Chunk {chunk_count}: Attempt {attempt + 1} failed: {e}")
                    if attempt == max_retries - 1:
                        logger.error(f"   📄 Chunk {chunk_count}: Failed after {max_retries} attempts")
                        # Continue to next range instead of failing completely
                        break
                    time.sleep(2 ** attempt)
            
            current_start = current_end + timedelta(days=1)
    
    else:
        # Standard paginated endpoints (id or timestamp based) - keep existing logic
        if _should_use_incremental(endpoint_name):
            if pagination_type == 'id':
                last_id_key = f"{endpoint_name}_last_id"
                with _STATE_LOCK:
                    last_value = _STATE_CACHE.get(last_id_key, 0)
                logger.info(f"🔁 Incremental: ID-based starting from {last_value}")
            
            elif pagination_type == 'timestamp':
                timestamp_key = f"{endpoint_name}_last_timestamp"
                with _STATE_LOCK:
                    stored_raw_timestamp = _STATE_CACHE.get(timestamp_key)
                
                if stored_raw_timestamp:
                    last_value = stored_raw_timestamp
                    logger.info(f"🔁 Incremental: Timestamp-based starting from {last_value}")
                else:
                    last_value = 0
                    logger.info(f"🔄 First Run: Timestamp-based starting from 0")
            else:
                last_value = 0
                logger.info(f"🔄 Full Refresh: Starting from 0")
        else:
            last_value = 0
            logger.info(f"🔄 No Incremental: Starting from 0")
        
        page_count = 0
        max_pages = 1000
        
        logger.info(f"📄 Pagination: Starting with limit={endpoint_config.get('limit', 'N/A')}, value={last_value}")
        
        while page_count < max_pages:
            page_count += 1
            endpoint_url = f"{base_url}/{endpoint_config['path']}/{last_value}"
            
            logger.info(f"   📄 Page {page_count:2d} | Value: {last_value:5} | Fetching...")
            
            for attempt in range(max_retries):
                try:
                    response = session.get(endpoint_url, timeout=CONFIG['api']['rate_limiting']['timeout_seconds'])
                    response.raise_for_status()
                    data = response.json()
                    
                    meta = data.get('MetaCollectionResult', {})
                    total_count = meta.get(endpoint_config['total_count_field'], 0)
                    
                    if pagination_type == 'timestamp':
                        new_value = meta.get(endpoint_config['timestamp_field'], 0)
                    else:
                        new_value = meta.get(endpoint_config['id_field'], 0)
                    
                    records = data.get(endpoint_config['data_field'], [])
                    
                    logger.info(f"   📄 Page {page_count:2d} | Got: {len(records):3d} records | Total: {len(all_data):5d}")
                    logger.info(f"   📄 Page {page_count:2d} | Meta: count={total_count}, new_value={new_value}")
                    
                    if not records or total_count == 0:
                        logger.info(f"   📄 Page {page_count:2d} | No more records - pagination complete")
                        return all_data
                    
                    all_data.extend(records)
                    
                    # Store pagination state (but don't save to file yet)
                    if pagination_type == 'id' and records:
                        id_field_name = endpoint_config.get('id_field', 'LastClientNoteID').replace('Last', '')
                        if id_field_name in records[-1]:
                            highest_id_in_batch = records[-1][id_field_name]
                            last_id_key = f"{endpoint_name}_last_id"
                            with _STATE_LOCK:
                                _STATE_CACHE[last_id_key] = highest_id_in_batch
                            logger.info(f"   📝 State: Stored last ID in memory: {highest_id_in_batch}")
                    
                    elif pagination_type == 'timestamp' and records:
                        if new_value and new_value > last_value:
                            timestamp_key = f"{endpoint_name}_last_timestamp"
                            with _STATE_LOCK:
                                _STATE_CACHE[timestamp_key] = new_value
                            logger.info(f"   📝 State: Stored API timestamp in memory: {new_value}")
                        else:
                            timestamp_field_name = endpoint_config.get('incremental_field', 'TimeStamp')
                            if timestamp_field_name in records[-1]:
                                latest_timestamp_in_batch = records[-1][timestamp_field_name]
                                timestamp_key = f"{endpoint_name}_last_timestamp"
                                with _STATE_LOCK:
                                    _STATE_CACHE[timestamp_key] = latest_timestamp_in_batch
                                logger.info(f"   📝 State: Stored record timestamp in memory: {latest_timestamp_in_batch}")
                    
                    # Check testing limit
                    if TESTING_MODE and MAX_RECORDS_PER_ENDPOINT and len(all_data) >= MAX_RECORDS_PER_ENDPOINT:
                        all_data = all_data[:MAX_RECORDS_PER_ENDPOINT]
                        logger.info(f"   📄 Page {page_count:2d} | Testing limit reached ({MAX_RECORDS_PER_ENDPOINT})")
                        return all_data
                    
                    # Update last_value for next iteration
                    if new_value > last_value:
                        last_value = new_value
                        logger.info(f"   📈 Progress: Updated pagination value to {last_value}")
                    else:
                        logger.info(f"   ⚠️ Progress: No advancement (new: {new_value}, last: {last_value}) - stopping")
                        return all_data
                        
                    if len(records) < endpoint_config['limit']:
                        logger.info(f"   📄 Page {page_count:2d} | Got fewer than limit ({len(records)} < {endpoint_config['limit']}) - end reached")
                        return all_data
                    
                    smart_rate_limit(response, CONFIG)
                    break
                    
                except Exception as e:
                    consecutive_failures += 1
                    logger.warning(f"   📄 Page {page_count:2d} | Error (attempt {attempt + 1}): {e}")
                    if attempt == max_retries - 1:
                        if page_count == 1:
                            raise  # Fail fast if first page fails
                        else:
                            logger.warning(f"   📄 Page {page_count:2d} | Giving up after {max_retries} attempts")
                            return all_data
                    time.sleep(2 ** attempt)
    
    # Apply testing limit
    if TESTING_MODE and MAX_RECORDS_PER_ENDPOINT:
        original_count = len(all_data)
        all_data = all_data[:MAX_RECORDS_PER_ENDPOINT]
        if original_count > MAX_RECORDS_PER_ENDPOINT:
            logger.info(f"🧪 Testing: Limited from {original_count} to {MAX_RECORDS_PER_ENDPOINT} records")
    
    logger.info(f"✅ Extraction: Collected {len(all_data)} total records")
    return all_data

# ---------------- SCHEDULING WITH DEPENDENCIES ---------------- #
def should_run_endpoint(endpoint_key, execution_dt: datetime, completed_endpoints: set = None):
    """Check if endpoint should run based on schedule and dependencies."""
    if completed_endpoints is None:
        completed_endpoints = set()
    
    endpoint_config = REPSLY_ENDPOINTS.get(endpoint_key, {})
    dependencies = endpoint_config.get('depends_on', [])
    
    # Check if all dependencies are completed
    for dep in dependencies:
        if dep not in completed_endpoints:
            logger.info(f"⏳ Dependencies: {endpoint_key} waiting for {dep}")
            return False
    
    return True

def get_endpoint_execution_order():
    """Get endpoints in dependency order."""
    endpoints = list(ENABLED_ENDPOINTS.keys())
    completed = set()
    ordered = []
    
    logger.info("📋 Execution Planning:")
    
    # Simple dependency resolution
    max_iterations = len(endpoints) * 2
    iteration = 0
    
    while endpoints and iteration < max_iterations:
        iteration += 1
        made_progress = False
        
        for endpoint in endpoints[:]:
            if should_run_endpoint(endpoint, None, completed):
                ordered.append(endpoint)
                completed.add(endpoint)
                endpoints.remove(endpoint)
                made_progress = True
                
                # Show endpoint details in execution order
                endpoint_config = REPSLY_ENDPOINTS[endpoint]
                pagination_type = endpoint_config.get('pagination_type', 'unknown').upper()
                dependencies = endpoint_config.get('depends_on', [])
                dep_info = f"depends on {', '.join(dependencies)}" if dependencies else "no dependencies"
                logger.info(f"   ├─ {endpoint:<25} [{pagination_type}] ({dep_info})")
        
        if not made_progress:
            # Add remaining endpoints (circular dependencies or missing deps)
            logger.warning(f"⚠️ Dependencies: Adding remaining without dependency check: {endpoints}")
            for endpoint in endpoints:
                endpoint_config = REPSLY_ENDPOINTS[endpoint]
                pagination_type = endpoint_config.get('pagination_type', 'unknown').upper()
                logger.info(f"   ├─ {endpoint:<25} [{pagination_type}] (forced)")
            ordered.extend(endpoints)
            break
    
    logger.info(f"✅ Final Order: {len(ordered)} endpoints planned")
    return ordered

# ---------------- MAIN EXTRACTION FUNCTION WITH ATOMIC STATE UPDATES ---------------- #
def extract_repsly_endpoint(endpoint_key, **context):
    """ATOMIC extraction with bulletproof state management."""
    logger.info("=" * 100)
    logger.info(f"🚀 ENDPOINT EXTRACTION START")
    logger.info("=" * 100)
    
    if endpoint_key not in ENABLED_ENDPOINTS:
        logger.warning(f"⏭️ Skip: {endpoint_key} not enabled in configuration")
        logger.info("=" * 100)
        return 0

    execution_dt = context.get('logical_date') or _utc_now()
    endpoint_config = REPSLY_ENDPOINTS.get(endpoint_key)
    
    if not endpoint_config:
        logger.error(f"❌ Error: Unknown endpoint {endpoint_key}")
        logger.info("=" * 100)
        return 0

    pagination_type = endpoint_config.get('pagination_type', 'unknown').upper()
    incremental_field = endpoint_config.get('incremental_field', 'None')
    dependencies = endpoint_config.get('depends_on', [])
    path = endpoint_config.get('path', 'unknown')
    incremental_enabled = _should_use_incremental(endpoint_key)
    
    # Header information
    logger.info(f"📊 Endpoint: {endpoint_key}")
    logger.info(f"📈 Pagination: {pagination_type}")
    logger.info(f"🔗 Path: {path}")
    logger.info(f"📅 Incremental Field: {incremental_field}")
    logger.info(f"🔄 Incremental Enabled: {'YES' if incremental_enabled else 'NO'}")
    logger.info(f"⏰ Execution: {execution_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    
    if dependencies:
        logger.info(f"🔄 Dependencies: {', '.join(dependencies)}")
    else:
        logger.info(f"🔄 Dependencies: None")
    
    logger.info("-" * 100)

    # CRITICAL: Backup current state before ANY changes
    state_backup = None
    with _STATE_LOCK:
        state_backup = _STATE_CACHE.copy()
    
    logger.info(f"💾 State: Backed up for rollback safety")

    try:
        # Step 1: Extract data (no state changes)
        session = create_authenticated_session()
        raw_data = get_paginated_data(session, endpoint_config, endpoint_key)

        if not raw_data:
            logger.warning(f"📭 No data returned from API")
            logger.info("=" * 100)
            return 0

        # Step 2: Process data (no state changes)
        logger.info(f"🔄 Processing: Flattening {len(raw_data)} records...")
        flattened = []
        for i, record in enumerate(raw_data):
            try:
                flattened.append(flatten_repsly_record(record))
            except Exception as e:
                logger.warning(f"   ⚠️ Failed to flatten record {i}: {e}")
                continue

        if not flattened:
            logger.warning(f"📭 No valid records after flattening")
            logger.info("=" * 100)
            return 0

        # Step 3: Create DataFrame
        df = pd.DataFrame(flattened)
        extracted_at = _utc_now()
        df['_extracted_at'] = extracted_at.isoformat()
        df['_source_system'] = 'repsly'
        df['_endpoint'] = endpoint_key

        logger.info(f"📊 DataFrame: {len(df)} rows × {len(df.columns)} columns")
        logger.info(f"⏰ Extracted: {extracted_at.strftime('%Y-%m-%d %H:%M:%S')} UTC")

        # Step 4: CRITICAL - Load to warehouse FIRST (before state updates)
        try:
            records_loaded = load_dataframe_to_warehouse_verified(df, endpoint_key, extracted_at)
            
            if records_loaded == 0:
                logger.warning(f"⚠️ WARNING: {endpoint_key} completed but no records loaded")
                logger.info("=" * 100)
                return 0
                
            logger.info(f"✅ SUCCESS: {endpoint_key} completed - {records_loaded} records loaded")
            
        except Exception as warehouse_error:
            logger.error(f"❌ WAREHOUSE LOAD FAILED: {warehouse_error}")
            logger.error("🔄 RESTORING original state (no changes made)")
            
            # Restore original state
            with _STATE_LOCK:
                _STATE_CACHE.clear()
                _STATE_CACHE.update(state_backup)
            
            logger.info("=" * 100)
            raise  # Fail the task

        # Step 5: ONLY NOW update state after verified load
        try:
            update_state_after_verified_load(endpoint_key, endpoint_config, df, raw_data, extracted_at)
            logger.info("✅ State: Updated after verified warehouse load")
            
        except Exception as state_error:
            logger.error(f"❌ STATE UPDATE FAILED: {state_error}")
            # Data is loaded successfully, so don't fail pipeline
            logger.warning("⚠️ Data loaded but state may be inconsistent - next run will detect")
        
        logger.info("=" * 100)
        return records_loaded

    except Exception as e:
        logger.error(f"❌ FAILED: {endpoint_key} extraction failed")
        logger.error(f"❌ Error: {str(e)}")
        
        # Restore original state on any failure
        logger.error("🔄 RESTORING original state due to extraction failure")
        with _STATE_LOCK:
            _STATE_CACHE.clear()
            _STATE_CACHE.update(state_backup)
        
        logger.info("=" * 100)
        raise


def update_state_after_verified_load(endpoint_key, endpoint_config, df, raw_data, extracted_at):
    """
    FIXED: Update state ONLY after verified warehouse load - handles ALL pagination types.
    """
    try:
        logger.info(f"📝 State Update: Processing {endpoint_key}")
        
        pagination_type = endpoint_config.get('pagination_type')
        incremental_field = endpoint_config.get('incremental_field')
        
        # ENHANCED: Handle different pagination types properly
        if pagination_type == 'id':
            # ID-based pagination: Update both ID state AND watermark
            if raw_data:
                # Update pagination ID state
                id_field_name = endpoint_config.get('id_field', 'LastUserID').replace('Last', '')
                if id_field_name in raw_data[-1]:
                    last_id_key = f"{endpoint_key}_last_id"
                    with _STATE_LOCK:
                        _STATE_CACHE[last_id_key] = raw_data[-1][id_field_name]
                    logger.info(f"🔢 ID State: Updated to {raw_data[-1][id_field_name]}")
            
            # CRITICAL FIX: Always update watermark for ID-based endpoints
            if incremental_field and not df.empty:
                # Try to find incremental field in DataFrame
                incremental_columns = [col for col in df.columns 
                                     if incremental_field.lower() in col.lower() or 
                                        incremental_field.replace('Date', '').lower() in col.lower()]
                
                if incremental_columns:
                    actual_field = incremental_columns[0]
                    timestamps = _parse_timestamp_column(df, actual_field)
                    valid_timestamps = [ts for ts in timestamps if ts is not None]
                    
                    if valid_timestamps:
                        max_ts = max(valid_timestamps)
                        _update_watermark(endpoint_key, max_ts)
                        logger.info(f"🕒 Watermark: ID endpoint from data max: {max_ts.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                    else:
                        # Fallback: Use extraction timestamp for ID-based endpoints
                        _update_watermark(endpoint_key, extracted_at)
                        logger.info(f"🕒 Watermark: ID endpoint fallback: {extracted_at.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                else:
                    # CRITICAL FIX: No incremental field found - use extraction timestamp
                    logger.warning(f"⚠️ Watermark: Field '{incremental_field}' not found in {endpoint_key} data")
                    _update_watermark(endpoint_key, extracted_at)
                    logger.info(f"🕒 Watermark: ID endpoint extraction time: {extracted_at.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            else:
                # No incremental field defined - still set watermark to extraction time
                _update_watermark(endpoint_key, extracted_at)
                logger.info(f"🕒 Watermark: ID endpoint no-incremental: {extracted_at.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        
        elif pagination_type == 'timestamp':
            # Timestamp-based pagination: Update timestamp state AND watermark
            if raw_data:
                timestamp_key = f"{endpoint_key}_last_timestamp"
                # Get timestamp from API response metadata
                if hasattr(raw_data, '__iter__') and len(raw_data) > 0:
                    # Try to get from last record or use current logic
                    pass  # Keep existing timestamp pagination logic
            
            # Update watermark from data
            if incremental_field and not df.empty:
                incremental_columns = [col for col in df.columns if incremental_field.lower() in col.lower()]
                if incremental_columns:
                    actual_field = incremental_columns[0]
                    timestamps = _parse_timestamp_column(df, actual_field)
                    valid_timestamps = [ts for ts in timestamps if ts is not None]
                    
                    if valid_timestamps:
                        max_ts = max(valid_timestamps)
                        _update_watermark(endpoint_key, max_ts)
                        logger.info(f"🕒 Watermark: Timestamp from data max: {max_ts.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                    else:
                        _update_watermark(endpoint_key, extracted_at)
                        logger.info(f"🕒 Watermark: Timestamp fallback: {extracted_at.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                else:
                    _update_watermark(endpoint_key, extracted_at)
                    logger.info(f"🕒 Watermark: Timestamp extraction time: {extracted_at.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            else:
                _update_watermark(endpoint_key, extracted_at)
                logger.info(f"🕒 Watermark: Timestamp no-incremental: {extracted_at.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        
        elif pagination_type == 'static':
            # Static endpoints: Only update watermark to extraction time
            _update_watermark(endpoint_key, extracted_at)
            logger.info(f"🕒 Watermark: Static endpoint: {extracted_at.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        
        elif pagination_type == 'datetime_range':
            # Date range endpoints: Update watermark from data or extraction time
            if incremental_field and not df.empty:
                incremental_columns = [col for col in df.columns if incremental_field.lower() in col.lower()]
                if incremental_columns:
                    actual_field = incremental_columns[0]
                    timestamps = _parse_timestamp_column(df, actual_field)
                    valid_timestamps = [ts for ts in timestamps if ts is not None]
                    
                    if valid_timestamps:
                        max_ts = max(valid_timestamps)
                        _update_watermark(endpoint_key, max_ts)
                        logger.info(f"🕒 Watermark: Date range from data max: {max_ts.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                    else:
                        _update_watermark(endpoint_key, extracted_at)
                        logger.info(f"🕒 Watermark: Date range fallback: {extracted_at.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                else:
                    _update_watermark(endpoint_key, extracted_at)
                    logger.info(f"🕒 Watermark: Date range extraction time: {extracted_at.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            else:
                _update_watermark(endpoint_key, extracted_at)
                logger.info(f"🕒 Watermark: Date range no-incremental: {extracted_at.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        
        elif pagination_type == 'query_params':
            # Query parameter pagination: Update watermark
            if incremental_field and not df.empty:
                incremental_columns = [col for col in df.columns if incremental_field.lower() in col.lower()]
                if incremental_columns:
                    actual_field = incremental_columns[0]
                    timestamps = _parse_timestamp_column(df, actual_field)
                    valid_timestamps = [ts for ts in timestamps if ts is not None]
                    
                    if valid_timestamps:
                        max_ts = max(valid_timestamps)
                        _update_watermark(endpoint_key, max_ts)
                        logger.info(f"🕒 Watermark: Query param from data max: {max_ts.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                    else:
                        _update_watermark(endpoint_key, extracted_at)
                        logger.info(f"🕒 Watermark: Query param fallback: {extracted_at.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                else:
                    _update_watermark(endpoint_key, extracted_at)
                    logger.info(f"🕒 Watermark: Query param extraction time: {extracted_at.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            else:
                _update_watermark(endpoint_key, extracted_at)
                logger.info(f"🕒 Watermark: Query param no-incremental: {extracted_at.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        
        else:
            # Unknown pagination type: Still set watermark
            logger.warning(f"⚠️ State: Unknown pagination type '{pagination_type}' for {endpoint_key}")
            _update_watermark(endpoint_key, extracted_at)
            logger.info(f"🕒 Watermark: Unknown type: {extracted_at.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        
        # CRITICAL: Save state atomically after all updates
        _save_state()
        logger.info("✅ State: Saved after verified load")
        
    except Exception as e:
        logger.error(f"❌ State: Failed to update - {e}")
        logger.error("❌ CRITICAL: Next run may have data gaps!")
        raise  # Raise to indicate state corruption


# ---------------- DAG INTEGRATION FUNCTIONS ---------------- #
def finalize_state_after_warehouse_load(context):
    """
    Finalize state updates after successful warehouse loading.
    State is already updated per endpoint, this is for final cleanup.
    """
    try:
        logger.info("=" * 80)
        logger.info("✅ STATE FINALIZATION")
        logger.info("=" * 80)
        logger.info("✅ All state updates finalized after successful warehouse loads")
        
        # Verify state file integrity
        try:
            with open(_state_path(), 'r') as f:
                state_content = f.read()
                json.loads(state_content)  # Verify it's valid JSON
            logger.info("✅ State file integrity verified")
        except Exception as e:
            logger.error(f"❌ State file integrity check failed: {e}")
        
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"❌ Failed to finalize state updates: {e}")
        # Don't raise - we don't want to fail the entire pipeline for state issues

# ---------------- EXTRACTION COORDINATION ---------------- #
def extract_all_endpoints_with_dependencies(**context):
    """Extract all endpoints respecting dependencies and atomic state management."""
    logger.info("=" * 100)
    logger.info("🚀 MULTI-ENDPOINT EXTRACTION START")
    logger.info("=" * 100)
    logger.info(f"📊 Mode: {'TESTING' if TESTING_MODE else 'PRODUCTION'}")
    
    endpoint_order = get_endpoint_execution_order()
    total_records = 0
    extraction_results = {}
    completed_endpoints = set()
    
    logger.info("=" * 100)
    
    for i, endpoint_key in enumerate(endpoint_order, 1):
        logger.info(f"🔄 Progress: [{i:2d}/{len(endpoint_order)}] Processing {endpoint_key}")
        
        try:
            # Check dependencies
            endpoint_config = REPSLY_ENDPOINTS.get(endpoint_key, {})
            dependencies = endpoint_config.get('depends_on', [])
            
            # Wait for dependencies (in real implementation, this would be handled by DAG)
            for dep in dependencies:
                if dep not in completed_endpoints and dep in ENABLED_ENDPOINTS:
                    logger.warning(f"⚠️ Dependencies: {endpoint_key} dependency {dep} not completed")
            
            records = extract_repsly_endpoint(endpoint_key, **context)
            total_records += records
            extraction_results[endpoint_key] = records
            completed_endpoints.add(endpoint_key)
            
            logger.info(f"✅ Completed: {endpoint_key} → {records} records")
            
        except Exception as e:
            logger.error(f"❌ Failed: {endpoint_key} → {str(e)}")
            extraction_results[endpoint_key] = f"Failed: {e}"
            # Continue with other endpoints instead of failing everything
            continue
    
    logger.info("=" * 100)
    logger.info("🎉 MULTI-ENDPOINT EXTRACTION COMPLETE")
    logger.info("=" * 100)
    logger.info(f"📊 Total Records: {total_records:,}")
    logger.info(f"✅ Successful: {len(completed_endpoints)}/{len(endpoint_order)} endpoints")
    logger.info(f"❌ Failed: {len(endpoint_order) - len(completed_endpoints)}/{len(endpoint_order)} endpoints")
    
    # Summary by category
    successful = [ep for ep, result in extraction_results.items() if isinstance(result, int)]
    failed = [ep for ep, result in extraction_results.items() if isinstance(result, str)]
    
    if successful:
        logger.info("✅ Successful Endpoints:")
        for ep in successful:
            endpoint_config = REPSLY_ENDPOINTS[ep]
            pagination_type = endpoint_config.get('pagination_type', 'unknown').upper()
            logger.info(f"   ├─ {ep:<25} [{pagination_type:<15}] → {extraction_results[ep]:>6,} records")
    
    if failed:
        logger.info("❌ Failed Endpoints:")
        for ep in failed:
            endpoint_config = REPSLY_ENDPOINTS[ep]
            pagination_type = endpoint_config.get('pagination_type', 'unknown').upper()
            logger.info(f"   ├─ {ep:<25} [{pagination_type:<15}] → {extraction_results[ep]}")
    
    # Category breakdown
    def format_endpoint_list(endpoint_list):
        """Format endpoint list with pagination type indicators"""
        formatted = []
        for ep in endpoint_list:
            endpoint_config = REPSLY_ENDPOINTS[ep]
            pagination_type = endpoint_config.get('pagination_type', 'unknown')
            # Use single letter indicators
            type_map = {'static': 'S', 'id': 'I', 'timestamp': 'T', 'datetime_range': 'D', 'query_params': 'Q'}
            type_indicator = type_map.get(pagination_type, 'U')
            formatted.append(f"{ep}[{type_indicator}]")
        return ', '.join(formatted)
    
    # Group by pagination type
    static_endpoints = [ep for ep in successful if REPSLY_ENDPOINTS.get(ep, {}).get('pagination_type') == 'static']
    id_endpoints = [ep for ep in successful if REPSLY_ENDPOINTS.get(ep, {}).get('pagination_type') == 'id']
    timestamp_endpoints = [ep for ep in successful if REPSLY_ENDPOINTS.get(ep, {}).get('pagination_type') == 'timestamp']
    datetime_endpoints = [ep for ep in successful if REPSLY_ENDPOINTS.get(ep, {}).get('pagination_type') == 'datetime_range']
    query_endpoints = [ep for ep in successful if REPSLY_ENDPOINTS.get(ep, {}).get('pagination_type') == 'query_params']
    
    if static_endpoints:
        logger.info(f"📊 Static Endpoints: {format_endpoint_list(static_endpoints)}")
    if id_endpoints:
        logger.info(f"🔢 ID-based Endpoints: {format_endpoint_list(id_endpoints)}")
    if timestamp_endpoints:
        logger.info(f"⏰ Timestamp Endpoints: {format_endpoint_list(timestamp_endpoints)}")
    if datetime_endpoints:
        logger.info(f"📅 Date Range Endpoints: {format_endpoint_list(datetime_endpoints)}")
    if query_endpoints:
        logger.info(f"🔍 Query Param Endpoints: {format_endpoint_list(query_endpoints)}")
    
    logger.info("📝 Legend: [S]=Static, [I]=ID-based, [T]=Timestamp, [D]=Date Range, [Q]=Query Params")
    logger.info("=" * 100)
    
    return {
        'total_records': total_records,
        'extraction_results': extraction_results,
        'completed_endpoints': list(completed_endpoints)
    }