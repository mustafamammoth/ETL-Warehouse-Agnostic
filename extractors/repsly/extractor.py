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
        'incremental_field': None
    },
    'users': {
        'path': 'export/users',
        'pagination_type': 'id',
        'limit': 50,
        'id_field': 'LastUserID',
        'data_field': 'Users',
        'total_count_field': 'TotalCount',
        'incremental_field': 'ModifiedDate'
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
        'incremental_field': 'Date'
    },
    'visit_schedules_extended': {
        'path': 'export/schedules',
        'pagination_type': 'datetime_range',
        'data_field': 'Schedules',
        'url_pattern': 'export/schedules/{start_date}/{end_date}',
        'incremental_field': 'Date'
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
        'incremental_field': 'ModifiedDate'
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
    logger.info(f"✅ Extractor initialized. Enabled endpoints: {list(ENABLED_ENDPOINTS.keys())}")
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
                logger.info("♻️ Reusing existing authenticated session")
                return _SESSION_CACHE
        except Exception:
            logger.info("🔄 Cached session invalid, creating new one")
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
        logger.info("✅ Authentication successful with pooled connection")
        
        # Cache the session
        _SESSION_CACHE = session
        return session
    except Exception as e:
        logger.error(f"❌ Authentication failed: {e}")
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
        logger.info("⏳ Near rate limit, slowing down")
    elif response.status_code == 429:
        # Hit rate limit, wait longer
        wait_time = int(reset_time) if reset_time else 60
        logger.warning(f"⏸️ Rate limited, waiting {wait_time}s")
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
            logger.info("📁 No existing state file found, starting fresh")
            return
        
        try:
            # Check if file is empty
            if os.path.getsize(path) == 0:
                logger.warning("📁 State file is empty, starting fresh")
                _STATE_CACHE = {}
                return
                
            with open(path, 'r') as f:
                content = f.read().strip()
                if not content:
                    logger.warning("📁 State file content is empty, starting fresh")
                    _STATE_CACHE = {}
                    return
                _STATE_CACHE = json.loads(content)
            logger.info(f"📁 Loaded state from {path}: {_STATE_CACHE}")
        except json.JSONDecodeError as e:
            logger.warning(f"⚠️ Invalid JSON in state file {path}: {e}. Starting fresh.")
            _STATE_CACHE = {}
        except Exception as e:
            logger.warning(f"⚠️ Failed to load state file {path}: {e}")
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
            
            logger.info(f"💾 Saving state to {path}: {_STATE_CACHE}")
            
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
            
            logger.info(f"💾 Successfully saved watermark state to {path} (checksum: {checksum[:8]})")
            
        except Exception as e:
            logger.error(f"❌ Failed to save state file {path}: {e}")
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
            logger.warning(f"⚠️ Invalid watermark format for {endpoint_key}: {iso}")
            return None

def _update_watermark(endpoint_key: str, new_ts: datetime):
    """Update watermark for endpoint - THREAD SAFE, NOT SAVED YET."""
    with _STATE_LOCK:
        if new_ts.tzinfo is None:
            new_ts = new_ts.replace(tzinfo=timezone.utc)
        # Always store in UTC ISO format with Z
        _STATE_CACHE[endpoint_key] = new_ts.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
        logger.info(f"🕒 Updated watermark in memory: {endpoint_key} = {_STATE_CACHE[endpoint_key]}")

# ---------------- IMPROVED INCREMENTAL HELPERS ---------------- #
def _parse_microsoft_json_date(date_str: str) -> Optional[datetime]:
    """Parse Microsoft JSON date format with robust error handling."""
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
            return datetime.fromisoformat(date_str)
        
        # Date only format
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
    """Check if endpoint should use incremental extraction."""
    if not CONFIG['extraction']['incremental'].get('enabled', False):
        return False
    
    endpoint_config = REPSLY_ENDPOINTS.get(endpoint_key, {})
    incremental_field = endpoint_config.get('incremental_field')
    
    # Static endpoints don't support incremental
    if endpoint_config.get('pagination_type') == 'static' or not incremental_field:
        return False
    
    return True

def _get_incremental_date_range(endpoint_key: str):
    """Get date range for incremental extraction."""
    if not _should_use_incremental(endpoint_key):
        # Default date range for full extraction
        end_date = datetime.now()
        if TESTING_MODE:
            start_date = end_date - timedelta(days=CONFIG['extraction']['testing']['date_range_days'])
        else:
            start_date = end_date - timedelta(days=CONFIG['extraction']['production']['date_range_days'])
        return start_date, end_date
    
    last_wm = _get_last_watermark(endpoint_key)
    end_date = datetime.now()
    
    if last_wm:
        lookback = CONFIG['extraction']['incremental'].get('lookback_minutes', 10)
        start_date = last_wm - timedelta(minutes=lookback)
        logger.info(f"🔁 Incremental extract for {endpoint_key} from {start_date.isoformat()}")
    else:
        # First run - use default range
        if TESTING_MODE:
            start_date = end_date - timedelta(days=CONFIG['extraction']['testing']['date_range_days'])
        else:
            start_date = end_date - timedelta(days=CONFIG['extraction']['production']['date_range_days'])
        logger.info(f"🔄 First run for {endpoint_key}, using date range from {start_date.isoformat()}")
    
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
        logger.info(f"🆕 Created partitioned table {full_table}")
        
        # Verify the table was created with partitions
        verify_query = f"SHOW CREATE TABLE {full_table}"
        result = client.query(verify_query)
        create_statement = result.result_rows[0][0]
        
        if "PARTITION BY" in create_statement:
            logger.info(f"✅ Verified {full_table} has partitioning")
        else:
            logger.error(f"❌ CRITICAL: {full_table} was created WITHOUT partitioning!")
            raise Exception(f"Table {full_table} created without partitioning")
            
    except Exception as e:
        logger.error(f"❌ Failed to create table {full_table}: {e}")
        raise

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
def get_paginated_data(session, endpoint_config, endpoint_name):
    """Get data from Repsly endpoint with robust pagination and error handling."""
    all_data = []
    base_url = CONFIG['api']['base_url']
    max_retries = 3
    consecutive_failures = 0
    
    if endpoint_config['pagination_type'] == 'static':
        # Static endpoints - single request
        endpoint_url = f"{base_url}/{endpoint_config['path']}"
        logger.info(f"   Fetching static data from: {endpoint_url}")
        
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
                break
                
            except Exception as e:
                consecutive_failures += 1
                logger.warning(f"   Attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
    
    elif endpoint_config['pagination_type'] == 'datetime_range':
        # Date range endpoints
        start_date, end_date = _get_incremental_date_range(endpoint_name)
        
        # Split large date ranges to avoid timeouts
        max_range_days = CONFIG.get('repsly_specific', {}).get('date_ranges', {}).get(endpoint_name, {}).get('max_range_days', 90)
        current_start = start_date
        
        while current_start < end_date:
            current_end = min(current_start + timedelta(days=max_range_days), end_date)
            
            start_date_str = current_start.strftime('%Y-%m-%d')
            end_date_str = current_end.strftime('%Y-%m-%d')
            
            endpoint_url = f"{base_url}/{endpoint_config['url_pattern'].format(start_date=start_date_str, end_date=end_date_str)}"
            logger.info(f"   Fetching date range data from: {endpoint_url}")
            
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
                    logger.info(f"   Retrieved {len(records)} records for date range {start_date_str} to {end_date_str}")
                    break
                    
                except Exception as e:
                    logger.warning(f"   Date range attempt {attempt + 1} failed: {e}")
                    if attempt == max_retries - 1:
                        logger.error(f"   Failed to fetch date range {start_date_str} to {end_date_str} after {max_retries} attempts")
                        # Continue to next range instead of failing completely
                        break
                    time.sleep(2 ** attempt)
            
            current_start = current_end + timedelta(days=1)
    
    elif endpoint_config['pagination_type'] == 'query_params':
        # Query parameter based pagination with improved error handling
        start_date, end_date = _get_incremental_date_range(endpoint_name)
        
        modified_date = start_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        skip = 0
        page_size = 50
        max_pages = 190
        page_count = 0
        
        logger.info(f"   Starting pagination with modified date: {modified_date}")
        
        while page_count < max_pages:
            page_count += 1
            
            if skip >= 9500:
                logger.info(f"   Reached skip limit (9500), stopping pagination")
                break
            
            endpoint_url = f"{base_url}/{endpoint_config['url_pattern']}?modified={modified_date}&skip={skip}"
            logger.info(f"   Page {page_count}: Fetching from {endpoint_url}")
            
            for attempt in range(max_retries):
                try:
                    response = session.get(endpoint_url, timeout=CONFIG['api']['rate_limiting']['timeout_seconds'])
                    
                    if response.status_code == 400:
                        logger.info(f"   400 Bad Request, trying newer date")
                        start_date = start_date + timedelta(days=30)
                        modified_date = start_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                        skip = 0
                        page_count = 0
                        break
                    
                    response.raise_for_status()
                    data = response.json()
                    
                    records = data.get(endpoint_config['data_field'], [])
                    logger.info(f"   Retrieved {len(records)} records (total so far: {len(all_data)})")
                    
                    if not records:
                        logger.info(f"   No records found, trying newer date")
                        start_date = start_date + timedelta(days=30)
                        modified_date = start_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                        skip = 0
                        
                        if start_date > datetime.now():
                            logger.info(f"   Reached current date, stopping pagination")
                            return all_data
                        break
                    
                    all_data.extend(records)
                    
                    # Check testing limit
                    if TESTING_MODE and MAX_RECORDS_PER_ENDPOINT and len(all_data) >= MAX_RECORDS_PER_ENDPOINT:
                        all_data = all_data[:MAX_RECORDS_PER_ENDPOINT]
                        logger.info(f"   Reached testing limit of {MAX_RECORDS_PER_ENDPOINT} records")
                        return all_data
                    
                    if len(records) < page_size:
                        logger.info(f"   Got fewer records than page size, moving to newer date")
                        start_date = start_date + timedelta(days=30)
                        modified_date = start_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                        skip = 0
                        
                        if start_date > datetime.now():
                            return all_data
                        break
                    
                    skip += page_size
                    smart_rate_limit(response, CONFIG)
                    break
                    
                except requests.exceptions.RequestException as e:
                    logger.warning(f"   Request error on page {page_count}, attempt {attempt + 1}: {e}")
                    if attempt == max_retries - 1:
                        if page_count == 1:
                            start_date = start_date + timedelta(days=30)
                            modified_date = start_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                            skip = 0
                            page_count = 0
                            break
                        else:
                            start_date = start_date + timedelta(days=30)
                            modified_date = start_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                            skip = 0
                            break
                    time.sleep(2 ** attempt)
                
                except Exception as e:
                    logger.error(f"   Unexpected error on page {page_count}, attempt {attempt + 1}: {e}")
                    if attempt == max_retries - 1:
                        logger.error(f"   Giving up on this page after {max_retries} attempts")
                        break
                    time.sleep(2 ** attempt)
    
    else:
        # Standard paginated endpoints (id or timestamp based) with improved error handling
        if _should_use_incremental(endpoint_name):
            if endpoint_config['pagination_type'] == 'id':
                last_id_key = f"{endpoint_name}_last_id"
                with _STATE_LOCK:
                    last_value = _STATE_CACHE.get(last_id_key, 0)
                logger.info(f"   🔁 INCREMENTAL (ID): Starting from last ID: {last_value}")
            
            elif endpoint_config['pagination_type'] == 'timestamp':
                timestamp_key = f"{endpoint_name}_last_timestamp"
                with _STATE_LOCK:
                    stored_raw_timestamp = _STATE_CACHE.get(timestamp_key)
                
                if stored_raw_timestamp:
                    last_value = stored_raw_timestamp
                    logger.info(f"   🔁 INCREMENTAL (TIMESTAMP): Starting from stored timestamp: {last_value}")
                else:
                    last_value = 0
                    logger.info(f"   🔄 FIRST RUN (TIMESTAMP): Starting from timestamp: 0")
            else:
                last_value = 0
                logger.info(f"   🔄 FULL REFRESH: Starting from 0")
        else:
            last_value = 0
            logger.info(f"   🔄 NO INCREMENTAL: Starting from 0")
        
        page_count = 0
        max_pages = 1000
        
        while page_count < max_pages:
            page_count += 1
            endpoint_url = f"{base_url}/{endpoint_config['path']}/{last_value}"
            
            logger.info(f"   Page {page_count}: Fetching from {endpoint_url}")
            
            for attempt in range(max_retries):
                try:
                    response = session.get(endpoint_url, timeout=CONFIG['api']['rate_limiting']['timeout_seconds'])
                    response.raise_for_status()
                    data = response.json()
                    
                    meta = data.get('MetaCollectionResult', {})
                    total_count = meta.get(endpoint_config['total_count_field'], 0)
                    
                    if endpoint_config['pagination_type'] == 'timestamp':
                        new_value = meta.get(endpoint_config['timestamp_field'], 0)
                    else:
                        new_value = meta.get(endpoint_config['id_field'], 0)
                    
                    records = data.get(endpoint_config['data_field'], [])
                    
                    logger.info(f"   Retrieved {len(records)} records (total: {len(all_data)})")
                    logger.info(f"   Meta result - Total count: {total_count}, New value: {new_value}")
                    
                    if not records or total_count == 0:
                        logger.info(f"   No more records available, stopping pagination")
                        return all_data
                    
                    all_data.extend(records)
                    
                    # Store pagination state (but don't save to file yet)
                    if endpoint_config['pagination_type'] == 'id' and records:
                        id_field_name = endpoint_config.get('id_field', 'LastClientNoteID').replace('Last', '')
                        if id_field_name in records[-1]:
                            highest_id_in_batch = records[-1][id_field_name]
                            last_id_key = f"{endpoint_name}_last_id"
                            with _STATE_LOCK:
                                _STATE_CACHE[last_id_key] = highest_id_in_batch
                            logger.info(f"   📝 Stored last ID in memory: {highest_id_in_batch}")
                    
                    elif endpoint_config['pagination_type'] == 'timestamp' and records:
                        if new_value and new_value > last_value:
                            timestamp_key = f"{endpoint_name}_last_timestamp"
                            with _STATE_LOCK:
                                _STATE_CACHE[timestamp_key] = new_value
                            logger.info(f"   📝 Stored API timestamp in memory: {new_value}")
                        else:
                            timestamp_field_name = endpoint_config.get('incremental_field', 'TimeStamp')
                            if timestamp_field_name in records[-1]:
                                latest_timestamp_in_batch = records[-1][timestamp_field_name]
                                timestamp_key = f"{endpoint_name}_last_timestamp"
                                with _STATE_LOCK:
                                    _STATE_CACHE[timestamp_key] = latest_timestamp_in_batch
                                logger.info(f"   📝 Stored record timestamp in memory: {latest_timestamp_in_batch}")
                    
                    # Check testing limit
                    if TESTING_MODE and MAX_RECORDS_PER_ENDPOINT and len(all_data) >= MAX_RECORDS_PER_ENDPOINT:
                        all_data = all_data[:MAX_RECORDS_PER_ENDPOINT]
                        logger.info(f"   Reached testing limit of {MAX_RECORDS_PER_ENDPOINT} records")
                        return all_data
                    
                    # Update last_value for next iteration
                    if new_value > last_value:
                        last_value = new_value
                        logger.info(f"   📈 Updated pagination value: {last_value}")
                    else:
                        logger.info(f"   ⚠️ No progress in pagination (new_value: {new_value}, last_value: {last_value}), stopping")
                        return all_data
                        
                    if len(records) < endpoint_config['limit']:
                        logger.info(f"   Got fewer records than limit ({len(records)} < {endpoint_config['limit']}), reached end")
                        return all_data
                    
                    smart_rate_limit(response, CONFIG)
                    break
                    
                except Exception as e:
                    consecutive_failures += 1
                    logger.warning(f"   Error on page {page_count}, attempt {attempt + 1}: {e}")
                    if attempt == max_retries - 1:
                        if page_count == 1:
                            raise  # Fail fast if first page fails
                        else:
                            logger.warning(f"   Giving up on page {page_count} after {max_retries} attempts")
                            return all_data
                    time.sleep(2 ** attempt)
    
    # Apply testing limit
    if TESTING_MODE and MAX_RECORDS_PER_ENDPOINT:
        all_data = all_data[:MAX_RECORDS_PER_ENDPOINT]
    
    logger.info(f"✅ {endpoint_name}: Collected {len(all_data)} total records")
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
            logger.info(f"⏳ {endpoint_key} waiting for dependency {dep}")
            return False
    
    return True

def get_endpoint_execution_order():
    """Get endpoints in dependency order."""
    endpoints = list(ENABLED_ENDPOINTS.keys())
    completed = set()
    ordered = []
    
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
        
        if not made_progress:
            # Add remaining endpoints (circular dependencies or missing deps)
            logger.warning(f"⚠️ Adding remaining endpoints without dependency check: {endpoints}")
            ordered.extend(endpoints)
            break
    
    logger.info(f"📋 Endpoint execution order: {ordered}")
    return ordered

# ---------------- MAIN EXTRACTION FUNCTION WITH ATOMIC STATE UPDATES ---------------- #
def extract_repsly_endpoint(endpoint_key, **context):
    """Main extraction function with atomic state management - PREVENTS DATA LOSS."""
    if endpoint_key not in ENABLED_ENDPOINTS:
        logger.warning(f"⚠️ Endpoint {endpoint_key} not enabled")
        return 0

    # execution_dt = context.get('execution_date') or _utc_now()
    execution_dt = context.get('logical_date') or _utc_now()

    endpoint_config = REPSLY_ENDPOINTS.get(endpoint_key)
    if not endpoint_config:
        logger.error(f"❌ Unknown endpoint: {endpoint_key}")
        return 0

    logger.info(f"🔄 Extracting {endpoint_key} ({endpoint_config['path']})")
    logger.info(f"   Incremental: {CONFIG['extraction']['incremental']['enabled']}")

    # Store initial state for rollback
    initial_state = None
    with _STATE_LOCK:
        initial_state = _STATE_CACHE.copy()

    try:
        session = create_authenticated_session()
        
        # Fetch data
        raw_data = get_paginated_data(session, endpoint_config, endpoint_key)

        if not raw_data:
            logger.warning(f"⚠️ No data returned for {endpoint_key}")
            return 0

        # Flatten nested structures
        logger.info(f"   Flattening {len(raw_data)} records...")
        flattened = []
        for i, record in enumerate(raw_data):
            try:
                flattened.append(flatten_repsly_record(record))
            except Exception as e:
                logger.warning(f"   Failed to flatten record {i}: {e}")
                continue

        if not flattened:
            logger.warning(f"⚠️ No valid records after flattening for {endpoint_key}")
            return 0

        # Create DataFrame
        df = pd.DataFrame(flattened)

        # Add metadata columns
        extracted_at = _utc_now()
        df['_extracted_at'] = extracted_at.isoformat()
        df['_source_system'] = 'repsly'
        df['_endpoint'] = endpoint_key

        logger.info(f"   DataFrame shape: {df.shape}")
        logger.info(f"   DataFrame columns: {list(df.columns)}")

        # CRITICAL: Attempt warehouse load BEFORE updating any state
        try:
            records_loaded = load_dataframe_to_warehouse(df, endpoint_key, extracted_at)
            
            if records_loaded > 0:
                logger.info(f"✅ {endpoint_key}: Loaded {records_loaded} records to warehouse")
                
                # ONLY update state after successful warehouse load
                update_extraction_state_after_successful_load(endpoint_key, endpoint_config, df, raw_data)
                
            else:
                logger.warning(f"⚠️ No records loaded for {endpoint_key}")
                # Restore initial state since no data was loaded
                with _STATE_LOCK:
                    _STATE_CACHE.clear()
                    _STATE_CACHE.update(initial_state)
                
            return records_loaded
            
        except Exception as warehouse_error:
            logger.error(f"❌ Failed to load {endpoint_key} to warehouse: {warehouse_error}")
            
            # CRITICAL: Restore initial state to prevent data loss on next run
            logger.error("🔄 ROLLING BACK state changes due to warehouse failure")
            with _STATE_LOCK:
                _STATE_CACHE.clear()
                _STATE_CACHE.update(initial_state)
            
            raise

    except Exception as e:
        logger.error(f"❌ Failed to extract {endpoint_key}: {e}")
        
        # Restore initial state on any failure
        logger.error("🔄 ROLLING BACK state changes due to extraction failure")
        with _STATE_LOCK:
            _STATE_CACHE.clear()
            _STATE_CACHE.update(initial_state)
        
        raise

# ---------------- DAG INTEGRATION FUNCTIONS ---------------- #
def finalize_state_after_warehouse_load(context):
    """
    Finalize state updates after successful warehouse loading.
    State is already updated per endpoint, this is for final cleanup.
    """
    try:
        logger.info("✅ All state updates finalized after successful warehouse loads")
        
        # Verify state file integrity
        try:
            with open(_state_path(), 'r') as f:
                state_content = f.read()
                json.loads(state_content)  # Verify it's valid JSON
            logger.info("✅ State file integrity verified")
        except Exception as e:
            logger.error(f"❌ State file integrity check failed: {e}")
        
    except Exception as e:
        logger.error(f"❌ Failed to finalize state updates: {e}")
        # Don't raise - we don't want to fail the entire pipeline for state issues

# ---------------- EXTRACTION COORDINATION ---------------- #
def extract_all_endpoints_with_dependencies(**context):
    """Extract all endpoints respecting dependencies and atomic state management."""
    logger.info("🚀 Starting coordinated extraction with dependency management...")
    
    endpoint_order = get_endpoint_execution_order()
    total_records = 0
    extraction_results = {}
    completed_endpoints = set()
    
    for endpoint_key in endpoint_order:
        try:
            # Check dependencies
            endpoint_config = REPSLY_ENDPOINTS.get(endpoint_key, {})
            dependencies = endpoint_config.get('depends_on', [])
            
            # Wait for dependencies (in real implementation, this would be handled by DAG)
            for dep in dependencies:
                if dep not in completed_endpoints and dep in ENABLED_ENDPOINTS:
                    logger.warning(f"⚠️ {endpoint_key} dependency {dep} not completed")
            
            logger.info(f"📊 Extracting {endpoint_key}...")
            records = extract_repsly_endpoint(endpoint_key, **context)
            total_records += records
            extraction_results[endpoint_key] = records
            completed_endpoints.add(endpoint_key)
            
            logger.info(f"✅ {endpoint_key}: {records} records loaded")
            
        except Exception as e:
            logger.error(f"❌ {endpoint_key} failed: {e}")
            extraction_results[endpoint_key] = f"Failed: {e}"
            # Continue with other endpoints instead of failing everything
            continue
    
    # Final summary
    logger.info("📈 Extraction Summary:")
    for endpoint, result in extraction_results.items():
        if isinstance(result, int):
            logger.info(f"   {endpoint}: {result} records")
        else:
            logger.info(f"   {endpoint}: {result}")
    
    logger.info(f"✅ Coordinated extraction complete. Total records: {total_records}")
    
    return {
        'total_records': total_records,
        'extraction_results': extraction_results,
        'completed_endpoints': list(completed_endpoints)
    }