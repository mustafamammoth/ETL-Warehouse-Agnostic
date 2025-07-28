# extractor.py - LeafLink API extractor with incremental logic
# Path: root/extractors/leaflink/extractor.py

from datetime import datetime, timedelta, timezone
import os
import time
import json
import requests
import pandas as pd
from typing import Dict, Any, List, Optional
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
LEAFLINK_ENDPOINTS = {
    'orders_received': {
        'path': 'orders-received',
        'pagination_type': 'offset_limit',
        'limit': 50,
        'data_field': 'results',
        'total_count_field': 'count',
        'next_field': 'next',
        'incremental_field': 'modified',  # Use modified field for incremental
        'priority': 'high',
        'supports_company_scope': True,  # This endpoint can be scoped to specific companies
        'date_filters': {
            'modified__gte': 'incremental_start',  # Filter for incremental extraction
            'modified__lte': 'incremental_end'
        },
        'include_children': ['line_items', 'customer', 'sales_reps'],  # Include related data
        'fields_add': ['created_by', 'last_changed_by']  # Additional fields
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
        for k, v in LEAFLINK_ENDPOINTS.items()
        if k in all_allowed and k not in disabled
    }
    
    _load_state()
    logger.info(f"✅ LeafLink Extractor initialized. Enabled endpoints: {list(ENABLED_ENDPOINTS.keys())}")
    return ENABLED_ENDPOINTS

# ---------------- AUTH WITH API KEY ---------------- #
def create_authenticated_session():
    """Create authenticated session with API key and connection pooling."""
    global _SESSION_CACHE
    
    # Return cached session if available
    if _SESSION_CACHE is not None:
        try:
            # Test if session is still valid with a simple request
            test_url = f"{CONFIG['api']['base_url']}/companies/"
            response = _SESSION_CACHE.get(test_url, timeout=5, params={'limit': 1})
            if response.status_code == 200:
                logger.info("♻️ Reusing existing authenticated session")
                return _SESSION_CACHE
        except Exception:
            logger.info("🔄 Cached session invalid, creating new one")
            _SESSION_CACHE = None
    
    api_key = os.getenv('LEAFLINK_API_KEY')
    base_url = CONFIG['api']['base_url']
    
    if not api_key:
        raise ValueError("Missing required environment variable: LEAFLINK_API_KEY")
    
    # Create session with API key authentication
    session = requests.Session()
    
    # LeafLink uses App {API_KEY} format
    session.headers.update({
        'Authorization': f'Token {api_key}',
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    })
    
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
    test_url = f"{base_url}/companies/"
    try:
        response = session.get(test_url, timeout=CONFIG['api']['rate_limiting']['timeout_seconds'], params={'limit': 1})
        response.raise_for_status()
        logger.info("✅ Authentication successful with API key")
        
        # Cache the session
        _SESSION_CACHE = session
        return session
    except Exception as e:
        logger.error(f"❌ Authentication failed: {e}")
        raise

# ---------------- UTILITIES ---------------- #
def flatten_leaflink_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten LeafLink record structure with special handling for nested objects."""
    flattened = {}
    
    if isinstance(record, dict):
        for key, value in record.items():
            if isinstance(value, dict):
                # Special handling for common LeafLink nested structures
                if key == 'total' and 'amount' in value:
                    # Handle total: {"amount": 450.0, "currency": "USD"}
                    flattened['total_amount'] = value.get('amount')
                    flattened['total_currency'] = value.get('currency')
                elif key == 'shipping_charge' and 'amount' in value:
                    # Handle shipping_charge: {"amount": 0.0, "currency": "USD"}
                    flattened['shipping_charge_amount'] = value.get('amount')
                    flattened['shipping_charge_currency'] = value.get('currency')
                elif key == 'customer' and isinstance(value, dict):
                    # Handle customer: {"id": 123, "display_name": "Name", ...}
                    for nested_key, nested_value in value.items():
                        flattened[f"customer_{nested_key}"] = nested_value
                elif key in ['ordered_unit_price', 'sale_price'] and 'amount' in value:
                    # Handle price objects in line items
                    flattened[f"{key}_amount"] = value.get('amount')
                    flattened[f"{key}_currency"] = value.get('currency')
                else:
                    # Standard nested object flattening
                    for nested_key, nested_value in value.items():
                        flattened[f"{key}_{nested_key}"] = nested_value
            elif isinstance(value, list):
                # Handle arrays - convert to JSON strings or flatten if simple
                if value and all(isinstance(item, dict) for item in value):
                    # Complex objects - store as JSON
                    flattened[key] = json.dumps(value) if value else None
                else:
                    # Simple arrays - join or store as JSON
                    flattened[key] = json.dumps(value) if value else None
            else:
                flattened[key] = value
    else:
        flattened['value'] = record
    
    return flattened

def smart_rate_limit(response, config):
    """Smart rate limiting based on response headers and status."""
    # Check for rate limit headers (LeafLink specific)
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
                os.fsync(f.fileno())
            
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
        _STATE_CACHE[endpoint_key] = new_ts.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
        logger.info(f"🕒 Updated watermark in memory: {endpoint_key} = {_STATE_CACHE[endpoint_key]}")

# ---------------- INCREMENTAL HELPERS ---------------- #
def _parse_leaflink_timestamp(date_str: str) -> Optional[datetime]:
    """Parse LeafLink timestamp format with timezone awareness."""
    if not date_str or date_str == '' or date_str is None:
        return None
    
    try:
        date_str = str(date_str).strip()
        
        # LeafLink uses ISO format with timezone: "2025-07-28T16:38:46.400810-04:00"
        if 'T' in date_str:
            # Handle timezone offset formats
            if date_str.endswith('Z'):
                return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            elif '+' in date_str[-6:] or '-' in date_str[-6:]:
                return datetime.fromisoformat(date_str)
            else:
                # No timezone info, assume UTC
                return datetime.fromisoformat(date_str + '+00:00')
        
        # Date only format - assume UTC
        return datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        
    except Exception as e:
        logger.warning(f"Failed to parse timestamp: {date_str} - {e}")
        return None

def _parse_timestamp_column(df: pd.DataFrame, timestamp_field: str) -> pd.Series:
    """Parse timestamp column with LeafLink timestamp handling."""
    if timestamp_field not in df.columns:
        return pd.Series([None] * len(df))
    
    return df[timestamp_field].apply(_parse_leaflink_timestamp)

def _should_use_incremental(endpoint_key: str) -> bool:
    """Check if endpoint should use incremental extraction."""
    if not CONFIG['extraction']['incremental'].get('enabled', False):
        return False
    
    endpoint_config = LEAFLINK_ENDPOINTS.get(endpoint_key, {})
    incremental_field = endpoint_config.get('incremental_field')
    
    return incremental_field is not None

def _get_incremental_date_range(endpoint_key: str):
    """Get date range for incremental extraction with proper timezone handling."""
    endpoint_config = LEAFLINK_ENDPOINTS.get(endpoint_key, {})
    
    if not _should_use_incremental(endpoint_key):
        # Default date range for full extraction
        end_date = _utc_now()
        if TESTING_MODE:
            start_date = end_date - timedelta(days=CONFIG['extraction']['testing']['date_range_days'])
        else:
            start_date = end_date - timedelta(days=CONFIG['extraction']['production']['date_range_days'])
        logger.info(f"🔄 Full extraction date range for {endpoint_key}: {start_date.isoformat()} to {end_date.isoformat()}")
        return start_date, end_date
    
    last_wm = _get_last_watermark(endpoint_key)
    end_date = _utc_now()
    
    if last_wm:
        if last_wm.tzinfo is None:
            last_wm = last_wm.replace(tzinfo=timezone.utc)
        
        lookback = CONFIG['extraction']['incremental'].get('lookback_minutes', 10)
        start_date = last_wm - timedelta(minutes=lookback)
        logger.info(f"🔁 Incremental extract for {endpoint_key} from {start_date.isoformat()}")
    else:
        # First run - use default range
        if TESTING_MODE:
            start_date = end_date - timedelta(days=CONFIG['extraction']['testing']['date_range_days'])
        else:
            start_date = end_date - timedelta(days=CONFIG['extraction']['production']['date_range_days'])
        logger.info(f"🔄 First run for {endpoint_key}, using default range")
    
    return start_date, end_date

# ---------------- WAREHOUSE LOADING FUNCTIONS ---------------- #
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
        # Ensure schema exists
        if raw_schema != 'default':
            try:
                client.command(f"CREATE DATABASE IF NOT EXISTS `{raw_schema}`")
                logger.info(f"✅ Database {raw_schema} ready")
            except Exception as db_error:
                logger.error(f"❌ Failed to create database {raw_schema}: {db_error}")
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
                logger.warning(f"⚠️ DUPLICATE PREVENTION: {existing_count} records already exist")
                logger.warning(f"   Timestamp: {extraction_timestamp}")
                logger.warning(f"   Skipping load to prevent duplicates")
                return 0
            
            logger.info(f"✅ No duplicates found for {extraction_timestamp}")
        else:
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
            raise ValueError(f"Load verification failed: expected {len(df)}, loaded {actual_loaded}")
        
        logger.info(f"✅ Load verified: {actual_loaded} records")
        return actual_loaded
        
    except Exception as e:
        logger.error(f"❌ Warehouse load failed: {e}")
        raise
    finally:
        client.close()

# ---------------- PAGINATION + FETCH WITH ERROR HANDLING ---------------- #
def get_paginated_data(session, endpoint_config, endpoint_name):
    """Get data from LeafLink endpoint with offset/limit pagination."""
    all_data = []
    base_url = CONFIG['api']['base_url']
    max_retries = 3
    
    # Get company ID from environment variable
    company_id = os.getenv('LEAFLINK_COMPANY_ID')
    
    if company_id and endpoint_config.get('supports_company_scope'):
        # Company-scoped endpoint
        endpoint_url_base = f"{base_url}/companies/{company_id}/{endpoint_config['path']}/"
        logger.info(f"   Using company-scoped endpoint for company ID: {company_id}")
    else:
        # Global endpoint
        endpoint_url_base = f"{base_url}/{endpoint_config['path']}/"
        if endpoint_config.get('supports_company_scope'):
            logger.warning("⚠️ Company ID not provided but endpoint supports company scoping")
            logger.warning("   Set LEAFLINK_COMPANY_ID environment variable for company-specific data")
    
    # Prepare query parameters
    params = {
        'limit': endpoint_config['limit']
    }
    
    # Add incremental filtering if enabled
    if _should_use_incremental(endpoint_name):
        start_date, end_date = _get_incremental_date_range(endpoint_name)
        
        # Add date filters based on endpoint configuration
        date_filters = endpoint_config.get('date_filters', {})
        for param_name, date_type in date_filters.items():
            if date_type == 'incremental_start':
                params[param_name] = start_date.isoformat()
            elif date_type == 'incremental_end':
                params[param_name] = end_date.isoformat()
    
    # Add additional parameters
    if endpoint_config.get('include_children'):
        params['include_children'] = ','.join(endpoint_config['include_children'])
    
    if endpoint_config.get('fields_add'):
        params['fields_add'] = ','.join(endpoint_config['fields_add'])
    
    offset = 0
    page_count = 0
    max_pages = 1000
    
    logger.info(f"   Starting pagination with params: {params}")
    
    while page_count < max_pages:
        page_count += 1
        params['offset'] = offset
        
        logger.info(f"   Page {page_count}: Fetching from {endpoint_url_base} (offset: {offset})")
        
        success = False
        for attempt in range(max_retries):
            try:
                response = session.get(endpoint_url_base, params=params, 
                                     timeout=CONFIG['api']['rate_limiting']['timeout_seconds'])
                response.raise_for_status()
                data = response.json()
                
                # Extract records based on data_field
                records = data.get(endpoint_config['data_field'], [])
                total_count = data.get(endpoint_config['total_count_field'], 0)
                next_url = data.get(endpoint_config['next_field'])
                
                logger.info(f"   Retrieved {len(records)} records (total available: {total_count})")
                logger.info(f"   Total collected so far: {len(all_data)}")
                
                if not records:
                    logger.info(f"   No more records available, stopping pagination")
                    return all_data
                
                all_data.extend(records)
                
                # Check testing limit
                if TESTING_MODE and MAX_RECORDS_PER_ENDPOINT and len(all_data) >= MAX_RECORDS_PER_ENDPOINT:
                    all_data = all_data[:MAX_RECORDS_PER_ENDPOINT]
                    logger.info(f"   Reached testing limit of {MAX_RECORDS_PER_ENDPOINT} records")
                    return all_data
                
                # Check if we have more data
                if len(records) < endpoint_config['limit'] or not next_url:
                    logger.info(f"   Reached end of data (got {len(records)} < {endpoint_config['limit']} or no next URL)")
                    return all_data
                
                offset += endpoint_config['limit']
                smart_rate_limit(response, CONFIG)
                success = True
                break
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"   Request error on page {page_count}, attempt {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    if page_count == 1:
                        raise  # Fail fast if first page fails
                    else:
                        logger.warning(f"   Giving up on page {page_count} after {max_retries} attempts")
                        return all_data
                time.sleep(2 ** attempt)
            
            except Exception as e:
                logger.error(f"   Unexpected error on page {page_count}, attempt {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    logger.error(f"   Giving up on page {page_count} after {max_retries} attempts")
                    return all_data
                time.sleep(2 ** attempt)
        
        if not success:
            logger.error(f"   Page {page_count} failed completely, stopping")
            break
    
    logger.info(f"✅ {endpoint_name}: Collected {len(all_data)} total records")
    return all_data

# ---------------- STATE MANAGEMENT AFTER SUCCESSFUL LOAD ---------------- #
def update_state_after_verified_load(endpoint_key, endpoint_config, df, raw_data, extracted_at):
    """Update state ONLY after verified warehouse load."""
    try:
        logger.info(f"📝 Updating state after VERIFIED load for {endpoint_key}")
        
        incremental_field = endpoint_config.get('incremental_field')
        
        if incremental_field and not df.empty:
            # Try to find incremental field in DataFrame
            incremental_columns = [col for col in df.columns 
                                 if incremental_field.lower() in col.lower()]
            
            if incremental_columns:
                actual_field = incremental_columns[0]
                timestamps = _parse_timestamp_column(df, actual_field)
                valid_timestamps = [ts for ts in timestamps if ts is not None]
                
                if valid_timestamps:
                    max_ts = max(valid_timestamps)
                    _update_watermark(endpoint_key, max_ts)
                    logger.info(f"🕒 Updated watermark from data: {max_ts.isoformat()}")
                else:
                    _update_watermark(endpoint_key, extracted_at)
                    logger.info(f"🕒 Fallback watermark: {extracted_at.isoformat()}")
            else:
                logger.warning(f"⚠️ Incremental field '{incremental_field}' not found in {endpoint_key} data")
                _update_watermark(endpoint_key, extracted_at)
                logger.info(f"🕒 Extraction timestamp watermark: {extracted_at.isoformat()}")
        else:
            _update_watermark(endpoint_key, extracted_at)
            logger.info(f"🕒 No incremental field watermark: {extracted_at.isoformat()}")
        
        # CRITICAL: Save state atomically after all updates
        _save_state()
        logger.info("✅ State saved after verified load")
        
    except Exception as e:
        logger.error(f"❌ Failed to update state: {e}")
        logger.error("❌ CRITICAL: Next run may have data gaps!")
        raise

# ---------------- MAIN EXTRACTION FUNCTION ---------------- #
def extract_leaflink_endpoint(endpoint_key, **context):
    """ATOMIC extraction with bulletproof state management."""
    if endpoint_key not in ENABLED_ENDPOINTS:
        logger.warning(f"⚠️ Endpoint {endpoint_key} not enabled")
        return 0

    execution_dt = context.get('logical_date') or _utc_now()
    endpoint_config = LEAFLINK_ENDPOINTS.get(endpoint_key)
    
    if not endpoint_config:
        logger.error(f"❌ Unknown endpoint: {endpoint_key}")
        return 0

    logger.info(f"🔄 ATOMIC extraction for {endpoint_key}")
    
    # CRITICAL: Backup current state before ANY changes
    state_backup = None
    with _STATE_LOCK:
        state_backup = _STATE_CACHE.copy()
    
    logger.info(f"💾 State backed up for rollback safety")

    try:
        # Step 1: Extract data (no state changes)
        session = create_authenticated_session()
        raw_data = get_paginated_data(session, endpoint_config, endpoint_key)

        if not raw_data:
            logger.warning(f"⚠️ No data returned for {endpoint_key}")
            return 0

        # Step 2: Process data (no state changes)
        flattened = []
        for i, record in enumerate(raw_data):
            try:
                flattened.append(flatten_leaflink_record(record))
            except Exception as e:
                logger.warning(f"   Failed to flatten record {i}: {e}")
                continue

        if not flattened:
            logger.warning(f"⚠️ No valid records after flattening for {endpoint_key}")
            return 0

        # Step 3: Create DataFrame
        df = pd.DataFrame(flattened)
        extracted_at = _utc_now()
        df['_extracted_at'] = extracted_at.isoformat()
        df['_source_system'] = 'leaflink'
        df['_endpoint'] = endpoint_key

        logger.info(f"   📊 Prepared {len(df)} records for warehouse load")

        # Step 4: CRITICAL - Load to warehouse FIRST (before state updates)
        try:
            records_loaded = load_dataframe_to_warehouse_verified(df, endpoint_key, extracted_at)
            
            if records_loaded == 0:
                logger.warning(f"⚠️ No records actually loaded for {endpoint_key}")
                return 0
                
            logger.info(f"✅ VERIFIED: {records_loaded} records loaded to warehouse")
            
        except Exception as warehouse_error:
            logger.error(f"❌ WAREHOUSE LOAD FAILED: {warehouse_error}")
            logger.error("🔄 RESTORING original state (no changes made)")
            
            # Restore original state
            with _STATE_LOCK:
                _STATE_CACHE.clear()
                _STATE_CACHE.update(state_backup)
            
            raise  # Fail the task

        # Step 5: ONLY NOW update state after verified load
        try:
            update_state_after_verified_load(endpoint_key, endpoint_config, df, raw_data, extracted_at)
            logger.info("✅ State updated after verified warehouse load")
            
        except Exception as state_error:
            logger.error(f"❌ STATE UPDATE FAILED: {state_error}")
            # Data is loaded successfully, so don't fail pipeline
            logger.warning("⚠️ Data loaded but state may be inconsistent - next run will detect")
            
        return records_loaded

    except Exception as e:
        logger.error(f"❌ EXTRACTION FAILED for {endpoint_key}: {e}")
        
        # Restore original state on any failure
        logger.error("🔄 RESTORING original state due to extraction failure")
        with _STATE_LOCK:
            _STATE_CACHE.clear()
            _STATE_CACHE.update(state_backup)
        
        raise

# ---------------- SCHEDULING AND DEPENDENCIES ---------------- #
def should_run_endpoint(endpoint_key, execution_dt: datetime, completed_endpoints: set = None):
    """Check if endpoint should run based on schedule and dependencies."""
    if completed_endpoints is None:
        completed_endpoints = set()
    
    endpoint_config = LEAFLINK_ENDPOINTS.get(endpoint_key, {})
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

# ---------------- DAG INTEGRATION FUNCTIONS ---------------- #
def finalize_state_after_warehouse_load(context):
    """Finalize state updates after successful warehouse loading."""
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

def extract_all_endpoints_with_dependencies(**context):
    """Extract all endpoints respecting dependencies and atomic state management."""
    logger.info("🚀 Starting coordinated LeafLink extraction...")
    
    endpoint_order = get_endpoint_execution_order()
    total_records = 0
    extraction_results = {}
    completed_endpoints = set()
    
    for endpoint_key in endpoint_order:
        try:
            # Check dependencies
            endpoint_config = LEAFLINK_ENDPOINTS.get(endpoint_key, {})
            dependencies = endpoint_config.get('depends_on', [])
            
            # Wait for dependencies (in real implementation, this would be handled by DAG)
            for dep in dependencies:
                if dep not in completed_endpoints and dep in ENABLED_ENDPOINTS:
                    logger.warning(f"⚠️ {endpoint_key} dependency {dep} not completed")
            
            logger.info(f"📊 Extracting {endpoint_key}...")
            records = extract_leaflink_endpoint(endpoint_key, **context)
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
    logger.info("📈 LeafLink Extraction Summary:")
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