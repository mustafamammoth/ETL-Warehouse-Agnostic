# extractor.py - Acumatica extraction logic (incremental enabled, fixed)
# Path: root/extractors/acumatica/extractor.py

from datetime import datetime, timedelta, timezone
import os
import time
import json
import requests
import pandas as pd
from typing import Dict, Any, List, Optional
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------- GLOBALS (initialized via init_extractor) ---------------- #
CONFIG: Dict[str, Any] = {}
TESTING_MODE: bool = False
MAX_RECORDS_PER_ENDPOINT: Optional[int] = None
ENDPOINTS_CONFIG: Dict[str, Dict[str, Any]] = {}
ENABLED_ENDPOINTS: Dict[str, str] = {}

# ---------------- STATE (watermarks) ---------------- #
_STATE_CACHE: Dict[str, str] = {}
_STATE_DIR_CREATED = False

def _utc_now():
    return datetime.utcnow().replace(tzinfo=timezone.utc)

# ---------------- INIT ---------------- #
def init_extractor(config):
    """Initialize extractor module with loaded YAML config."""
    global CONFIG, TESTING_MODE, MAX_RECORDS_PER_ENDPOINT, ENDPOINTS_CONFIG, ENABLED_ENDPOINTS
    CONFIG = config
    ENDPOINTS_CONFIG = config['extraction']['endpoints']

    TESTING_MODE = config['extraction']['mode'] == 'testing'
    if TESTING_MODE:
        MAX_RECORDS_PER_ENDPOINT = config['extraction']['testing']['max_records_per_endpoint']
    else:
        MAX_RECORDS_PER_ENDPOINT = config['extraction']['production']['max_records_per_endpoint']

    ENABLED_ENDPOINTS = {
        k: v_cfg['path']
        for k, v_cfg in ENDPOINTS_CONFIG.items()
        if v_cfg.get('enabled', False)
    }
    _load_state()
    logger.info(f"✅ Extractor initialized. Enabled endpoints: {list(ENABLED_ENDPOINTS.keys())}")
    return ENABLED_ENDPOINTS

# ---------------- AUTH ---------------- #
def create_authenticated_session():
    """Create authenticated session for Acumatica."""
    base_url = os.getenv('ACUMATICA_BASE_URL')
    username = os.getenv('ACUMATICA_USERNAME')
    password = os.getenv('ACUMATICA_PASSWORD')

    if not all([base_url, username, password]):
        missing = []
        if not base_url: missing.append('ACUMATICA_BASE_URL')
        if not username: missing.append('ACUMATICA_USERNAME')
        if not password: missing.append('ACUMATICA_PASSWORD')
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    session = requests.Session()
    login_url = f"{base_url.rstrip('/')}/{CONFIG['api']['auth_endpoint']}"
    login_data = {"name": username, "password": password}
    
    try:
        resp = session.post(
            login_url, 
            json=login_data, 
            timeout=CONFIG['api']['rate_limiting']['timeout_seconds']
        )
        resp.raise_for_status()
        logger.info("✅ Authentication successful")
        return session
    except Exception as e:
        logger.error(f"❌ Authentication failed: {e}")
        raise

# ---------------- UTILITIES ---------------- #
def flatten_acumatica_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten Acumatica's nested {'value': 'data'} structure."""
    flattened = {}
    for key, value in record.items():
        if isinstance(value, dict) and 'value' in value and len(value) == 1:
            flattened[key] = value['value']
        elif isinstance(value, dict) and not value:
            flattened[key] = None
        elif isinstance(value, list):
            # Handle arrays - convert to JSON string
            flattened[key] = json.dumps(value) if value else None
        else:
            flattened[key] = value
    return flattened

def _get_endpoint_custom(endpoint_key):
    """Get custom configuration for endpoint."""
    cfg = ENDPOINTS_CONFIG.get(endpoint_key, {})
    return cfg.get('custom', {}) or {}

# ---------------- INCREMENTAL STATE ---------------- #
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
    """Load watermark state JSON (endpoint -> ISO timestamp)."""
    global _STATE_CACHE
    _ensure_state_dir()
    path = _state_path()
    if not os.path.exists(path):
        _STATE_CACHE = {}
        logger.info("📁 No existing state file found, starting fresh")
        return
    
    try:
        with open(path, 'r') as f:
            _STATE_CACHE = json.load(f)
        logger.info(f"📁 Loaded state from {path}")
    except Exception as e:
        logger.warning(f"⚠️ Failed to load state file {path}: {e}")
        _STATE_CACHE = {}

def _save_state():
    """Save watermark state to file."""
    _ensure_state_dir()
    path = _state_path()
    try:
        with open(path, 'w') as f:
            json.dump(_STATE_CACHE, f, indent=2, sort_keys=True)
        logger.info(f"💾 Saved watermark state -> {path}")
    except Exception as e:
        logger.error(f"❌ Failed to save state file {path}: {e}")
        raise

def _get_last_watermark(endpoint_key: str) -> Optional[datetime]:
    """Get last watermark for endpoint."""
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
    """Update watermark for endpoint."""
    if new_ts.tzinfo is None:
        new_ts = new_ts.replace(tzinfo=timezone.utc)
    # Always store in UTC ISO format with Z
    _STATE_CACHE[endpoint_key] = new_ts.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')

# ---------------- INCREMENTAL HELPERS ---------------- #
def _parse_timestamp_column(df: pd.DataFrame, timestamp_field: str) -> pd.Series:
    """Parse timestamp column with multiple format attempts."""
    if timestamp_field not in df.columns:
        return pd.Series([None] * len(df))
    
    def safe_parse_datetime(x):
        if pd.isna(x) or x == '' or x is None:
            return None
        try:
            # Try pandas datetime parsing first
            result = pd.to_datetime(x, utc=True)
            return result.to_pydatetime()
        except:
            try:
                # Manual parsing for common formats
                x_str = str(x).strip()
                if 'T' in x_str:
                    if x_str.endswith('Z'):
                        x_str = x_str[:-1] + '+00:00'
                    return datetime.fromisoformat(x_str)
                else:
                    # Date only format
                    return datetime.strptime(x_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            except:
                logger.warning(f"Failed to parse timestamp: {x}")
                return None
    
    return df[timestamp_field].apply(safe_parse_datetime)

def _build_incremental_filter(endpoint_key: str):
    """Build incremental filter parameters."""
    inc_cfg = CONFIG['extraction']['incremental']
    if not inc_cfg.get('enabled', False):
        logger.info(f"🔄 Incremental disabled for {endpoint_key}, performing full extract")
        return {}, None

    custom = _get_endpoint_custom(endpoint_key)
    # Get timestamp field (custom override or default)
    timestamp_field = custom.get('timestamp_field') or inc_cfg.get('default_timestamp_field')
    if not timestamp_field:
        logger.info(f"🔄 No timestamp field configured for {endpoint_key}, performing full extract")
        return {}, None

    last_wm = _get_last_watermark(endpoint_key)
    if last_wm:
        lookback = inc_cfg.get('lookback_minutes', 5)
        from_ts = last_wm - timedelta(minutes=lookback)
        logger.info(f"🔁 Incremental extract for {endpoint_key} from {from_ts.isoformat()}")
    else:
        logger.info(f"🔄 First run for {endpoint_key}, performing full extract")
        return {}, timestamp_field

    # Build OData filter
    from_iso = from_ts.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    odata_filter = f"{timestamp_field} ge {from_iso}"
    logger.info(f"🔁 Filter: {odata_filter}")
    return {'$filter': odata_filter}, timestamp_field

# ---------------- PAGINATION + FETCH ---------------- #
def get_paginated_data(session, endpoint_url, endpoint_key, extra_params=None):
    """Fetch paginated data with retry logic."""
    all_data: List[Dict[str, Any]] = []
    skip = 0
    page_size = CONFIG['api']['pagination']['default_page_size']
    custom = _get_endpoint_custom(endpoint_key)
    max_pages = custom.get('max_pages', 1000)
    page_count = 0
    max_retries = CONFIG['api']['rate_limiting']['max_retries']
    backoff_factor = CONFIG['api']['rate_limiting']['backoff_factor']
    
    while page_count < max_pages:
        page_count += 1

        # Testing mode record cap
        if TESTING_MODE and MAX_RECORDS_PER_ENDPOINT and len(all_data) >= MAX_RECORDS_PER_ENDPOINT:
            logger.info(f"   Testing mode: reached max records limit ({MAX_RECORDS_PER_ENDPOINT})")
            break

        if TESTING_MODE and MAX_RECORDS_PER_ENDPOINT:
            current_page_size = min(page_size, MAX_RECORDS_PER_ENDPOINT - len(all_data))
        else:
            current_page_size = page_size

        params = {'$top': current_page_size, '$skip': skip}
        if extra_params:
            params.update(extra_params)

        logger.info(f"   Page {page_count}: Fetching {current_page_size} records (skip={skip})")

        # Retry logic
        for retry in range(max_retries + 1):
            try:
                response = session.get(
                    endpoint_url,
                    params=params,
                    timeout=CONFIG['api']['rate_limiting']['timeout_seconds']
                )
                response.raise_for_status()
                data = response.json()

                # Handle different response formats
                if isinstance(data, list):
                    records = data
                elif isinstance(data, dict) and 'value' in data:
                    records = data['value']
                elif isinstance(data, dict) and data:
                    records = [data]
                else:
                    records = []

                if not records:
                    logger.info("   No more records found")
                    return all_data

                all_data.extend(records)
                logger.info(f"   Retrieved {len(records)} records (cumulative: {len(all_data)})")

                # Break pagination if we got fewer records than requested
                if len(records) < current_page_size:
                    logger.info("   Received fewer records than requested, pagination complete")
                    return all_data

                break  # Success, exit retry loop

            except Exception as e:
                if retry < max_retries:
                    wait_time = backoff_factor ** retry
                    logger.warning(f"   Retry {retry + 1}/{max_retries} after {wait_time}s: {e}")
                    time.sleep(wait_time)
                else:
                    logger.error(f"   Failed after {max_retries} retries: {e}")
                    if page_count == 1:
                        raise  # Fail completely if first page fails
                    else:
                        return all_data  # Return partial data if later pages fail

        skip += current_page_size
        # Rate limiting
        time.sleep(1.0 / CONFIG['api']['rate_limiting']['requests_per_second'])

    # Apply testing limit if needed
    if TESTING_MODE and MAX_RECORDS_PER_ENDPOINT:
        all_data = all_data[:MAX_RECORDS_PER_ENDPOINT]
        logger.info(f"   Applied testing limit: {len(all_data)} records")

    logger.info(f"✅ {endpoint_key}: Collected {len(all_data)} records total")
    return all_data

# ---------------- SCHEDULING ---------------- #
def should_run_endpoint(endpoint_key, execution_dt: datetime):
    """Check if endpoint should run based on schedule."""
    cfg = ENDPOINTS_CONFIG.get(endpoint_key, {})
    sched = cfg.get('schedule', {})
    times = sched.get('times')
    
    if not times:
        return True
    
    # Check if current hour:minute matches any scheduled time
    current_time = execution_dt.strftime("%H:%M")
    if current_time in times:
        logger.info(f"⏱ Endpoint {endpoint_key} scheduled for {current_time} -> run")
        return True
    
    logger.info(f"⏭ Endpoint {endpoint_key} not scheduled for {current_time} -> skip")
    return False

# ---------------- EXTRACTION ENTRYPOINT ---------------- #
def extract_acumatica_endpoint(endpoint_key, **context):
    """Main extraction function for an endpoint."""
    if endpoint_key not in ENABLED_ENDPOINTS:
        logger.warning(f"⚠️ Endpoint {endpoint_key} not enabled")
        return 0

    execution_dt = context.get('execution_date') or _utc_now()
    if not should_run_endpoint(endpoint_key, execution_dt):
        return 0

    base_url = os.getenv('ACUMATICA_BASE_URL', '').rstrip('/')
    api_path = CONFIG['api']['entity_endpoint']
    version = CONFIG['api']['default_version']
    endpoint_path = ENABLED_ENDPOINTS[endpoint_key]

    logger.info(f"🔄 Extracting {endpoint_key} ({endpoint_path})")
    logger.info(f"   Incremental: {CONFIG['extraction']['incremental']['enabled']}")

    try:
        session = create_authenticated_session()
        # endpoint_url = f"{base_url}/{api_path}/{version}/{endpoint_path}"
        endpoint_url = f"{base_url}/{endpoint_path}"
        logger.info(f"   URL: {endpoint_url}")

        # Build incremental filter
        params, ts_field = _build_incremental_filter(endpoint_key)
        
        # Fetch data
        raw_data = get_paginated_data(session, endpoint_url, endpoint_key, extra_params=params)

        if not raw_data:
            logger.warning(f"⚠️ No data returned for {endpoint_key}")
            return 0

        # Flatten nested structures
        logger.info(f"   Flattening {len(raw_data)} records...")
        flattened = []
        for i, record in enumerate(raw_data):
            try:
                flattened.append(flatten_acumatica_record(record))
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
        df['_source_system'] = 'acumatica'
        df['_endpoint'] = endpoint_key

        # Save to CSV
        raw_dir = CONFIG['extraction']['paths']['raw_data_directory']
        os.makedirs(raw_dir, exist_ok=True)
        filename = f"{raw_dir}/{endpoint_key}.csv"

        # Handle incremental vs full refresh
        inc_cfg = CONFIG['extraction']['incremental']
        if inc_cfg.get('enabled', False) and os.path.exists(filename):
            # Incremental: append to existing file
            try:
                existing = pd.read_csv(filename)
                combined = pd.concat([existing, df], ignore_index=True)
                combined.to_csv(filename, index=False)
                logger.info(f"✅ {endpoint_key}: Appended {len(df)} records (total: {len(combined)}) -> {filename}")
            except Exception as e:
                logger.error(f"❌ Failed to append to existing file: {e}")
                # Fallback: overwrite
                df.to_csv(filename, index=False)
                logger.info(f"✅ {endpoint_key}: Overwrote file with {len(df)} records -> {filename}")
        else:
            # Full refresh: overwrite file
            df.to_csv(filename, index=False)
            logger.info(f"✅ {endpoint_key}: Wrote {len(df)} records -> {filename}")

        # Update watermark if timestamp field exists
        if ts_field and inc_cfg.get('enabled', False):
            try:
                timestamps = _parse_timestamp_column(df, ts_field)
                valid_timestamps = [ts for ts in timestamps if ts is not None]
                
                if valid_timestamps:
                    max_ts = max(valid_timestamps)
                    _update_watermark(endpoint_key, max_ts)
                    _save_state()
                    logger.info(f"🕒 Updated watermark for {endpoint_key} -> {max_ts.isoformat()}")
                else:
                    logger.warning(f"⚠️ No valid timestamps found in {ts_field} column")
            except Exception as e:
                logger.error(f"❌ Failed to update watermark: {e}")

        logger.info(f"   Columns: {list(df.columns)[:10]}{'...' if len(df.columns) > 10 else ''}")
        return len(df)

    except Exception as e:
        logger.error(f"❌ Failed to extract {endpoint_key}: {e}")
        raise