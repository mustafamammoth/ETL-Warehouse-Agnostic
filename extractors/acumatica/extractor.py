# extractor.py - Acumatica extraction logic (incremental enabled)
# Path: root/extractors/acumatica/extractor.py

from datetime import datetime, timedelta, timezone
import os
import time
import json
import requests
import pandas as pd
from typing import Dict, Any, List, Optional

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
    """
    Initialize extractor module with loaded YAML config.
    """
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
    login_url = f"{base_url}/{CONFIG['api']['auth_endpoint']}"
    login_data = {"name": username, "password": password}
    resp = session.post(login_url, json=login_data, timeout=CONFIG['api']['rate_limiting']['timeout_seconds'])
    resp.raise_for_status()
    print("✅ Authentication successful")
    return session


# ---------------- UTILITIES ---------------- #
def flatten_acumatica_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten Acumatica's nested {'value': 'data'} structure."""
    flattened = {}
    for key, value in record.items():
        if isinstance(value, dict) and 'value' in value and len(value) == 1:
            flattened[key] = value['value']
        elif isinstance(value, dict) and not value:
            flattened[key] = None
        else:
            flattened[key] = value
    return flattened


def _get_endpoint_custom(endpoint_key):
    cfg = ENDPOINTS_CONFIG.get(endpoint_key, {})
    return cfg.get('custom', {}) or {}


# ---------------- INCREMENTAL STATE ---------------- #
def _state_path():
    return CONFIG['extraction']['incremental']['state_path']


def _ensure_state_dir():
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
        return
    try:
        with open(path, 'r') as f:
            _STATE_CACHE = json.load(f)
    except Exception as e:
        print(f"⚠️ Failed to load state file {path}: {e}")
        _STATE_CACHE = {}


def _save_state():
    path = _state_path()
    try:
        with open(path, 'w') as f:
            json.dump(_STATE_CACHE, f, indent=2, sort_keys=True)
        print(f"💾 Saved watermark state -> {path}")
    except Exception as e:
        print(f"⚠️ Failed to save state file {path}: {e}")


def _get_last_watermark(endpoint_key: str) -> Optional[datetime]:
    iso = _STATE_CACHE.get(endpoint_key)
    if not iso:
        return None
    try:
        return datetime.fromisoformat(iso.replace('Z', '+00:00'))
    except Exception:
        return None


def _update_watermark(endpoint_key: str, new_ts: datetime):
    # Always store in UTC ISO format with Z
    _STATE_CACHE[endpoint_key] = new_ts.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')


# ---------------- PAGINATION + FETCH ---------------- #
def get_paginated_data(session, endpoint_url, endpoint_key, extra_params=None):
    """
    Fetch paginated data. Supports incremental by injecting $filter.
    """
    all_data: List[Dict[str, Any]] = []
    skip = 0
    page_size = CONFIG['api']['pagination']['default_page_size']
    custom = _get_endpoint_custom(endpoint_key)
    max_pages = custom.get('max_pages', 1000)
    page_count = 0

    while page_count < max_pages:
        page_count += 1

        # Testing mode record cap
        if TESTING_MODE and MAX_RECORDS_PER_ENDPOINT and len(all_data) >= MAX_RECORDS_PER_ENDPOINT:
            break

        if TESTING_MODE and MAX_RECORDS_PER_ENDPOINT:
            current_page_size = min(page_size, MAX_RECORDS_PER_ENDPOINT - len(all_data))
        else:
            current_page_size = page_size

        params = {'$top': current_page_size, '$skip': skip}
        if extra_params:
            params.update(extra_params)

        print(f"   Page {page_count}: Fetching {current_page_size} (skip={skip}) params={params}")

        try:
            response = session.get(
                endpoint_url,
                params=params,
                timeout=CONFIG['api']['rate_limiting']['timeout_seconds']
            )
            response.raise_for_status()
            data = response.json()

            if isinstance(data, list):
                records = data
            elif isinstance(data, dict) and 'value' in data:
                records = data['value']
            else:
                records = [data] if data else []

            if not records:
                print("   No more records.")
                break

            all_data.extend(records)
            print(f"   Retrieved {len(records)} (cumulative {len(all_data)})")

            if len(records) < current_page_size:
                break

            skip += current_page_size
            time.sleep(1.0 / CONFIG['api']['rate_limiting']['requests_per_second'])
        except Exception as e:
            print(f"   Error on page {page_count}: {e}")
            if page_count == 1:
                raise
            else:
                break

    if TESTING_MODE and MAX_RECORDS_PER_ENDPOINT:
        all_data = all_data[:MAX_RECORDS_PER_ENDPOINT]

    print(f"✅ {endpoint_key}: Collected {len(all_data)} records (post-pagination)")
    return all_data


# ---------------- SCHEDULING ---------------- #
def should_run_endpoint(endpoint_key, execution_dt: datetime):
    cfg = ENDPOINTS_CONFIG.get(endpoint_key, {})
    sched = cfg.get('schedule', {})
    times = sched.get('times')
    if not times:
        return True
    hhmm = execution_dt.strftime("%H:%M")
    if hhmm in times:
        print(f"⏱ Endpoint {endpoint_key} scheduled for {hhmm} -> run.")
        return True
    print(f"⏭ Endpoint {endpoint_key} not scheduled for {hhmm} -> skip.")
    return False


# ---------------- INCREMENTAL HELPERS ---------------- #
def _build_incremental_filter(endpoint_key: str):
    """
    Returns (params_dict, timestamp_field) if incremental active, else ({}, field or None)
    """
    inc_cfg = CONFIG['extraction']['incremental']
    if not inc_cfg.get('enabled', False):
        return {}, None

    custom = _get_endpoint_custom(endpoint_key)
    # Allow per-endpoint override
    timestamp_field = custom.get('timestamp_field') or inc_cfg.get('default_timestamp_field')
    if not timestamp_field:
        return {}, None

    last_wm = _get_last_watermark(endpoint_key)
    if last_wm:
        lookback = inc_cfg.get('lookback_minutes', 5)
        from_ts = last_wm - timedelta(minutes=lookback)
    else:
        # No watermark yet => full (no filter) OR (optional) start_date override
        # If you want to force a full pull first time, return {}.
        # If you want to cap, put a default start.
        from_ts = None

    if from_ts is None:
        print(f"🔄 First run (no watermark) for {endpoint_key}: performing full extract.")
        return {}, timestamp_field

    # Acumatica OData style filter (adjust if needed):
    # Example: $filter=LastModifiedDateTime ge 2025-07-17T12:00:00Z
    from_iso = from_ts.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    odata_filter = f"{timestamp_field} ge {from_iso}"
    print(f"🔁 Incremental filter for {endpoint_key}: {odata_filter}")
    return {'$filter': odata_filter}, timestamp_field


# ---------------- EXTRACTION ENTRYPOINT ---------------- #
def extract_acumatica_endpoint(endpoint_key, **context):
    if endpoint_key not in ENABLED_ENDPOINTS:
        print(f"⚠️ Endpoint {endpoint_key} disabled.")
        return 0

    execution_dt = context.get('execution_date') or _utc_now()
    if not should_run_endpoint(endpoint_key, execution_dt):
        return 0

    base_url = os.getenv('ACUMATICA_BASE_URL')
    api_path = CONFIG['api']['entity_endpoint']
    version = CONFIG['api']['default_version']
    endpoint_path = ENABLED_ENDPOINTS[endpoint_key]

    print(f"🔄 Extracting {endpoint_key} ({endpoint_path}) incremental={CONFIG['extraction']['incremental']['enabled']}")

    try:
        session = create_authenticated_session()
        endpoint_url = f"{base_url}/{api_path}/{version}/{endpoint_path}"
        print(f"   URL: {endpoint_url}")

        params, ts_field = _build_incremental_filter(endpoint_key)
        raw_data = get_paginated_data(session, endpoint_url, endpoint_key, extra_params=params)

        if not raw_data:
            print(f"⚠️ No data for {endpoint_key} (possibly no changes).")
            return 0

        print(f"   Flattening {len(raw_data)} records...")
        flattened = [flatten_acumatica_record(r) for r in raw_data]
        df = pd.DataFrame(flattened)

        # Add lineage columns
        extracted_at = _utc_now()
        df['_extracted_at'] = extracted_at
        df['_source_system'] = 'acumatica'
        df['_endpoint'] = endpoint_key

        raw_dir = CONFIG['extraction']['paths']['raw_data_directory']
        os.makedirs(raw_dir, exist_ok=True)
        filename = f"{raw_dir}/{endpoint_key}.csv"

        # Append (create if missing)
        if os.path.exists(filename):
            existing = pd.read_csv(filename)
            combined = pd.concat([existing, df], ignore_index=True)
            combined.to_csv(filename, index=False)
            print(f"✅ {endpoint_key}: Appended {len(df)} (total file rows now {len(combined)}) -> {filename}")
        else:
            df.to_csv(filename, index=False)
            print(f"✅ {endpoint_key}: Wrote {len(df)} rows -> {filename}")

        # Update watermark (if timestamp field present)
        if ts_field and ts_field in df.columns:
            # Robust parse - attempt multiple formats
            def parse_ts(x):
                if pd.isna(x):
                    return None
                try:
                    return pd.to_datetime(x, utc=True)
                except Exception:
                    return None
            parsed = df[ts_field].apply(parse_ts)
            max_ts = parsed.max()
            if pd.notna(max_ts):
                _update_watermark(endpoint_key, max_ts.to_pydatetime())
                _save_state()
                print(f"🕒 Updated watermark for {endpoint_key} -> {max_ts.isoformat()}")

        print(f"   Sample columns: {list(df.columns)[:10]}")
        return len(df)
    except Exception as e:
        print(f"❌ Failed to extract {endpoint_key}: {e}")
        raise
