# extractor.py - Acumatica ERP API extractor with incremental logic (single schema)
# Path: root/extractors/acumatica/extractor.py

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
from urllib.parse import quote, urlencode

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# GLOBAL VARIABLES
# ============================================================================

CONFIG: Dict[str, Any] = {}
TESTING_MODE: bool = False
MAX_RECORDS_PER_ENDPOINT: Optional[int] = None
ENDPOINTS_CONFIG: Dict[str, Dict[str, Any]] = {}
ENABLED_ENDPOINTS: Dict[str, str] = {}

# Thread-safe state management
_STATE_CACHE: Dict[str, str] = {}
_STATE_LOCK = threading.Lock()
_STATE_DIR_CREATED = False

# Session management
_SESSION_CACHE = None
_SESSION_REQUEST_COUNT = 0
_SESSION_LAST_REFRESH = None

# ============================================================================
# ENDPOINT DEFINITIONS
# ============================================================================

ACUMATICA_ENDPOINTS = {
    # Core Financial (High Priority)
    'bill': {
        'entity': 'Bill',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'high',
        'required_fields': ['VendorID', 'Date', 'Status', 'CuryOrigDocAmt'],
        'business_critical': True
    },
    'vendor': {
        'entity': 'Vendor',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'high',
        'required_fields': ['VendorID', 'VendorName'],
        'business_critical': True
    },
    'customer': {
        'entity': 'Customer',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'high',
        'required_fields': ['CustomerID', 'CustomerName'],
        'business_critical': True
    },
    'business_account': {
        'entity': 'BusinessAccount',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'high',
        'required_fields': ['BusinessAccountID', 'BusinessAccountName'],
        'business_critical': True
    },
    'appointment': {
        'entity': 'Appointment',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'high',
        'required_fields': ['AppointmentID', 'StartDate'],
        'business_critical': True
    },
    'invoice': {
        'entity': 'Invoice',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'high',
        'required_fields': ['CustomerID', 'ReferenceNbr'],
        'business_critical': True,
        'depends_on': ['customer']
    },
    'purchase_order': {
        'entity': 'PurchaseOrder',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'high',
        'required_fields': ['OrderNbr', 'VendorID'],
        'business_critical': True
    },
    'sales_order': {
        'entity': 'SalesOrder',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'high',
        'required_fields': ['OrderNbr', 'CustomerID'],
        'business_critical': True
    },
    'warehouse': {
        'entity': 'Warehouse',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'high',
        'required_fields': ['WarehouseID'],
        'business_critical': True
    },
    'stock_item': {
        'entity': 'StockItem',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'high',
        'required_fields': ['InventoryID'],
        'business_critical': True
    },
    'sales_invoice': {
        'entity': 'SalesInvoice',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'high',
        'required_fields': ['ReferenceNbr'],
        'business_critical': True
    },
    'purchase_receipt': {
        'entity': 'PurchaseReceipt',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'high',
        'required_fields': ['ReceiptNbr'],
        'business_critical': True,
        'depends_on': ['purchase_order']
    },
    'shipment': {
        'entity': 'Shipment',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'high',
        'required_fields': ['ShipmentNbr'],
        'business_critical': True
    },
    
    # Inventory & Items (Medium Priority)
    'inventory_adjustment': {
        'entity': 'InventoryAdjustment',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'medium',
        'required_fields': ['ReferenceNbr'],
        'business_critical': False
    },
    'inventory_issue': {
        'entity': 'InventoryIssue',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'medium',
        'required_fields': ['ReferenceNbr'],
        'business_critical': False
    },
    'inventory_receipt': {
        'entity': 'InventoryReceipt',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'medium',
        'required_fields': ['ReferenceNbr'],
        'business_critical': False
    },
    'transfer_order': {
        'entity': 'TransferOrder',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'medium',
        'required_fields': ['ReferenceNbr'],
        'business_critical': False
    },
    'non_stock_item': {
        'entity': 'NonStockItem',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'medium',
        'required_fields': ['InventoryID'],
        'business_critical': False
    },
    'item_class': {
        'entity': 'ItemClass',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'medium',
        'required_fields': ['ClassID'],
        'business_critical': False
    },
    'customer_location': {
        'entity': 'CustomerLocation',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'medium',
        'required_fields': ['CustomerID', 'LocationID'],
        'business_critical': False,
        'depends_on': ['customer']
    },
    'item_warehouse': {
        'entity': 'ItemWarehouse',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'medium',
        'required_fields': ['InventoryID', 'WarehouseID'],
        'business_critical': False,
        'depends_on': ['stock_item', 'warehouse']
    },
    'kit_assembly': {
        'entity': 'KitAssembly',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'medium',
        'required_fields': ['ReferenceNbr'],
        'business_critical': False
    },
    'kit_specification': {
        'entity': 'KitSpecification',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'medium',
        'required_fields': ['KitInventoryID'],
        'business_critical': False
    },
    
    # CRM & Activities (Medium Priority)
    'contact': {
        'entity': 'Contact',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'medium',
        'required_fields': ['ContactID'],
        'business_critical': False,
        'depends_on': ['business_account']
    },
    'opportunity': {
        'entity': 'Opportunity',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'medium',
        'required_fields': ['OpportunityID'],
        'business_critical': False
    },
    'case': {
        'entity': 'Case',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'medium',
        'required_fields': ['CaseID'],
        'business_critical': False
    },
    'activity': {
        'entity': 'Activity',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'medium',
        'required_fields': ['NoteID'],
        'business_critical': False
    },
    'event': {
        'entity': 'Event',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'medium',
        'required_fields': ['NoteID'],
        'business_critical': False
    },
    'task': {
        'entity': 'Task',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'medium',
        'required_fields': ['NoteID'],
        'business_critical': False
    },
    'employee': {
        'entity': 'Employee',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'medium',
        'required_fields': ['EmployeeID'],
        'business_critical': False
    },
    
    # Supporting Data (Medium Priority)
    'discount': {
        'entity': 'Discount',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'medium',
        'required_fields': ['DiscountID'],
        'business_critical': False
    },
    'discount_code': {
        'entity': 'DiscountCode',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'medium',
        'required_fields': ['DiscountCodeID'],
        'business_critical': False
    },
    'salesperson': {
        'entity': 'Salesperson',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'medium',
        'required_fields': ['SalespersonID'],
        'business_critical': False
    },
    'service_order': {
        'entity': 'ServiceOrder',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'medium',
        'required_fields': ['ServiceOrderNbr'],
        'business_critical': False
    },
    'sales_price_worksheet': {
        'entity': 'SalesPriceWorksheet',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'medium',
        'required_fields': ['ReferenceNbr'],
        'business_critical': False
    },
    'physical_inventory_review': {
        'entity': 'PhysicalInventoryReview',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'medium',
        'required_fields': ['ReferenceNbr'],
        'business_critical': False
    },
    
    # Low Priority
    'lead': {
        'entity': 'Lead',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'low',
        'required_fields': ['LeadID'],
        'business_critical': False
    },
    'expense_claim': {
        'entity': 'ExpenseClaim',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'low',
        'required_fields': ['RefNbr'],
        'business_critical': False
    },
    'expense_receipt': {
        'entity': 'ExpenseReceipt',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'low',
        'required_fields': ['RefNbr'],
        'business_critical': False
    },
    'vendor_price_worksheet': {
        'entity': 'VendorPriceWorksheet',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'low',
        'required_fields': ['ReferenceNbr'],
        'business_critical': False
    },
    'attribute_definition': {
        'entity': 'AttributeDefinition',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'low',
        'required_fields': ['AttributeID'],
        'business_critical': False
    },
    'email': {
        'entity': 'Email',
        'incremental_field': 'LastModifiedDateTime',
        'priority': 'low',
        'required_fields': ['NoteID'],
        'business_critical': False
    }
}

# Add default fields to all endpoints
for endpoint_name, config in ACUMATICA_ENDPOINTS.items():
    config.setdefault('select_fields', ['*'])
    config.setdefault('expand_fields', [])

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def _utc_now():
    """Get current UTC datetime."""
    return datetime.utcnow().replace(tzinfo=timezone.utc)

def flatten_acumatica_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten Acumatica record structure with special handling for nested objects."""
    flattened = {}
    
    if isinstance(record, dict):
        for key, value in record.items():
            if isinstance(value, dict):
                # Special handling for common Acumatica nested structures
                if key in ['Amount', 'Balance', 'CuryAmount', 'CuryBalance'] and 'value' in value:
                    flattened[f"{key}_value"] = value.get('value')
                elif key.endswith('Address') and isinstance(value, dict):
                    for addr_key, addr_value in value.items():
                        flattened[f"{key}_{addr_key}"] = addr_value
                elif key.endswith('Contact') and isinstance(value, dict):
                    for contact_key, contact_value in value.items():
                        flattened[f"{key}_{contact_key}"] = contact_value
                else:
                    # Flatten all nested objects
                    for nested_key, nested_value in value.items():
                        flattened[f"{key}_{nested_key}"] = nested_value
            elif isinstance(value, list):
                if value:
                    flattened[f"{key}_json"] = json.dumps(value)
                    flattened[f"{key}_count"] = len(value)
                else:
                    flattened[f"{key}_json"] = None
                    flattened[f"{key}_count"] = 0
            else:
                flattened[key] = value
    else:
        flattened['value'] = record
    
    return flattened

def smart_rate_limit(response, config):
    """Smart rate limiting for Acumatica API."""
    if response.status_code == 429:
        wait_time = 60  # Default wait time for rate limiting
        logger.warning(f"⏸️ Rate Limit: Hit rate limit, waiting {wait_time}s")
        time.sleep(wait_time)
    else:
        time.sleep(1.0 / config['api']['rate_limiting']['requests_per_second'])

def _parse_acumatica_timestamp(date_str: str) -> Optional[datetime]:
    """Parse Acumatica timestamp format with timezone awareness."""
    if not date_str or date_str == '' or date_str is None:
        return None
    
    try:
        date_str = str(date_str).strip()
        
        # Handle various Acumatica timestamp formats
        if 'T' in date_str:
            if date_str.endswith('Z'):
                return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            elif '+' in date_str[-6:] or '-' in date_str[-6:]:
                return datetime.fromisoformat(date_str)
            else:
                return datetime.fromisoformat(date_str + '+00:00')
        
        return datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        
    except Exception as e:
        logger.warning(f"Failed to parse timestamp: {date_str} - {e}")
        return None

# ============================================================================
# AUTHENTICATION & SESSION MANAGEMENT
# ============================================================================

def create_authenticated_session():
    """Create authenticated session with Acumatica login and session management."""
    global _SESSION_CACHE, _SESSION_REQUEST_COUNT, _SESSION_LAST_REFRESH
    
    # Check if we need to refresh session
    max_requests = CONFIG['api']['session'].get('max_session_reuse', 50)
    session_timeout = CONFIG['api']['session'].get('session_timeout_minutes', 30)
    
    should_refresh = (
        _SESSION_CACHE is None or
        _SESSION_REQUEST_COUNT >= max_requests or
        (_SESSION_LAST_REFRESH and 
         (datetime.now() - _SESSION_LAST_REFRESH).total_seconds() > (session_timeout * 60))
    )
    
    if _SESSION_CACHE and not should_refresh:
        try:
            # Test if session is still valid with a simple request
            test_url = f"{CONFIG['api']['base_url']}/Bill"
            response = _SESSION_CACHE.get(test_url, timeout=10, params={'$top': 1})
            if response.status_code == 200:
                logger.info("🔄 Session: Reusing existing authenticated session")
                _SESSION_REQUEST_COUNT += 1
                return _SESSION_CACHE
        except Exception:
            logger.info("🔄 Session: Cached session invalid, creating new one")
            _SESSION_CACHE = None
    
    username = os.getenv('ACUMATICA_USERNAME')
    password = os.getenv('ACUMATICA_PASSWORD')
    
    if not username or not password:
        raise ValueError("Missing required environment variables: ACUMATICA_USERNAME, ACUMATICA_PASSWORD")
    
    # Create new session
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=CONFIG['api']['rate_limiting']['max_retries'],
        backoff_factor=CONFIG['api']['rate_limiting']['backoff_factor'],
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    
    # Configure connection pooling
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=5,
        pool_maxsize=10,
        pool_block=False
    )
    
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Set headers
    session.headers.update({
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    })
    
    # Perform login - single login, no branch switching needed
    login_url = CONFIG['api']['login_url']
    login_payload = {
        "name": username,
        "password": password,
        "company": "Mammoth Distribution",
        "branch": "MAIN",  # Default branch for authentication
        "locale": "en-US"
    }
    
    try:
        logger.info(f"🔐 Authentication: Logging into Acumatica as {username}")
        response = session.post(login_url, json=login_payload, timeout=CONFIG['api']['rate_limiting']['timeout_seconds'])
        response.raise_for_status()
        
        # Check if login was successful
        if response.status_code == 204 or response.status_code == 200:
            logger.info("✅ Authentication: Successfully authenticated with Acumatica")
            _SESSION_CACHE = session
            _SESSION_REQUEST_COUNT = 0
            _SESSION_LAST_REFRESH = datetime.now()
            return session
        else:
            raise ValueError(f"Login failed with status code: {response.status_code}")
            
    except Exception as e:
        logger.error(f"❌ Authentication: Failed to authenticate - {e}")
        raise

# ============================================================================
# STATE MANAGEMENT (CLICKHOUSE BACKEND)
# ============================================================================

def _get_state_client():
    """Minimal ClickHouse client for state I/O."""
    import clickhouse_connect
    host = os.getenv('CLICKHOUSE_HOST')
    port = int(os.getenv('CLICKHOUSE_PORT', 8443))
    database = os.getenv('CLICKHOUSE_DATABASE', 'default')
    username = os.getenv('CLICKHOUSE_USER', 'default')
    password = os.getenv('CLICKHOUSE_PASSWORD')

    if not all([host, password]):
        raise ValueError("ClickHouse connection parameters missing for state backend")

    return clickhouse_connect.get_client(
        host=host, port=port, username=username, password=password,
        database=database, secure=True
    )

def _ensure_state_dir():
    """Create metadata tables for state management."""
    global _STATE_DIR_CREATED
    if _STATE_DIR_CREATED:
        return

    client = _get_state_client()
    try:
        client.command("CREATE DATABASE IF NOT EXISTS meta")

        client.command("""
            CREATE TABLE IF NOT EXISTS meta.pipeline_state_latest (
                source_system String,
                state_key     String,
                state_value   String,
                updated_at    DateTime64(3) DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY (source_system, state_key)
        """)

        client.command("""
            CREATE TABLE IF NOT EXISTS meta.pipeline_state_history (
                source_system String,
                state_key     String,
                state_value   String,
                updated_at    DateTime64(3)
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(updated_at)
            ORDER BY (source_system, state_key, updated_at)
        """)

        logger.info("✅ metadata tables ready (meta.pipeline_state_latest & history)")
        _STATE_DIR_CREATED = True
    finally:
        client.close()

def _load_state():
    """Hydrate cache from ClickHouse (latest rows only)."""
    import json
    global _STATE_CACHE

    with _STATE_LOCK:
        try:
            _ensure_state_dir()
        except Exception as e:
            logger.warning(f"⚠️  ClickHouse unreachable – state starts empty ({e})")
            _STATE_CACHE = {}
            return

        src = CONFIG.get("extraction", {}).get("source_system", "acumatica")

        client = _get_state_client()
        try:
            rows = client.query(
                """
                SELECT state_key, anyLast(state_value)
                  FROM meta.pipeline_state_latest
                 WHERE source_system = %(src)s
                 GROUP BY state_key
                """,
                {"src": src}
            ).result_rows
        finally:
            client.close()

        _STATE_CACHE = {}
        for k, v in rows:
            try:
                _STATE_CACHE[k] = json.loads(v)
            except Exception:
                _STATE_CACHE[k] = v

        logger.info(f"📁 Loaded {len(_STATE_CACHE)} state rows for '{src}'")

def _save_state():
    """Write the in-memory cache to ClickHouse."""
    import json
    from datetime import datetime

    with _STATE_LOCK:
        _ensure_state_dir()
        if not _STATE_CACHE:
            logger.info("💾 Cache empty – nothing to persist")
            return

        src = CONFIG.get("extraction", {}).get("source_system", "acumatica")
        now = datetime.utcnow()
        rows = [(src, k, json.dumps(v), now) for k, v in _STATE_CACHE.items()]

        client = _get_state_client()
        try:
            client.insert("meta.pipeline_state_history", rows,
                          column_names=["source_system", "state_key", "state_value", "updated_at"])
            client.insert("meta.pipeline_state_latest", rows,
                          column_names=["source_system", "state_key", "state_value", "updated_at"])
            logger.info(f"💾 Persisted {len(rows)} state rows for '{src}'")
        finally:
            client.close()

def _get_last_watermark(endpoint_key: str) -> Optional[datetime]:
    """Get last watermark for endpoint - THREAD SAFE."""
    with _STATE_LOCK:
        iso = _STATE_CACHE.get(endpoint_key)
        if not iso:
            return None
        
        try:
            if isinstance(iso, str):
                if iso.endswith('Z'):
                    iso = iso.replace('Z', '+00:00')
                return datetime.fromisoformat(iso)
            else:
                return None
        except Exception as e:
            logger.warning(f"⚠️ State: Invalid watermark format for {endpoint_key}: {iso}")
            return None

def _update_watermark(endpoint_key: str, new_ts: datetime):
    """Update watermark for endpoint - THREAD SAFE, NOT SAVED YET."""
    with _STATE_LOCK:
        if new_ts.tzinfo is None:
            new_ts = new_ts.replace(tzinfo=timezone.utc)
        _STATE_CACHE[endpoint_key] = new_ts.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
        logger.info(f"🕒 Watermark: Updated in memory {endpoint_key} → {_STATE_CACHE[endpoint_key]}")

# ============================================================================
# INCREMENTAL EXTRACTION LOGIC
# ============================================================================

def _get_incremental_date_range(endpoint_key: str):
    """Get date range for incremental extraction with proper timezone handling."""
    endpoint_config = ACUMATICA_ENDPOINTS.get(endpoint_key, {})
    
    last_wm = _get_last_watermark(endpoint_key)
    end_date = _utc_now()
    
    if last_wm:
        if last_wm.tzinfo is None:
            last_wm = last_wm.replace(tzinfo=timezone.utc)
        
        lookback = CONFIG['extraction']['incremental'].get('lookback_minutes', 60)
        start_date = last_wm - timedelta(minutes=lookback)
        logger.info(f"🔁 Incremental: {endpoint_key} from {start_date.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    else:
        configured_start_date = CONFIG.get('dag', {}).get('start_date')
        if configured_start_date:
            try:
                if isinstance(configured_start_date, str):
                    start_date = datetime.strptime(configured_start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                else:
                    start_date = configured_start_date.replace(tzinfo=timezone.utc)
                logger.info(f"🗓️ First Run: Using configured start_date for {endpoint_key}: {start_date.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            except Exception as e:
                logger.warning(f"⚠️ Config: Failed to parse start_date '{configured_start_date}': {e}")
                if TESTING_MODE:
                    start_date = end_date - timedelta(days=CONFIG['extraction']['testing']['date_range_days'])
                else:
                    start_date = end_date - timedelta(days=CONFIG['extraction']['production']['date_range_days'])
                logger.info(f"🔄 Fallback: Using default range for first run of {endpoint_key}")
        else:
            if TESTING_MODE:
                start_date = end_date - timedelta(days=CONFIG['extraction']['testing']['date_range_days'])
            else:
                start_date = end_date - timedelta(days=CONFIG['extraction']['production']['date_range_days'])
            logger.info(f"🔄 First Run: Using default range for {endpoint_key} ({CONFIG['extraction']['testing' if TESTING_MODE else 'production']['date_range_days']} days)")
    
    return start_date, end_date

def update_state_after_verified_load(endpoint_key, endpoint_config, df, raw_data, extracted_at):
    """Update state ONLY after verified warehouse load."""
    try:
        logger.info(f"📝 State Update: Processing {endpoint_key}")
        
        incremental_field = endpoint_config.get('incremental_field')
        
        if incremental_field and not df.empty:
            # Look for the incremental field in the DataFrame
            if incremental_field in df.columns:
                timestamps = df[incremental_field].apply(_parse_acumatica_timestamp)
                valid_timestamps = [ts for ts in timestamps if ts is not None]
                
                if valid_timestamps:
                    max_ts = max(valid_timestamps)
                    _update_watermark(endpoint_key, max_ts)
                    logger.info(f"🕒 Watermark: Updated from data max timestamp: {max_ts.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                else:
                    _update_watermark(endpoint_key, extracted_at)
                    logger.info(f"🕒 Watermark: Fallback to extraction time: {extracted_at.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            else:
                logger.warning(f"⚠️ Watermark: Field '{incremental_field}' not found in {endpoint_key} data")
                _update_watermark(endpoint_key, extracted_at)
        else:
            _update_watermark(endpoint_key, extracted_at)
            logger.info(f"🕒 Watermark: Using extraction timestamp: {extracted_at.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        
        _save_state()
        logger.info("✅ State: Successfully saved after verified load")
        
    except Exception as e:
        logger.error(f"❌ State: Failed to update - {e}")
        logger.error("❌ CRITICAL: Next run may have data gaps!")
        raise

# ============================================================================
# WAREHOUSE LOADING FUNCTIONS
# ============================================================================

def table_exists(client, full_table):
    """Check if table exists in ClickHouse."""
    try:
        client.query(f"DESCRIBE TABLE {full_table}")
        return True
    except Exception:
        return False

def get_table_columns(client, full_table):
    """Get existing table columns."""
    try:
        result = client.query(f"DESCRIBE TABLE {full_table}")
        return {row[0] for row in result.result_rows}
    except Exception:
        return set()

def add_missing_columns(client, full_table, df, existing_columns):
    """Add missing columns to existing table to support schema evolution."""
    new_columns = set(df.columns) - existing_columns
    
    if not new_columns:
        return
    
    logger.info(f"🔧 Schema: Adding {len(new_columns)} new columns to {full_table}")
    for column in sorted(new_columns):
        logger.info(f"   ├─ Adding column: {column}")
    
    for column in sorted(new_columns):
        try:
            alter_sql = f"ALTER TABLE {full_table} ADD COLUMN `{column}` String"
            client.command(alter_sql)
            logger.info(f"✅ Schema: Added column `{column}` to {full_table}")
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.info(f"ℹ️ Schema: Column `{column}` already exists in {full_table}")
            else:
                logger.error(f"❌ Schema: Failed to add column `{column}` to {full_table}: {e}")
                raise

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
        logger.info(f"🆕 Table: Created partitioned table {full_table}")
        
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

def load_dataframe_to_warehouse_verified(df, endpoint_key, extracted_at, raw_schema):
    """Load DataFrame to ClickHouse with verification."""
    import clickhouse_connect
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
        if raw_schema != 'default':
            client.command(f"CREATE DATABASE IF NOT EXISTS `{raw_schema}`")
            logger.info(f"✅ Database: {raw_schema} ready")

        table = f"`{raw_schema}`.`raw_{endpoint_key}`"
        ts = extracted_at.isoformat()

        if table_exists(client, table):
            dup = client.query(f"SELECT count() FROM {table} WHERE _extracted_at = '{ts}'").result_rows[0][0]
            if dup:
                logger.warning(f"⚠️ Warehouse: Duplicates detected - skipping load for {table}")
                return 0
            add_missing_columns(client, table, df, get_table_columns(client, table))
        else:
            create_table_from_dataframe(client, table, df)

        df_clean = df.copy()
        df_clean.columns = [c.replace(' ', '_').replace('-', '_') for c in df_clean.columns]
        df_clean = df_clean.astype(str).replace(['nan', 'None', 'null', '<NA>'], '')
        client.insert_df(table=table, df=df_clean)

        rows = client.query(f"SELECT count() FROM {table} WHERE _extracted_at = '{ts}'").result_rows[0][0]
        if rows != len(df):
            raise ValueError(f"Warehouse: Load verification failed - expected {len(df)}, got {rows}")

        logger.info(f"✅ Warehouse: Loaded {rows} rows into {table}")
        return rows
    finally:
        client.close()

# ============================================================================
# DATA EXTRACTION FUNCTIONS
# ============================================================================

def get_paginated_data(session, endpoint_config, endpoint_name):
    """Fetch all pages from an Acumatica endpoint using OData pagination."""
    all_data = []
    base_url = CONFIG['api']['base_url']
    entity = endpoint_config['entity']
    max_page_size = CONFIG.get('acumatica_specific', {}).get('api_settings', {}).get('max_page_size', 1000)
    
    # Build base URL
    endpoint_url = f"{base_url}/{entity}"
    
    # Build query parameters manually to avoid URL encoding issues
    query_params = []
    
    # Add incremental date filtering
    start_date, end_date = _get_incremental_date_range(endpoint_name)
    if start_date and end_date and endpoint_config.get('incremental_field'):
        # Convert to PST timezone (Acumatica's timezone)
        pst_offset = timedelta(hours=7)  # PST is UTC-7 (adjust for PDT if needed)
        start_pst = (start_date - pst_offset).strftime('%Y-%m-%dT%H:%M:%S-07:00')
        end_pst = (end_date - pst_offset).strftime('%Y-%m-%dT%H:%M:%S-07:00')
        
        # Build filter string exactly like Postman - NO spaces around 'and'
        filter_str = f"LastModifiedDateTime ge datetimeoffset'{start_pst}'and LastModifiedDateTime lt datetimeoffset'{end_pst}'"
        query_params.append(f"$filter={filter_str}")
        
        logger.info(f"📅 OData Filter: {filter_str}")
    
    # Add select fields if specified
    if endpoint_config.get('select_fields') and endpoint_config['select_fields'] != ['*']:
        select_str = ','.join(endpoint_config['select_fields'])
        query_params.append(f"$select={select_str}")
    
    # Add expand fields if specified
    if endpoint_config.get('expand_fields'):
        expand_str = ','.join(endpoint_config['expand_fields'])
        query_params.append(f"$expand={expand_str}")
    
    logger.info(f"📡 API: {endpoint_url}")
    logger.info(f"🗄️  Schema: Single schema (all data)")
    
    page = 0
    max_pages = CONFIG.get('acumatica_specific', {}).get('api_settings', {}).get('max_pages_per_endpoint', 10000)
    
    while page < max_pages:
        page += 1
        skip_value = (page - 1) * max_page_size
        
        # Build complete URL manually like Postman
        current_params = query_params.copy()
        current_params.append(f"$top={max_page_size}")
        current_params.append(f"$skip={skip_value}")
        
        # Join all parameters with &
        query_string = "&".join(current_params)
        full_url = f"{endpoint_url}?{query_string}"
        
        logger.info(f"   📄 Page {page:2d} | Skip: {skip_value:5d} | Fetching...")
        logger.info(f"   🔗 Full URL: {full_url}")
        
        try:
            global _SESSION_REQUEST_COUNT
            
            # Use the manually built URL with no additional params
            response = session.get(full_url, timeout=CONFIG['api']['rate_limiting']['timeout_seconds'])
            response.raise_for_status()
            _SESSION_REQUEST_COUNT += 1
            
            data = response.json()
            
            # Handle different OData response formats
            if isinstance(data, dict):
                if 'value' in data:
                    records = data['value']
                elif 'd' in data and 'results' in data['d']:
                    records = data['d']['results']
                else:
                    records = [data]  # Single record response
            elif isinstance(data, list):
                records = data
            else:
                records = []
            
            if not records:
                logger.info(f"   📄 Page {page:2d} | No more records - pagination complete")
                break
            
            all_data.extend(records)
            logger.info(f"   📄 Page {page:2d} | Got: {len(records):3d} records | Total: {len(all_data):5d}")
            
            # Check if we got fewer records than requested (last page)
            if len(records) < max_page_size:
                logger.info(f"   📄 Page {page:2d} | Last page reached (got {len(records)} < {max_page_size})")
                break
            
            smart_rate_limit(response, CONFIG)
            
        except Exception as e:
            logger.warning(f"   📄 Page {page:2d} | API Error: {e}")
            if page == 1:
                raise
            break
    
    logger.info(f"✅ Extraction: Collected {len(all_data)} total records from {page} pages")
    return all_data

def extract_acumatica_endpoint(endpoint_key, raw_schema, **context):
    """Run a single endpoint extraction into the single schema."""
    logger.info("=" * 100)
    logger.info(f"🚀 ENDPOINT EXTRACTION START")
    logger.info("=" * 100)
    
    if endpoint_key not in ENABLED_ENDPOINTS:
        logger.info(f"⏭️ Skip: {endpoint_key} is disabled in configuration")
        logger.info("=" * 100)
        return 0

    endpoint_cfg = ACUMATICA_ENDPOINTS[endpoint_key]
    base_url = CONFIG['api']['base_url']
    entity = endpoint_cfg['entity']
    
    # Build actual URL that will be used
    actual_url = f"{base_url}/{entity}/"
    
    # Header information
    logger.info(f"📊 Endpoint: {endpoint_key}")
    logger.info(f"🗄️  Schema: {raw_schema} (single schema)")
    logger.info(f"🏷️  Entity: {entity}")
    logger.info(f"🔗 URL: {actual_url}")
    
    # Dependencies
    dependencies = endpoint_cfg.get('depends_on', [])
    if dependencies:
        logger.info(f"🔄 Dependencies: {', '.join(dependencies)}")
    
    logger.info("-" * 100)

    state_backup = _STATE_CACHE.copy()
    try:
        session = create_authenticated_session()
        raw = get_paginated_data(session, endpoint_cfg, endpoint_key)
        
        if not raw:
            logger.info("📭 No data returned from API")
            logger.info("=" * 100)
            return 0

        logger.info(f"🔄 Processing: Flattening {len(raw)} records...")
        flat = [flatten_acumatica_record(r) for r in raw]
        df = pd.DataFrame(flat)
        extracted_at = _utc_now()
        df['_extracted_at'] = extracted_at.isoformat()
        df['_source_system'] = 'acumatica'
        df['_endpoint'] = endpoint_key

        logger.info(f"📊 DataFrame: {len(df)} rows × {len(df.columns)} columns")
        logger.info(f"⏰ Extracted: {extracted_at.strftime('%Y-%m-%d %H:%M:%S')} UTC")

        loaded = load_dataframe_to_warehouse_verified(df, endpoint_key, extracted_at, raw_schema)
        if loaded:
            update_state_after_verified_load(endpoint_key, endpoint_cfg, df, raw, extracted_at)
            logger.info(f"✅ SUCCESS: {endpoint_key} completed - {loaded} records loaded")
        else:
            logger.warning(f"⚠️ WARNING: {endpoint_key} completed but no records loaded")
            
        logger.info("=" * 100)
        return loaded

    except Exception as e:
        _STATE_CACHE.clear()
        _STATE_CACHE.update(state_backup)
        logger.error(f"❌ FAILED: {endpoint_key} extraction failed")
        logger.error(f"❌ Error: {str(e)}")
        logger.info("=" * 100)
        raise

# ============================================================================
# DEPENDENCY MANAGEMENT & SCHEDULING
# ============================================================================

def should_run_endpoint(endpoint_key, execution_dt: datetime, completed_endpoints: set = None):
    """Check if endpoint should run based on dependencies."""
    if completed_endpoints is None:
        completed_endpoints = set()
    
    endpoint_config = ACUMATICA_ENDPOINTS.get(endpoint_key, {})
    dependencies = endpoint_config.get('depends_on', [])
    
    for dep in dependencies:
        if dep not in completed_endpoints:
            logger.info(f"⏳ Dependencies: {endpoint_key} waiting for {dep}")
            return False
    
    return True

def get_endpoint_execution_order():
    """Get endpoints in dependency order for Acumatica (single schema)."""
    endpoints = list(ENABLED_ENDPOINTS.keys())
    completed = set()
    ordered = []
    
    # Execution tiers based on business criticality and dependencies
    execution_tiers = [
        # Tier 1: Core master data (no dependencies)
        ['vendor', 'customer', 'business_account', 'employee', 'warehouse', 'stock_item'],
        
        # Tier 2: Core transactional data
        ['bill', 'appointment', 'purchase_order', 'sales_order'],
        
        # Tier 3: Extended transactional data
        ['invoice', 'sales_invoice', 'purchase_receipt', 'shipment'],
        
        # Tier 4: Inventory and items
        ['inventory_adjustment', 'inventory_issue', 'inventory_receipt', 'transfer_order', 'non_stock_item', 'item_class'],
        
        # Tier 5: Dependent data
        ['customer_location', 'item_warehouse', 'kit_assembly', 'kit_specification'],
        
        # Tier 6: CRM and activities
        ['contact', 'opportunity', 'lead', 'case', 'activity', 'event', 'task'],
        
        # Tier 7: Supporting data
        ['discount', 'discount_code', 'salesperson', 'service_order'],
        
        # Tier 8: Worksheets and admin
        ['sales_price_worksheet', 'vendor_price_worksheet', 'physical_inventory_review', 'attribute_definition'],
        
        # Tier 9: Communications and expenses
        ['email', 'expense_claim', 'expense_receipt']
    ]
    
    logger.info("📋 Execution Planning (Single Schema):")
    
    for tier_num, tier in enumerate(execution_tiers, 1):
        tier_endpoints = [ep for ep in tier if ep in endpoints]
        
        if tier_endpoints:
            tier_endpoints.sort(key=lambda ep: {
                'high': 0, 'medium': 1, 'low': 2
            }.get(ACUMATICA_ENDPOINTS.get(ep, {}).get('priority', 'medium'), 1))
            
            logger.info(f"   Tier {tier_num}:")
            for ep in tier_endpoints:
                endpoint_cfg = ACUMATICA_ENDPOINTS[ep]
                entity = endpoint_cfg['entity']
                priority = endpoint_cfg.get('priority', 'medium')
                business_critical = "🔥" if endpoint_cfg.get('business_critical', False) else "📄"
                
                logger.info(f"      ├─ {ep:<30} {business_critical} ({priority}) → {entity}")
            
            for endpoint in tier_endpoints:
                if should_run_endpoint(endpoint, None, completed):
                    ordered.append(endpoint)
                    completed.add(endpoint)
                    if endpoint in endpoints:
                        endpoints.remove(endpoint)
    
    # Handle remaining endpoints with dependencies
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
            logger.warning(f"⚠️ Dependencies: Adding remaining endpoints without dependency check: {endpoints}")
            ordered.extend(endpoints)
            break
    
    logger.info(f"✅ Final Order: {len(ordered)} endpoints planned")
    
    # Summary statistics
    financial_endpoints = [ep for ep in ordered if ep in ['bill', 'invoice', 'sales_invoice', 'purchase_order', 'sales_order', 'purchase_receipt']]
    master_data_endpoints = [ep for ep in ordered if ep in ['vendor', 'customer', 'business_account', 'employee', 'warehouse']]
    inventory_endpoints = [ep for ep in ordered if ep in ['stock_item', 'non_stock_item', 'inventory_adjustment', 'inventory_issue', 'inventory_receipt', 'transfer_order']]
    crm_endpoints = [ep for ep in ordered if ep in ['contact', 'opportunity', 'lead', 'case', 'appointment', 'activity', 'event', 'task']]
    
    logger.info(f"   💰 Financial: {len(financial_endpoints)} endpoints")
    logger.info(f"   👥 Master Data: {len(master_data_endpoints)} endpoints") 
    logger.info(f"   📦 Inventory: {len(inventory_endpoints)} endpoints")
    logger.info(f"   🤝 CRM: {len(crm_endpoints)} endpoints")
    logger.info("   📝 Legend: 🔥 = Business Critical, 📄 = Standard")
    
    return ordered

# ============================================================================
# MAIN PIPELINE FUNCTIONS
# ============================================================================

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
        k: v['entity']
        for k, v in ACUMATICA_ENDPOINTS.items()
        if k in all_allowed and k not in disabled
    }
    
    _load_state()
    logger.info("=" * 80)
    logger.info("🚀 ACUMATICA EXTRACTOR INITIALIZATION (SINGLE SCHEMA)")
    logger.info("=" * 80)
    logger.info(f"📋 Enabled Endpoints: {len(ENABLED_ENDPOINTS)} endpoints")
    logger.info(f"🗄️  Target Schema: {config['warehouse']['schemas']['raw_schema']}")
    logger.info(f"📊 Mode: {'TESTING' if TESTING_MODE else 'PRODUCTION'}")
    
    for endpoint in sorted(ENABLED_ENDPOINTS.keys()):
        endpoint_cfg = ACUMATICA_ENDPOINTS[endpoint]
        entity = endpoint_cfg['entity']
        priority = endpoint_cfg.get('priority', 'medium')
        business_critical = "🔥" if endpoint_cfg.get('business_critical', False) else "📄"
        
        logger.info(f"   ├─ {endpoint:<30} {business_critical} ({priority}) → {entity}")
    
    logger.info("=" * 80)
    return ENABLED_ENDPOINTS

def extract_all_endpoints_with_dependencies(raw_schema, **context):
    """Runs all enabled endpoints into the single schema, respecting dependencies."""
    logger.info("=" * 100)
    logger.info("🚀 MULTI-ENDPOINT EXTRACTION START (SINGLE SCHEMA)")
    logger.info("=" * 100)
    logger.info(f"🗄️  Schema: {raw_schema}")
    logger.info(f"📊 Mode: {'TESTING' if TESTING_MODE else 'PRODUCTION'}")
    logger.info(f"🌐 Strategy: Single schema for all data (no branch filtering)")
    
    order = get_endpoint_execution_order()
    total, results, done = 0, {}, set()
    
    logger.info("=" * 100)

    for i, ep in enumerate(order, 1):
        logger.info(f"🔄 Progress: [{i:2d}/{len(order)}] Processing {ep}")
        
        try:
            recs = extract_acumatica_endpoint(ep, raw_schema, **context)
            total += recs
            results[ep] = recs
            done.add(ep)
            logger.info(f"✅ Completed: {ep} → {recs} records")
        except Exception as e:
            results[ep] = f"Failed: {e}"
            logger.error(f"❌ Failed: {ep} → {str(e)}")

    logger.info("=" * 100)
    logger.info("🎉 MULTI-ENDPOINT EXTRACTION COMPLETE")
    logger.info("=" * 100)
    logger.info(f"🗄️  Schema: {raw_schema}")
    logger.info(f"📊 Total Records: {total:,}")
    logger.info(f"✅ Successful: {len(done)}/{len(order)} endpoints")
    logger.info(f"❌ Failed: {len(order) - len(done)}/{len(order)} endpoints")
    
    # Summary by category
    successful = [ep for ep, result in results.items() if isinstance(result, int)]
    failed = [ep for ep, result in results.items() if isinstance(result, str)]
    
    if successful:
        logger.info("✅ Successful Endpoints:")
        for ep in successful:
            endpoint_cfg = ACUMATICA_ENDPOINTS[ep]
            priority = endpoint_cfg.get('priority', 'medium')
            business_critical = "🔥" if endpoint_cfg.get('business_critical', False) else "📄"
            logger.info(f"   ├─ {ep:<30} {business_critical} ({priority}) → {results[ep]:>6,} records")
    
    if failed:
        logger.info("❌ Failed Endpoints:")
        for ep in failed:
            endpoint_cfg = ACUMATICA_ENDPOINTS[ep]
            priority = endpoint_cfg.get('priority', 'medium')
            business_critical = "🔥" if endpoint_cfg.get('business_critical', False) else "📄"
            logger.info(f"   ├─ {ep:<30} {business_critical} ({priority}) → {results[ep]}")
    
    logger.info("=" * 100)
    
    return {
        'total_records': total,
        'extraction_results': results,
        'completed_endpoints': list(done)
    }

def finalize_state_after_warehouse_load(context):
    """Simple end-of-DAG sanity check for ClickHouse-backed state."""
    try:
        _ensure_state_dir()
        client = _get_state_client()
        total = client.query(
            "SELECT count() FROM meta.pipeline_state_latest"
        ).result_rows[0][0]
        client.close()
        logger.info(f"✅ State table integrity verified – {total} keys stored")
    except Exception as e:
        logger.error(f"❌ State integrity check failed: {e}")