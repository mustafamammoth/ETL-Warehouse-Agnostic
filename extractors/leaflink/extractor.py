# extractor.py - LeafLink API extractor with incremental logic and schema evolution support
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

# GLOBAL REFERENCE DATA REFRESH INTERVAL (in hours)
# REFERENCE_DATA_REFRESH_HOURS = 24  # Change this value to adjust refresh frequency for all reference data

# ---------------- THREAD-SAFE STATE MANAGEMENT ---------------- #
_STATE_CACHE: Dict[str, str] = {}
_STATE_LOCK = threading.Lock()
_STATE_DIR_CREATED = False

# Global session cache for connection pooling
_SESSION_CACHE = None

def _utc_now():
    return datetime.utcnow().replace(tzinfo=timezone.utc)

# ---------------- ENDPOINT DEFINITIONS (FIXED WITH PROPER INCREMENTAL LOGIC) ---------------- #
LEAFLINK_ENDPOINTS = {
    # ========== ORDER-RELATED ENDPOINTS ========== #
    'orders_received': {
        'path': 'orders-received',
        'pagination_type': 'offset_limit',
        'limit': 50,
        'data_field': 'results',
        'total_count_field': 'count',
        'next_field': 'next',
        'incremental_field': 'modified',  # CONFIRMED: API supports modified filters
        'priority': 'high',
        'supports_company_scope': True,
        'date_filters': {
            'modified__gte': 'incremental_start',  # CORRECT: API parameter exists
            'modified__lte': 'incremental_end'     # CORRECT: API parameter exists
        },
        'include_children': ['line_items', 'customer', 'sales_reps'],
        'fields_add': ['created_by', 'last_changed_by']
    },
    
    'order_sales_reps': {
        'path': 'order-sales-reps',
        'pagination_type': 'offset_limit',
        'limit': 100,
        'data_field': 'results',
        'total_count_field': 'count',
        'next_field': 'next',
        'incremental_field': None,  # FIXED: No date filters available, use offset-based incremental
        'incremental_strategy': 'offset_based',  # NEW: Use offset-based incremental for sub-endpoints
        'priority': 'high',
        'supports_company_scope': False,
        'date_filters': {},  # FIXED: No date filters available
        'depends_on': ['orders_received']
    },
    
    'order_payments': {
        'path': 'order-payments',
        'pagination_type': 'offset_limit',
        'limit': 100,
        'data_field': 'results',
        'total_count_field': 'count',
        'next_field': 'next',
        'incremental_field': None,  # FIXED: No date filters available
        'incremental_strategy': 'offset_based',  # NEW: Use offset-based incremental
        'priority': 'high',
        'supports_company_scope': False,
        'date_filters': {},  # FIXED: No date filters available
        'depends_on': ['orders_received'],
        'include_children': [],
        'fields_add': ['recorded_by', 'recorder_name']
    },
    
    'order_event_logs': {
        'path': 'order-event-logs',
        'pagination_type': 'offset_limit',
        'limit': 100,
        'data_field': 'results',
        'total_count_field': 'count',
        'next_field': 'next',
        'incremental_field': None,  # FIXED: No date filters available
        'incremental_strategy': 'offset_based',  # NEW: Use offset-based incremental
        'priority': 'high',
        'supports_company_scope': False,
        'date_filters': {},  # FIXED: No date filters available
        'depends_on': ['orders_received'],
        'include_children': [],
        'fields_add': ['author_name', 'event_type', 'source']
    },
    
    'line_items': {
        'path': 'line-items',
        'pagination_type': 'offset_limit',
        'limit': 100,
        'data_field': 'results',
        'total_count_field': 'count',
        'next_field': 'next',
        'incremental_field': None,  # FIXED: API doesn't actually support order__modified filters
        'incremental_strategy': 'offset_based',  # FIXED: Use offset-based since date filters don't work
        'priority': 'high',
        'supports_company_scope': True,
        'date_filters': {},  # FIXED: Remove date filters that don't work
        'depends_on': ['orders_received'],
        'include_children': ['product', 'batch'],
        'fields_add': ['unit_multiplier', 'bulk_units', 'frozen_data']
    },
    
    # ========== PRODUCT CATALOG ENDPOINTS ========== #
    'products': {
        'path': 'products',
        'pagination_type': 'offset_limit',
        'limit': 100,
        'data_field': 'results',
        'total_count_field': 'count',
        'next_field': 'next',
        'incremental_field': 'modified',  # CONFIRMED: API supports modified filters
        'priority': 'high',
        'supports_company_scope': True,
        'date_filters': {
            'modified__gte': 'incremental_start',  # CORRECT: API parameter exists
            'modified__lte': 'incremental_end'     # CORRECT: API parameter exists
        },
        'include_children': [],
        'fields_add': []
    },
    
    'product_categories': {
        'path': 'product-categories',
        'pagination_type': 'offset_limit',
        'limit': 200,
        'data_field': 'results',
        'total_count_field': 'count',
        'next_field': 'next',
        'incremental_field': None,  # Reference data - uses global refresh interval
        'incremental_strategy': 'offset_based',  # NEW: Use offset-based incremental

        'priority': 'high',
        'supports_company_scope': False,
        'date_filters': {},
        'include_children': [],
        'fields_add': ['slug', 'description']
    },
    
    'product_subcategories': {
        'path': 'product-subcategories',
        'pagination_type': 'offset_limit',
        'limit': 200,
        'data_field': 'results',
        'total_count_field': 'count',
        'next_field': 'next',
        'incremental_field': None,  # Reference data - uses global refresh interval
        'incremental_strategy': 'offset_based',  # FIXED: Use offset-based since date filters don't work

        'priority': 'high',
        'supports_company_scope': False,
        'date_filters': {},
        'depends_on': ['product_categories'],
        'include_children': ['category'],
        'fields_add': ['slug', 'description']
    },
    
    'product_images': {
        'path': 'product-images',
        'pagination_type': 'offset_limit',
        'limit': 200,
        'data_field': 'results',
        'total_count_field': 'count',
        'next_field': 'next',
        'incremental_field': None,  # FIXED: No date filters available
        'incremental_strategy': 'offset_based',  # NEW: Use offset-based incremental
        'priority': 'high',
        'supports_company_scope': False,
        'date_filters': {},
        'depends_on': ['products'],
        'include_children': ['product'],
        'fields_add': ['position']
    },
    
    'product_lines': {
        'path': 'product-lines',
        'pagination_type': 'offset_limit',
        'limit': 200,
        'data_field': 'results',
        'total_count_field': 'count',
        'next_field': 'next',
        'incremental_field': None,  # Reference data - uses global refresh interval
        'incremental_strategy': 'offset_based',  # NEW: Use offset-based incremental

        'priority': 'high',
        'supports_company_scope': True,
        'date_filters': {},
        'include_children': ['brand'],
        'fields_add': ['menu_position']
    },
    
    'strains': {
        'path': 'strains',
        'pagination_type': 'offset_limit',
        'limit': 200,
        'data_field': 'results',
        'total_count_field': 'count',
        'next_field': 'next',
        'incremental_field': None,  # Reference data - uses global refresh interval
        'incremental_strategy': 'offset_based',  # NEW: Use offset-based incremental

        'priority': 'high',
        'supports_company_scope': False,
        'incremental_strategy': 'offset_based',  # FIXED: Use offset-based since date filters don't work

        'date_filters': {},
        'include_children': ['parents', 'company'],
        'fields_add': ['strain_classification']
    },
    
    # ========== BATCH-RELATED ENDPOINTS ========== #
    'product_batches': {
        'path': 'product-batches',
        'pagination_type': 'offset_limit',
        'limit': 200,
        'data_field': 'results',
        'total_count_field': 'count',
        'next_field': 'next',
        'incremental_field': None,  # FIXED: No date filters available
        'incremental_strategy': 'offset_based',  # NEW: Use offset-based incremental
        'priority': 'high',
        'supports_company_scope': False,
        'date_filters': {},
        'depends_on': ['products'],
        'include_children': ['product'],
        'fields_add': []
    },
    
    'batch_documents': {
        'path': 'batch-documents',
        'pagination_type': 'offset_limit',
        'limit': 200,
        'data_field': 'results',
        'total_count_field': 'count',
        'next_field': 'next',
        'incremental_field': None,  # FIXED: API docs don't show date filters
        'incremental_strategy': 'offset_based',  # NEW: Use offset-based incremental
        'priority': 'high',
        'supports_company_scope': False,
        'date_filters': {},
        'depends_on': ['product_batches'],
        'include_children': ['batch'],
        'fields_add': ['created_on', 'summary', 'document']
    },
    
    # ========== CRM-RELATED ENDPOINTS ========== #
    'customers': {
        'path': 'customers',
        'pagination_type': 'offset_limit',
        'limit': 100,
        'data_field': 'results',
        'total_count_field': 'count',
        'next_field': 'next',
        'incremental_field': 'modified',  # CONFIRMED: API supports modified filters
        'priority': 'high',
        'supports_company_scope': True,
        'date_filters': {
            'modified__gte': 'incremental_start',  # CORRECT: API parameter exists
            'modified__lte': 'incremental_end'     # CORRECT: API parameter exists
        },
        'include_children': ['tags', 'service_zone', 'managers', 'contacts', 'license_type'],
        'fields_add': []
    },
    
    'contacts': {
        'path': 'contacts',
        'pagination_type': 'offset_limit',
        'limit': 100,
        'data_field': 'results',
        'total_count_field': 'count',
        'next_field': 'next',
        'incremental_field': 'modified',  # CONFIRMED: API supports modified filters
        'priority': 'high',
        'supports_company_scope': False,
        'date_filters': {
            'modified__gte': 'incremental_start',  # CORRECT: API parameter exists
            'modified__lte': 'incremental_end'     # CORRECT: API parameter exists
        },
        'include_children': [],
        'fields_add': []
    },
    
    'activity_entries': {
        'path': 'activity-entries',
        'pagination_type': 'offset_limit',
        'limit': 100,
        'data_field': 'results',
        'total_count_field': 'count',
        'next_field': 'next',
        'incremental_field': 'modified',  # CONFIRMED: API supports modified filters
        'priority': 'medium',
        'supports_company_scope': False,
        'date_filters': {
            'modified__gte': 'incremental_start',  # CORRECT: API parameter exists
            'modified__lte': 'incremental_end'     # CORRECT: API parameter exists
        },
        'depends_on': ['customers', 'contacts'],
        'include_children': [],
        'fields_add': []
    },
    
    'customer_status': {
        'path': 'customer-statuses',  # FIXED: Should be plural
        'pagination_type': 'offset_limit',
        'limit': 200,
        'data_field': 'results',
        'total_count_field': 'count',
        'next_field': 'next',
        'incremental_field': None,  # Reference data - uses global refresh interval
        'incremental_strategy': 'offset_based',  # NEW: Use offset-based incremental

        'priority': 'low',
        'supports_company_scope': False,
        'date_filters': {},
        'include_children': [],
        'fields_add': []
    },
    
    'customer_tiers': {
        'path': 'customer-tiers',
        'pagination_type': 'offset_limit',
        'limit': 200,
        'data_field': 'results',
        'total_count_field': 'count',
        'next_field': 'next',
        'incremental_field': None,  # Reference data - uses global refresh interval
        'incremental_strategy': 'offset_based',  # NEW: Use offset-based incremental

        'priority': 'low',
        'supports_company_scope': False,
        'date_filters': {},
        'include_children': [],
        'fields_add': []
    },
    
    # ========== COMPANY & OPERATIONAL ENDPOINTS ========== #
    'companies': {
        'path': 'companies',
        'pagination_type': 'offset_limit',
        'limit': 100,
        'data_field': 'results',
        'total_count_field': 'count',
        'next_field': 'next',
        'incremental_field': None,  # Reference data - company info rarely changes
        'incremental_strategy': 'offset_based',  # NEW: Use offset-based incremental

        'priority': 'high',
        'supports_company_scope': False,
        'date_filters': {},
        'include_children': [],
        'fields_add': []
    },
    
    'company_staff': {
        'path': 'company-staff',
        'pagination_type': 'offset_limit',
        'limit': 100,
        'data_field': 'results',
        'total_count_field': 'count',
        'next_field': 'next',
        'incremental_field': None,  # Staff changes infrequently
        'priority': 'medium',
        'supports_company_scope': False,
        'date_filters': {},
        'depends_on': ['companies'],
        'include_children': [],
        'fields_add': []
    },
    
    'licenses': {
        'path': 'licenses',
        'pagination_type': 'offset_limit',
        'limit': 100,
        'data_field': 'results',
        'total_count_field': 'count',
        'next_field': 'next',
        'incremental_field': None,  # License info changes infrequently
        'priority': 'medium',
        'supports_company_scope': True,
        'date_filters': {},
        'depends_on': ['companies'],
        'include_children': [],
        'fields_add': []
    },
    
    'license_types': {
        'path': 'license-types',
        'pagination_type': 'offset_limit',
        'limit': 200,
        'data_field': 'results',
        'total_count_field': 'count',
        'next_field': 'next',
        'incremental_field': None,  # Reference data - uses global refresh interval
        'incremental_strategy': 'offset_based',  # NEW: Use offset-based incremental

        'priority': 'low',
        'supports_company_scope': True,
        'date_filters': {},
        'include_children': [],
        'fields_add': []
    },
    
    'brands': {
        'path': 'brands',
        'pagination_type': 'offset_limit',
        'limit': 100,
        'data_field': 'results',
        'total_count_field': 'count',
        'next_field': 'next',
        'incremental_field': None,  # Brand info changes infrequently
        'priority': 'medium',
        'supports_company_scope': True,
        'date_filters': {},
        'depends_on': ['companies'],
        'include_children': [],
        'fields_add': []
    },
    
    'promocodes': {
        'path': 'promocodes',
        'pagination_type': 'offset_limit',
        'limit': 100,
        'data_field': 'results',
        'total_count_field': 'count',
        'next_field': 'next',
        'incremental_field': None,  # Promocodes change occasionally but no date filters available
        'incremental_strategy': 'offset_based',  # Use offset-based for moderate frequency changes
        'priority': 'medium',
        'supports_company_scope': True,
        'date_filters': {},
        'depends_on': ['brands'],
        'include_children': [],
        'fields_add': []
    },
    
    'reports': {
        'path': 'reports',
        'pagination_type': 'offset_limit',
        'limit': 100,
        'data_field': 'results',
        'total_count_field': 'count',
        'next_field': 'next',
        'incremental_field': None,  # Reports are generated periodically
        'incremental_strategy': 'offset_based',  # Use offset-based for new reports
        'priority': 'low',
        'supports_company_scope': True,
        'date_filters': {},
        'depends_on': ['companies'],
        'include_children': [],
        'fields_add': []
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
    logger.info("=" * 80)
    logger.info("🚀 LEAFLINK EXTRACTOR INITIALIZATION")
    logger.info("=" * 80)
    logger.info(f"📋 Enabled Endpoints: {len(ENABLED_ENDPOINTS)} endpoints")
    base_url = CONFIG['api']['base_url']
    for endpoint in sorted(ENABLED_ENDPOINTS.keys()):
        endpoint_cfg = LEAFLINK_ENDPOINTS[endpoint]
        strategy = _get_incremental_strategy(endpoint)
        supports_company = endpoint_cfg.get('supports_company_scope', False)
        path = endpoint_cfg['path']
        
        if supports_company:
            url_pattern = f"{base_url}/companies/{{company_id}}/{path}/"
            scope_indicator = "[COMPANY-SCOPED]"
        else:
            url_pattern = f"{base_url}/{path}/"
            scope_indicator = "[GLOBAL]"
            
        logger.info(f"   ├─ {endpoint:<25} {scope_indicator}")
        logger.info(f"   │  └─ URL: {url_pattern}")
        logger.info(f"   │  └─ Strategy: {strategy}")
    logger.info("=" * 80)
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
                logger.info("🔄 Session: Reusing existing authenticated session")
                return _SESSION_CACHE
        except Exception:
            logger.info("🔄 Session: Cached session invalid, creating new one")
            _SESSION_CACHE = None
    
    api_key = os.getenv('LEAFLINK_API_KEY')
    base_url = CONFIG['api']['base_url']
    
    if not api_key:
        raise ValueError("Missing required environment variable: LEAFLINK_API_KEY")
    
    # Create session with API key authentication
    session = requests.Session()
    
    # LeafLink uses Token {API_KEY} format
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
        logger.info("✅ Authentication: Successfully authenticated with API key")
        
        # Cache the session
        _SESSION_CACHE = session
        return session
    except Exception as e:
        logger.error(f"❌ Authentication: Failed to authenticate - {e}")
        raise

# ---------------- UTILITIES ---------------- #
# Enhanced flatten_leaflink_record function with specific column handling
# Add this to replace the existing flatten_leaflink_record function in extractor.py

def _get_state_client():
    """
    Minimal ClickHouse client for state I/O.  Re-uses the same env vars as data loads.
    """
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


def flatten_leaflink_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten LeafLink record structure with special handling for nested objects and enhanced JSON flattening."""
    flattened = {}
    
    if isinstance(record, dict):
        for key, value in record.items():
            if isinstance(value, dict):
                # Special handling for common LeafLink nested structures
                if key == 'total' and 'amount' in value:
                    flattened['total_amount'] = value.get('amount')
                    flattened['total_currency'] = value.get('currency')
                elif key == 'shipping_charge' and 'amount' in value:
                    flattened['shipping_charge_amount'] = value.get('amount')
                    flattened['shipping_charge_currency'] = value.get('currency')
                elif key == 'customer' and isinstance(value, dict):
                    for nested_key, nested_value in value.items():
                        flattened[f"customer_{nested_key}"] = nested_value
                elif key in ['ordered_unit_price', 'sale_price', 'wholesale_price', 'retail_price', 'price_schedule_price'] and 'amount' in value:
                    flattened[f"{key}_amount"] = value.get('amount')
                    flattened[f"{key}_currency"] = value.get('currency')
                elif key == 'unit_denomination' and isinstance(value, dict):
                    for nested_key, nested_value in value.items():
                        flattened[f"unit_denomination_{nested_key}"] = nested_value
                elif key == 'license' and isinstance(value, dict):
                    for nested_key, nested_value in value.items():
                        flattened[f"license_{nested_key}"] = nested_value
                elif key == 'external_ids' and isinstance(value, dict):
                    if value:
                        flattened['external_ids_json'] = json.dumps(value)
                    else:
                        flattened['external_ids_json'] = None
                elif key == 'changed_fields' and isinstance(value, dict):
                    flattened['changed_fields_json'] = json.dumps(value) if value else None
                elif key == 'frozen_data' and isinstance(value, dict):
                    flattened['frozen_data_json'] = json.dumps(value) if value else None
                # NEW: Enhanced address flattening for orders_received
                elif key in ['customer_corporate_address', 'customer_delivery_address'] and isinstance(value, dict):
                    for addr_key, addr_value in value.items():
                        flattened[f"{key}_{addr_key}"] = addr_value
                    # Keep original JSON for backward compatibility
                    flattened[f"{key}_json"] = json.dumps(value) if value else None
                # NEW: Enhanced payment method/term flattening for orders_received
                elif key in ['selected_payment_option_payment_method', 'selected_payment_option_payment_term'] and isinstance(value, dict):
                    for payment_key, payment_value in value.items():
                        flattened[f"{key}_{payment_key}"] = payment_value
                    # Keep original JSON for backward compatibility
                    flattened[f"{key}_json"] = json.dumps(value) if value else None
                else:
                    for nested_key, nested_value in value.items():
                        flattened[f"{key}_{nested_key}"] = nested_value
            elif isinstance(value, list):
                if key == 'strains' and value:
                    flattened['strains_json'] = json.dumps(value)
                    flattened['strains_count'] = len(value)
                elif key == 'parents' and value:
                    flattened['parents_json'] = json.dumps(value)
                    flattened['parents_count'] = len(value)
                elif key == 'product_data_items' and value:
                    flattened['product_data_items_json'] = json.dumps(value)
                    flattened['product_data_items_count'] = len(value)
                elif key == 'brand_ids' and value:
                    flattened['brand_ids_json'] = json.dumps(value)
                    flattened['brand_ids_count'] = len(value)
                elif key == 'package_tags' and value:
                    flattened['package_tags_json'] = json.dumps(value)
                    flattened['package_tags_count'] = len(value)
                # NEW: Enhanced array flattening for specific columns
                elif key == 'managers' and value:
                    flattened['managers_json'] = json.dumps(value)
                    flattened['managers_count'] = len(value)
                    # Extract key fields from first manager for easier querying
                    if value and isinstance(value[0], dict):
                        first_manager = value[0]
                        flattened['primary_manager_id'] = first_manager.get('id')
                        flattened['primary_manager_email'] = first_manager.get('email')
                        flattened['primary_manager_phone'] = first_manager.get('phone')
                        flattened['primary_manager_position'] = first_manager.get('position')
                elif key == 'tags' and value:
                    flattened['tags_json'] = json.dumps(value)
                    flattened['tags_count'] = len(value)
                    # Extract tag names for easier filtering
                    if value and all(isinstance(tag, dict) and 'name' in tag for tag in value):
                        tag_names = [tag['name'] for tag in value]
                        flattened['tag_names'] = '|'.join(tag_names)  # Pipe-separated for easy filtering
                elif key == 'contacts' and value:
                    flattened['contacts_json'] = json.dumps(value)
                    flattened['contacts_count'] = len(value)
                    # Extract primary contact info
                    primary_contact = next((c for c in value if c.get('is_primary')), value[0] if value else None)
                    if primary_contact and isinstance(primary_contact, dict):
                        flattened['primary_contact_id'] = primary_contact.get('id')
                        flattened['primary_contact_first_name'] = primary_contact.get('first_name')
                        flattened['primary_contact_last_name'] = primary_contact.get('last_name')
                        flattened['primary_contact_email'] = primary_contact.get('email')
                        flattened['primary_contact_phone'] = primary_contact.get('phone')
                        flattened['primary_contact_role'] = primary_contact.get('role')
                elif key == 'sales_reps' and value:
                    flattened['sales_reps_json'] = json.dumps(value)
                    flattened['sales_reps_count'] = len(value)
                    # Extract primary sales rep info
                    if value and isinstance(value[0], dict):
                        first_rep = value[0]
                        flattened['primary_sales_rep_id'] = first_rep.get('id')
                        flattened['primary_sales_rep_email'] = first_rep.get('email')
                        flattened['primary_sales_rep_phone'] = first_rep.get('phone')
                        flattened['primary_sales_rep_position'] = first_rep.get('position')
                elif key == 'order_taxes' and value:
                    flattened['order_taxes_json'] = json.dumps(value)
                    flattened['order_taxes_count'] = len(value)
                    # Calculate total tax amount
                    if value and all(isinstance(tax, dict) for tax in value):
                        tax_names = [tax.get('name', '') for tax in value]
                        flattened['tax_names'] = '|'.join(filter(None, tax_names))
                elif key == 'customer_available_payment_options' and value:
                    flattened['available_options_json'] = json.dumps(value)
                    flattened['available_options_count'] = len(value)
                    # Extract default payment option info
                    default_option = next((opt for opt in value if opt.get('is_default')), value[0] if value else None)
                    if default_option and isinstance(default_option, dict):
                        payment_method = default_option.get('payment_method', {})
                        payment_term = default_option.get('payment_term', {})
                        flattened['default_payment_method_id'] = payment_method.get('id') if payment_method else None
                        flattened['default_payment_method'] = payment_method.get('method') if payment_method else None
                        flattened['default_payment_term_id'] = payment_term.get('id') if payment_term else None
                        flattened['default_payment_term'] = payment_term.get('term') if payment_term else None
                        flattened['default_payment_term_days'] = payment_term.get('days') if payment_term else None
                # NEW: Enhanced frozen_data handling for line_items
                elif key == 'frozen_data' and value and isinstance(value, dict):
                    flattened['frozen_data_json'] = json.dumps(value)
                    # Extract product info from frozen_data if it exists
                    if 'product' in value and isinstance(value['product'], dict):
                        product_data = value['product']
                        for prod_key, prod_value in product_data.items():
                            flattened[f"frozen_product_{prod_key}"] = prod_value
                elif value and all(isinstance(item, dict) for item in value):
                    flattened[f"{key}_json"] = json.dumps(value)
                    flattened[f"{key}_count"] = len(value)
                elif value:
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
    """Smart rate limiting based on response headers and status."""
    remaining = response.headers.get('X-RateLimit-Remaining')
    reset_time = response.headers.get('X-RateLimit-Reset')
    
    if remaining and int(remaining) < 5:
        time.sleep(2.0)
        logger.info("⏳ Rate Limit: Near limit, slowing down (remaining: <5)")
    elif response.status_code == 429:
        wait_time = int(reset_time) if reset_time else 60
        logger.warning(f"⏸️ Rate Limit: Hit rate limit, waiting {wait_time}s")
        time.sleep(wait_time)
    else:
        time.sleep(1.0 / config['api']['rate_limiting']['requests_per_second'])


def _get_state_client():
    """
    Lightweight ClickHouse client for state metadata operations.
    """
    import clickhouse_connect
    host = os.getenv('CLICKHOUSE_HOST')
    port = int(os.getenv('CLICKHOUSE_PORT', 8443))
    database = os.getenv('CLICKHOUSE_DATABASE', 'default')
    username = os.getenv('CLICKHOUSE_USER', 'default')
    password = os.getenv('CLICKHOUSE_PASSWORD')

    if not all([host, password]):
        raise ValueError("ClickHouse connection parameters missing for state backend")

    return clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
        secure=True,
    )



# ---------------- THREAD-SAFE INCREMENTAL STATE MANAGEMENT ---------------- #
def _state_path():
    return "<state stored in ClickHouse – no file path>"



def _ensure_state_dir():
    """
    Creates metadata tables exactly once:
      • meta.pipeline_state_latest   – ReplacingMergeTree  (latest per key)
      • meta.pipeline_state_history  – MergeTree          (full audit)
    """
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
    """
    Hydrate cache from ClickHouse (latest rows only).
    """
    import json
    global _STATE_CACHE

    with _STATE_LOCK:
        try:
            _ensure_state_dir()
        except Exception as e:
            logger.warning(f"⚠️  ClickHouse unreachable – state starts empty ({e})")
            _STATE_CACHE = {}
            return

        src = CONFIG.get("extraction", {}).get("source_system", "leaflink")

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
    """
    Write the in-memory cache to ClickHouse:
      ▸ append to history
      ▸ insert/replace into latest
    """
    import json
    from datetime import datetime

    with _STATE_LOCK:
        _ensure_state_dir()
        if not _STATE_CACHE:
            logger.info("💾 Cache empty – nothing to persist")
            return

        src = CONFIG.get("extraction", {}).get("source_system", "leaflink")
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
        
        # Handle both datetime watermarks and offset watermarks
        if isinstance(iso, str) and iso.startswith('offset:'):
            return None  # Offset watermarks don't convert to datetime
        
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

def _get_last_offset(endpoint_key: str) -> Optional[int]:
    """Get last offset for offset-based incremental endpoints - THREAD SAFE."""
    with _STATE_LOCK:
        value = _STATE_CACHE.get(endpoint_key)
        if not value:
            return None
        
        if isinstance(value, str) and value.startswith('offset:'):
            try:
                return int(value.split(':', 1)[1])
            except (ValueError, IndexError):
                logger.warning(f"⚠️ State: Invalid offset format for {endpoint_key}: {value}")
                return None
        
        return None

def _update_watermark(endpoint_key: str, new_ts: datetime):
    """Update watermark for endpoint - THREAD SAFE, NOT SAVED YET."""
    with _STATE_LOCK:
        if new_ts.tzinfo is None:
            new_ts = new_ts.replace(tzinfo=timezone.utc)
        _STATE_CACHE[endpoint_key] = new_ts.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
        logger.info(f"🕒 Watermark: Updated in memory {endpoint_key} → {_STATE_CACHE[endpoint_key]}")

def _update_offset(endpoint_key: str, new_offset: int):
    """Update offset for offset-based incremental endpoints - THREAD SAFE, NOT SAVED YET."""
    with _STATE_LOCK:
        _STATE_CACHE[endpoint_key] = f"offset:{new_offset}"
        logger.info(f"🔢 Offset: Updated in memory {endpoint_key} → {new_offset}")

# ---------------- INCREMENTAL HELPERS ---------------- #
def _parse_leaflink_timestamp(date_str: str) -> Optional[datetime]:
    """Parse LeafLink timestamp format with timezone awareness."""
    if not date_str or date_str == '' or date_str is None:
        return None
    
    try:
        date_str = str(date_str).strip()
        
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
    incremental_strategy = endpoint_config.get('incremental_strategy')
    
    # Support both date-based and offset-based incremental
    return incremental_field is not None or incremental_strategy in ['offset_based', 'parent_driven']

def _get_incremental_strategy(endpoint_key: str) -> str:
    """Get the incremental strategy for an endpoint."""
    endpoint_config = LEAFLINK_ENDPOINTS.get(endpoint_key, {})
    
    if endpoint_config.get('incremental_field'):
        return 'date_based'
    elif endpoint_config.get('incremental_strategy') == 'offset_based':
        return 'offset_based'
    elif endpoint_config.get('incremental_strategy') == 'parent_driven':
        return 'parent_driven'
    else:
        return 'full_refresh'

# def _should_skip_reference_data_extraction(endpoint_key: str) -> bool:
#     """Check if reference data endpoint should be skipped based on global refresh interval."""
#     endpoint_config = LEAFLINK_ENDPOINTS.get(endpoint_key, {})
    
#     # Only apply to endpoints without any incremental strategy (pure reference data)
#     if _get_incremental_strategy(endpoint_key) != 'full_refresh':
#         return False
    
#     # Use global refresh interval for all reference data
#     refresh_interval_hours = REFERENCE_DATA_REFRESH_HOURS
    
#     # Check when this endpoint was last extracted
#     last_extraction = _get_last_watermark(endpoint_key)
#     if not last_extraction:
#         # Never extracted before, need to extract
#         return False
    
#     # Calculate if enough time has passed
#     now = _utc_now()
#     time_since_last = now - last_extraction
#     refresh_interval = timedelta(hours=refresh_interval_hours)
    
#     should_skip = time_since_last < refresh_interval
    
#     if should_skip:
#         remaining_time = refresh_interval - time_since_last
#         hours_remaining = remaining_time.total_seconds() / 3600
#         logger.info(f"⏭️ Skipping {endpoint_key} - last extracted {time_since_last} ago, refresh interval is {refresh_interval_hours}h (next refresh in {hours_remaining:.1f}h)")
#         return True
#     else:
#         logger.info(f"🔄 {endpoint_key} ready for refresh - last extracted {time_since_last} ago, refresh interval is {refresh_interval_hours}h")
#         return False

def _get_incremental_date_range(endpoint_key: str):
    """Get date range for incremental extraction with proper timezone handling and start_date support."""
    endpoint_config = LEAFLINK_ENDPOINTS.get(endpoint_key, {})
    
    strategy = _get_incremental_strategy(endpoint_key)
    
    if strategy != 'date_based':
        # For non-date-based strategies, return None to indicate no date filtering
        return None, None
    
    last_wm = _get_last_watermark(endpoint_key)
    end_date = _utc_now()
    
    if last_wm:
        if last_wm.tzinfo is None:
            last_wm = last_wm.replace(tzinfo=timezone.utc)
        
        lookback = CONFIG['extraction']['incremental'].get('lookback_minutes', 10)
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

def _get_incremental_offset_range(endpoint_key: str):
    """Get offset range for offset-based incremental extraction."""
    last_offset = _get_last_offset(endpoint_key)
    
    if last_offset is not None:
        # Start from where we left off
        start_offset = last_offset
        logger.info(f"🔢 Incremental: {endpoint_key} continuing from offset {start_offset}")
    else:
        # First run, start from 0
        start_offset = 0
        logger.info(f"🔢 First Run: {endpoint_key} starting from offset 0")
    
    return start_offset

# ---------------- WAREHOUSE LOADING FUNCTIONS WITH SCHEMA EVOLUTION ---------------- #
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
    """
    Same verification logic, but the ClickHouse schema is passed in by caller
    (one schema per company).
    """
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



def get_paginated_data(session, endpoint_config, endpoint_name, company_id):
    """Fetch all pages from a LeafLink endpoint, now company-aware."""
    all_data = []
    base_url = CONFIG['api']['base_url']
    max_retries = 3

    # Build the URL – company-scoped when a company_id is supplied
    if company_id and endpoint_config.get('supports_company_scope'):
        endpoint_url_base = f"{base_url}/companies/{company_id}/{endpoint_config['path']}/"
        scope_info = f"Company-scoped: /companies/{company_id}/{endpoint_config['path']}/"
    else:
        endpoint_url_base = f"{base_url}/{endpoint_config['path']}/"
        scope_info = f"Global: /{endpoint_config['path']}/"

    params = {'limit': endpoint_config['limit']}
    strategy = _get_incremental_strategy(endpoint_name)

    logger.info(f"📡 API: {scope_info}")

    # Date-based incremental
    if strategy == 'date_based':
        start_date, end_date = _get_incremental_date_range(endpoint_name)
        if start_date and end_date:
            for p, t in endpoint_config.get('date_filters', {}).items():
                params[p] = start_date.isoformat() if t == 'incremental_start' else end_date.isoformat()
            logger.info(f"📅 Date Filter: {start_date.strftime('%Y-%m-%d %H:%M:%S')} → {end_date.strftime('%Y-%m-%d %H:%M:%S')} UTC")

    # Offset incremental
    offset = _get_incremental_offset_range(endpoint_name) if strategy == 'offset_based' else 0
    last_successful_offset = offset
    page = 0

    logger.info(f"📄 Pagination: Starting with limit={endpoint_config['limit']}, offset={offset}")

    while page < 1000:                               # safety-cap
        page += 1
        params['offset'] = offset
        
        # More compact pagination logging
        logger.info(f"   📄 Page {page:2d} | Offset: {offset:5d} | Fetching...")

        try:
            response = session.get(endpoint_url_base,
                                    params=params,
                                    timeout=CONFIG['api']['rate_limiting']['timeout_seconds'])
            response.raise_for_status()
            data = response.json()

            records = data.get(endpoint_config['data_field'], [])
            if not records:
                logger.info(f"   📄 Page {page:2d} | No more records - pagination complete")
                break

            all_data.extend(records)
            last_successful_offset = offset + len(records)
            
            logger.info(f"   📄 Page {page:2d} | Got: {len(records):3d} records | Total: {len(all_data):5d}")

            if len(records) < endpoint_config['limit'] or not data.get(endpoint_config['next_field']):
                logger.info(f"   📄 Page {page:2d} | Last page reached (got {len(records)} < {endpoint_config['limit']})")
                break

            offset += endpoint_config['limit']
            smart_rate_limit(response, CONFIG)

        except Exception as e:
            logger.warning(f"   📄 Page {page:2d} | API Error: {e}")
            if page == 1:
                raise
            break

    if strategy == 'offset_based':
        _update_offset(endpoint_name, last_successful_offset)

    logger.info(f"✅ Extraction: Collected {len(all_data)} total records from {page} pages")
    return all_data



# ---------------- STATE MANAGEMENT AFTER SUCCESSFUL LOAD (UPDATED FOR MULTIPLE STRATEGIES) ---------------- #
def update_state_after_verified_load(endpoint_key, endpoint_config, df, raw_data, extracted_at):
    """Update state ONLY after verified warehouse load - supports multiple incremental strategies."""
    try:
        logger.info(f"📝 State Update: Processing {endpoint_key}")
        
        strategy = _get_incremental_strategy(endpoint_key)
        
        if strategy == 'date_based':
            incremental_field = endpoint_config.get('incremental_field')
            
            if incremental_field and not df.empty:
                # Look for columns containing the incremental field name
                incremental_columns = [col for col in df.columns 
                                     if incremental_field.lower() in col.lower()]
                
                if incremental_columns:
                    actual_field = incremental_columns[0]
                    timestamps = _parse_timestamp_column(df, actual_field)
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
                    logger.info(f"🕒 Watermark: Using extraction timestamp: {extracted_at.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            else:
                _update_watermark(endpoint_key, extracted_at)
                logger.info(f"🕒 Watermark: No incremental field, using extraction time: {extracted_at.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        
        elif strategy == 'offset_based':
            # Offset already updated during pagination in get_paginated_data
            logger.info(f"🔢 Offset: Already updated during pagination")
        
        elif strategy == 'parent_driven':
            # For parent-driven endpoints, update extraction timestamp
            _update_watermark(endpoint_key, extracted_at)
            logger.info(f"🔗 Parent-driven: Watermark set to extraction time: {extracted_at.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        
        else:  # full_refresh
            _update_watermark(endpoint_key, extracted_at)
            logger.info(f"🔄 Full Refresh: Watermark set to extraction time: {extracted_at.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        
        _save_state()
        logger.info("✅ State: Successfully saved after verified load")
        
    except Exception as e:
        logger.error(f"❌ State: Failed to update - {e}")
        logger.error("❌ CRITICAL: Next run may have data gaps!")
        raise

# ---------------- MAIN EXTRACTION FUNCTION (UPDATED FOR MULTIPLE STRATEGIES) ---------------- #
def extract_leaflink_endpoint(endpoint_key, company_id, raw_schema, **context):
    """Run a single endpoint for a specific company / schema."""
    logger.info("=" * 100)
    logger.info(f"🚀 ENDPOINT EXTRACTION START")
    logger.info("=" * 100)
    
    if endpoint_key not in ENABLED_ENDPOINTS:
        logger.info(f"⏭️ Skip: {endpoint_key} is disabled in configuration")
        logger.info("=" * 100)
        return 0

    # if _should_skip_reference_data_extraction(endpoint_key):
    #     return 0

    endpoint_cfg = LEAFLINK_ENDPOINTS[endpoint_key]
    strategy = _get_incremental_strategy(endpoint_key)
    base_url = CONFIG['api']['base_url']
    
    # Build actual URL that will be used
    if company_id and endpoint_cfg.get('supports_company_scope'):
        actual_url = f"{base_url}/companies/{company_id}/{endpoint_cfg['path']}/"
        scope_info = "COMPANY-SCOPED"
    else:
        actual_url = f"{base_url}/{endpoint_cfg['path']}/"
        scope_info = "GLOBAL"
    
    # Header information
    logger.info(f"📊 Endpoint: {endpoint_key}")
    logger.info(f"🏢 Company: {company_id}")
    logger.info(f"🗄️  Schema: {raw_schema}")
    logger.info(f"📈 Strategy: {strategy.upper()}")
    logger.info(f"🌐 Scope: {scope_info}")
    logger.info(f"🔗 URL: {actual_url}")
    
    # Dependencies
    dependencies = endpoint_cfg.get('depends_on', [])
    if dependencies:
        logger.info(f"🔄 Dependencies: {', '.join(dependencies)}")
    
    logger.info("-" * 100)

    state_backup = _STATE_CACHE.copy()
    try:
        session = create_authenticated_session()
        raw = get_paginated_data(session, endpoint_cfg, endpoint_key, company_id)
        
        if not raw:
            logger.info("📭 No data returned from API")
            logger.info("=" * 100)
            return 0

        logger.info(f"🔄 Processing: Flattening {len(raw)} records...")
        flat = [flatten_leaflink_record(r) for r in raw]
        df = pd.DataFrame(flat)
        extracted_at = _utc_now()
        df['_extracted_at'] = extracted_at.isoformat()
        df['_source_system'] = 'leaflink'
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



# ---------------- SCHEDULING AND DEPENDENCIES ---------------- #
def should_run_endpoint(endpoint_key, execution_dt: datetime, completed_endpoints: set = None):
    """Check if endpoint should run based on schedule and dependencies."""
    if completed_endpoints is None:
        completed_endpoints = set()
    
    endpoint_config = LEAFLINK_ENDPOINTS.get(endpoint_key, {})
    dependencies = endpoint_config.get('depends_on', [])
    
    for dep in dependencies:
        if dep not in completed_endpoints:
            logger.info(f"⏳ Dependencies: {endpoint_key} waiting for {dep}")
            return False
    
    return True

def get_endpoint_execution_order():
    """Get endpoints in dependency order with enhanced logic for product catalog."""
    endpoints = list(ENABLED_ENDPOINTS.keys())
    completed = set()
    ordered = []
    
    execution_tiers = [
        ['product_categories', 'product_lines', 'strains', 'customer_status', 'customer_tiers', 'license_types', 'companies'],
        ['product_subcategories', 'company_staff', 'licenses', 'brands'],
        ['orders_received', 'products', 'customers', 'promocodes'],
        ['product_batches', 'contacts'],
        ['order_sales_reps', 'order_payments', 'line_items'],
        ['product_images', 'batch_documents', 'activity_entries', 'reports'],
        ['order_event_logs']
    ]
    
    logger.info("📋 Execution Planning:")
    base_url = CONFIG['api']['base_url']
    
    for tier_num, tier in enumerate(execution_tiers, 1):
        tier_endpoints = [ep for ep in tier if ep in endpoints]
        
        if tier_endpoints:
            tier_endpoints.sort(key=lambda ep: {
                'high': 0, 'medium': 1, 'low': 2
            }.get(LEAFLINK_ENDPOINTS.get(ep, {}).get('priority', 'medium'), 1))
            
            logger.info(f"   Tier {tier_num}:")
            for ep in tier_endpoints:
                endpoint_cfg = LEAFLINK_ENDPOINTS[ep]
                supports_company = endpoint_cfg.get('supports_company_scope', False)
                if supports_company:
                    scope_indicator = "[COMPANY-SCOPED]"
                else:
                    scope_indicator = "[GLOBAL]"
                logger.info(f"      ├─ {ep:<25} {scope_indicator}")
            
            for endpoint in tier_endpoints:
                if should_run_endpoint(endpoint, None, completed):
                    ordered.append(endpoint)
                    completed.add(endpoint)
                    endpoints.remove(endpoint)
    
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
    
    # Categorize endpoints for summary
    order_related = [ep for ep in ordered if ep.startswith('order') or ep in ['line_items']]
    product_related = [ep for ep in ordered if ep.startswith('product') or ep in ['products', 'strains']]
    crm_related = [ep for ep in ordered if ep.startswith('customer') or ep in ['customers', 'contacts', 'activity_entries']]
    company_related = [ep for ep in ordered if ep.startswith('company') or ep.startswith('license') or ep in ['companies', 'brands', 'promocodes', 'reports']]
    
    def format_endpoint_list(endpoint_list):
        """Format endpoint list with scope indicators"""
        formatted = []
        for ep in endpoint_list:
            endpoint_cfg = LEAFLINK_ENDPOINTS[ep]
            scope = "[C]" if endpoint_cfg.get('supports_company_scope') else "[G]"
            formatted.append(f"{ep}{scope}")
        return ', '.join(formatted)
    
    if order_related:
        logger.info(f"   📦 Order Endpoints: {format_endpoint_list(order_related)}")
    if product_related:
        logger.info(f"   🏷️ Product Endpoints: {format_endpoint_list(product_related)}")
    if crm_related:
        logger.info(f"   👥 CRM Endpoints: {format_endpoint_list(crm_related)}")
    if company_related:
        logger.info(f"   🏢 Company Endpoints: {format_endpoint_list(company_related)}")
    
    logger.info("   📝 Legend: [C] = Company-scoped, [G] = Global")
    
    return ordered

# ---------------- DAG INTEGRATION FUNCTIONS ---------------- #
def finalize_state_after_warehouse_load(context):
    """
    Simple end-of-DAG sanity check for ClickHouse-backed state.
    """
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


def extract_all_endpoints_with_dependencies(company_id, raw_schema, **context):
    """
    Runs all enabled endpoints for a given company_id / schema, respecting
    dependencies exactly as before.
    """
    logger.info("=" * 100)
    logger.info("🚀 MULTI-ENDPOINT EXTRACTION START")
    logger.info("=" * 100)
    logger.info(f"🏢 Company ID: {company_id}")
    logger.info(f"🗄️  Schema: {raw_schema}")
    logger.info(f"📊 Mode: {'TESTING' if TESTING_MODE else 'PRODUCTION'}")
    
    order = get_endpoint_execution_order()
    total, results, done = 0, {}, set()
    
    logger.info("=" * 100)

    for i, ep in enumerate(order, 1):
        logger.info(f"🔄 Progress: [{i:2d}/{len(order)}] Processing {ep}")
        
        try:
            recs = extract_leaflink_endpoint(ep, company_id, raw_schema, **context)
            total += recs
            results[ep] = recs
            done.add(ep)
            logger.info(f"✅ Completed: {ep} → {recs} records")
        except Exception as e:
            results[ep] = f"Failed: {e}"
            logger.error(f"❌ Failed: {ep} → {str(e)}")
            logger.info("=" * 100)

    logger.info("=" * 100)
    logger.info("🎉 MULTI-ENDPOINT EXTRACTION COMPLETE")
    logger.info("=" * 100)
    logger.info(f"🏢 Company: {company_id}")
    logger.info(f"📊 Total Records: {total:,}")
    logger.info(f"✅ Successful: {len(done)}/{len(order)} endpoints")
    logger.info(f"❌ Failed: {len(order) - len(done)}/{len(order)} endpoints")
    
    # Summary by category
    successful = [ep for ep, result in results.items() if isinstance(result, int)]
    failed = [ep for ep, result in results.items() if isinstance(result, str)]
    
    if successful:
        logger.info("✅ Successful Endpoints:")
        for ep in successful:
            endpoint_cfg = LEAFLINK_ENDPOINTS[ep]
            scope_indicator = "[COMPANY-SCOPED]" if endpoint_cfg.get('supports_company_scope') else "[GLOBAL]"
            logger.info(f"   ├─ {ep:<25} {scope_indicator:<15} → {results[ep]:>6,} records")
    
    if failed:
        logger.info("❌ Failed Endpoints:")
        for ep in failed:
            endpoint_cfg = LEAFLINK_ENDPOINTS[ep]
            scope_indicator = "[COMPANY-SCOPED]" if endpoint_cfg.get('supports_company_scope') else "[GLOBAL]"
            logger.info(f"   ├─ {ep:<25} {scope_indicator:<15} → {results[ep]}")
    
    logger.info("=" * 100)
    
    return {
        'total_records': total,
        'extraction_results': results,
        'completed_endpoints': list(done)
    }