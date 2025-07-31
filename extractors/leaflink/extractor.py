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
REFERENCE_DATA_REFRESH_HOURS = 24  # Change this value to adjust refresh frequency for all reference data

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
        'supports_company_scope': False,
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
        'supports_company_scope': False,
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
        'priority': 'high',
        'supports_company_scope': False,
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
        'priority': 'high',
        'supports_company_scope': False,
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
        'supports_company_scope': False,
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
        'supports_company_scope': False,
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
        'priority': 'low',
        'supports_company_scope': False,
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
        'supports_company_scope': False,
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
        'supports_company_scope': False,
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
        'supports_company_scope': False,
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
        logger.info("✅ Authentication successful with API key")
        
        # Cache the session
        _SESSION_CACHE = session
        return session
    except Exception as e:
        logger.error(f"❌ Authentication failed: {e}")
        raise

# ---------------- UTILITIES ---------------- #

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
        logger.info("⏳ Near rate limit, slowing down")
    elif response.status_code == 429:
        wait_time = int(reset_time) if reset_time else 60
        logger.warning(f"⏸️ Rate limited, waiting {wait_time}s")
        time.sleep(wait_time)
    else:
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
    """Load watermark state JSON (endpoint -> ISO timestamp or offset) - THREAD SAFE."""
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
            state_json = json.dumps(_STATE_CACHE, indent=2, sort_keys=True)
            checksum = hashlib.md5(state_json.encode()).hexdigest()
            
            logger.info(f"💾 Saving state to {path}: {_STATE_CACHE}")
            
            with open(temp_path, 'w') as f:
                f.write(state_json)
                f.flush()
                os.fsync(f.fileno())
            
            with open(temp_path, 'r') as f:
                verify_content = f.read()
                verify_checksum = hashlib.md5(verify_content.encode()).hexdigest()
                
            if verify_checksum != checksum:
                raise ValueError("State file verification failed - checksum mismatch")
            
            if os.name == 'nt':
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
            logger.warning(f"⚠️ Invalid watermark format for {endpoint_key}: {iso}")
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
                logger.warning(f"⚠️ Invalid offset format for {endpoint_key}: {value}")
                return None
        
        return None

def _update_watermark(endpoint_key: str, new_ts: datetime):
    """Update watermark for endpoint - THREAD SAFE, NOT SAVED YET."""
    with _STATE_LOCK:
        if new_ts.tzinfo is None:
            new_ts = new_ts.replace(tzinfo=timezone.utc)
        _STATE_CACHE[endpoint_key] = new_ts.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
        logger.info(f"🕒 Updated watermark in memory: {endpoint_key} = {_STATE_CACHE[endpoint_key]}")

def _update_offset(endpoint_key: str, new_offset: int):
    """Update offset for offset-based incremental endpoints - THREAD SAFE, NOT SAVED YET."""
    with _STATE_LOCK:
        _STATE_CACHE[endpoint_key] = f"offset:{new_offset}"
        logger.info(f"🔢 Updated offset in memory: {endpoint_key} = {new_offset}")

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

def _should_skip_reference_data_extraction(endpoint_key: str) -> bool:
    """Check if reference data endpoint should be skipped based on global refresh interval."""
    endpoint_config = LEAFLINK_ENDPOINTS.get(endpoint_key, {})
    
    # Only apply to endpoints without any incremental strategy (pure reference data)
    if _get_incremental_strategy(endpoint_key) != 'full_refresh':
        return False
    
    # Use global refresh interval for all reference data
    refresh_interval_hours = REFERENCE_DATA_REFRESH_HOURS
    
    # Check when this endpoint was last extracted
    last_extraction = _get_last_watermark(endpoint_key)
    if not last_extraction:
        # Never extracted before, need to extract
        return False
    
    # Calculate if enough time has passed
    now = _utc_now()
    time_since_last = now - last_extraction
    refresh_interval = timedelta(hours=refresh_interval_hours)
    
    should_skip = time_since_last < refresh_interval
    
    if should_skip:
        remaining_time = refresh_interval - time_since_last
        hours_remaining = remaining_time.total_seconds() / 3600
        logger.info(f"⏭️ Skipping {endpoint_key} - last extracted {time_since_last} ago, refresh interval is {refresh_interval_hours}h (next refresh in {hours_remaining:.1f}h)")
        return True
    else:
        logger.info(f"🔄 {endpoint_key} ready for refresh - last extracted {time_since_last} ago, refresh interval is {refresh_interval_hours}h")
        return False

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
        logger.info(f"🔁 Incremental extract for {endpoint_key} from {start_date.isoformat()}")
    else:
        configured_start_date = CONFIG.get('dag', {}).get('start_date')
        if configured_start_date:
            try:
                if isinstance(configured_start_date, str):
                    start_date = datetime.strptime(configured_start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                else:
                    start_date = configured_start_date.replace(tzinfo=timezone.utc)
                logger.info(f"🗓️ First incremental run using configured start_date for {endpoint_key}: {start_date.isoformat()}")
            except Exception as e:
                logger.warning(f"⚠️ Failed to parse configured start_date '{configured_start_date}': {e}")
                if TESTING_MODE:
                    start_date = end_date - timedelta(days=CONFIG['extraction']['testing']['date_range_days'])
                else:
                    start_date = end_date - timedelta(days=CONFIG['extraction']['production']['date_range_days'])
                logger.info(f"🔄 Fallback to default range for first incremental run of {endpoint_key}")
        else:
            if TESTING_MODE:
                start_date = end_date - timedelta(days=CONFIG['extraction']['testing']['date_range_days'])
            else:
                start_date = end_date - timedelta(days=CONFIG['extraction']['production']['date_range_days'])
            logger.info(f"🔄 First incremental run using default range for {endpoint_key}")
    
    return start_date, end_date

def _get_incremental_offset_range(endpoint_key: str):
    """Get offset range for offset-based incremental extraction."""
    last_offset = _get_last_offset(endpoint_key)
    
    if last_offset is not None:
        # Start from where we left off
        start_offset = last_offset
        logger.info(f"🔢 Continuing offset-based extraction for {endpoint_key} from offset {start_offset}")
    else:
        # First run, start from 0
        start_offset = 0
        logger.info(f"🔢 First offset-based extraction for {endpoint_key} starting from offset 0")
    
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
    
    logger.info(f"🔧 Adding {len(new_columns)} new columns to {full_table}: {sorted(new_columns)}")
    
    for column in sorted(new_columns):
        try:
            alter_sql = f"ALTER TABLE {full_table} ADD COLUMN `{column}` String"
            client.command(alter_sql)
            logger.info(f"✅ Added column `{column}` to {full_table}")
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.info(f"ℹ️ Column `{column}` already exists in {full_table}")
            else:
                logger.error(f"❌ Failed to add column `{column}` to {full_table}: {e}")
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
        logger.info(f"🆕 Created partitioned table {full_table}")
        
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
    """Load DataFrame with comprehensive verification, duplicate prevention, and schema evolution support."""
    import clickhouse_connect
    
    raw_schema = CONFIG['warehouse']['schemas']['raw_schema']
    
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
            
            existing_columns = get_table_columns(client, full_table)
            add_missing_columns(client, full_table, df, existing_columns)
            
        else:
            create_table_from_dataframe(client, full_table, df)
        
        unique_timestamps = df['_extracted_at'].nunique()
        if unique_timestamps != 1:
            raise ValueError(f"Data integrity error: {unique_timestamps} different timestamps")
        
        actual_timestamp = df['_extracted_at'].iloc[0]
        if actual_timestamp != extraction_timestamp:
            raise ValueError(f"Timestamp mismatch: expected {extraction_timestamp}, got {actual_timestamp}")
        
        df_clean = df.copy()
        df_clean.columns = [col.replace(' ', '_').replace('-', '_') for col in df_clean.columns]
        df_str = df_clean.astype(str).replace(['nan', 'None', 'null', '<NA>'], '')
        
        client.insert_df(table=full_table, df=df_str)
        
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

# ---------------- PAGINATION + FETCH WITH ERROR HANDLING (UPDATED FOR INCREMENTAL STRATEGIES) ---------------- #
def get_paginated_data(session, endpoint_config, endpoint_name):
    """Get data from LeafLink endpoint with proper incremental strategy support."""
    all_data = []
    base_url = CONFIG['api']['base_url']
    max_retries = 3
    
    company_id = os.getenv('LEAFLINK_COMPANY_ID')
    
    if company_id and endpoint_config.get('supports_company_scope'):
        endpoint_url_base = f"{base_url}/companies/{company_id}/{endpoint_config['path']}/"
        logger.info(f"   Using company-scoped endpoint for company ID: {company_id}")
    else:
        endpoint_url_base = f"{base_url}/{endpoint_config['path']}/"
        if endpoint_config.get('supports_company_scope'):
            logger.warning("⚠️ Company ID not provided but endpoint supports company scoping")
            logger.warning("   Set LEAFLINK_COMPANY_ID environment variable for company-specific data")
    
    params = {
        'limit': endpoint_config['limit']
    }
    
    # Determine incremental strategy and set parameters accordingly
    strategy = _get_incremental_strategy(endpoint_name)
    
    if strategy == 'date_based':
        start_date, end_date = _get_incremental_date_range(endpoint_name)
        if start_date and end_date:
            date_filters = endpoint_config.get('date_filters', {})
            for param_name, date_type in date_filters.items():
                if date_type == 'incremental_start':
                    params[param_name] = start_date.isoformat()
                elif date_type == 'incremental_end':
                    params[param_name] = end_date.isoformat()
            logger.info(f"   Using date-based incremental: {start_date.isoformat()} to {end_date.isoformat()}")
    
    elif strategy == 'offset_based':
        start_offset = _get_incremental_offset_range(endpoint_name)
        logger.info(f"   Using offset-based incremental starting from offset: {start_offset}")
        # We'll handle offset in the pagination loop
    
    elif strategy == 'parent_driven':
        logger.info(f"   Using parent-driven incremental - will extract all data related to recent parent changes")
        # Parent-driven endpoints extract all data but rely on parent endpoint's incremental logic
    
    else:  # full_refresh
        logger.info(f"   Using full refresh strategy")
    
    # Add include_children and fields_add if specified
    if endpoint_config.get('include_children'):
        params['include_children'] = ','.join(endpoint_config['include_children'])
    
    if endpoint_config.get('fields_add'):
        params['fields_add'] = ','.join(endpoint_config['fields_add'])
    
    # Initialize offset handling
    if strategy == 'offset_based':
        offset = _get_incremental_offset_range(endpoint_name)
    else:
        offset = 0
    
    page_count = 0
    max_pages = 1000
    last_successful_offset = offset
    
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
                
                records = data.get(endpoint_config['data_field'], [])
                total_count = data.get(endpoint_config['total_count_field'], 0)
                next_url = data.get(endpoint_config['next_field'])
                
                logger.info(f"   Retrieved {len(records)} records (total available: {total_count})")
                logger.info(f"   Total collected so far: {len(all_data)}")
                
                if not records:
                    logger.info(f"   No more records available, stopping pagination")
                    # For offset-based incremental, update the final offset
                    if strategy == 'offset_based':
                        _update_offset(endpoint_name, last_successful_offset)
                    return all_data
                
                all_data.extend(records)
                last_successful_offset = offset + len(records)
                
                if TESTING_MODE and MAX_RECORDS_PER_ENDPOINT and len(all_data) >= MAX_RECORDS_PER_ENDPOINT:
                    all_data = all_data[:MAX_RECORDS_PER_ENDPOINT]
                    logger.info(f"   Reached testing limit of {MAX_RECORDS_PER_ENDPOINT} records")
                    # For offset-based incremental, update offset even in testing mode
                    if strategy == 'offset_based':
                        _update_offset(endpoint_name, last_successful_offset)
                    return all_data
                
                if len(records) < endpoint_config['limit'] or not next_url:
                    logger.info(f"   Reached end of data (got {len(records)} < {endpoint_config['limit']} or no next URL)")
                    # For offset-based incremental, update the final offset
                    if strategy == 'offset_based':
                        _update_offset(endpoint_name, last_successful_offset)
                    return all_data
                
                offset += endpoint_config['limit']
                smart_rate_limit(response, CONFIG)
                success = True
                break
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"   Request error on page {page_count}, attempt {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    if page_count == 1:
                        raise
                    else:
                        logger.warning(f"   Giving up on page {page_count} after {max_retries} attempts")
                        # For offset-based incremental, save progress even on failure
                        if strategy == 'offset_based' and last_successful_offset > _get_incremental_offset_range(endpoint_name):
                            _update_offset(endpoint_name, last_successful_offset)
                        return all_data
                time.sleep(2 ** attempt)
            
            except Exception as e:
                logger.error(f"   Unexpected error on page {page_count}, attempt {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    logger.error(f"   Giving up on page {page_count} after {max_retries} attempts")
                    # For offset-based incremental, save progress even on failure
                    if strategy == 'offset_based' and last_successful_offset > _get_incremental_offset_range(endpoint_name):
                        _update_offset(endpoint_name, last_successful_offset)
                    return all_data
                time.sleep(2 ** attempt)
        
        if not success:
            logger.error(f"   Page {page_count} failed completely, stopping")
            # For offset-based incremental, save progress even on failure
            if strategy == 'offset_based' and last_successful_offset > _get_incremental_offset_range(endpoint_name):
                _update_offset(endpoint_name, last_successful_offset)
            break
    
    logger.info(f"✅ {endpoint_name}: Collected {len(all_data)} total records")
    
    # For offset-based incremental, save final offset
    if strategy == 'offset_based':
        _update_offset(endpoint_name, last_successful_offset)
    
    return all_data

# ---------------- STATE MANAGEMENT AFTER SUCCESSFUL LOAD (UPDATED FOR MULTIPLE STRATEGIES) ---------------- #
def update_state_after_verified_load(endpoint_key, endpoint_config, df, raw_data, extracted_at):
    """Update state ONLY after verified warehouse load - supports multiple incremental strategies."""
    try:
        logger.info(f"📝 Updating state after VERIFIED load for {endpoint_key}")
        
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
                        logger.info(f"🕒 Updated date-based watermark from data: {max_ts.isoformat()}")
                    else:
                        _update_watermark(endpoint_key, extracted_at)
                        logger.info(f"🕒 Fallback date-based watermark: {extracted_at.isoformat()}")
                else:
                    logger.warning(f"⚠️ Incremental field '{incremental_field}' not found in {endpoint_key} data")
                    _update_watermark(endpoint_key, extracted_at)
                    logger.info(f"🕒 Extraction timestamp watermark: {extracted_at.isoformat()}")
            else:
                _update_watermark(endpoint_key, extracted_at)
                logger.info(f"🕒 No incremental field watermark: {extracted_at.isoformat()}")
        
        elif strategy == 'offset_based':
            # Offset already updated during pagination in get_paginated_data
            logger.info(f"🔢 Offset-based watermark already updated during pagination")
            # Also set extraction time for reference
            # _update_watermark(endpoint_key, extracted_at)
        
        elif strategy == 'parent_driven':
            # For parent-driven endpoints, update extraction timestamp
            _update_watermark(endpoint_key, extracted_at)
            logger.info(f"🔗 Parent-driven watermark: {extracted_at.isoformat()}")
        
        else:  # full_refresh
            _update_watermark(endpoint_key, extracted_at)
            logger.info(f"🔄 Full refresh watermark: {extracted_at.isoformat()}")
        
        _save_state()
        logger.info("✅ State saved after verified load")
        
    except Exception as e:
        logger.error(f"❌ Failed to update state: {e}")
        logger.error("❌ CRITICAL: Next run may have data gaps!")
        raise

# ---------------- MAIN EXTRACTION FUNCTION (UPDATED FOR MULTIPLE STRATEGIES) ---------------- #
def extract_leaflink_endpoint(endpoint_key, **context):
    """ATOMIC extraction with bulletproof state management and multiple incremental strategies."""
    if endpoint_key not in ENABLED_ENDPOINTS:
        logger.warning(f"⚠️ Endpoint {endpoint_key} not enabled")
        return 0

    execution_dt = context.get('logical_date') or _utc_now()
    endpoint_config = LEAFLINK_ENDPOINTS.get(endpoint_key)
    
    if not endpoint_config:
        logger.error(f"❌ Unknown endpoint: {endpoint_key}")
        return 0

    # Check if we should skip reference data extraction
    if _should_skip_reference_data_extraction(endpoint_key):
        return 0

    strategy = _get_incremental_strategy(endpoint_key)
    logger.info(f"🔄 ATOMIC extraction for {endpoint_key} using {strategy} strategy")
    
    state_backup = None
    with _STATE_LOCK:
        state_backup = _STATE_CACHE.copy()
    
    logger.info(f"💾 State backed up for rollback safety")

    try:
        session = create_authenticated_session()
        raw_data = get_paginated_data(session, endpoint_config, endpoint_key)

        if not raw_data:
            logger.warning(f"⚠️ No data returned for {endpoint_key}")
            # For reference data and some strategies, still update watermark even if no data
            if strategy in ['full_refresh', 'offset_based']:
                if strategy == 'full_refresh':
                    _update_watermark(endpoint_key, _utc_now())
                # offset_based already updated in get_paginated_data
                _save_state()
                logger.info(f"✅ Updated watermark for {strategy} strategy with no results")
            return 0

        flattened = []
        for i, record in enumerate(raw_data):
            try:
                flattened.append(flatten_leaflink_record(record))
            except Exception as e:
                logger.warning(f"   Failed to flatten record {i}: {e}")
                continue

        if not flattened:
            logger.warning(f"⚠️ No valid records after flattening for {endpoint_key}")
            # For reference data and some strategies, still update watermark even if no valid data
            if strategy in ['full_refresh', 'offset_based']:
                if strategy == 'full_refresh':
                    _update_watermark(endpoint_key, _utc_now())
                # offset_based already updated in get_paginated_data
                _save_state()
                logger.info(f"✅ Updated watermark for {strategy} strategy with no valid records")
            return 0

        df = pd.DataFrame(flattened)
        extracted_at = _utc_now()
        df['_extracted_at'] = extracted_at.isoformat()
        df['_source_system'] = 'leaflink'
        df['_endpoint'] = endpoint_key

        logger.info(f"   📊 Prepared {len(df)} records for warehouse load")

        try:
            records_loaded = load_dataframe_to_warehouse_verified(df, endpoint_key, extracted_at)
            
            if records_loaded == 0:
                logger.warning(f"⚠️ No records actually loaded for {endpoint_key}")
                return 0
                
            logger.info(f"✅ VERIFIED: {records_loaded} records loaded to warehouse")
            
        except Exception as warehouse_error:
            logger.error(f"❌ WAREHOUSE LOAD FAILED: {warehouse_error}")
            logger.error("🔄 RESTORING original state (no changes made)")
            
            with _STATE_LOCK:
                _STATE_CACHE.clear()
                _STATE_CACHE.update(state_backup)
            
            raise

        try:
            update_state_after_verified_load(endpoint_key, endpoint_config, df, raw_data, extracted_at)
            logger.info("✅ State updated after verified warehouse load")
            
        except Exception as state_error:
            logger.error(f"❌ STATE UPDATE FAILED: {state_error}")
            logger.warning("⚠️ Data loaded but state may be inconsistent - next run will detect")
            
        return records_loaded

    except Exception as e:
        logger.error(f"❌ EXTRACTION FAILED for {endpoint_key}: {e}")
        
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
    
    for dep in dependencies:
        if dep not in completed_endpoints:
            logger.info(f"⏳ {endpoint_key} waiting for dependency {dep}")
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
    
    for tier in execution_tiers:
        tier_endpoints = [ep for ep in tier if ep in endpoints]
        
        tier_endpoints.sort(key=lambda ep: {
            'high': 0, 'medium': 1, 'low': 2
        }.get(LEAFLINK_ENDPOINTS.get(ep, {}).get('priority', 'medium'), 1))
        
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
            logger.warning(f"⚠️ Adding remaining endpoints without dependency check: {endpoints}")
            ordered.extend(endpoints)
            break
    
    logger.info(f"📋 Endpoint execution order: {ordered}")
    
    order_related = [ep for ep in ordered if ep.startswith('order') or ep in ['line_items']]
    product_related = [ep for ep in ordered if ep.startswith('product') or ep in ['products', 'strains']]
    crm_related = [ep for ep in ordered if ep.startswith('customer') or ep in ['customers', 'contacts', 'activity_entries']]
    company_related = [ep for ep in ordered if ep.startswith('company') or ep.startswith('license') or ep in ['companies', 'brands', 'promocodes', 'reports']]
    
    if order_related:
        logger.info(f"📦 Order-related endpoints: {order_related}")
    if product_related:
        logger.info(f"🏷️ Product catalog endpoints: {product_related}")
    if crm_related:
        logger.info(f"👥 CRM-related endpoints: {crm_related}")
    if company_related:
        logger.info(f"🏢 Company & operational endpoints: {company_related}")
    
    return ordered

# ---------------- DAG INTEGRATION FUNCTIONS ---------------- #
def finalize_state_after_warehouse_load(context):
    """Finalize state updates after successful warehouse loading."""
    try:
        logger.info("✅ All state updates finalized after successful warehouse loads")
        
        try:
            with open(_state_path(), 'r') as f:
                state_content = f.read()
                json.loads(state_content)
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
            endpoint_config = LEAFLINK_ENDPOINTS.get(endpoint_key, {})
            dependencies = endpoint_config.get('depends_on', [])
            strategy = _get_incremental_strategy(endpoint_key)
            
            for dep in dependencies:
                if dep not in completed_endpoints and dep in ENABLED_ENDPOINTS:
                    logger.warning(f"⚠️ {endpoint_key} dependency {dep} not completed")
            
            logger.info(f"📊 Extracting {endpoint_key} using {strategy} strategy...")
            records = extract_leaflink_endpoint(endpoint_key, **context)
            total_records += records
            extraction_results[endpoint_key] = records
            completed_endpoints.add(endpoint_key)
            
            logger.info(f"✅ {endpoint_key}: {records} records loaded")
            
        except Exception as e:
            logger.error(f"❌ {endpoint_key} failed: {e}")
            extraction_results[endpoint_key] = f"Failed: {e}"
            continue
    
    logger.info("📈 LeafLink Extraction Summary:")
    for endpoint, result in extraction_results.items():
        if isinstance(result, int):
            strategy = _get_incremental_strategy(endpoint)
            logger.info(f"   {endpoint} ({strategy}): {result} records")
        else:
            logger.info(f"   {endpoint}: {result}")
    
    logger.info(f"✅ Coordinated extraction complete. Total records: {total_records}")
    
    return {
        'total_records': total_records,
        'extraction_results': extraction_results,
        'completed_endpoints': list(completed_endpoints)
    }