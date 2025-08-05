# extractor.py - Google Sheets extractor with full refresh and incremental loading support
# Path: root/extractors/google_sheets/extractor.py

from datetime import datetime, timedelta, timezone
import os
import json
import pandas as pd
from typing import Dict, Any, List, Optional
import logging
import threading
import hashlib
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------- GLOBALS (initialized via init_extractor) ---------------- #
CONFIG: Dict[str, Any] = {}
TESTING_MODE: bool = False
MAX_RECORDS_PER_SHEET: Optional[int] = None
SHEETS_CONFIG: Dict[str, Dict[str, Any]] = {}
ENABLED_SHEETS: Dict[str, str] = {}

# ---------------- THREAD-SAFE STATE MANAGEMENT ---------------- #
_GOOGLE_SERVICE = None

def _utc_now():
    return datetime.utcnow().replace(tzinfo=timezone.utc)

# ---------------- SHEET DEFINITIONS ---------------- #
def _get_sheet_configs():
    """Get sheet configurations from config."""
    return CONFIG.get('extraction', {}).get('sheets', {})

# ---------------- INIT ---------------- #
def init_extractor(config):
    """Initialize extractor module with loaded YAML config."""
    global CONFIG, TESTING_MODE, MAX_RECORDS_PER_SHEET, SHEETS_CONFIG, ENABLED_SHEETS
    CONFIG = config
    
    TESTING_MODE = config['extraction']['mode'] == 'testing'
    if TESTING_MODE:
        MAX_RECORDS_PER_SHEET = config['extraction']['testing']['max_records_per_sheet']
    else:
        MAX_RECORDS_PER_SHEET = config['extraction']['production']['max_records_per_sheet']
    
    # Build enabled sheets from config
    SHEETS_CONFIG = _get_sheet_configs()
    ENABLED_SHEETS = {
        sheet_name: sheet_config.get('range', f'{sheet_name}!A:Z')
        for sheet_name, sheet_config in SHEETS_CONFIG.items()
        if sheet_config.get('enabled', True)
    }
    
    logger.info("=" * 80)
    logger.info("🚀 GOOGLE SHEETS EXTRACTOR INITIALIZATION")
    logger.info("=" * 80)
    logger.info(f"📋 Enabled Sheets: {len(ENABLED_SHEETS)} sheets")
    
    # Get spreadsheet info for logging
    try:
        spreadsheet_id = get_spreadsheet_id()
        logger.info(f"📊 Spreadsheet ID: {spreadsheet_id}")
    except:
        logger.info(f"📊 Spreadsheet ID: <will be determined at runtime>")
    
    for sheet_name in sorted(ENABLED_SHEETS.keys()):
        sheet_config = SHEETS_CONFIG.get(sheet_name, {})
        refresh_type = "FULL REFRESH" if sheet_config.get('full_refresh', False) else "INCREMENTAL"
        range_spec = sheet_config.get('range', f'{sheet_name}!A:Z')
        skip_rows = sheet_config.get('skip_rows', 0)
        priority = sheet_config.get('priority', 'medium').upper()
        
        logger.info(f"   ├─ {sheet_name:<25} [{refresh_type}]")
        logger.info(f"   │  ├─ Range: {range_spec}")
        logger.info(f"   │  ├─ Skip Rows: {skip_rows}")
        logger.info(f"   │  └─ Priority: {priority}")
    
    logger.info("=" * 80)
    return ENABLED_SHEETS

# ---------------- GOOGLE SHEETS AUTH ---------------- #
def create_google_sheets_service():
    """Create authenticated Google Sheets service."""
    global _GOOGLE_SERVICE
    
    # Return cached service if available
    if _GOOGLE_SERVICE is not None:
        logger.info("🔄 Service: Reusing existing Google Sheets service")
        return _GOOGLE_SERVICE
    
    # Get credentials path from environment
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if not credentials_path:
        raise ValueError("Missing required environment variable: GOOGLE_APPLICATION_CREDENTIALS")
    
    if not os.path.exists(credentials_path):
        raise ValueError(f"Google credentials file not found: {credentials_path}")
    
    try:
        # Load service account credentials
        credentials = Credentials.from_service_account_file(
            credentials_path,
            scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
        )
        
        # Build the service
        service = build('sheets', 'v4', credentials=credentials)
        
        # Test the service with a simple call
        spreadsheet_id = get_spreadsheet_id()
        service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        
        logger.info("✅ Authentication: Successfully authenticated Google Sheets service")
        
        # Cache the service
        _GOOGLE_SERVICE = service
        return service
        
    except Exception as e:
        logger.error(f"❌ Authentication: Failed to create Google Sheets service - {e}")
        raise

def get_spreadsheet_id():
    """Get spreadsheet ID from config, supporting both ID and URL formats."""
    # Try to get spreadsheet ID directly
    spreadsheet_id = CONFIG['google_sheets'].get('spreadsheet_id')
    
    if spreadsheet_id:
        # If it looks like a URL, extract the ID
        if 'docs.google.com/spreadsheets' in spreadsheet_id:
            # Extract ID from URL: https://docs.google.com/spreadsheets/d/{ID}/edit...
            import re
            match = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', spreadsheet_id)
            if match:
                extracted_id = match.group(1)
                logger.info(f"📋 Config: Extracted spreadsheet ID from URL: {extracted_id}")
                return extracted_id
            else:
                raise ValueError(f"Could not extract spreadsheet ID from URL: {spreadsheet_id}")
        else:
            # It's already an ID
            return spreadsheet_id
    
    # Try alternative URL field (if configured)
    spreadsheet_url = CONFIG['google_sheets'].get('spreadsheet_url')
    if spreadsheet_url:
        import re
        match = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', spreadsheet_url)
        if match:
            extracted_id = match.group(1)
            logger.info(f"📋 Config: Extracted spreadsheet ID from URL: {extracted_id}")
            return extracted_id
        else:
            raise ValueError(f"Could not extract spreadsheet ID from URL: {spreadsheet_url}")
    
    raise ValueError("Missing spreadsheet configuration. Set GOOGLE_SPREADSHEET_ID environment variable.")

def test_google_sheets_connection():
    """Test Google Sheets connection."""
    try:
        service = create_google_sheets_service()
        spreadsheet_id = get_spreadsheet_id()
        
        # Get spreadsheet metadata
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        title = spreadsheet.get('properties', {}).get('title', 'Unknown')
        sheets = spreadsheet.get('sheets', [])
        
        logger.info("=" * 80)
        logger.info("✅ CONNECTION TEST SUCCESSFUL")
        logger.info("=" * 80)
        logger.info(f"📊 Spreadsheet: '{title}'")
        logger.info(f"📋 Total Sheets: {len(sheets)}")
        logger.info(f"📊 Spreadsheet ID: {spreadsheet_id}")
        
        # List available sheets
        sheet_names = [sheet['properties']['title'] for sheet in sheets]
        logger.info(f"📋 Available Sheets:")
        for sheet_name in sheet_names:
            logger.info(f"   ├─ {sheet_name}")
        logger.info("=" * 80)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Connection: Google Sheets connection test failed - {e}")
        raise

# ---------------- UTILITIES ---------------- #
def clean_sheet_data(df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    """Clean and prepare sheet data for warehouse loading."""
    if df.empty:
        return df
    
    logger.info(f"🧹 Cleaning: Processing {sheet_name} data...")
    
    # Remove completely empty rows
    original_rows = len(df)
    df = df.dropna(how='all')
    empty_rows_removed = original_rows - len(df)
    
    # Remove completely empty columns
    original_cols = len(df.columns)
    df = df.dropna(axis=1, how='all')
    empty_cols_removed = original_cols - len(df.columns)
    
    if empty_rows_removed > 0:
        logger.info(f"   ├─ Removed {empty_rows_removed} empty rows")
    if empty_cols_removed > 0:
        logger.info(f"   ├─ Removed {empty_cols_removed} empty columns")
    
    if df.empty:
        logger.warning(f"⚠️ Cleaning: {sheet_name} is empty after removing empty rows/columns")
        return df
    
    # Clean column names
    df.columns = df.columns.astype(str)
    original_columns = df.columns.copy()
    df.columns = [col.strip().replace(' ', '_').replace('-', '_').replace('.', '_') 
                  for col in df.columns]
    
    # Log column name changes
    changed_columns = [(orig, new) for orig, new in zip(original_columns, df.columns) if orig != new]
    if changed_columns:
        logger.info(f"   ├─ Cleaned {len(changed_columns)} column names")
    
    # Remove duplicate column names by adding suffix
    cols = pd.Series(df.columns)
    duplicates_found = len(cols[cols.duplicated()])
    if duplicates_found > 0:
        for dup in cols[cols.duplicated()].unique():
            cols[cols[cols == dup].index.values.tolist()] = [f"{dup}_{i}" if i != 0 else dup 
                                                            for i in range(sum(cols == dup))]
        df.columns = cols
        logger.info(f"   ├─ Resolved {duplicates_found} duplicate column names")
    
    # Convert all data to string to handle mixed types
    for col in df.columns:
        df[col] = df[col].astype(str).replace(['nan', 'None', 'null', '<NA>', ''], None)
    
    # Add row number for tracking
    df['_row_number'] = range(1, len(df) + 1)
    
    logger.info(f"✅ Cleaning: {sheet_name} → {len(df)} rows × {len(df.columns)} columns")
    return df

# ---------------- WAREHOUSE LOADING FUNCTIONS ---------------- #
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

def create_table_from_dataframe(client, full_table, df, is_full_refresh=False):
    """Create ClickHouse table from DataFrame structure with proper partitioning."""
    columns = []
    for col in df.columns:
        columns.append(f"`{col}` String")
    
    if is_full_refresh:
        # For full refresh tables, partition by a simple hash for better distribution
        create_ddl = f"""
        CREATE TABLE {full_table} (
            {', '.join(columns)}
        ) ENGINE = MergeTree()
        PARTITION BY toDate(parseDateTimeBestEffort(_extracted_at))
        ORDER BY (_extracted_at, {df.columns[0] if len(df.columns) > 0 else '_extracted_at'})
        SETTINGS index_granularity = 8192
        """
    else:
        # For incremental tables, partition by extraction date for better performance
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
        refresh_type = "full refresh" if is_full_refresh else "incremental"
        logger.info(f"🆕 Table: Created partitioned table {full_table} ({refresh_type})")
        
        # Verify partitioning
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

def load_dataframe_to_warehouse_verified(df, sheet_name, extracted_at, is_full_refresh=False):
    """Load DataFrame with comprehensive verification and full refresh support."""
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
                logger.info(f"✅ Database: {raw_schema} ready")
            except Exception as db_error:
                logger.error(f"❌ Database: Failed to create {raw_schema}: {db_error}")
                raise ValueError(f"Cannot access or create database {raw_schema}: {db_error}")
        
        table_name = f"raw_{sheet_name}"
        full_table = f"`{raw_schema}`.`{table_name}`"
        extraction_timestamp = extracted_at.isoformat()
        
        if is_full_refresh:
            # For full refresh, drop and recreate the table
            if table_exists(client, full_table):
                logger.info(f"🔄 Full Refresh: Dropping existing table {full_table}")
                client.command(f"DROP TABLE {full_table}")
            
            create_table_from_dataframe(client, full_table, df, is_full_refresh=True)
            logger.info(f"✅ Full Refresh: Table {full_table} created")
        else:
            # For incremental, check for duplicates and handle schema evolution
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
                    return 0
                
                logger.info(f"✅ Warehouse: No duplicates found for {extraction_timestamp}")
                
                existing_columns = get_table_columns(client, full_table)
                add_missing_columns(client, full_table, df, existing_columns)
                
            else:
                create_table_from_dataframe(client, full_table, df, is_full_refresh=False)
        
        # Validate timestamps in data
        unique_timestamps = df['_extracted_at'].nunique()
        if unique_timestamps != 1:
            raise ValueError(f"Data integrity error: {unique_timestamps} different timestamps")
        
        actual_timestamp = df['_extracted_at'].iloc[0]
        if actual_timestamp != extraction_timestamp:
            raise ValueError(f"Timestamp mismatch: expected {extraction_timestamp}, got {actual_timestamp}")
        
        # Prepare data for loading
        df_clean = df.copy()
        df_clean.columns = [col.replace(' ', '_').replace('-', '_') for col in df_clean.columns]
        df_str = df_clean.astype(str).replace(['nan', 'None', 'null', '<NA>'], '')
        
        # Load data
        client.insert_df(table=full_table, df=df_str)
        
        # Verify load
        if is_full_refresh:
            # For full refresh, count all records
            verification_check = client.query(f"""
                SELECT count() 
                FROM {full_table}
            """)
        else:
            # For incremental, count records with this timestamp
            verification_check = client.query(f"""
                SELECT count() 
                FROM {full_table} 
                WHERE _extracted_at = '{extraction_timestamp}'
            """)
        
        actual_loaded = verification_check.result_rows[0][0]
        
        if actual_loaded != len(df):
            raise ValueError(f"Warehouse: Load verification failed - expected {len(df)}, loaded {actual_loaded}")
        
        refresh_type = "full refresh" if is_full_refresh else "incremental"
        logger.info(f"✅ Warehouse: Loaded {actual_loaded} rows into {full_table} ({refresh_type})")
        return actual_loaded
        
    except Exception as e:
        logger.error(f"❌ Warehouse: Load failed - {e}")
        raise
    finally:
        client.close()

# ---------------- SHEET EXTRACTION ---------------- #
def get_sheet_data(service, sheet_name, sheet_config):
    """Get data from Google Sheets."""
    try:
        spreadsheet_id = get_spreadsheet_id()
        
        # Get the range to read
        range_name = sheet_config.get('range', f'{sheet_name}!A:Z')
        
        # Check if we should skip header rows
        skip_rows = sheet_config.get('skip_rows', 0)
        
        logger.info(f"📊 API: Reading range {range_name}")
        if skip_rows > 0:
            logger.info(f"   ├─ Skip Rows: {skip_rows}")
        
        # Get the data
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueRenderOption='UNFORMATTED_VALUE',
            dateTimeRenderOption='FORMATTED_STRING'
        ).execute()
        
        values = result.get('values', [])
        
        if not values:
            logger.warning(f"⚠️ API: No data found in sheet {sheet_name}")
            return pd.DataFrame()
        
        logger.info(f"   ├─ Raw Rows: {len(values)}")
        
        # Skip header rows if specified
        if skip_rows > 0:
            if len(values) <= skip_rows:
                logger.warning(f"⚠️ API: Not enough rows to skip {skip_rows} rows in {sheet_name}")
                return pd.DataFrame()
            values = values[skip_rows:]
            logger.info(f"   ├─ After Skip: {len(values)} rows")
        
        # Create DataFrame
        if len(values) > 0:
            # Use first row as headers
            headers = values[0] if values else []
            data_rows = values[1:] if len(values) > 1 else []
            
            # Ensure all rows have the same number of columns
            max_cols = len(headers) if headers else 0
            if data_rows:
                max_cols = max(max_cols, max(len(row) for row in data_rows))
            
            # Pad headers if necessary
            original_header_count = len(headers)
            while len(headers) < max_cols:
                headers.append(f'Column_{len(headers) + 1}')
            
            if len(headers) > original_header_count:
                logger.info(f"   ├─ Added {len(headers) - original_header_count} auto-generated column headers")
            
            # Pad data rows if necessary
            padded_rows = []
            for row in data_rows:
                padded_row = row + [''] * (max_cols - len(row))
                padded_rows.append(padded_row)
            
            df = pd.DataFrame(padded_rows, columns=headers)
        else:
            df = pd.DataFrame()
        
        logger.info(f"✅ API: Retrieved {len(df)} data rows × {len(df.columns)} columns from {sheet_name}")
        return df
        
    except Exception as e:
        logger.error(f"❌ API: Failed to get data from sheet {sheet_name}: {e}")
        raise

# ---------------- MAIN EXTRACTION FUNCTION ---------------- #
def extract_google_sheet(sheet_name, **context):
    """Extract data from a single Google Sheet."""
    logger.info("=" * 100)
    logger.info(f"🚀 SHEET EXTRACTION START")
    logger.info("=" * 100)
    
    if sheet_name not in ENABLED_SHEETS:
        logger.warning(f"⏭️ Skip: {sheet_name} not enabled in configuration")
        logger.info("=" * 100)
        return 0

    execution_dt = context.get('logical_date') or _utc_now()
    sheet_config = SHEETS_CONFIG.get(sheet_name, {})
    is_full_refresh = sheet_config.get('full_refresh', False)
    
    refresh_type = "FULL REFRESH" if is_full_refresh else "INCREMENTAL"
    range_spec = sheet_config.get('range', f'{sheet_name}!A:Z')
    skip_rows = sheet_config.get('skip_rows', 0)
    priority = sheet_config.get('priority', 'medium').upper()
    
    # Header information
    logger.info(f"📊 Sheet: {sheet_name}")
    logger.info(f"📈 Strategy: {refresh_type}")
    logger.info(f"📋 Range: {range_spec}")
    logger.info(f"⏩ Skip Rows: {skip_rows}")
    logger.info(f"⚡ Priority: {priority}")
    logger.info(f"⏰ Execution: {execution_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    
    logger.info("-" * 100)

    try:
        service = create_google_sheets_service()
        
        # Get raw data from sheet
        raw_df = get_sheet_data(service, sheet_name, sheet_config)

        if raw_df.empty:
            logger.warning(f"📭 No data returned from Google Sheets API")
            logger.info("=" * 100)
            return 0

        # Apply testing limits if in testing mode
        original_rows = len(raw_df)
        if TESTING_MODE and MAX_RECORDS_PER_SHEET and len(raw_df) > MAX_RECORDS_PER_SHEET:
            raw_df = raw_df.head(MAX_RECORDS_PER_SHEET)
            logger.info(f"🧪 Testing: Limited from {original_rows} to {MAX_RECORDS_PER_SHEET} records")

        # Clean the data
        df = clean_sheet_data(raw_df, sheet_name)
        
        if df.empty:
            logger.warning(f"📭 No valid data after cleaning")
            logger.info("=" * 100)
            return 0

        # Add metadata columns
        extracted_at = _utc_now()
        df['_extracted_at'] = extracted_at.isoformat()
        df['_source_system'] = 'google_sheets'
        df['_sheet_name'] = sheet_name
        df['_refresh_type'] = refresh_type

        logger.info(f"📊 DataFrame: {len(df)} rows × {len(df.columns)} columns")
        logger.info(f"⏰ Extracted: {extracted_at.strftime('%Y-%m-%d %H:%M:%S')} UTC")

        try:
            records_loaded = load_dataframe_to_warehouse_verified(
                df, sheet_name, extracted_at, is_full_refresh=is_full_refresh
            )
            
            if records_loaded == 0:
                logger.warning(f"⚠️ WARNING: {sheet_name} completed but no records loaded")
                logger.info("=" * 100)
                return 0
                
            logger.info(f"✅ SUCCESS: {sheet_name} completed - {records_loaded} records loaded ({refresh_type})")
            logger.info("=" * 100)
            return records_loaded
            
        except Exception as warehouse_error:
            logger.error(f"❌ WAREHOUSE LOAD FAILED: {warehouse_error}")
            logger.info("=" * 100)
            raise

    except Exception as e:
        logger.error(f"❌ FAILED: {sheet_name} extraction failed")
        logger.error(f"❌ Error: {str(e)}")
        logger.info("=" * 100)
        raise

# ---------------- SHEET EXECUTION ORDER ---------------- #
def get_sheet_execution_order():
    """Get sheets in execution order based on dependencies."""
    sheets = list(ENABLED_SHEETS.keys())
    ordered = []
    
    # Simple ordering: full refresh sheets first, then incremental
    full_refresh_sheets = []
    incremental_sheets = []
    
    for sheet in sheets:
        sheet_config = SHEETS_CONFIG.get(sheet, {})
        if sheet_config.get('full_refresh', False):
            full_refresh_sheets.append(sheet)
        else:
            incremental_sheets.append(sheet)
    
    # Sort by priority if specified
    def get_priority(sheet_name):
        priority_map = {'high': 0, 'medium': 1, 'low': 2}
        sheet_config = SHEETS_CONFIG.get(sheet_name, {})
        return priority_map.get(sheet_config.get('priority', 'medium'), 1)
    
    full_refresh_sheets.sort(key=get_priority)
    incremental_sheets.sort(key=get_priority)
    
    ordered = full_refresh_sheets + incremental_sheets
    
    logger.info("📋 Execution Planning:")
    
    if full_refresh_sheets:
        logger.info("   Full Refresh Sheets:")
        for sheet in full_refresh_sheets:
            sheet_config = SHEETS_CONFIG.get(sheet, {})
            range_spec = sheet_config.get('range', f'{sheet}!A:Z')
            priority = sheet_config.get('priority', 'medium').upper()
            logger.info(f"      ├─ {sheet:<25} [{priority:<6}] {range_spec}")
    
    if incremental_sheets:
        logger.info("   Incremental Sheets:")
        for sheet in incremental_sheets:
            sheet_config = SHEETS_CONFIG.get(sheet, {})
            range_spec = sheet_config.get('range', f'{sheet}!A:Z')
            priority = sheet_config.get('priority', 'medium').upper()
            logger.info(f"      ├─ {sheet:<25} [{priority:<6}] {range_spec}")
    
    logger.info(f"✅ Final Order: {len(ordered)} sheets planned")
    
    return ordered

# ---------------- DAG INTEGRATION FUNCTIONS ---------------- #
def extract_filtered_sheets(sheets_to_extract, **context):
    """Extract only the specified sheets."""
    logger.info("=" * 100)
    logger.info("🚀 MULTI-SHEET EXTRACTION START")
    logger.info("=" * 100)
    logger.info(f"📊 Sheets to Extract: {len(sheets_to_extract)} sheets")
    logger.info(f"📊 Mode: {'TESTING' if TESTING_MODE else 'PRODUCTION'}")
    
    # Show what will be extracted
    for sheet_name in sheets_to_extract:
        sheet_config = SHEETS_CONFIG.get(sheet_name, {})
        refresh_type = "FULL REFRESH" if sheet_config.get('full_refresh', False) else "INCREMENTAL"
        range_spec = sheet_config.get('range', f'{sheet_name}!A:Z')
        logger.info(f"   ├─ {sheet_name:<25} [{refresh_type}] {range_spec}")
    
    logger.info("=" * 100)
    
    total_records = 0
    extraction_results = {}
    completed_sheets = set()
    
    for i, sheet_name in enumerate(sheets_to_extract, 1):
        logger.info(f"🔄 Progress: [{i:2d}/{len(sheets_to_extract)}] Processing {sheet_name}")
        
        try:
            sheet_config = SHEETS_CONFIG.get(sheet_name, {})
            is_full_refresh = sheet_config.get('full_refresh', False)
            refresh_type = "Full Refresh" if is_full_refresh else "Incremental"
            
            records = extract_google_sheet(sheet_name, **context)
            total_records += records
            extraction_results[sheet_name] = records
            completed_sheets.add(sheet_name)
            
            logger.info(f"✅ Completed: {sheet_name} → {records} records ({refresh_type})")
            
        except Exception as e:
            logger.error(f"❌ Failed: {sheet_name} → {str(e)}")
            extraction_results[sheet_name] = f"Failed: {e}"
            continue
    
    logger.info("=" * 100)
    logger.info("🎉 MULTI-SHEET EXTRACTION COMPLETE")
    logger.info("=" * 100)
    logger.info(f"📊 Total Records: {total_records:,}")
    logger.info(f"✅ Successful: {len(completed_sheets)}/{len(sheets_to_extract)} sheets")
    logger.info(f"❌ Failed: {len(sheets_to_extract) - len(completed_sheets)}/{len(sheets_to_extract)} sheets")
    
    # Summary by category
    successful = [sheet for sheet, result in extraction_results.items() if isinstance(result, int)]
    failed = [sheet for sheet, result in extraction_results.items() if isinstance(result, str)]
    
    if successful:
        logger.info("✅ Successful Sheets:")
        for sheet in successful:
            sheet_config = SHEETS_CONFIG.get(sheet, {})
            refresh_indicator = "[FULL REFRESH]" if sheet_config.get('full_refresh') else "[INCREMENTAL]"
            logger.info(f"   ├─ {sheet:<25} {refresh_indicator:<15} → {extraction_results[sheet]:>6,} records")
    
    if failed:
        logger.info("❌ Failed Sheets:")
        for sheet in failed:
            sheet_config = SHEETS_CONFIG.get(sheet, {})
            refresh_indicator = "[FULL REFRESH]" if sheet_config.get('full_refresh') else "[INCREMENTAL]"
            logger.info(f"   ├─ {sheet:<25} {refresh_indicator:<15} → {extraction_results[sheet]}")
    
    # Category breakdown
    full_refresh_sheets = [s for s in successful if SHEETS_CONFIG.get(s, {}).get('full_refresh', False)]
    incremental_sheets = [s for s in successful if not SHEETS_CONFIG.get(s, {}).get('full_refresh', False)]
    
    def format_sheet_list(sheet_list):
        """Format sheet list with refresh type indicators"""
        formatted = []
        for sheet in sheet_list:
            sheet_config = SHEETS_CONFIG.get(sheet, {})
            refresh_type = "[F]" if sheet_config.get('full_refresh') else "[I]"
            formatted.append(f"{sheet}{refresh_type}")
        return ', '.join(formatted)
    
    if full_refresh_sheets:
        logger.info(f"🔄 Full Refresh Sheets: {format_sheet_list(full_refresh_sheets)}")
    if incremental_sheets:
        logger.info(f"➕ Incremental Sheets: {format_sheet_list(incremental_sheets)}")
    
    logger.info("📝 Legend: [F] = Full Refresh, [I] = Incremental")
    logger.info("=" * 100)
    
    return {
        'total_records': total_records,
        'extraction_results': extraction_results,
        'completed_sheets': list(completed_sheets)
    }

def extract_all_sheets(**context):
    """Extract all sheets with proper handling of full refresh vs incremental."""
    # Get all enabled sheets
    all_sheets = [name for name, config in SHEETS_CONFIG.items() if config.get('enabled', True)]
    return extract_filtered_sheets(all_sheets, **context)