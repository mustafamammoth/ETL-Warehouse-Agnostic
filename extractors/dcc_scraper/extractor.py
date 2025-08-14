# extractor.py - DCC Cannabis License scraper with warehouse loading
# Path: root/extractors/dcc_scraper/extractor.py

from datetime import datetime, timezone
import os
import time
import pandas as pd
import logging
import shutil
from typing import Dict, Any, List, Optional
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.service import Service

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------- GLOBALS (initialized via init_extractor) ---------------- #
CONFIG: Dict[str, Any] = {}
TESTING_MODE: bool = False
MAX_RECORDS_PER_PREFIX: Optional[int] = None
LICENSE_PREFIXES: List[str] = []


import re

def _to_snake(name: str) -> str:
    """Convert column names to snake_case: handles spaces, dashes, dots, and camelCase."""
    s = str(name).strip()
    s = re.sub(r'[\s\.-]+', '_', s)                       # spaces/dots/dashes -> _
    s = re.sub(r'(?<=[a-z0-9])([A-Z])', r'_\1', s)        # fooBar -> foo_Bar
    s = re.sub(r'__+', '_', s)                            # collapse __
    return s.lower()


def _resolve_chrome_paths():
    """
    Resolve Chrome/Chromium and Chromedriver paths from env or common locations.
    Avoids Selenium Manager (which is failing on linux/aarch64).
    """
    chrome_bin_env = os.getenv("CHROME_BIN")
    driver_path_env = os.getenv("CHROMEDRIVER_PATH")

    # Candidates for Chrome/Chromium
    chrome_candidates = [
        chrome_bin_env,
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
        "/snap/bin/chromium",
        "/usr/bin/google-chrome",
    ]
    chrome_candidates = [p for p in chrome_candidates if p]
    chrome_bin = next((p for p in chrome_candidates if os.path.exists(p)), None)

    # Candidates for chromedriver
    driver_candidates = [
        driver_path_env,
        "/usr/bin/chromedriver",
        "/usr/lib/chromium/chromedriver",
        "/usr/bin/chromium-chromedriver",
    ]
    driver_candidates = [p for p in driver_candidates if p]
    driver_path = next((p for p in driver_candidates if os.path.exists(p)), None)

    if not chrome_bin or not driver_path:
        raise RuntimeError(
            f"Chrome/driver not found. Resolved: chrome={chrome_bin}, driver={driver_path}. "
            f"Check CHROME_BIN/CHROMEDRIVER_PATH envs and installed packages."
        )
    return chrome_bin, driver_path


def _utc_now():
    return datetime.utcnow().replace(tzinfo=timezone.utc)

# ---------------- INIT ---------------- #
def init_extractor(config):
    """Initialize extractor module with loaded YAML config."""
    global CONFIG, TESTING_MODE, MAX_RECORDS_PER_PREFIX, LICENSE_PREFIXES
    CONFIG = config
    
    TESTING_MODE = config['extraction']['mode'] == 'testing'
    if TESTING_MODE:
        MAX_RECORDS_PER_PREFIX = config['extraction']['testing']['max_records_per_prefix']
    else:
        MAX_RECORDS_PER_PREFIX = config['extraction']['production']['max_records_per_prefix']
    
    LICENSE_PREFIXES = config['extraction']['license_prefixes']
    
    logger.info(f"✅ DCC Scraper initialized. License prefixes: {LICENSE_PREFIXES}")
    return LICENSE_PREFIXES

# ---------------- DCC SCRAPER CLASS ---------------- #
class DCCLicenseScraper:
    def __init__(self, headless=True):
        """Initialize the scraper with Chrome WebDriver"""
        self.driver = None
        self.wait = None
        self.setup_driver(headless)
        
    def setup_driver(self, headless=True):
        """Setup Chrome/Chromium WebDriver with explicit binaries (bypass Selenium Manager)."""
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")

        # Set download directory (keeps your existing logic)
        download_dir = CONFIG['scraper']['downloads']['directory']
        os.makedirs(download_dir, exist_ok=True)
        prefs = {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        }
        chrome_options.add_experimental_option("prefs", prefs)

        # NEW: resolve and set explicit binaries
        chrome_bin, driver_path = _resolve_chrome_paths()
        chrome_options.binary_location = chrome_bin
        service = Service(executable_path=driver_path)

        # Construct driver explicitly with service+options
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

        timeout = CONFIG['scraper']['webdriver']['timeout_seconds']
        self.wait = WebDriverWait(self.driver, timeout)

        
    def search_licenses(self, license_prefix):
        """Search for licenses with given prefix using direct URL"""
        # Remove dash from prefix for URL (C10- becomes c10)
        search_term = license_prefix.replace("-", "").lower()
        base_url = CONFIG['scraper']['dcc_website']['base_url']
        results_url = f"{base_url}/results?searchQuery={search_term}"
        
        try:
            logger.info(f"🔍 Navigating to results for {license_prefix}...")
            self.driver.get(results_url)
            
            # Wait for results table to load
            self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table, .results-table, .license-results"))
            )
            
            # Additional wait for data to populate
            time.sleep(3)
            
            logger.info(f"✅ Successfully loaded results for {license_prefix}")
            return True
            
        except TimeoutException:
            logger.error(f"❌ Timeout waiting for search results for {license_prefix}")
            return False
        except Exception as e:
            logger.error(f"❌ Error loading results for {license_prefix}: {str(e)}")
            return False
    
    def export_all_data(self):
        """Click the Export button and select All Data to download CSV"""
        try:
            logger.info("📊 Looking for Export button...")
            
            # Wait for and click the Export button
            export_button = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Export')]"))
            )
            export_button.click()
            logger.info("✅ Export button clicked")
            
            # Wait for the export dialog to appear
            time.sleep(3)
            
            # Look for "All Data" radio button using multiple strategies
            all_data_selected = False
            
            # Strategy 1: Look for radio button with value containing "all"
            try:
                all_data_radio = self.driver.find_element(By.XPATH, "//input[@type='radio' and contains(@value, 'all')]")
                all_data_radio.click()
                logger.info("✅ All Data selected via value attribute")
                all_data_selected = True
            except:
                pass
            
            # Strategy 2: Look for the second radio button (assuming first is "Only Visible Data")
            if not all_data_selected:
                try:
                    radio_buttons = self.driver.find_elements(By.XPATH, "//input[@type='radio' and @name='dataToExport']")
                    if len(radio_buttons) >= 2:
                        radio_buttons[1].click()  # Second radio button should be "All Data"
                        logger.info("✅ All Data selected via second radio button")
                        all_data_selected = True
                except Exception as e:
                    logger.warning(f"⚠️ Strategy 2 failed: {e}")
            
            # Strategy 3: JavaScript click to find the right radio button
            if not all_data_selected:
                try:
                    result = self.driver.execute_script("""
                        const radios = document.querySelectorAll('input[type="radio"]');
                        for (let radio of radios) {
                            const parent = radio.parentElement;
                            const text = parent.textContent || parent.innerText || '';
                            if (text.toLowerCase().includes('all data')) {
                                radio.click();
                                return true;
                            }
                        }
                        // Fallback: click second radio button
                        if (radios.length >= 2) {
                            radios[1].click();
                            return true;
                        }
                        return false;
                    """)
                    if result:
                        logger.info("✅ All Data selected via JavaScript")
                        all_data_selected = True
                except Exception as e:
                    logger.warning(f"⚠️ Strategy 3 failed: {e}")
            
            if not all_data_selected:
                logger.warning("⚠️ Could not select All Data option! Export will only include visible data.")
            
            # Verify CSV format is selected
            try:
                csv_radio = self.driver.find_element(By.XPATH, "//input[@type='radio' and (@value='csv' or @value='CSV' or contains(@value, 'csv'))]")
                if not csv_radio.is_selected():
                    csv_radio.click()
                    logger.info("✅ CSV format selected")
            except:
                logger.info("ℹ️ CSV format check skipped")
            
            # Click Submit button
            submit_button = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Submit')]"))
            )
            submit_button.click()
            logger.info("✅ Submit button clicked - download should start")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error during export: {str(e)}")
            return False
    
    def wait_for_download(self, timeout=None):
        """Wait for download to complete and return the downloaded file path"""
        if timeout is None:
            timeout = CONFIG['scraper']['dcc_website']['download_timeout']
            
        logger.info("⏳ Waiting for download to complete...")
        start_time = time.time()
        
        download_dir = CONFIG['scraper']['downloads']['directory']
        
        # Get initial file list
        initial_files = set(os.listdir(download_dir)) if os.path.exists(download_dir) else set()
        
        while time.time() - start_time < timeout:
            if os.path.exists(download_dir):
                current_files = set(os.listdir(download_dir))
                new_files = current_files - initial_files
                
                # Check for completed downloads (no .crdownload extension)
                completed_files = [f for f in new_files if not f.endswith('.crdownload') and not f.endswith('.tmp')]
                
                if completed_files:
                    # Return the most recent file
                    latest_file = max([os.path.join(download_dir, f) for f in completed_files], 
                                    key=os.path.getctime)
                    logger.info(f"✅ Download completed: {os.path.basename(latest_file)}")
                    return latest_file
                
                # Check if download is still in progress
                in_progress = [f for f in current_files if f.endswith('.crdownload')]
                if in_progress:
                    logger.info("⏳ Download in progress...")
            
            time.sleep(2)
        
        logger.error("❌ Download timeout reached")
        return None
    
    def scrape_license_prefix(self, license_prefix):
        """Scrape data for a single license prefix"""
        try:
            logger.info(f"🚀 Processing licenses starting with {license_prefix}")
            
            if not self.search_licenses(license_prefix):
                return None
            
            # Try to export all data
            if not self.export_all_data():
                logger.error(f"❌ Export failed for {license_prefix}")
                return None
            
            # Wait for download to complete
            downloaded_file = self.wait_for_download()
            if not downloaded_file:
                logger.error(f"❌ Download failed for {license_prefix}")
                return None
            
            # Rename file to include prefix for organization
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            new_filename = f"dcc_licenses_{license_prefix.replace('-', '')}_{timestamp}.csv"
            new_path = os.path.join(os.path.dirname(downloaded_file), new_filename)
            
            try:
                os.rename(downloaded_file, new_path)
                logger.info(f"✅ File saved as: {os.path.basename(new_path)}")
                return new_path
            except Exception as e:
                logger.warning(f"⚠️ Could not rename file: {e}")
                logger.info(f"✅ File downloaded as: {os.path.basename(downloaded_file)}")
                return downloaded_file
                
        except Exception as e:
            logger.error(f"❌ Failed to scrape {license_prefix}: {e}")
            return None
    
    def close(self):
        """Close the browser"""
        if self.driver:
            self.driver.quit()

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
    """Add missing columns to existing table."""
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

def create_dcc_table(client, full_table, df):
    """Create ClickHouse table for DCC data with proper partitioning."""
    # Determine a stable secondary sort key
    if 'license_number' in df.columns:
        order_second = 'license_number'
    elif 'licensenumber' in df.columns:   # in case someone normalized differently
        order_second = 'licensenumber'
    elif 'licenseNumber' in df.columns:   # legacy camelCase (pre-normalization)
        order_second = 'licenseNumber'
    else:
        order_second = '_row_number' if '_row_number' in df.columns else df.columns[0]

    columns = [f"`{col}` String" for col in df.columns]

    create_ddl = f"""
    CREATE TABLE {full_table} (
        {', '.join(columns)}
    ) ENGINE = MergeTree()
    PARTITION BY toDate(parseDateTimeBestEffort(_extracted_at))
    ORDER BY (`_extracted_at`, `{order_second}`)
    SETTINGS index_granularity = 8192
    """

    try:
        client.command(create_ddl)
        logger.info(f"🆕 Created partitioned DCC table {full_table}")

        # Verify partitioning
        result = client.query(f"SHOW CREATE TABLE {full_table}")
        create_statement = result.result_rows[0][0]
        if "PARTITION BY" in create_statement:
            logger.info(f"✅ Verified {full_table} has partitioning")
        else:
            logger.error(f"❌ CRITICAL: {full_table} was created WITHOUT partitioning!")
            raise Exception(f"Table {full_table} created without partitioning")

    except Exception as e:
        logger.error(f"❌ Failed to create table {full_table}: {e}")
        raise


def load_dataframe_to_warehouse(df, extracted_at):
    """Load DataFrame to ClickHouse warehouse."""
    import clickhouse_connect
    
    raw_schema = CONFIG['warehouse']['schemas']['raw_schema']
    table_name = CONFIG['warehouse']['clickhouse']['table_name']
    
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
        # Create database if needed
        if raw_schema != 'default':
            try:
                client.command(f"CREATE DATABASE IF NOT EXISTS `{raw_schema}`")
                logger.info(f"✅ Database {raw_schema} ready")
            except Exception as db_error:
                logger.error(f"❌ Failed to create database {raw_schema}: {db_error}")
                raise ValueError(f"Cannot access or create database {raw_schema}: {db_error}")
        
        full_table = f"`{raw_schema}`.`{table_name}`"
        extraction_timestamp = extracted_at.isoformat()
        
        # For DCC data, we always do full refresh (drop and recreate)
        if table_exists(client, full_table):
            logger.info(f"🔄 Full refresh: dropping existing table {full_table}")
            client.command(f"DROP TABLE {full_table}")
        
        create_dcc_table(client, full_table, df)
        logger.info(f"✅ DCC table {full_table} created")
        
        # Prepare data for loading
        df_clean = df.copy()
        df_clean.columns = [ _to_snake(c) for c in df_clean.columns ]
        df_str = df_clean.astype(str).replace(['nan', 'None', 'null', '<NA>'], '')

        
        # Load data
        client.insert_df(table=full_table, df=df_str)
        
        # Verify load
        verification_check = client.query(f"SELECT count() FROM {full_table}")
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

# ---------------- DATA PROCESSING ---------------- #
def clean_dcc_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and prepare DCC data for warehouse loading."""
    if df.empty:
        return df

    # Remove completely empty rows/cols
    df = df.dropna(how='all')
    df = df.dropna(axis=1, how='all')
    if df.empty:
        logger.warning("⚠️ DCC data is empty after cleaning")
        return df

    # Normalize column names -> snake_case
    df.columns = [ _to_snake(c) for c in df.columns ]

    # De-dup column names by suffixing _1, _2, ...
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique():
        idxs = cols[cols == dup].index.tolist()
        for i, idx in enumerate(idxs):
            if i > 0:
                cols[idx] = f"{dup}_{i}"
    df.columns = cols

    # Convert values to strings, blank out typical null sentinels
    for col in df.columns:
        df[col] = df[col].astype(str).replace(['nan', 'None', 'null', '<NA>', ''], None)

    # Add row number
    df['_row_number'] = range(1, len(df) + 1)

    logger.info(f"📊 Cleaned DCC data: {len(df)} rows, {len(df.columns)} columns")
    return df


def process_downloaded_files(downloaded_files):
    """Process all downloaded CSV files and combine into single DataFrame."""
    all_data = []

    for file_path in downloaded_files:
        if not file_path or not os.path.exists(file_path):
            continue

        try:
            logger.info(f"📊 Processing file: {os.path.basename(file_path)}")
            df = pd.read_csv(file_path)
            if df.empty:
                logger.warning(f"⚠️ Empty file: {os.path.basename(file_path)}")
                continue

            filename = os.path.basename(file_path)
            fl = filename.lower()

            if '_c9_' in fl:
                license_prefix = 'C9-'
            elif '_c10_' in fl:
                license_prefix = 'C10-'
            elif '_c11_' in fl:
                license_prefix = 'C11-'
            elif '_c12_' in fl:
                license_prefix = 'C12-'
            else:
                license_prefix = 'Unknown'

            df['_license_prefix'] = license_prefix
            df['_source_file'] = os.path.basename(file_path)

            if TESTING_MODE and MAX_RECORDS_PER_PREFIX and len(df) > MAX_RECORDS_PER_PREFIX:
                df = df.head(MAX_RECORDS_PER_PREFIX)
                logger.info(f"🧪 Limited to {MAX_RECORDS_PER_PREFIX} records for testing")

            all_data.append(df)
            logger.info(f"✅ Processed {len(df)} records from {license_prefix}")

        except Exception as e:
            logger.error(f"❌ Failed to process {os.path.basename(file_path)}: {e}")
            continue

    if not all_data:
        logger.warning("⚠️ No valid data files to process")
        return pd.DataFrame()

    combined_df = pd.concat(all_data, ignore_index=True)
    logger.info(f"📊 Combined {len(combined_df)} total records from {len(all_data)} files")
    return combined_df


# ---------------- CONNECTION TEST ---------------- #
def test_selenium_connection():
    """Test selenium Chrome/Chromium setup with explicit binaries."""
    try:
        logger.info("🧪 Testing Selenium Chrome connection...")

        chrome_bin, driver_path = _resolve_chrome_paths()

        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.binary_location = chrome_bin

        service = Service(executable_path=driver_path)
        driver = webdriver.Chrome(service=service, options=chrome_options)

        driver.get("https://www.google.com")
        title = driver.title or ""
        if "Google" not in title:
            raise ValueError(f"Unexpected title: {title!r}")

        driver.quit()
        logger.info("✅ Selenium Chrome connection successful")
        return True

    except Exception as e:
        logger.error(f"❌ Selenium connection test failed: {e}")
        raise


# ---------------- MAIN EXTRACTION FUNCTIONS ---------------- #
def extract_all_licenses(**context):
    """Extract all DCC license data."""
    logger.info("🚀 Starting DCC Cannabis License data extraction...")
    
    try:
        execution_date = context.get('logical_date') or _utc_now()
        extracted_at = _utc_now()
        
        # Initialize scraper
        headless = CONFIG['scraper']['webdriver']['headless']
        scraper = DCCLicenseScraper(headless=headless)
        
        try:
            downloaded_files = []
            extraction_results = {}
            
            # Process each license prefix
            for prefix in LICENSE_PREFIXES:
                try:
                    logger.info(f"🔍 Processing prefix: {prefix}")
                    
                    file_path = scraper.scrape_license_prefix(prefix)
                    if file_path:
                        downloaded_files.append(file_path)
                        extraction_results[prefix] = "Downloaded"
                        logger.info(f"✅ {prefix}: File downloaded successfully")
                    else:
                        extraction_results[prefix] = "Failed: Download failed"
                        logger.error(f"❌ {prefix}: Download failed")
                    
                    # Delay between requests
                    delay = CONFIG['scraper']['dcc_website']['delay_between_requests']
                    time.sleep(delay)
                    
                except Exception as e:
                    logger.error(f"❌ Failed to process {prefix}: {e}")
                    extraction_results[prefix] = f"Failed: {e}"
                    continue
            
            # Process downloaded files
            if downloaded_files:
                logger.info(f"📊 Processing {len(downloaded_files)} downloaded files...")
                
                combined_df = process_downloaded_files(downloaded_files)
                
                if not combined_df.empty:
                    # Clean the data
                    df = clean_dcc_data(combined_df)
                    
                    if not df.empty:
                        # Add metadata columns
                        df['_extracted_at'] = extracted_at.isoformat()
                        df['_source_system'] = 'dcc_scraper'
                        df['_refresh_type'] = 'Full Refresh'
                        
                        logger.info(f"📊 Prepared {len(df)} records for warehouse load")
                        
                        # Load to warehouse
                        try:
                            records_loaded = load_dataframe_to_warehouse(df, extracted_at)
                            
                            # Update extraction results with actual record counts
                            total_records = records_loaded
                            
                            # Calculate records per prefix for results
                            for prefix in LICENSE_PREFIXES:
                                if prefix in extraction_results and extraction_results[prefix] == "Downloaded":
                                    prefix_records = len(df[df['_license_prefix'] == prefix])
                                    extraction_results[prefix] = prefix_records
                            
                            logger.info(f"✅ VERIFIED: {records_loaded} total records loaded to warehouse")
                            
                        except Exception as warehouse_error:
                            logger.error(f"❌ WAREHOUSE LOAD FAILED: {warehouse_error}")
                            raise
                    else:
                        logger.warning("⚠️ No valid data after cleaning")
                        total_records = 0
                else:
                    logger.warning("⚠️ No data found in downloaded files")
                    total_records = 0
                
                # Cleanup downloaded files if configured
                if CONFIG['scraper']['downloads']['cleanup_after_processing']:
                    cleanup_downloads(downloaded_files)
            else:
                logger.warning("⚠️ No files were downloaded")
                total_records = 0
            
        finally:
            scraper.close()
        
        logger.info("📈 DCC Extraction Summary:")
        for prefix, result in extraction_results.items():
            if isinstance(result, int):
                logger.info(f"   {prefix}: {result} records")
            else:
                logger.info(f"   {prefix}: {result}")
        
        logger.info(f"✅ DCC extraction complete. Total records: {total_records}")
        
        return {
            'total_records': total_records,
            'extraction_results': extraction_results
        }
        
    except Exception as e:
        logger.error(f"❌ DCC extraction failed: {e}")
        raise

def cleanup_downloads(file_paths):
    """Clean up downloaded files after processing."""
    try:
        download_dir = CONFIG['scraper']['downloads']['directory']
        
        for file_path in file_paths:
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    logger.info(f"🗑️ Cleaned up: {os.path.basename(file_path)}")
                except Exception as e:
                    logger.warning(f"⚠️ Could not clean up {os.path.basename(file_path)}: {e}")
        
        # Also clean up any remaining files in download directory
        if os.path.exists(download_dir):
            try:
                for file in os.listdir(download_dir):
                    file_path = os.path.join(download_dir, file)
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                logger.info(f"🗑️ Cleaned up download directory: {download_dir}")
            except Exception as e:
                logger.warning(f"⚠️ Could not clean up download directory: {e}")
                
    except Exception as e:
        logger.warning(f"⚠️ Cleanup failed: {e}")