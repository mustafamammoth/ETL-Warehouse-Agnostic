"""
DCC Cannabis License Data Scraper
Automates the collection of license data from the California Department of Cannabis Control website
"""

import time
import pandas as pd
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import csv
from datetime import datetime

class DCCLicenseScraper:
    def __init__(self, headless=True):
        """Initialize the scraper with Chrome WebDriver"""
        self.driver = None
        self.setup_driver(headless)
        
    def setup_driver(self, headless=True):
        """Setup Chrome WebDriver with appropriate options"""
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.wait = WebDriverWait(self.driver, 30)
        
    def search_licenses(self, license_prefix):
        """Search for licenses with given prefix using direct URL"""
        # Remove dash from prefix for URL (C10- becomes c10)
        search_term = license_prefix.replace("-", "").lower()
        results_url = f"https://search.cannabis.ca.gov/results?searchQuery={search_term}"
        
        try:
            print(f"Navigating to results for {license_prefix}...")
            self.driver.get(results_url)
            
            # Wait for results table to load
            self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table, .results-table, .license-results"))
            )
            
            # Additional wait for data to populate
            time.sleep(3)
            
            return True
            
        except TimeoutException:
            print(f"Timeout waiting for search results for {license_prefix}")
            return False
        except Exception as e:
            print(f"Error loading results for {license_prefix}: {str(e)}")
            return False
    
    def export_all_data(self):
        """Click the Export button and select All Data to download CSV"""
        try:
            print("Looking for Export button...")
            
            # Wait for and click the Export button
            export_button = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Export')]"))
            )
            export_button.click()
            print("Export button clicked")
            
            # Wait for the export dialog to appear
            time.sleep(3)
            
            # Look for ALL radio buttons and identify the "All Data" one
            print("Looking for All Data radio button...")
            
            # First, let's debug what's available in the dialog
            try:
                radio_buttons = self.driver.find_elements(By.XPATH, "//input[@type='radio']")
                print(f"Found {len(radio_buttons)} radio buttons")
                
                for i, radio in enumerate(radio_buttons):
                    try:
                        # Get the associated label text
                        parent = radio.find_element(By.XPATH, "./..")
                        label_text = parent.text.strip()
                        value = radio.get_attribute("value") or ""
                        name = radio.get_attribute("name") or ""
                        print(f"Radio {i+1}: value='{value}', name='{name}', label='{label_text}'")
                    except:
                        print(f"Radio {i+1}: Could not get details")
            except Exception as e:
                print(f"Debug failed: {e}")
            
            # Try multiple strategies to find and click "All Data"
            all_data_selected = False
            
            # Strategy 1: Look for radio button with value containing "all"
            try:
                all_data_radio = self.driver.find_element(By.XPATH, "//input[@type='radio' and contains(@value, 'all')]")
                all_data_radio.click()
                print("All Data selected via value attribute")
                all_data_selected = True
            except:
                pass
            
            # Strategy 2: Look for the second radio button (assuming first is "Only Visible Data")
            if not all_data_selected:
                try:
                    radio_buttons = self.driver.find_elements(By.XPATH, "//input[@type='radio' and @name='dataToExport']")
                    if len(radio_buttons) >= 2:
                        radio_buttons[1].click()  # Second radio button should be "All Data"
                        print("All Data selected via second radio button")
                        all_data_selected = True
                except Exception as e:
                    print(f"Strategy 2 failed: {e}")
            
            # Strategy 3: Look for radio button by position in form
            if not all_data_selected:
                try:
                    all_data_radio = self.driver.find_element(By.XPATH, "(//input[@type='radio'])[2]")
                    all_data_radio.click()
                    print("All Data selected via position")
                    all_data_selected = True
                except Exception as e:
                    print(f"Strategy 3 failed: {e}")
            
            # Strategy 4: Try to find by looking for text "All Data" near radio buttons
            if not all_data_selected:
                try:
                    # Look for text containing "All Data" and find nearby radio button
                    elements = self.driver.find_elements(By.XPATH, "//*[contains(text(), 'All Data')]")
                    for element in elements:
                        try:
                            # Look for radio button in same container
                            radio = element.find_element(By.XPATH, ".//input[@type='radio'] | ./preceding-sibling::input[@type='radio'] | ./following-sibling::input[@type='radio']")
                            radio.click()
                            print("All Data selected via text search")
                            all_data_selected = True
                            break
                        except:
                            continue
                except Exception as e:
                    print(f"Strategy 4 failed: {e}")
            
            # Strategy 5: JavaScript click on all radio buttons to find the right one
            if not all_data_selected:
                try:
                    # Use JavaScript to click the radio button with "All Data"
                    self.driver.execute_script("""
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
                    print("All Data selected via JavaScript")
                    all_data_selected = True
                except Exception as e:
                    print(f"Strategy 5 failed: {e}")
            
            if not all_data_selected:
                print("ERROR: Could not select All Data option! Export will only include visible data.")
                print("Manual intervention may be required.")
            
            # Verify CSV format is selected
            try:
                csv_radio = self.driver.find_element(By.XPATH, "//input[@type='radio' and (@value='csv' or @value='CSV' or contains(@value, 'csv'))]")
                if not csv_radio.is_selected():
                    csv_radio.click()
                    print("CSV format selected")
            except:
                print("CSV format check skipped")
            
            # Click Submit button
            submit_button = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Submit')]"))
            )
            submit_button.click()
            print("Submit button clicked - download should start")
            
            return True
            
        except Exception as e:
            print(f"Error during export: {str(e)}")
            return False
    
    def scrape_table_data(self):
        """Scrape the license data from the results table"""
        try:
            # Wait for table to be present
            table = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table"))
            )
            
            # Get all rows
            rows = table.find_elements(By.TAG_NAME, "tr")
            
            if not rows:
                print("No table rows found")
                return []
            
            # Get headers from first row
            header_row = rows[0]
            headers = []
            header_cells = header_row.find_elements(By.TAG_NAME, "th")
            if not header_cells:
                header_cells = header_row.find_elements(By.TAG_NAME, "td")
            
            for cell in header_cells:
                headers.append(cell.text.strip())
            
            print(f"Found headers: {headers}")
            
            data = []
            
            # Process data rows (skip header row)
            for i, row in enumerate(rows[1:], 1):
                cells = row.find_elements(By.TAG_NAME, "td")
                if cells and len(cells) >= len(headers):
                    row_data = {}
                    for j, cell in enumerate(cells):
                        if j < len(headers):
                            row_data[headers[j]] = cell.text.strip()
                    data.append(row_data)
                    if i <= 3:  # Print first few rows for debugging
                        print(f"Row {i}: {row_data}")
            
            return data
            
        except TimeoutException:
            print("Timeout waiting for table data")
            return []
        except Exception as e:
            print(f"Error scraping table data: {str(e)}")
            return []
    
    def wait_for_download(self, timeout=60):
        """Wait for download to complete and return the downloaded file path"""
        print("Waiting for download to complete...")
        start_time = time.time()
        
        # Set up download directory if not exists
        download_dir = os.path.expanduser("~/Downloads")
        
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
                    print(f"Download completed: {latest_file}")
                    return latest_file
                
                # Check if download is still in progress
                in_progress = [f for f in current_files if f.endswith('.crdownload')]
                if in_progress:
                    print("Download in progress...")
            
            time.sleep(2)
        
        print("Download timeout reached")
        return None
    
    def collect_all_license_data(self, license_prefixes=None):
        """Collect data for all specified license prefixes using export functionality"""
        if license_prefixes is None:
            license_prefixes = ["C9-", "C10-", "C11-", "C12-"]
        
        downloaded_files = []
        
        for prefix in license_prefixes:
            print(f"\n{'='*50}")
            print(f"Processing licenses starting with {prefix}")
            print(f"{'='*50}")
            
            if self.search_licenses(prefix):
                # Try to export all data
                if self.export_all_data():
                    # Wait for download to complete
                    downloaded_file = self.wait_for_download()
                    if downloaded_file:
                        # Rename file to include prefix for organization
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        new_filename = f"dcc_licenses_{prefix.replace('-', '')}_{timestamp}.csv"
                        new_path = os.path.join(os.path.dirname(downloaded_file), new_filename)
                        
                        try:
                            os.rename(downloaded_file, new_path)
                            downloaded_files.append(new_path)
                            print(f"File saved as: {new_path}")
                        except Exception as e:
                            print(f"Could not rename file: {e}")
                            downloaded_files.append(downloaded_file)
                            print(f"File downloaded as: {downloaded_file}")
                    else:
                        print(f"Download failed for {prefix}")
                else:
                    print(f"Export failed for {prefix}")
            else:
                print(f"Failed to load results for prefix {prefix}")
            
            time.sleep(3)  # Pause between searches to be respectful
        
        return downloaded_files
    
    def save_to_csv(self, data, filename=None):
        """Save collected data to CSV file"""
        if not data:
            print("No data to save")
            return
        
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"dcc_licenses_{timestamp}.csv"
        
        # Get all unique keys from all records
        all_keys = set()
        for record in data:
            all_keys.update(record.keys())
        
        fieldnames = sorted(list(all_keys))
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        
        print(f"Data saved to {filename}")
        print(f"Total records: {len(data)}")
    
    def close(self):
        """Close the browser"""
        if self.driver:
            self.driver.quit()

def main():
    """Main function to run the scraper"""
    scraper = DCCLicenseScraper(headless=False)  # Set to True for headless mode
    
    try:
        # Define license prefixes to search for
        prefixes = ["C9-", "C10-", "C11-", "C12-"]
        
        print("Starting DCC License Data Collection...")
        print(f"Target prefixes: {', '.join(prefixes)}")
        
        # Collect all license data using export functionality
        downloaded_files = scraper.collect_all_license_data(prefixes)
        
        # Print summary
        print(f"\n{'='*50}")
        print("EXPORT SUMMARY")
        print(f"{'='*50}")
        print(f"Total files downloaded: {len(downloaded_files)}")
        
        for file_path in downloaded_files:
            print(f"✓ {os.path.basename(file_path)}")
            
        if not downloaded_files:
            print("No files were downloaded")
            
    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        scraper.close()

if __name__ == "__main__":
    main()