import gspread
import json
from pprint import pprint
import pandas as pd

def test_with_service_account():
    """
    Test Google Sheets API with service account
    """
    # Path to your downloaded service account JSON file
    SERVICE_ACCOUNT_FILE = 'google-sheets-api.json'  # Same folder as script
    SPREADSHEET_ID = "1AtUSHJ4Rsy75NLVHkzfcvWgWp742wwRXIc5mipTDRfQ"
    
    try:
        print("🔐 Authenticating with service account...")
        
        # Method 1: Using file path
        gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
        
        # Method 2: Using credentials dict (if you prefer)
        # with open(SERVICE_ACCOUNT_FILE, 'r') as f:
        #     service_account_info = json.load(f)
        # gc = gspread.service_account_from_dict(service_account_info)
        
        print("✅ Authentication successful!")
        
        # Open spreadsheet by ID
        print(f"📊 Opening spreadsheet: {SPREADSHEET_ID}")
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        
        print(f"📋 Spreadsheet title: {spreadsheet.title}")
        print(f"📋 Available worksheets: {[ws.title for ws in spreadsheet.worksheets()]}")
        
        # Get the first worksheet
        worksheet = spreadsheet.sheet1
        print(f"📄 Working with worksheet: {worksheet.title}")
        
        # Get all data - handle header issues gracefully
        print("📥 Fetching all data...")
        
        try:
            # Try to get records normally first
            all_data = worksheet.get_all_records()
        except gspread.exceptions.GSpreadException as e:
            if "header row" in str(e) and "duplicates" in str(e):
                print("⚠️ Header row has issues (empty/duplicate columns). Using alternative method...")
                
                # Get all values as raw data
                all_values = worksheet.get_all_values()
                
                if not all_values:
                    print("⚠️ No data found in spreadsheet")
                    all_data = []
                else:
                    # Fix headers and convert to records
                    headers = all_values[0]  # First row as headers
                    data_rows = all_values[1:]  # Remaining rows as data
                    
                    # Clean and fix headers
                    cleaned_headers = []
                    for i, header in enumerate(headers):
                        if not header or header.strip() == '':
                            cleaned_headers.append(f'column_{i+1}')  # column_1, column_2, etc.
                        else:
                            cleaned_headers.append(header.strip())
                    
                    # Handle duplicate headers
                    final_headers = []
                    header_counts = {}
                    for header in cleaned_headers:
                        if header in header_counts:
                            header_counts[header] += 1
                            final_headers.append(f"{header}_{header_counts[header]}")
                        else:
                            header_counts[header] = 1
                            final_headers.append(header)
                    
                    print(f"🔧 Fixed headers: {final_headers}")
                    
                    # Convert to list of dictionaries
                    all_data = []
                    for row in data_rows:
                        # Pad row with empty strings if shorter than headers
                        padded_row = row + [''] * (len(final_headers) - len(row))
                        record = dict(zip(final_headers, padded_row))
                        all_data.append(record)
            else:
                raise  # Re-raise if it's a different error
        
        print(f"✅ Successfully retrieved {len(all_data)} records")
        
        if all_data:
            print("\n📊 Sample data (first record):")
            pprint(all_data[0])
            
            print(f"\n📊 Column headers: {list(all_data[0].keys())}")
            
            # Convert to DataFrame for better analysis
            df = pd.DataFrame(all_data)
            print(f"\n📊 DataFrame shape: {df.shape}")
            print(f"📊 Data types:")
            print(df.dtypes)
            
            # Show first few rows
            print(f"\n📊 First 3 rows:")
            print(df.head(3))
            
        else:
            print("⚠️ No data found in the spreadsheet")
            
        # Test different sheet access methods
        print("\n🔍 Testing different access methods...")
        
        # Method 1: Get raw values (works even with header issues)
        all_values = worksheet.get_all_values()
        print(f"📍 All values: {len(all_values)} rows")
        if all_values:
            print(f"📍 First row (headers): {all_values[0]}")
            if len(all_values) > 1:
                print(f"📍 Second row (sample data): {all_values[1]}")
        
        # Method 2: Get specific range
        try:
            range_data = worksheet.get('A1:E5')  # First 5 columns, 5 rows
            print(f"📍 Range A1:E5: {len(range_data)} rows")
        except Exception as e:
            print(f"📍 Range query failed: {e}")
        
        # Method 3: Get specific columns
        try:
            if all_values and len(all_values[0]) > 0:
                col_a = worksheet.col_values(1)  # Column A
                print(f"📍 Column A: {len(col_a)} values")
        except Exception as e:
            print(f"📍 Column query failed: {e}")
        
        return {
            'success': True,
            'records': len(all_data),
            'columns': list(all_data[0].keys()) if all_data else [],
            'worksheets': [ws.title for ws in spreadsheet.worksheets()],
            'sample_data': all_data[:2] if all_data else []
        }
        
    except FileNotFoundError:
        print(f"❌ Service account file not found: {SERVICE_ACCOUNT_FILE}")
        print("💡 Make sure to:")
        print("   1. Download the JSON file from Google Cloud Console")
        print("   2. Update SERVICE_ACCOUNT_FILE path in this script")
        return {'success': False, 'error': 'Service account file not found'}
        
    except gspread.SpreadsheetNotFound:
        print("❌ Spreadsheet not found or no access")
        print("💡 Make sure to:")
        print("   1. Share spreadsheet with service account email")
        print("   2. Give 'Viewer' or 'Editor' permissions")
        return {'success': False, 'error': 'Spreadsheet access denied'}
    
    except gspread.exceptions.APIError as e:
        print(f"❌ Google API Error: {e}")
        print("💡 This might be a permissions issue")
        return {'success': False, 'error': f'API Error: {e}'}
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print(f"❌ Error type: {type(e).__name__}")
        import traceback
        print("❌ Full traceback:")
        traceback.print_exc()
        return {'success': False, 'error': str(e)}

def show_service_account_email():
    """
    Show the service account email from JSON file
    """
    SERVICE_ACCOUNT_FILE = 'google-sheets-api.json'  # Same folder as script
    
    try:
        with open(SERVICE_ACCOUNT_FILE, 'r') as f:
            service_account_info = json.load(f)
        
        email = service_account_info.get('client_email')
        project_id = service_account_info.get('project_id')
        
        print(f"📧 Service Account Email: {email}")
        print(f"🏷️  Project ID: {project_id}")
        print(f"📋 Share your spreadsheet with: {email}")
        
    except FileNotFoundError:
        print(f"❌ Service account file not found: {SERVICE_ACCOUNT_FILE}")
        print("💡 Update the SERVICE_ACCOUNT_FILE path first")
    except Exception as e:
        print(f"❌ Error reading service account file: {e}")

if __name__ == "__main__":
    print("🔍 Testing Google Sheets API with Service Account...\n")
    
    # First, show service account email for sharing
    print("1️⃣ Service Account Information:")
    show_service_account_email()
    
    print("\n" + "="*50)
    
    # Then test the API
    print("2️⃣ Testing API Access:")
    result = test_with_service_account()
    
    if result['success']:
        print(f"\n🎉 SUCCESS! Retrieved {result['records']} records")
        print(f"📊 Columns: {result['columns']}")
        print(f"📋 Worksheets: {result['worksheets']}")
    else:
        print(f"\n❌ FAILED: {result['error']}")
    
    print("\n💡 Setup checklist:")
    print("   ✅ Install gspread: pip install gspread")
    print("   ✅ Create service account in Google Cloud Console")
    print("   ✅ Download JSON key file")
    print("   ✅ Update SERVICE_ACCOUNT_FILE path in script")
    print("   ✅ Share spreadsheet with service account email")
    print("   ✅ Give 'Viewer' permissions to service account")