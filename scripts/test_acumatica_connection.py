#!/usr/bin/env python3
"""
Test script for Acumatica API connection
Run this to verify your credentials and API access
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Get the project root directory
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables
load_dotenv()

from extractors.acumatica.extractor import AcumaticaExtractor

def main():
    """Test Acumatica connection and extract sample data"""
    
    # Get configuration from environment
    config = {
        'base_url': os.getenv('ACUMATICA_BASE_URL'),
        'username': os.getenv('ACUMATICA_USERNAME'),
        'password': os.getenv('ACUMATICA_PASSWORD'),
        'company': os.getenv('ACUMATICA_COMPANY'),
        'branch': os.getenv('ACUMATICA_BRANCH')
    }
    
    print("🔧 Testing Acumatica API Connection...")
    print(f"Base URL: {config['base_url']}")
    print(f"Username: {config['username']}")
    print(f"Company: {config['company']}")
    print(f"Branch: {config['branch']}")
    print("-" * 50)
    
    # Validate configuration
    if not config['base_url'] or not config['username'] or not config['password']:
        print("❌ Missing required environment variables!")
        print("Please check your .env file contains:")
        print("  ACUMATICA_BASE_URL")
        print("  ACUMATICA_USERNAME") 
        print("  ACUMATICA_PASSWORD")
        return
    
    # Initialize extractor
    extractor = AcumaticaExtractor(config)
    
    # Test connection
    print("1. Testing API connection...")
    try:
        if extractor.test_connection():
            print("✅ Connection successful!")
        else:
            print("❌ Connection failed!")
            return
    except Exception as e:
        print(f"❌ Connection error: {e}")
        return
    
    # Test data extraction
    print("\n2. Testing data extraction...")
    try:
        # Extract customers (usually available in most Acumatica instances)
        df = extractor.extract('customers', save_to_file=True)
        
        print(f"✅ Successfully extracted {len(df)} customer records")
        
        if not df.empty:
            print(f"\nColumn names: {list(df.columns)[:10]}...")  # Show first 10 columns
            print(f"\nFirst record preview:")
            first_record = df.iloc[0].to_dict()
            for i, (col, value) in enumerate(first_record.items()):
                if i < 10:  # Show first 10 fields
                    print(f"  {col}: {value}")
                elif i == 10:
                    print(f"  ... and {len(first_record)-10} more columns")
                    break
        
    except Exception as e:
        print(f"❌ Data extraction failed: {e}")
        print("\nTrying alternative endpoint...")
        
        # Try a different endpoint if customers fails
        try:
            df = extractor.extract('items', save_to_file=True) 
            print(f"✅ Successfully extracted {len(df)} item records")
        except Exception as e2:
            print(f"❌ Alternative extraction also failed: {e2}")
            return
    
    print("\n🎉 Basic test passed! Your Acumatica setup is working.")
    print("\nNext steps:")
    print("1. Check the CSV files in data/raw/acumatica/")
    print("2. Start the full pipeline with: ./scripts/start_services.sh")

if __name__ == "__main__":
    main()