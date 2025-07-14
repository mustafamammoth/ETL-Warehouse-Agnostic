#!/usr/bin/env python3
"""
Analyze the structure of extracted Acumatica data
Run this after successful extraction to understand data cleaning needs
"""

import pandas as pd
import os
from pathlib import Path

def analyze_data_structure():
    """Analyze the structure of extracted data files"""
    
    data_dir = Path("data/raw/acumatica")
    
    if not data_dir.exists():
        print("❌ No data directory found. Run the extraction first.")
        return
    
    # Find all CSV files
    csv_files = list(data_dir.glob("*_latest.csv"))
    
    if not csv_files:
        print("❌ No CSV files found. Run the extraction first.")
        return
    
    print("📊 Analyzing Acumatica Data Structure")
    print("=" * 50)
    
    for csv_file in csv_files:
        endpoint_name = csv_file.stem.replace("_latest", "")
        
        try:
            df = pd.read_csv(csv_file)
            
            print(f"\n🔍 {endpoint_name.upper()} ({len(df)} records)")
            print("-" * 30)
            
            # Basic info
            print(f"Columns: {len(df.columns)}")
            print(f"Rows: {len(df)}")
            
            # Key columns analysis
            key_columns = [col for col in df.columns if any(keyword in col.lower() 
                          for keyword in ['id', 'name', 'email', 'date', 'amount', 'status'])]
            
            print(f"\nKey columns found:")
            for col in key_columns[:10]:  # Show first 10
                sample_values = df[col].dropna().head(3).tolist()
                print(f"  {col}: {sample_values}")
            
            # Data quality issues
            print(f"\nData Quality:")
            null_counts = df.isnull().sum()
            high_null_cols = null_counts[null_counts > len(df) * 0.5].head(5)
            if len(high_null_cols) > 0:
                print(f"  High null columns: {list(high_null_cols.index)}")
            
            # Empty string columns
            empty_string_cols = []
            for col in df.columns:
                if df[col].dtype == 'object':
                    empty_count = (df[col] == '').sum()
                    if empty_count > len(df) * 0.3:
                        empty_string_cols.append(col)
            
            if empty_string_cols:
                print(f"  Many empty strings: {empty_string_cols[:3]}")
            
            print(f"\n  Sample first record:")
            first_record = df.iloc[0]
            # Fixed the iteration issue
            for i, (col, value) in enumerate(first_record.items()):
                if i >= 8:  # Show first 8 fields
                    break
                print(f"    {col}: {value}")
                
        except Exception as e:
            print(f"❌ Error analyzing {endpoint_name}: {e}")
    
    print("\n" + "=" * 50)
    print("💡 Next Steps:")
    print("1. Review the key columns for each endpoint")
    print("2. Identify which fields need cleaning")
    print("3. Decide on primary keys for each table")
    print("4. Plan the dbt transformation models")

if __name__ == "__main__":
    analyze_data_structure()