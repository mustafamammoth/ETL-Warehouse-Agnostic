#!/usr/bin/env python3
import sys
import os
from pathlib import Path

# Add config directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'config'))

from warehouse_config import get_dbt_connection_config, get_active_warehouse
import yaml

def generate_profiles_yml():
    """Generate dbt profiles.yml for active warehouse"""
    
    try:
        warehouse = get_active_warehouse()
        connection_config = get_dbt_connection_config(warehouse)
        
        profiles = {
            'data_platform': {
                'target': 'dev',
                'outputs': {
                    'dev': connection_config,
                    'prod': connection_config.copy()
                }
            }
        }
        
        # Ensure dbt directory exists
        dbt_dir = Path(__file__).parent.parent / 'dbt'
        dbt_dir.mkdir(exist_ok=True)
        
        # Write to dbt/profiles.yml
        profiles_path = dbt_dir / 'profiles.yml'
        with open(profiles_path, 'w') as f:
            yaml.dump(profiles, f, default_flow_style=False, indent=2)
        
        # Display warehouse-specific info
        warehouse_type = connection_config['type']
        host = connection_config.get('host', 'N/A')
        
        # Handle different database field names
        if warehouse_type == 'postgres':
            database = connection_config.get('dbname', 'N/A')
            port = connection_config.get('port', 'N/A')
        elif warehouse_type == 'clickhouse':
            database = connection_config.get('database', 'N/A')
            port = connection_config.get('port', 'N/A')
            secure = connection_config.get('secure', False)
        elif warehouse_type == 'snowflake':
            database = connection_config.get('database', 'N/A')
            warehouse_name = connection_config.get('warehouse', 'N/A')
            account = connection_config.get('account', 'N/A')
        else:
            database = connection_config.get('database', connection_config.get('dbname', 'N/A'))
            port = connection_config.get('port', 'N/A')
        
        print(f"✅ Generated {profiles_path} for {warehouse}")
        print(f"   Warehouse Type: {warehouse_type}")
        print(f"   Host: {host}")
        
        if warehouse_type == 'snowflake':
            print(f"   Account: {account}")
            print(f"   Warehouse: {warehouse_name}")
            print(f"   Database: {database}")
        elif warehouse_type == 'clickhouse':
            print(f"   Database: {database}")
            print(f"   Port: {port}")
            print(f"   Secure: {secure}")
        else:  # postgres
            print(f"   Database: {database}")
            print(f"   Port: {port}")
        
        # Show the generated config for debugging
        print(f"\n🔍 Generated dbt profile config:")
        print(f"   Type: {connection_config['type']}")
        for key, value in connection_config.items():
            if 'password' not in key.lower():  # Don't print passwords
                print(f"   {key}: {value}")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to generate profiles.yml: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = generate_profiles_yml()
    sys.exit(0 if success else 1)