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
                    'prod': connection_config.copy()  # Same config for now
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
        
        print(f"✅ Generated {profiles_path} for {warehouse}")
        print(f"   Warehouse: {connection_config['type']}")
        print(f"   Host: {connection_config.get('host', 'N/A')}")
        print(f"   Database: {connection_config.get('dbname', connection_config.get('database', 'N/A'))}")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to generate profiles.yml: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = generate_profiles_yml()
    sys.exit(0 if success else 1)