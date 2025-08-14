# # warehouse_config.py
# warehouse_config.py - CONNECTION ONLY, no schema definitions

import yaml
import os
from pathlib import Path

def load_warehouse_config(warehouse_type="postgres"):
    """Load warehouse configuration from YAML files"""
    
    config_path = Path(__file__).parent / "warehouses" / f"{warehouse_type}.yml"
    
    if not config_path.exists():
        raise FileNotFoundError(f"Warehouse config not found: {config_path}")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    return config

def get_active_warehouse():
    """Get the active warehouse from environment variable"""
    return os.getenv('ACTIVE_WAREHOUSE', 'postgres')

def get_connection_string(warehouse_type=None):
    """Get SQLAlchemy connection string for warehouse"""
    
    if warehouse_type is None:
        warehouse_type = get_active_warehouse()
    
    config = load_warehouse_config(warehouse_type)
    conn = config['connection']
    
    if warehouse_type == 'postgres':
        return f"postgresql://{conn['user']}:{conn['password']}@{conn['host']}:{conn['port']}/{conn['database']}"
    
    elif warehouse_type == 'snowflake':
        return f"snowflake://{conn['user']}:{conn['password']}@{conn['account']}/{conn['database']}/{conn['schema']}?warehouse={conn['warehouse']}&role={conn.get('role', 'ACCOUNTADMIN')}"
    
    elif warehouse_type == 'clickhouse':
        protocol = 'https' if conn.get('secure', True) else 'http'
        return f"clickhouse+{protocol}://{conn['user']}:{conn['password']}@{conn['host']}:{conn['port']}/{conn['database']}"
    
    else:
        raise ValueError(f"Unsupported warehouse type: {warehouse_type}")

def get_dbt_connection_config(warehouse_type=None):
    """Generate dbt profiles.yml configuration - NO SCHEMA (dbt handles this)"""
    
    if warehouse_type is None:
        warehouse_type = get_active_warehouse()
    
    config = load_warehouse_config(warehouse_type)
    conn = config['connection']
    
    if warehouse_type == 'postgres':
        return {
            'type': 'postgres',
            'host': conn['host'],
            'user': conn['user'],
            'password': conn['password'],
            'port': conn['port'],
            'dbname': conn['database'],
            'schema': 'bronze_acumatica',  # Default schema, dbt models override this
            'threads': 4,
            'keepalives_idle': 0,
            'connect_timeout': 10,
            'retries': 1
        }
    
    elif warehouse_type == 'snowflake':
        return {
            'type': 'snowflake',
            'account': conn['account'],
            'user': conn['user'],
            'password': conn['password'],
            'warehouse': conn['warehouse'],
            'database': conn['database'],
            'schema': 'bronze_acumatica',  # Default schema, dbt models override this
            'role': conn.get('role', 'ACCOUNTADMIN'),
            'threads': 4,
            'client_session_keep_alive': False,
            'query_tag': 'dbt'
        }
    
    elif warehouse_type == 'clickhouse':
        return {
            'type': 'clickhouse',
            'host': conn['host'],
            'port': conn['port'],
            'user': conn['user'],
            'password': conn['password'],
            'schema': conn['database'],  # Use database as default schema
            'secure': conn.get('secure', True),
            'threads': 4,
            'connection_timeout': 20,
            'receive_timeout': 300,
            'send_timeout': 300
        }
    
    else:
        raise ValueError(f"Unsupported warehouse type: {warehouse_type}")

def get_required_packages(warehouse_type=None):
    """Get required Python packages for warehouse"""
    
    if warehouse_type is None:
        warehouse_type = get_active_warehouse()
    
    # Try to read from requirements.txt first
    requirements_file = Path(__file__).parent.parent / 'requirements.txt'
    if requirements_file.exists():
        with open(requirements_file, 'r') as f:
            packages = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        return packages
    
    # Fallback to warehouse-specific packages if no requirements.txt
    base_packages = [
        'selenium==4.35.0',
        'webdriver-manager==4.0.2',
        'requests==2.32.4',
        'pandas==2.3.1',
        'python-dotenv==1.1.1',
        'gspread==6.2.1',
        'google-auth==2.40.3',
        'google-auth-oauthlib==1.2.2'
    ]
    
    warehouse_packages = {
        'postgres': ['dbt-postgres', 'psycopg2-binary', 'sqlalchemy'],
        'snowflake': ['dbt-snowflake', 'snowflake-connector-python', 'sqlalchemy'],
        'clickhouse': ['dbt-clickhouse', 'clickhouse-connect', 'sqlalchemy']
    }
    
    specific_packages = warehouse_packages.get(warehouse_type, ['dbt-core'])
    return base_packages + specific_packages

# def get_required_packages(warehouse_type=None):
#     """Get required Python packages for warehouse"""
    
#     if warehouse_type is None:
#         warehouse_type = get_active_warehouse()
    
#     packages = {
#         'postgres': ['dbt-postgres', 'psycopg2-binary', 'sqlalchemy'],
#         'snowflake': ['dbt-snowflake', 'snowflake-connector-python', 'sqlalchemy'],
#         'clickhouse': ['dbt-clickhouse', 'clickhouse-connect', 'sqlalchemy']
#     }
    
#     return packages.get(warehouse_type, ['dbt-core'])

# import yaml
# import os
# from pathlib import Path

# def load_warehouse_config(warehouse_type="postgres"):
#     """Load warehouse configuration from YAML files"""
    
#     config_path = Path(__file__).parent / "warehouses" / f"{warehouse_type}.yml"
    
#     if not config_path.exists():
#         raise FileNotFoundError(f"Warehouse config not found: {config_path}")
    
#     with open(config_path, 'r') as f:
#         config = yaml.safe_load(f)
    
#     return config

# def get_active_warehouse():
#     """Get the active warehouse from environment variable"""
#     return os.getenv('ACTIVE_WAREHOUSE', 'postgres')

# def get_connection_string(warehouse_type=None):
#     """Get SQLAlchemy connection string for warehouse"""
    
#     if warehouse_type is None:
#         warehouse_type = get_active_warehouse()
    
#     config = load_warehouse_config(warehouse_type)
#     conn = config['connection']
    
#     if warehouse_type == 'postgres':
#         return f"postgresql://{conn['user']}:{conn['password']}@{conn['host']}:{conn['port']}/{conn['database']}"
    
#     elif warehouse_type == 'snowflake':
#         return f"snowflake://{conn['user']}:{conn['password']}@{conn['account']}/{conn['database']}/{conn['schema']}?warehouse={conn['warehouse']}&role={conn.get('role', 'ACCOUNTADMIN')}"
    
#     elif warehouse_type == 'clickhouse':
#         protocol = 'https' if conn.get('secure', True) else 'http'
#         return f"clickhouse+{protocol}://{conn['user']}:{conn['password']}@{conn['host']}:{conn['port']}/{conn['database']}"
    
#     else:
#         raise ValueError(f"Unsupported warehouse type: {warehouse_type}")

# def get_dbt_connection_config(warehouse_type=None):
#     """Generate dbt profiles.yml configuration"""
    
#     if warehouse_type is None:
#         warehouse_type = get_active_warehouse()
    
#     config = load_warehouse_config(warehouse_type)
#     conn = config['connection']
    
#     if warehouse_type == 'postgres':
#         return {
#             'type': 'postgres',
#             'host': conn['host'],
#             'user': conn['user'],
#             'password': conn['password'],
#             'port': conn['port'],
#             'dbname': conn['database'],
#             'schema': conn['schema'],
#             'threads': 4,
#             'keepalives_idle': 0,
#             'connect_timeout': 10,
#             'retries': 1
#         }
    
#     elif warehouse_type == 'snowflake':
#         return {
#             'type': 'snowflake',
#             'account': conn['account'],
#             'user': conn['user'],
#             'password': conn['password'],
#             'warehouse': conn['warehouse'],
#             'database': conn['database'],
#             'schema': conn['schema'],
#             'role': conn.get('role', 'ACCOUNTADMIN'),
#             'threads': 4,
#             'client_session_keep_alive': False,
#             'query_tag': 'dbt'
#         }
    
#     elif warehouse_type == 'clickhouse':
#         # ✅ FIX: For ClickHouse, database and schema must be the same or omit database
#         return {
#             'type': 'clickhouse',
#             'host': conn['host'],
#             'port': conn['port'],
#             'user': conn['user'],
#             'password': conn['password'],
#             'schema': conn['database'],  # ✅ Use database as schema
#             'secure': conn.get('secure', True),
#             'threads': 4,
#             'connection_timeout': 20,
#             'receive_timeout': 300,
#             'send_timeout': 300
#         }
    
#     else:
#         raise ValueError(f"Unsupported warehouse type: {warehouse_type}")

# def get_required_packages(warehouse_type=None):
#     """Get required Python packages for warehouse"""
    
#     if warehouse_type is None:
#         warehouse_type = get_active_warehouse()
    
#     packages = {
#         'postgres': ['dbt-postgres', 'psycopg2-binary', 'sqlalchemy'],
#         'snowflake': ['dbt-snowflake', 'snowflake-connector-python', 'sqlalchemy'],
#         'clickhouse': ['dbt-clickhouse', 'clickhouse-connect', 'sqlalchemy']  # ✅ clickhouse-connect is the key
#     }
    
#     return packages.get(warehouse_type, ['dbt-core'])