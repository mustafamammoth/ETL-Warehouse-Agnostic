#!/usr/bin/env python3
import sys
import os
from pathlib import Path

# Add config directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'config'))

from warehouse_config import load_warehouse_config, get_active_warehouse, get_required_packages
import yaml

def generate_docker_compose():
    """Generate docker-compose.yml for active warehouse"""
    
    try:
        warehouse = get_active_warehouse()
        config = load_warehouse_config(warehouse)
        packages = get_required_packages(warehouse)
        
        # Base environment variables
        base_env = [
            'AIRFLOW__CORE__EXECUTOR=LocalExecutor',
            f'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:postgres@postgres/airflow',
            'AIRFLOW__CORE__FERNET_KEY=fb0c_0-l6HiTRHDke8sL-8jz9YOV-tD3Zn3wWZ2CUKxNDlc=',
            'AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true',
            'AIRFLOW__CORE__LOAD_EXAMPLES=false',
            'AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session',
            'ACUMATICA_BASE_URL=https://mammoth.klearsystems.com',
            'ACUMATICA_USERNAME=mustafa.zaki',
            'ACUMATICA_PASSWORD=Dingdongbell@123',
            f'ACTIVE_WAREHOUSE={warehouse}'
        ]
        
        # Base volumes
        base_volumes = [
            './airflow/dags:/opt/airflow/dags',
            './airflow/logs:/opt/airflow/logs',
            './airflow/config:/opt/airflow/config',
            './airflow/plugins:/opt/airflow/plugins',
            './data:/opt/airflow/data',
            './dbt:/opt/airflow/dbt',
            './config:/opt/airflow/config',
            './scripts:/opt/airflow/scripts'
        ]
        
        # Package installation command
        pip_install_cmd = ' '.join(packages)
        
        # Generate services
        services = {}
        
        # Always include PostgreSQL for Airflow metadata
        services['postgres'] = {
            'image': 'postgres:14',
            'environment': {
                'POSTGRES_USER': 'postgres',
                'POSTGRES_PASSWORD': 'postgres',
                'POSTGRES_DB': 'airflow'
            },
            'ports': ['5432:5432'],
            'volumes': ['postgres_data:/var/lib/postgresql/data'],
            'healthcheck': {
                'test': ['CMD', 'pg_isready', '-U', 'postgres'],
                'interval': '5s',
                'retries': 5
            }
        }
        
        # Add warehouse-specific service if needed
        if warehouse == 'postgres':
            # PostgreSQL is already included above
            pass
        elif warehouse == 'clickhouse':
            services['clickhouse'] = {
                'image': 'clickhouse/clickhouse-server:latest',
                'ports': ['8123:8123', '9000:9000', '9440:9440'],
                'environment': {
                    'CLICKHOUSE_DB': 'analytics_db',
                    'CLICKHOUSE_USER': 'default',
                    'CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT': '1'
                },
                'volumes': ['clickhouse_data:/var/lib/clickhouse'],
                'healthcheck': {
                    'test': ['CMD', 'wget', '--no-verbose', '--tries=1', '--spider', 'http://localhost:8123/ping'],
                    'interval': '10s',
                    'timeout': '5s',
                    'retries': 3
                }
            }
        
        # Airflow Init Service
        services['airflow-init'] = {
            'image': 'apache/airflow:2.7.3-python3.11',
            'depends_on': {
                'postgres': {'condition': 'service_healthy'}
            },
            'environment': base_env,
            'volumes': base_volumes,
            'user': '${AIRFLOW_UID:-50000}:0',
            'entrypoint': '/bin/bash',
            'command': f'''
                -c "
                pip install {pip_install_cmd}
                python /opt/airflow/scripts/generate_dbt_profiles.py
                airflow db migrate
                airflow users create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
                "
            '''
        }
        
        # Airflow Webserver
        services['airflow-webserver'] = {
            'image': 'apache/airflow:2.7.3-python3.11',
            'depends_on': {
                'airflow-init': {'condition': 'service_completed_successfully'}
            },
            'environment': base_env,
            'volumes': base_volumes,
            'ports': ['8080:8080'],
            'user': '${AIRFLOW_UID:-50000}:0',
            'entrypoint': '/bin/bash',
            'command': f'''
                -c "
                pip install {pip_install_cmd}
                airflow webserver
                "
            '''
        }
        
        # Airflow Scheduler
        services['airflow-scheduler'] = {
            'image': 'apache/airflow:2.7.3-python3.11',
            'depends_on': {
                'airflow-init': {'condition': 'service_completed_successfully'}
            },
            'environment': base_env,
            'volumes': base_volumes,
            'user': '${AIRFLOW_UID:-50000}:0',
            'entrypoint': '/bin/bash',
            'command': f'''
                -c "
                pip install {pip_install_cmd}
                airflow scheduler
                "
            '''
        }
        
        # Create volumes list
        volumes = ['postgres_data:']
        if warehouse == 'clickhouse':
            volumes.append('clickhouse_data:')
        
        # Final docker-compose structure
        docker_compose = {
            'services': services,
            'volumes': {vol.rstrip(':'): None for vol in volumes}
        }
        
        # Write docker-compose.yml
        compose_path = Path(__file__).parent.parent / 'docker-compose.yml'
        with open(compose_path, 'w') as f:
            yaml.dump(docker_compose, f, default_flow_style=False, indent=2, sort_keys=False)
        
        print(f"✅ Generated {compose_path} for {warehouse}")
        print(f"   Warehouse: {config['warehouse']['name']}")
        print(f"   Services: {list(services.keys())}")
        print(f"   Packages: {packages}")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to generate docker-compose.yml: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = generate_docker_compose()
    sys.exit(0 if success else 1)