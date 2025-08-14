#!/usr/bin/env python3 
import sys
import os
from pathlib import Path

# Add config directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'config'))

from warehouse_config import load_warehouse_config, get_active_warehouse, get_required_packages
import yaml

def get_chrome_install_command():
    """Chrome/Chromium install for x86_64 + ARM64 (Debian/Ubuntu) with safe cleanup."""
    return r'''
        set -e

        # Ensure apt lists path exists (avoids edge errors)
        mkdir -p /var/lib/apt/lists/partial
        chmod 755 /var/lib/apt/lists /var/lib/apt/lists/partial || true

        apt-get update
        # base libs for headless
        apt-get install -y gnupg wget unzip xvfb fonts-liberation libnss3 libxss1 libasound2 libgbm1 libgtk-3-0

        ARCH=$(uname -m)
        echo "Detected architecture: $ARCH"

        if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then
            echo "Installing Chromium/Chromedriver for ARM64..."
            # Try Debian names first, then Ubuntu alternates
            apt-get install -y chromium chromium-driver || apt-get install -y chromium-browser chromium-chromedriver
        else
            echo "Installing Google Chrome (x86_64) and Chromedriver..."
            wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add -
            echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list
            apt-get update
            apt-get install -y google-chrome-stable
            # Chromedriver from distro; version mismatch is usually OK for automation
            apt-get install -y chromium-driver || true
        fi

        # Show versions for debugging
        (chromium --version || chromium-browser --version || google-chrome --version || true) 2>/dev/null || true
        (chromedriver --version || chromium-chromedriver --version || true) 2>/dev/null || true

        # Clean apt caches; DO NOT remove the mounted /tmp/dcc_downloads volume
        apt-get clean || true
        rm -rf /var/lib/apt/lists/* /var/tmp/* || true
        # Prune /tmp but keep the downloads mount; ignore errors
        find /tmp -mindepth 1 -maxdepth 1 -not -path '/tmp/dcc_downloads' -exec rm -rf {} + || true

        # (Re)create downloads dir after cleanup
        mkdir -p /tmp/dcc_downloads
        chmod 777 /tmp/dcc_downloads
    '''



def generate_docker_compose():
    """Generate docker-compose.yml for active warehouse"""
    
    try:
        warehouse = get_active_warehouse()
        config = load_warehouse_config(warehouse)
        packages = get_required_packages(warehouse)
        
        # Base environment variables - using environment variable substitution
        base_env = [
            'AIRFLOW__CORE__EXECUTOR=${AIRFLOW__CORE__EXECUTOR}',
            'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://${DATABASE_USER}:${DATABASE_PASSWORD}@postgres/${DATABASE_NAME}',
            'AIRFLOW__CORE__FERNET_KEY=${AIRFLOW__CORE__FERNET_KEY}',
            'AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true',
            'AIRFLOW__CORE__LOAD_EXAMPLES=false',
            'AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session',
            
            # SMTP Configuration for Email Notifications
            'AIRFLOW__SMTP__SMTP_HOST=${SMTP_HOST:-smtp.gmail.com}',
            'AIRFLOW__SMTP__SMTP_STARTTLS=True',
            'AIRFLOW__SMTP__SMTP_SSL=False',
            'AIRFLOW__SMTP__SMTP_PORT=${SMTP_PORT:-587}',
            'AIRFLOW__SMTP__SMTP_USER=${SMTP_USER}',
            'AIRFLOW__SMTP__SMTP_PASSWORD=${SMTP_PASSWORD}',
            'AIRFLOW__SMTP__SMTP_MAIL_FROM=${SMTP_FROM_EMAIL}',
            
            # Acumatica Configuration
            'ACUMATICA_BASE_URL=${ACUMATICA_BASE_URL}',
            'ACUMATICA_USERNAME=${ACUMATICA_USERNAME}',
            'ACUMATICA_PASSWORD=${ACUMATICA_PASSWORD}',
            'ACUMATICA_COMPANY=${ACUMATICA_COMPANY}',
            'ACUMATICA_BRANCH=${ACUMATICA_BRANCH}',
            # Repsly Configuration
            'REPSLY_BASE_URL=${REPSLY_BASE_URL}',
            'REPSLY_USERNAME=${REPSLY_USERNAME}',
            'REPSLY_PASSWORD=${REPSLY_PASSWORD}',
            # Leaflink Configuration
            'LEAFLINK_API_KEY=${LEAFLINK_API_KEY}',
            'LEAFLINK_COMPANY_ID=${LEAFLINK_COMPANY_ID}',
            # Google Spreadsheet Configuration
            'GOOGLE_SPREADSHEET_ID=${GOOGLE_SPREADSHEET_ID}',
            'GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS}',
            # Warehouse Configuration
            f'ACTIVE_WAREHOUSE={warehouse}',
            'DATABASE_HOST=${DATABASE_HOST}',
            'DATABASE_PORT=${DATABASE_PORT}',
            'DATABASE_NAME=${DATABASE_NAME}',
            'DATABASE_USER=${DATABASE_USER}',
            'DATABASE_PASSWORD=${DATABASE_PASSWORD}',
            # Future warehouse configs
            'SNOWFLAKE_ACCOUNT=${SNOWFLAKE_ACCOUNT}',
            'SNOWFLAKE_USER=${SNOWFLAKE_USER}',
            'SNOWFLAKE_PASSWORD=${SNOWFLAKE_PASSWORD}',
            'SNOWFLAKE_WAREHOUSE=${SNOWFLAKE_WAREHOUSE}',
            'SNOWFLAKE_DATABASE=${SNOWFLAKE_DATABASE}',
            'CLICKHOUSE_HOST=${CLICKHOUSE_HOST}',
            'CLICKHOUSE_PORT=${CLICKHOUSE_PORT}',
            'CLICKHOUSE_DATABASE=${CLICKHOUSE_DATABASE}',
            'CLICKHOUSE_USER=${CLICKHOUSE_USER}',
            'CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD}',
            
            # Chrome/Selenium Configuration for DCC Scraper
            'CHROME_BIN=${CHROME_BIN}',
            'CHROMEDRIVER_PATH=${CHROMEDRIVER_PATH}',
            'DISPLAY=${DISPLAY}'
        ]
        
        # Base volumes
        base_volumes = [
            './airflow/dags:/opt/airflow/dags',
            './airflow/logs:/opt/airflow/logs',
            './airflow/config:/opt/airflow/config',
            './airflow/plugins:/opt/airflow/plugins',
            './airflow/state:/opt/airflow/state',
            './data:/opt/airflow/data',
            './dbt:/opt/airflow/dbt',
            './config:/opt/airflow/config',
            './scripts:/opt/airflow/scripts',
            './extractors:/opt/airflow/extractors',
            './macros:/opt/airflow/macros',
            './google-sheets-api.json:/opt/airflow/credentials/google-sheets-api.json:ro',
            # Add volume for DCC downloads
            './tmp/dcc_downloads:/tmp/dcc_downloads'
        ]
        
        # Package installation command
        pip_install_cmd = ' '.join(packages) if packages else ''
        
                # Chrome installation command
        chrome_install_cmd = get_chrome_install_command()
        # Prevent docker compose variable interpolation inside the script
        chrome_install_cmd = chrome_install_cmd.replace('$', '$$')

        
        # Generate services
        services = {}
        
        # Always include PostgreSQL for Airflow metadata
        services['postgres'] = {
            'image': 'postgres:14',
            'environment': {
                'POSTGRES_USER': '${DATABASE_USER}',
                'POSTGRES_PASSWORD': '${DATABASE_PASSWORD}',
                'POSTGRES_DB': '${DATABASE_NAME}'
            },
            'ports': ['${DATABASE_PORT}:5432'],
            'volumes': ['postgres_data:/var/lib/postgresql/data'],
            'healthcheck': {
                'test': ['CMD', 'pg_isready', '-U', '${DATABASE_USER}'],
                'interval': '5s',
                'retries': 5
            }
        }
        
        # Add warehouse-specific service if needed
        if warehouse == 'clickhouse':
            services['clickhouse'] = {
                'image': 'clickhouse/clickhouse-server:latest',
                'ports': ['${CLICKHOUSE_PORT:-8123}:8123', '9000:9000', '9440:9440'],
                'environment': {
                    'CLICKHOUSE_DB': '${CLICKHOUSE_DATABASE}',
                    'CLICKHOUSE_USER': '${CLICKHOUSE_USER}',
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
        
        # Determine dependencies based on warehouse
        dependencies = {'postgres': {'condition': 'service_healthy'}}
        if warehouse == 'clickhouse':
            dependencies['clickhouse'] = {'condition': 'service_healthy'}
        
        # Airflow Init Service
        services['airflow-init'] = {
            'image': 'apache/airflow:2.7.3-python3.11',
            'depends_on': dependencies,
            'environment': base_env,
            'volumes': base_volumes,
            'user': '0:0',
            'entrypoint': '/bin/bash',
            'command': [
                '-c',
                f'''( {chrome_install_cmd} ) && \
            Xvfb :99 -screen 0 1920x1080x24 & \
            gosu airflow bash -lc "pip install {pip_install_cmd} && \
            python /opt/airflow/scripts/generate_dbt_profiles.py && \
            airflow db migrate && \
            airflow users create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin"'''
            ]
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
            'user': '0:0',
            'entrypoint': '/bin/bash',
            'command': [
                '-c',
                f'''( {chrome_install_cmd} ) && \
            Xvfb :99 -screen 0 1920x1080x24 & \
            gosu airflow bash -lc "pip install {pip_install_cmd} && \
            airflow webserver"'''
            ]

        }
        
        # Airflow Scheduler
        services['airflow-scheduler'] = {
            'image': 'apache/airflow:2.7.3-python3.11',
            'depends_on': {
                'airflow-init': {'condition': 'service_completed_successfully'}
            },
            'environment': base_env,
            'volumes': base_volumes,
            'user': '0:0',
            'entrypoint': '/bin/bash',
            'command': [
                '-c',
                f'''( {chrome_install_cmd} ) && \
            Xvfb :99 -screen 0 1920x1080x24 & \
            gosu airflow bash -lc "pip install {pip_install_cmd} && \
            airflow scheduler"'''
            ]

        }
        
        # Create volumes list
        volumes = ['postgres_data']
        if warehouse == 'clickhouse':
            volumes.append('clickhouse_data')
        
        # Final docker-compose structure with proper YAML formatting
        docker_compose = {
            'version': '3.8',
            'services': services,
            'volumes': {vol: None for vol in volumes}
        }
        
        # Write docker-compose.yml with proper YAML formatting
        compose_path = Path(__file__).parent.parent / 'docker-compose.yml'
        with open(compose_path, 'w') as f:
            # Custom YAML formatting to match your existing style
            f.write("version: '3.8'\n\n")
            f.write("services:\n")
            
            for service_name, service_config in services.items():
                f.write(f"  {service_name}:\n")
                
                # Write image
                f.write(f"    image: {service_config['image']}\n")
                
                # Write depends_on if exists
                if 'depends_on' in service_config:
                    f.write("    depends_on:\n")
                    for dep, condition in service_config['depends_on'].items():
                        f.write(f"      {dep}:\n")
                        if isinstance(condition, dict):
                            for k, v in condition.items():
                                f.write(f"        {k}: {v}\n")
                
                # Write environment with proper anchoring for reuse
                if 'environment' in service_config:
                    if service_name == 'airflow-init':
                        f.write("    environment: &airflow_common_env\n")
                    elif service_name in ['airflow-webserver', 'airflow-scheduler']:
                        f.write("    environment: *airflow_common_env\n")
                    else:
                        f.write("    environment:\n")
                    
                    if service_name == 'airflow-init' or service_name == 'postgres' or service_name == 'clickhouse':
                        env_vars = service_config['environment']
                        if isinstance(env_vars, list):
                            for env_var in env_vars:
                                f.write(f"      - {env_var}\n")
                        else:
                            for key, value in env_vars.items():
                                f.write(f"      {key}: {value}\n")
                
                # Write volumes with proper anchoring for reuse
                if 'volumes' in service_config:
                    if service_name == 'airflow-init':
                        f.write("    volumes: &airflow_common_volumes\n")
                    elif service_name in ['airflow-webserver', 'airflow-scheduler']:
                        f.write("    volumes: *airflow_common_volumes\n")
                    else:
                        f.write("    volumes:\n")
                    
                    if service_name == 'airflow-init' or service_name == 'postgres' or service_name == 'clickhouse':
                        for volume in service_config['volumes']:
                            f.write(f"      - {volume}\n")
                
                # Write ports
                if 'ports' in service_config:
                    f.write("    ports:\n")
                    for port in service_config['ports']:
                        f.write(f"      - \"{port}\"\n")
                
                # Write user
                if 'user' in service_config:
                    f.write(f"    user: \"{service_config['user']}\"\n")
                
                # Write entrypoint
                if 'entrypoint' in service_config:
                    f.write(f"    entrypoint: {service_config['entrypoint']}\n")
                
                # Write command with proper formatting (supports multiline safely)
                if 'command' in service_config:
                    command = service_config['command']

                    def _write_block(lines, indent):
                        for line in lines:
                            f.write(f"{indent}{line.rstrip()}\n")

                    if isinstance(command, list):
                        f.write("    command:\n")
                        for cmd_part in command:
                            if isinstance(cmd_part, str) and '\n' in cmd_part:
                                # Render multiline list item as a block scalar
                                f.write("      - |-\n")
                                _write_block(cmd_part.strip().split('\n'), "          ")
                            else:
                                # Single-line list item
                                f.write(f"      - {cmd_part}\n")
                    else:
                        # Single multiline string command
                        f.write("    command: |-\n")
                        _write_block(command.strip().split('\n'), "      ")

                
                # Write healthcheck
                if 'healthcheck' in service_config:
                    f.write("    healthcheck:\n")
                    hc = service_config['healthcheck']
                    f.write(f"      test: {hc['test']}\n")
                    f.write(f"      interval: {hc['interval']}\n")
                    f.write(f"      retries: {hc['retries']}\n")
                    if 'timeout' in hc:
                        f.write(f"      timeout: {hc['timeout']}\n")
                
                f.write("\n")
            
            # Write volumes
            f.write("volumes:\n")
            for volume in volumes:
                f.write(f"  {volume}:\n")
        
        # Create temporary directory for DCC downloads
        dcc_downloads_dir = Path(__file__).parent.parent / 'tmp' / 'dcc_downloads'
        dcc_downloads_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"✅ Generated {compose_path} for {warehouse}")
        print(f"   Warehouse: {config['warehouse']['name']}")
        print(f"   Services: {list(services.keys())}")
        print(f"   Packages: {packages}")
        print(f"   🔐 Using environment variables for credentials")
        print(f"   📧 SMTP configuration included")
        print(f"   🌐 Chrome/Chromium support added for DCC scraper")
        print(f"   📁 DCC downloads directory created: {dcc_downloads_dir}")
        print(f"   🏗️ Multi-architecture support (x86_64 + ARM64)")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to generate docker-compose.yml: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = generate_docker_compose()
    sys.exit(0 if success else 1)

    
# #!/usr/bin/env python3 (generate_docker_compose.py)
# import sys
# import os
# from pathlib import Path

# # Add config directory to path
# sys.path.insert(0, str(Path(__file__).parent.parent / 'config'))

# from warehouse_config import load_warehouse_config, get_active_warehouse, get_required_packages
# import yaml

# def generate_docker_compose():
#     """Generate docker-compose.yml for active warehouse"""
    
#     try:
#         warehouse = get_active_warehouse()
#         config = load_warehouse_config(warehouse)
#         packages = get_required_packages(warehouse)
        
#         # Base environment variables - using environment variable substitution
#         base_env = [
#             'AIRFLOW__CORE__EXECUTOR=${AIRFLOW__CORE__EXECUTOR}',
#             'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://${DATABASE_USER}:${DATABASE_PASSWORD}@postgres/${DATABASE_NAME}',
#             'AIRFLOW__CORE__FERNET_KEY=${AIRFLOW__CORE__FERNET_KEY}',
#             'AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true',
#             'AIRFLOW__CORE__LOAD_EXAMPLES=false',
#             'AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session',
            
#             # SMTP Configuration for Email Notifications
#             'AIRFLOW__SMTP__SMTP_HOST=${SMTP_HOST:-smtp.gmail.com}',
#             'AIRFLOW__SMTP__SMTP_STARTTLS=True',
#             'AIRFLOW__SMTP__SMTP_SSL=False',
#             'AIRFLOW__SMTP__SMTP_PORT=${SMTP_PORT:-587}',
#             'AIRFLOW__SMTP__SMTP_USER=${SMTP_USER}',
#             'AIRFLOW__SMTP__SMTP_PASSWORD=${SMTP_PASSWORD}',
#             'AIRFLOW__SMTP__SMTP_MAIL_FROM=${SMTP_FROM_EMAIL}',
            
#             # Acumatica Configuration
#             'ACUMATICA_BASE_URL=${ACUMATICA_BASE_URL}',
#             'ACUMATICA_USERNAME=${ACUMATICA_USERNAME}',
#             'ACUMATICA_PASSWORD=${ACUMATICA_PASSWORD}',
#             'ACUMATICA_COMPANY=${ACUMATICA_COMPANY}',
#             'ACUMATICA_BRANCH=${ACUMATICA_BRANCH}',
#             # Repsly Configuration
#             'REPSLY_BASE_URL=${REPSLY_BASE_URL}',
#             'REPSLY_USERNAME=${REPSLY_USERNAME}',
#             'REPSLY_PASSWORD=${REPSLY_PASSWORD}',
#             # Leaflink Configuration
#             'LEAFLINK_API_KEY=${LEAFLINK_API_KEY}',
#             'LEAFLINK_COMPANY_ID=${LEAFLINK_COMPANY_ID}',
#             # Google Spreadsheet Configuration
#             'GOOGLE_SPREADSHEET_ID=${GOOGLE_SPREADSHEET_ID}',
#             'GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS}',
#             # Warehouse Configuration
#             f'ACTIVE_WAREHOUSE={warehouse}',
#             'DATABASE_HOST=${DATABASE_HOST}',
#             'DATABASE_PORT=${DATABASE_PORT}',
#             'DATABASE_NAME=${DATABASE_NAME}',
#             'DATABASE_USER=${DATABASE_USER}',
#             'DATABASE_PASSWORD=${DATABASE_PASSWORD}',
#             # Future warehouse configs
#             'SNOWFLAKE_ACCOUNT=${SNOWFLAKE_ACCOUNT}',
#             'SNOWFLAKE_USER=${SNOWFLAKE_USER}',
#             'SNOWFLAKE_PASSWORD=${SNOWFLAKE_PASSWORD}',
#             'SNOWFLAKE_WAREHOUSE=${SNOWFLAKE_WAREHOUSE}',
#             'SNOWFLAKE_DATABASE=${SNOWFLAKE_DATABASE}',
#             'CLICKHOUSE_HOST=${CLICKHOUSE_HOST}',
#             'CLICKHOUSE_PORT=${CLICKHOUSE_PORT}',
#             'CLICKHOUSE_DATABASE=${CLICKHOUSE_DATABASE}',
#             'CLICKHOUSE_USER=${CLICKHOUSE_USER}',
#             'CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD}'
#         ]
        
#         # Base volumes
#         base_volumes = [
#             './airflow/dags:/opt/airflow/dags',
#             './airflow/logs:/opt/airflow/logs',
#             './airflow/config:/opt/airflow/config',
#             './airflow/plugins:/opt/airflow/plugins',
#             './airflow/state:/opt/airflow/state',
#             './data:/opt/airflow/data',
#             './dbt:/opt/airflow/dbt',
#             './config:/opt/airflow/config',
#             './scripts:/opt/airflow/scripts',
#             './extractors:/opt/airflow/extractors',
#             './macros:/opt/airflow/macros',
#             './google-sheets-api.json:/opt/airflow/credentials/google-sheets-api.json:ro'
#         ]
        
#         # Package installation command
#         pip_install_cmd = ' '.join(packages)
        
#         # Generate services
#         services = {}
        
#         # Always include PostgreSQL for Airflow metadata
#         services['postgres'] = {
#             'image': 'postgres:14',
#             'environment': {
#                 'POSTGRES_USER': '${DATABASE_USER}',
#                 'POSTGRES_PASSWORD': '${DATABASE_PASSWORD}',
#                 'POSTGRES_DB': '${DATABASE_NAME}'
#             },
#             'ports': ['${DATABASE_PORT}:5432'],
#             'volumes': ['postgres_data:/var/lib/postgresql/data'],
#             'healthcheck': {
#                 'test': ['CMD', 'pg_isready', '-U', '${DATABASE_USER}'],
#                 'interval': '5s',
#                 'retries': 5
#             }
#         }
        
#         # Add warehouse-specific service if needed
#         if warehouse == 'clickhouse':
#             services['clickhouse'] = {
#                 'image': 'clickhouse/clickhouse-server:latest',
#                 'ports': ['${CLICKHOUSE_PORT:-8123}:8123', '9000:9000', '9440:9440'],
#                 'environment': {
#                     'CLICKHOUSE_DB': '${CLICKHOUSE_DATABASE}',
#                     'CLICKHOUSE_USER': '${CLICKHOUSE_USER}',
#                     'CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT': '1'
#                 },
#                 'volumes': ['clickhouse_data:/var/lib/clickhouse'],
#                 'healthcheck': {
#                     'test': ['CMD', 'wget', '--no-verbose', '--tries=1', '--spider', 'http://localhost:8123/ping'],
#                     'interval': '10s',
#                     'timeout': '5s',
#                     'retries': 3
#                 }
#             }
        
#         # Determine dependencies based on warehouse
#         dependencies = {'postgres': {'condition': 'service_healthy'}}
#         if warehouse == 'clickhouse':
#             dependencies['clickhouse'] = {'condition': 'service_healthy'}
        
#         # Airflow Init Service
#         services['airflow-init'] = {
#             'image': 'apache/airflow:2.7.3-python3.11',
#             'depends_on': dependencies,
#             'environment': base_env,
#             'volumes': base_volumes,
#             'user': '${AIRFLOW_UID:-50000}:0',
#             'entrypoint': '/bin/bash',
#             'command': f'''
#                 -c "
#                 pip install {pip_install_cmd}
#                 python /opt/airflow/scripts/generate_dbt_profiles.py
#                 airflow db migrate
#                 airflow users create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
#                 "
#             '''
#         }
        
#         # Airflow Webserver
#         services['airflow-webserver'] = {
#             'image': 'apache/airflow:2.7.3-python3.11',
#             'depends_on': {
#                 'airflow-init': {'condition': 'service_completed_successfully'}
#             },
#             'environment': base_env,
#             'volumes': base_volumes,
#             'ports': ['8080:8080'],
#             'user': '${AIRFLOW_UID:-50000}:0',
#             'entrypoint': '/bin/bash',
#             'command': f'''
#                 -c "
#                 pip install {pip_install_cmd}
#                 airflow webserver
#                 "
#             '''
#         }
        
#         # Airflow Scheduler
#         services['airflow-scheduler'] = {
#             'image': 'apache/airflow:2.7.3-python3.11',
#             'depends_on': {
#                 'airflow-init': {'condition': 'service_completed_successfully'}
#             },
#             'environment': base_env,
#             'volumes': base_volumes,
#             'user': '${AIRFLOW_UID:-50000}:0',
#             'entrypoint': '/bin/bash',
#             'command': f'''
#                 -c "
#                 pip install {pip_install_cmd}
#                 airflow scheduler
#                 "
#             '''
#         }
        
#         # Create volumes list
#         volumes = ['postgres_data']
#         if warehouse == 'clickhouse':
#             volumes.append('clickhouse_data')
        
#         # Final docker-compose structure with proper YAML formatting
#         docker_compose = {
#             'version': '3.8',
#             'services': services,
#             'volumes': {vol: None for vol in volumes}
#         }
        
#         # Write docker-compose.yml with proper YAML formatting
#         compose_path = Path(__file__).parent.parent / 'docker-compose.yml'
#         with open(compose_path, 'w') as f:
#             # Custom YAML formatting to match your existing style
#             f.write("version: '3.8'\n\n")
#             f.write("services:\n")
            
#             for service_name, service_config in services.items():
#                 f.write(f"  {service_name}:\n")
                
#                 # Write image
#                 f.write(f"    image: {service_config['image']}\n")
                
#                 # Write depends_on if exists
#                 if 'depends_on' in service_config:
#                     f.write("    depends_on:\n")
#                     for dep, condition in service_config['depends_on'].items():
#                         f.write(f"      {dep}:\n")
#                         if isinstance(condition, dict):
#                             for k, v in condition.items():
#                                 f.write(f"        {k}: {v}\n")
                
#                 # Write environment with proper anchoring for reuse
#                 if 'environment' in service_config:
#                     if service_name == 'airflow-init':
#                         f.write("    environment: &airflow_common_env\n")
#                     elif service_name in ['airflow-webserver', 'airflow-scheduler']:
#                         f.write("    environment: *airflow_common_env\n")
#                     else:
#                         f.write("    environment:\n")
                    
#                     if service_name == 'airflow-init' or service_name == 'postgres' or service_name == 'clickhouse':
#                         env_vars = service_config['environment']
#                         if isinstance(env_vars, list):
#                             for env_var in env_vars:
#                                 f.write(f"      - {env_var}\n")
#                         else:
#                             for key, value in env_vars.items():
#                                 f.write(f"      {key}: {value}\n")
                
#                 # Write volumes with proper anchoring for reuse
#                 if 'volumes' in service_config:
#                     if service_name == 'airflow-init':
#                         f.write("    volumes: &airflow_common_volumes\n")
#                     elif service_name in ['airflow-webserver', 'airflow-scheduler']:
#                         f.write("    volumes: *airflow_common_volumes\n")
#                     else:
#                         f.write("    volumes:\n")
                    
#                     if service_name == 'airflow-init' or service_name == 'postgres' or service_name == 'clickhouse':
#                         for volume in service_config['volumes']:
#                             f.write(f"      - {volume}\n")
                
#                 # Write ports
#                 if 'ports' in service_config:
#                     f.write("    ports:\n")
#                     for port in service_config['ports']:
#                         f.write(f"      - \"{port}\"\n")
                
#                 # Write user
#                 if 'user' in service_config:
#                     f.write(f"    user: \"{service_config['user']}\"\n")
                
#                 # Write entrypoint
#                 if 'entrypoint' in service_config:
#                     f.write(f"    entrypoint: {service_config['entrypoint']}\n")
                
#                 # Write command with proper multiline formatting
#                 if 'command' in service_config:
#                     command = service_config['command'].strip()
#                     f.write("    command: |\n")
#                     for line in command.split('\n'):
#                         if line.strip():
#                             f.write(f"      {line.strip()}\n")
                
#                 # Write healthcheck
#                 if 'healthcheck' in service_config:
#                     f.write("    healthcheck:\n")
#                     hc = service_config['healthcheck']
#                     f.write(f"      test: {hc['test']}\n")
#                     f.write(f"      interval: {hc['interval']}\n")
#                     f.write(f"      retries: {hc['retries']}\n")
#                     if 'timeout' in hc:
#                         f.write(f"      timeout: {hc['timeout']}\n")
                
#                 f.write("\n")
            
#             # Write volumes
#             f.write("volumes:\n")
#             for volume in volumes:
#                 f.write(f"  {volume}:\n")
        
#         print(f"✅ Generated {compose_path} for {warehouse}")
#         print(f"   Warehouse: {config['warehouse']['name']}")
#         print(f"   Services: {list(services.keys())}")
#         print(f"   Packages: {packages}")
#         print(f"   🔐 Using environment variables for credentials")
#         print(f"   📧 SMTP configuration included")
        
#         return True
        
#     except Exception as e:
#         print(f"❌ Failed to generate docker-compose.yml: {e}")
#         import traceback
#         traceback.print_exc()
#         return False

# if __name__ == "__main__":
#     success = generate_docker_compose()
#     sys.exit(0 if success else 1)