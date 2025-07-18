# acumatica_dag.py - Dynamic, incremental-aware pipeline
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import os
import subprocess
import sys
import yaml
import re
from glob import glob

# Allow importing extractor
sys.path.append('/opt/airflow')
sys.path.append('/opt/airflow/config')

from extractors.acumatica import extractor as acx  # incremental logic inside

ENV_PATTERN = re.compile(r'^\$\{([A-Z0-9_]+)\}$')
def _sub_env(obj):
    if isinstance(obj, dict):
        return {k: _sub_env(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sub_env(v) for v in obj]
    if isinstance(obj, str):
        m = ENV_PATTERN.match(obj.strip())
        if m:
            return os.getenv(m.group(1), obj)
    return obj

def load_acumatica_config():
    config_path = '/opt/airflow/config/sources/acumatica.yml'
    try:
        with open(config_path, 'r') as f:
            raw = yaml.safe_load(f)
        return _sub_env(raw)
    except FileNotFoundError:
        print(f"❌ Configuration file not found: {config_path}")
        raise
    except yaml.YAMLError as e:
        print(f"❌ YAML parse error: {e}")
        raise

def get_schedule_interval(config):
    schedule_config = config['dag']['schedule']
    stype = schedule_config.get('type')
    if stype == 'manual': return None
    if stype == 'hourly': return "0 * * * *"
    if stype == 'daily':
        t = schedule_config.get('time', "01:00")
        h, m = t.split(':'); return f"{m} {h} * * *"
    if stype == 'weekly':
        t = schedule_config.get('time', "01:00")
        h, m = t.split(':')
        days = {'monday': 1,'tuesday':2,'wednesday':3,'thursday':4,'friday':5,'saturday':6,'sunday':0}
        dnum = days.get(schedule_config.get('day_of_week', 'monday').lower(), 1)
        return f"{m} {h} * * {dnum}"
    if stype == 'monthly':
        t = schedule_config.get('time', "01:00")
        h, m = t.split(':')
        day = schedule_config.get('day_of_month', 1)
        return f"{m} {h} {day} * *"
    if stype == 'cron':
        return schedule_config.get('cron_expression', '0 1 * * *')
    return '0 1 * * *'

config = load_acumatica_config()
acx.init_extractor(config)
print(f"✅ Enabled endpoints: {list(acx.ENABLED_ENDPOINTS.keys())}")

# ---------------- EMAIL CALLBACKS ---------------- #
def send_success_email(context):
    from airflow.utils.email import send_email
    recipients = config['notifications']['email'].get('success_recipients', [])
    if not recipients: return
    dag_run = context['dag_run']
    subject = f"✅ SUCCESS: {dag_run.dag_id}"
    html = f"<h3>Acumatica Pipeline Success</h3><p>Run ID: {dag_run.run_id}</p>"
    try:
        send_email(to=recipients, subject=subject, html_content=html)
    except Exception as e:
        print(f"❌ Email send failed: {e}")

def send_failure_email(context):
    from airflow.utils.email import send_email
    recipients = config['notifications']['email'].get('failure_recipients', [])
    if not recipients: return
    ti = context['task_instance']
    ex = context.get('exception')
    subject = f"❌ FAILURE: {ti.dag_id}"
    html = f"<h3>Failure {ti.dag_id}:{ti.task_id}</h3><pre>{ex}</pre>"
    try:
        send_email(to=recipients, subject=subject, html_content=html)
    except Exception as e:
        print(f"❌ Email send failed: {e}")

default_args = {
    'owner': config['dag']['owner'],
    'depends_on_past': False,
    'start_date': datetime.strptime(config['dag']['start_date'], '%Y-%m-%d'),
    'email_on_failure': config['dag']['email_on_failure'],
    'email_on_retry': config['dag']['email_on_retry'],
    'retries': config['dag']['retries'],
    'retry_delay': timedelta(minutes=config['dag']['retry_delay_minutes']),
    'email': config['notifications']['email']['failure_recipients'],
    'on_failure_callback': send_failure_email,
}

dag = DAG(
    config['dag']['dag_id'],
    default_args=default_args,
    description=config['dag']['description'],
    schedule_interval=get_schedule_interval(config),
    max_active_runs=config['dag']['max_active_runs'],
    tags=config['dag']['tags']
)

# ---------------- LOADING (Incremental Upsert) ---------------- #
def test_connection(**context):
    acx.create_authenticated_session()
    print("✅ Connection test successful")

def load_csv_to_warehouse(**context):
    print("📊 Loading CSV files to warehouse (append/upsert)...")
    wh = os.getenv('ACTIVE_WAREHOUSE')
    if wh == 'postgres':
        return load_to_postgres_warehouse()
    if wh == 'snowflake':
        return load_to_snowflake_warehouse()
    if wh == 'clickhouse':
        return load_to_clickhouse_warehouse()
    raise ValueError(f"Unsupported warehouse: {wh}")

def load_to_postgres_warehouse():
    from sqlalchemy import create_engine, text
    import pandas as pd
    sys.path.append('/opt/airflow/config')
    from warehouse_config import get_connection_string
    engine = create_engine(get_connection_string('postgres'))
    bronze_schema = config['warehouse']['schemas']['bronze_schema']
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {bronze_schema}"))
    csv_dir = config['extraction']['paths']['raw_data_directory']
    total = 0
    for csv_path in glob(os.path.join(csv_dir, '*.csv')):
        endpoint_key = os.path.splitext(os.path.basename(csv_path))[0]
        table_name = f"raw_{endpoint_key}"
        df = pd.read_csv(csv_path)

        # Determine candidate PK columns
        pk_candidates = [c for c in df.columns if c.lower() in ('id', 'guid', 'bill_guid', 'customerid')]
        pk_col = pk_candidates[0] if pk_candidates else None

        with engine.begin() as conn:
            # Create table if not exists (all text for flexibility)
            cols = []
            for c in df.columns:
                safe = c.lower().replace(' ', '_').replace('-', '_')
                cols.append(f'"{safe}" TEXT')
            create_sql = f'CREATE TABLE IF NOT EXISTS "{bronze_schema}"."{table_name}" ({", ".join(cols)}'
            if pk_col:
                create_sql += f', PRIMARY KEY ("{pk_col}")'
            create_sql += ");"
            conn.execute(text(create_sql))

            # Upsert or append
            if pk_col:
                temp_table = f"__stg_{table_name}"
                conn.execute(text(f'DROP TABLE IF EXISTS "{bronze_schema}"."{temp_table}"'))
                conn.execute(text(f'CREATE TEMP TABLE "{temp_table}" AS SELECT * FROM "{bronze_schema}"."{table_name}" LIMIT 0'))
                df.to_sql(name=temp_table, schema=bronze_schema, con=conn, if_exists='append', index=False, method='multi')
                columns = [f'"{c.lower()}"' for c in df.columns]
                updates = [f'{c}=EXCLUDED.{c}' for c in columns if c.strip('"') != pk_col]
                insert_sql = f"""
                    INSERT INTO "{bronze_schema}"."{table_name}" ({', '.join(columns)})
                    SELECT {', '.join(columns)} FROM "{bronze_schema}"."{temp_table}"
                    ON CONFLICT ("{pk_col}") DO UPDATE SET {', '.join(updates)};
                """
                conn.execute(text(insert_sql))
            else:
                # No PK => append (duplicates possible intentionally)
                df.to_sql(name=table_name, schema=bronze_schema, con=conn, if_exists='append', index=False, method='multi')

        total += len(df)
        print(f"✅ Loaded/Upserted {len(df)} rows -> {bronze_schema}.{table_name} (pk={pk_col})")
    engine.dispose()
    print(f"✅ PostgreSQL total processed: {total}")
    return total

def load_to_snowflake_warehouse():
    print("⚠️ Snowflake loading not implemented")
    return 0

def load_to_clickhouse_warehouse():
    import pandas as pd
    import clickhouse_connect
    bronze_schema = config['warehouse']['schemas']['bronze_schema']
    print("📊 Loading CSV files to ClickHouse (ReplacingMergeTree)...")
    host = os.getenv('CLICKHOUSE_HOST')
    port = int(os.getenv('CLICKHOUSE_PORT', 8443))
    database = os.getenv('CLICKHOUSE_DATABASE', 'default')
    username = os.getenv('CLICKHOUSE_USER', 'default')
    password = os.getenv('CLICKHOUSE_PASSWORD')
    client = clickhouse_connect.get_client(
        host=host, port=port, username=username, password=password,
        database=database, secure=True
    )
    if bronze_schema != 'default':
        client.command(f"CREATE DATABASE IF NOT EXISTS {bronze_schema}")

    csv_dir = config['extraction']['paths']['raw_data_directory']
    total = 0
    for csv_path in glob(os.path.join(csv_dir, '*.csv')):
        endpoint_key = os.path.splitext(os.path.basename(csv_path))[0]
        table_name = f"raw_{endpoint_key}"
        full_table = f"{bronze_schema}.{table_name}"
        df = pd.read_csv(csv_path)

        # Choose primary identifier if available
        pk_candidates = [c for c in df.columns if c.lower() in ('id', 'guid', 'bill_guid', 'reference_number')]
        pk_col = pk_candidates[0] if pk_candidates else None

        # Version column preference
        version_candidates = [c for c in df.columns if 'last_modified' in c.lower()] + ['_extracted_at']
        version_col = None
        for c in version_candidates:
            if c in df.columns:
                version_col = c
                break

        # Ensure safe identifiers
        def safe(c):
            return c.replace(' ', '_').replace('-', '_').replace('.', '_')
        df.columns = [safe(c) for c in df.columns]
        if version_col:
            version_col = safe(version_col)
        if pk_col:
            pk_col = safe(pk_col)

        # Create table if not exists
        existing = False
        try:
            client.query(f"DESCRIBE TABLE {full_table}")
            existing = True
        except Exception:
            existing = False

        if not existing:
            cols = []
            for c in df.columns:
                cols.append(f"`{c}` String")
            order_expr = pk_col if pk_col else "tuple()"
            engine = f"ReplacingMergeTree({version_col})" if version_col else "MergeTree()"
            ddl = f"CREATE TABLE {full_table} ({', '.join(cols)}) ENGINE={engine} ORDER BY {order_expr}"
            client.command(ddl)
            print(f"🆕 Created table {full_table} (engine={engine}, order_by={order_expr})")

        # Insert (ReplacingMergeTree will unify latest versions)
        df_str = df.astype(str).replace('nan', '')
        client.insert_df(table=full_table, df=df_str)
        total += len(df)
        print(f"✅ Inserted {len(df)} rows into {full_table} (pk={pk_col}, version={version_col})")

    client.close()
    print(f"✅ ClickHouse total processed: {total}")
    return total

# ---------------- DBT TRANSFORM ---------------- #
def list_dbt_models():
    dbt_dir = config['dbt']['project_dir']
    cmd = ["dbt", "ls", "--resource-type", "model", "--output", "name"]
    res = subprocess.run(cmd, cwd=dbt_dir, capture_output=True, text=True)
    if res.returncode != 0:
        print(f"⚠️ dbt ls failed: {res.stderr}")
        return set()
    models = {l.strip() for l in res.stdout.splitlines() if l.strip()}
    print(f"🧾 Discovered {len(models)} dbt models.")
    return models

def derive_raw_model_name(endpoint_key, project_models):
    candidates = [f"{endpoint_key}_raw"]
    if endpoint_key.endswith('s'):
        singular = endpoint_key[:-1]
        candidates.append(f"{singular}_raw")
    else:
        candidates.append(f"{endpoint_key}s_raw")
    seen = set()
    for c in candidates:
        if c not in seen:
            seen.add(c)
            if c in project_models:
                return c
    return None

def run_dbt_transformations(**context):
    print("🔧 Running dbt transformations (dynamic).")
    dbt_dir = config['dbt']['project_dir']
    fail_fast = "--fail-fast" if config['dbt']['execution']['fail_fast'] else ""
    full_refresh = "--full-refresh" if config['dbt']['execution']['full_refresh'] else ""
    threads = f"--threads {config['dbt']['execution']['threads']}"
    project_models = list_dbt_models()

    csv_dir = config['extraction']['paths']['raw_data_directory']
    csv_files = glob(os.path.join(csv_dir, '*.csv'))
    all_eps = [os.path.splitext(os.path.basename(p))[0] for p in csv_files]

    enabled_eps = set(acx.ENABLED_ENDPOINTS.keys())
    endpoints_with_data = [ep for ep in all_eps if ep in enabled_eps]

    if not endpoints_with_data:
        print("⚠️ No enabled endpoints with data; skip dbt.")
        return

    raw_models = []
    for ep in endpoints_with_data:
        rm = derive_raw_model_name(ep, project_models)
        if rm:
            raw_models.append((ep, rm))
        else:
            print(f"⏭ No raw model for {ep}")

    executed_raw = []
    for ep, rm in raw_models:
        cmd = f"dbt run --select {rm} {threads} {fail_fast} {full_refresh}"
        cmd_clean = ' '.join(cmd.split())
        print(f"▶️ RAW: {cmd_clean}")
        res = subprocess.run(cmd_clean.split(), cwd=dbt_dir, capture_output=True, text=True)
        if res.returncode != 0:
            print(res.stdout); print(res.stderr)
            raise Exception(f"dbt raw model failed: {rm}")
        executed_raw.append(rm)
        print(f"✅ RAW model {rm} done")

    business_models = []
    for _, rm in raw_models:
        base = rm[:-4]
        if base in project_models:
            business_models.append(base)
        elif base.endswith('s') and base[:-1] in project_models:
            business_models.append(base[:-1])
        else:
            print(f"🔍 No business model for {rm}")

    for bm in business_models:
        cmd = f"dbt run --select {bm} {threads} {fail_fast} {full_refresh}"
        cmd_clean = ' '.join(cmd.split())
        print(f"▶️ BUSINESS: {cmd_clean}")
        res = subprocess.run(cmd_clean.split(), cwd=dbt_dir, capture_output=True, text=True)
        if res.returncode != 0:
            print(res.stdout); print(res.stderr)
            raise Exception(f"dbt business model failed: {bm}")
        print(f"✅ BUSINESS model {bm} done")

    print("✅ dbt transformation sequence complete.")

def check_transformed_data(**context):
    print("🔍 Data quality (light)")
    try:
        import clickhouse_connect
        host = os.getenv('CLICKHOUSE_HOST')
        port = int(os.getenv('CLICKHOUSE_PORT', 8443))
        database = os.getenv('CLICKHOUSE_DATABASE', 'default')
        username = os.getenv('CLICKHOUSE_USER', 'default')
        password = os.getenv('CLICKHOUSE_PASSWORD')
        client = clickhouse_connect.get_client(
            host=host, port=port, username=username, password=password,
            database=database, secure=True
        )
        staging_schema = config['warehouse']['schemas']['staging_schema']
        result = client.query(f"SHOW TABLES FROM {staging_schema}")
        print(f"📊 Tables in {staging_schema}: {[r[0] for r in result.result_rows]}")
        client.close()
    except Exception as e:
        print(f"⚠️ Quality check skipped: {e}")

def send_pipeline_success_notification(**context):
    send_success_email(context)
    return "Success notification sent"

with dag:
    start_task = EmptyOperator(task_id='start_pipeline')
    test_task = PythonOperator(task_id='test_acumatica_connection', python_callable=test_connection)

    extraction_tasks = []
    for endpoint_key in acx.ENABLED_ENDPOINTS.keys():
        extraction_tasks.append(
            PythonOperator(
                task_id=f"extract_{endpoint_key}",
                python_callable=acx.extract_acumatica_endpoint,
                op_kwargs={'endpoint_key': endpoint_key},
                provide_context=True
            )
        )

    load_task = PythonOperator(task_id='load_to_warehouse', python_callable=load_csv_to_warehouse)
    transform_task = PythonOperator(task_id='run_dbt_transformations', python_callable=run_dbt_transformations)
    quality_check_task = PythonOperator(task_id='check_data_quality', python_callable=check_transformed_data)
    success_notification_task = PythonOperator(task_id='send_success_notification', python_callable=send_pipeline_success_notification)
    end_task = EmptyOperator(task_id='end_pipeline')

    start_task >> test_task >> extraction_tasks >> load_task >> transform_task >> quality_check_task >> success_notification_task >> end_task
