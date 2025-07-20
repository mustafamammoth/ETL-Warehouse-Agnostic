# Incremental Acumatica Pipeline

This project contains an Airflow DAG and dbt models for extracting data from the Acumatica API and loading it into a warehouse. Extraction runs incrementally by tracking the last processed timestamp in a state file.

## How Incremental Extraction Works

1. The extractor reads a JSON state file that stores the most recent `LastModifiedDateTime` per endpoint.
2. When the DAG executes, the extractor requests records newer than the stored timestamp.
3. After each endpoint finishes, the state file is updated with the maximum timestamp returned.
4. On the first run, if the state file is empty, all available records are pulled.

Keep the state file in a persistent location (for example `data/state/acumatica_state.json`) so subsequent DAG runs can continue from the last checkpoint.

## Configuration

Configuration values live in `config/sources/acumatica.yml`. Key sections are:

- **DAG settings** including schedule and retries【F:config/sources/acumatica.yml†L4-L39】.
- **Extraction options** like mode, record limits and the `raw_data_directory` for CSV output【F:config/sources/acumatica.yml†L88-L101】.
- **Warehouse selection** and dbt model sequence used during transformation【F:config/sources/acumatica.yml†L131-L181】.

Environment variables must supply API credentials and warehouse information. The DAG explicitly checks for the following variables:

```python
BASE_URL = os.getenv('ACUMATICA_BASE_URL')
USERNAME = os.getenv('ACUMATICA_USERNAME')
PASSWORD = os.getenv('ACUMATICA_PASSWORD')
```
【F:airflow/dags/acumatica_dag.py†L196-L209】

During loading the DAG reads `ACTIVE_WAREHOUSE` to decide which warehouse integration to run【F:airflow/dags/acumatica_dag.py†L388-L402】. Additional variables such as `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_DATABASE`, `CLICKHOUSE_USER` and `CLICKHOUSE_PASSWORD` are required when using ClickHouse【F:airflow/dags/acumatica_dag.py†L610-L625】.

## Running the DAG with Incremental Loading

1. Edit `config/sources/acumatica.yml` with your desired schedule and enabled endpoints.
2. Export all required environment variables (Acumatica credentials, warehouse details and Airflow settings).
3. Create a persistent state file (e.g. `data/state/acumatica_state.json`) containing an empty JSON object `{}` on first run.
4. Generate profiles and Docker Compose files:
   ```bash
   python scripts/generate_dbt_profiles.py
   python scripts/generate_docker_compose.py
   ```
5. Start the services with Docker Compose:
   ```bash
   docker-compose up -d
   ```
6. In the Airflow UI trigger the `acumatica_extract_transform` DAG. Each run will update the state file and only process new records.

### dbt Models

The models listed under `model_sequence` in the configuration will run after loading completes to build the bronze and silver layers. Ensure the generated `dbt/profiles.yml` points to the same warehouse.

## Prerequisites

- Python packages required by the pipeline are listed in `requirements.txt`.
- A persistent location for the state file so incremental progress can be tracked.
- Access credentials for Acumatica and your target warehouse.

