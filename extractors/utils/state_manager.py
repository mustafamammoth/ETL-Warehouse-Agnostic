import os
import json
from datetime import datetime
from typing import Optional, Dict


STATE_FILE_PATH = os.environ.get("ACUMATICA_STATE_FILE", "/opt/airflow/state/acumatica_state.json")


def _load_state(path: str = STATE_FILE_PATH) -> Dict[str, str]:
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _save_state(state: Dict[str, str], path: str = STATE_FILE_PATH) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(state, f, indent=2)


def get_last_timestamp(endpoint: str, path: str = STATE_FILE_PATH) -> Optional[str]:
    """Return the last successful extraction timestamp for the endpoint."""
    state = _load_state(path)
    return state.get(endpoint)


def update_last_timestamp(endpoint: str, timestamp: str | datetime, path: str = STATE_FILE_PATH) -> None:
    """Update the last successful extraction timestamp for the endpoint."""
    if isinstance(timestamp, datetime):
        timestamp = timestamp.isoformat()

    state = _load_state(path)
    state[endpoint] = timestamp
    _save_state(state, path)
