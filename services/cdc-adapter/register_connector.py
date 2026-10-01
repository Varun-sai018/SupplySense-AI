"""
Debezium Connector Registration Utility for SupplySense AI.

Registers, updates, inspects, and deletes the MySQL Debezium CDC Connector
via the Debezium Connect REST API.
"""

import os
import sys
import json
import logging
import time
import requests

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from config import settings

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), 'connector_config.json')


def get_connector_payload() -> dict:
    """Loads and populates the connector configuration with active settings."""
    with open(CONFIG_FILE_PATH, 'r') as f:
        data = json.load(f)

    # Populate credentials from settings
    data['config']['database.user'] = settings.DB_USER
    data['config']['database.password'] = settings.DB_PASSWORD
    return data


def register_or_update_connector(connect_url: str = None) -> dict:
    """
    Registers or updates the Debezium MySQL connector.
    
    Args:
        connect_url: Base URL of Debezium Connect (default from settings).
        
    Returns:
        dict: Connector registration response or status.
    """
    base_url = (connect_url or getattr(settings, 'DEBEZIUM_CONNECT_URL', 'http://localhost:8083')).rstrip('/')
    payload = get_connector_payload()
    connector_name = payload['name']
    config = payload['config']

    endpoint = f"{base_url}/connectors/{connector_name}/config"
    logger.info(f"Registering/Updating Debezium connector '{connector_name}' at {endpoint}...")

    try:
        response = requests.put(
            endpoint,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            json=config,
            timeout=10
        )
        if response.status_code in (200, 201):
            logger.info(f"Connector '{connector_name}' successfully configured (HTTP {response.status_code}).")
            return response.json()
        else:
            error_msg = f"Failed to register connector: HTTP {response.status_code} - {response.text}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
    except requests.exceptions.RequestException as e:
        logger.error(f"Error communicating with Debezium Connect: {e}")
        raise


def get_connector_status(connect_url: str = None, connector_name: str = "supplysense-mysql-cdc") -> dict:
    """Fetches the runtime status of the Debezium connector."""
    base_url = (connect_url or getattr(settings, 'DEBEZIUM_CONNECT_URL', 'http://localhost:8083')).rstrip('/')
    endpoint = f"{base_url}/connectors/{connector_name}/status"

    try:
        response = requests.get(endpoint, timeout=10)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            return {"name": connector_name, "connector": {"state": "NOT_FOUND"}, "tasks": []}
        else:
            response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error checking connector status: {e}")
        raise


def wait_for_connector_running(connect_url: str = None, connector_name: str = "supplysense-mysql-cdc", timeout: int = 30) -> bool:
    """Waits until the connector and all its tasks are in RUNNING state."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            status = get_connector_status(connect_url, connector_name)
            conn_state = status.get('connector', {}).get('state')
            tasks = status.get('tasks', [])
            task_states = [t.get('state') for t in tasks]

            if conn_state == 'RUNNING' and tasks and all(s == 'RUNNING' for s in task_states):
                logger.info(f"Connector '{connector_name}' is RUNNING with {len(tasks)} task(s).")
                return True
            logger.info(f"Connector state: {conn_state}, task states: {task_states}. Waiting...")
        except Exception as e:
            logger.debug(f"Waiting for connector status: {e}")
        time.sleep(2)
    return False


def delete_connector(connect_url: str = None, connector_name: str = "supplysense-mysql-cdc") -> bool:
    """Deletes the connector if it exists."""
    base_url = (connect_url or getattr(settings, 'DEBEZIUM_CONNECT_URL', 'http://localhost:8083')).rstrip('/')
    endpoint = f"{base_url}/connectors/{connector_name}"

    try:
        response = requests.delete(endpoint, timeout=10)
        if response.status_code in (204, 404):
            logger.info(f"Connector '{connector_name}' deleted or not found.")
            return True
        else:
            logger.warning(f"Delete returned HTTP {response.status_code}: {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Error deleting connector: {e}")
        return False


if __name__ == '__main__':
    try:
        register_or_update_connector()
        is_running = wait_for_connector_running(timeout=15)
        print("Connector Running:", is_running)
        status = get_connector_status()
        print(json.dumps(status, indent=2))
    except Exception as exc:
        print(f"Registration failed: {exc}")
        sys.exit(1)
