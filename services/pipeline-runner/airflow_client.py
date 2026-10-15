"""
SupplySense AI - Apache Airflow REST API Client

Provides an HTTP client for dispatching and tracking DAG runs via the Airflow REST API:
  - Deterministic DAG Run IDs based on execution_id and retry_count.
  - Idempotent trigger handling (handles HTTP 409 conflict by verifying existing run).
  - Safe timeout and network retry resolution with verification lookup.
  - Basic Authentication without exposing credentials in logs.
"""

import logging
from typing import Dict, Any, Optional
import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)


class AirflowClientError(Exception):
    """Base exception for Airflow API client errors."""
    pass


class AirflowAuthenticationError(AirflowClientError):
    """Raised when Airflow REST API returns 401 Unauthorized or 403 Forbidden."""
    pass


class AirflowConnectionError(AirflowClientError):
    """Raised when unable to establish connection to Airflow REST API."""
    pass


class AirflowTimeoutError(AirflowClientError):
    """Raised when an Airflow API request times out."""
    pass


class AirflowExecutionMismatchError(AirflowClientError):
    """Raised when an existing DAG run has conflicting execution parameters."""
    pass


def build_dag_run_id(execution_id: int, retry_count: int = 0) -> str:
    """
    Generates a deterministic DAG run ID based on execution_id and retry attempt number.
    Ensures identical requests within the same attempt reuse the exact same run ID.

    Format: supplysense_exec_{execution_id}_attempt_{retry_count}
    """
    return f"supplysense_exec_{execution_id}_attempt_{retry_count}"


class AirflowClient:
    """
    Lightweight, testable HTTP client for Apache Airflow REST API (v1).
    Supports dependency injection of requests.Session for unit testing without live servers.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        username: str = "admin",
        password: str = "admin",
        timeout: int = 10,
        session: Optional[requests.Session] = None
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._auth = HTTPBasicAuth(username, password)
        self.session = session or requests.Session()

    def get_dag_run(self, dag_id: str, dag_run_id: str) -> Optional[Dict[str, Any]]:
        """
        Looks up a specific DAG run by its deterministic run ID.
        Returns the DAG run metadata dict, or None if HTTP 404.
        """
        url = f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}"
        try:
            resp = self.session.get(url, auth=self._auth, timeout=self.timeout)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 404:
                return None
            if resp.status_code in (401, 403):
                raise AirflowAuthenticationError(
                    f"Authentication failed querying DAG run '{dag_run_id}' on DAG '{dag_id}' (HTTP {resp.status_code})"
                )
            raise AirflowClientError(
                f"Airflow API error querying DAG run '{dag_run_id}' (HTTP {resp.status_code}): {resp.text}"
            )
        except requests.Timeout as e:
            raise AirflowTimeoutError(f"Timeout querying DAG run '{dag_run_id}': {e}") from e
        except requests.ConnectionError as e:
            raise AirflowConnectionError(f"Connection error querying DAG run '{dag_run_id}': {e}") from e
        except requests.RequestException as e:
            raise AirflowClientError(f"Request exception querying DAG run '{dag_run_id}': {e}") from e

    def trigger_dag_run(
        self,
        dag_id: str,
        execution_id: int,
        pipeline_name: str,
        retry_count: int = 0
    ) -> Dict[str, Any]:
        """
        Triggers a new DAG run with a deterministic dag_run_id and passes execution context.
        Handles idempotent recovery on timeouts and HTTP 409 conflicts.

        Returns:
            Dict containing:
                - dag_id (str)
                - dag_run_id (str)
                - state (str)
                - execution_date (str)
                - is_reused (bool)
        """
        dag_run_id = build_dag_run_id(execution_id, retry_count)
        url = f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns"
        payload = {
            "dag_run_id": dag_run_id,
            "conf": {
                "execution_id": execution_id,
                "pipeline_name": pipeline_name
            }
        }

        try:
            resp = self.session.post(url, json=payload, auth=self._auth, timeout=self.timeout)
        except requests.Timeout as timeout_exc:
            logger.warning(
                f"Timeout triggering DAG '{dag_id}' run '{dag_run_id}'. "
                "Performing idempotent lookup to verify whether request was received..."
            )
            # Verify if Airflow received and created the run before the socket timed out
            try:
                existing = self.get_dag_run(dag_id, dag_run_id)
            except Exception as lookup_exc:
                logger.error(f"Idempotent lookup failed after timeout: {lookup_exc}")
                raise AirflowTimeoutError(
                    f"Trigger timed out and subsequent verification failed for DAG '{dag_id}' run '{dag_run_id}'"
                ) from timeout_exc

            if existing:
                self._validate_existing_conf(existing, execution_id, pipeline_name, dag_run_id)
                logger.info(f"Verified existing DAG run '{dag_run_id}' after timeout. State: {existing.get('state')}.")
                return {
                    "dag_id": dag_id,
                    "dag_run_id": existing.get("dag_run_id", dag_run_id),
                    "state": existing.get("state", "queued"),
                    "execution_date": existing.get("execution_date"),
                    "is_reused": True
                }
            raise AirflowTimeoutError(
                f"Trigger request to Airflow for DAG '{dag_id}' run '{dag_run_id}' timed out and run was not created."
            ) from timeout_exc

        except requests.ConnectionError as conn_exc:
            raise AirflowConnectionError(
                f"Could not connect to Airflow REST API at {self.base_url}: {conn_exc}"
            ) from conn_exc

        if resp.status_code == 201:
            try:
                data = resp.json()
            except ValueError as e:
                raise AirflowClientError(f"Malformed JSON response from Airflow REST API: {resp.text}") from e

            returned_run_id = data.get("dag_run_id")
            if not returned_run_id:
                raise AirflowClientError(f"Airflow API 201 response missing 'dag_run_id': {data}")

            logger.info(f"Successfully triggered DAG '{dag_id}' run '{returned_run_id}' for execution #{execution_id}.")
            return {
                "dag_id": dag_id,
                "dag_run_id": returned_run_id,
                "state": data.get("state", "queued"),
                "execution_date": data.get("execution_date"),
                "is_reused": False
            }

        if resp.status_code == 409:
            # Conflict: DAG run with this ID already exists. Validate it.
            logger.info(f"Received HTTP 409 Conflict for run '{dag_run_id}'. Verifying existing DAG run...")
            existing = self.get_dag_run(dag_id, dag_run_id)
            if not existing:
                raise AirflowClientError(
                    f"Received HTTP 409 for '{dag_run_id}' but lookup returned 404."
                )
            self._validate_existing_conf(existing, execution_id, pipeline_name, dag_run_id)
            logger.info(f"Reusing existing DAG run '{dag_run_id}'. Current state: {existing.get('state')}.")
            return {
                "dag_id": dag_id,
                "dag_run_id": existing.get("dag_run_id", dag_run_id),
                "state": existing.get("state", "queued"),
                "execution_date": existing.get("execution_date"),
                "is_reused": True
            }

        if resp.status_code in (401, 403):
            raise AirflowAuthenticationError(
                f"Airflow REST API authentication failed (HTTP {resp.status_code}). Check configured credentials."
            )

        raise AirflowClientError(
            f"Airflow REST API trigger failed (HTTP {resp.status_code}): {resp.text}"
        )

    def _validate_existing_conf(
        self,
        existing: Dict[str, Any],
        expected_execution_id: int,
        expected_pipeline_name: str,
        dag_run_id: str
    ) -> None:
        """Ensures an existing DAG run was triggered with matching execution parameters."""
        conf = existing.get("conf") or {}
        run_exec_id = conf.get("execution_id")
        if run_exec_id is not None and int(run_exec_id) != int(expected_execution_id):
            raise AirflowExecutionMismatchError(
                f"Existing DAG run '{dag_run_id}' has execution_id {run_exec_id}, "
                f"expected {expected_execution_id}."
            )
        run_pipe_name = conf.get("pipeline_name")
        if run_pipe_name is not None and run_pipe_name != expected_pipeline_name:
            raise AirflowExecutionMismatchError(
                f"Existing DAG run '{dag_run_id}' has pipeline_name '{run_pipe_name}', "
                f"expected '{expected_pipeline_name}'."
            )
