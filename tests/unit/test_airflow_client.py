"""
SupplySense AI - Unit Tests for Apache Airflow REST API Client (Phase C)

Verifies:
  1. Successful DAG run trigger.
  2. Deterministic DAG run ID generation.
  3. Duplicate / Conflict response (HTTP 409) with verification lookup.
  4. Request timeout followed by successful idempotent lookup.
  5. Request timeout when no DAG run was created.
  6. Authentication failure handling (HTTP 401/403).
  7. Connection failure handling.
  8. Mismatched execution configuration detection.
  9. Malformed API response handling.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock
import requests
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import importlib
af_mod = importlib.import_module('services.pipeline-runner.airflow_client')
AirflowClient = af_mod.AirflowClient
build_dag_run_id = af_mod.build_dag_run_id
AirflowClientError = af_mod.AirflowClientError
AirflowAuthenticationError = af_mod.AirflowAuthenticationError
AirflowConnectionError = af_mod.AirflowConnectionError
AirflowTimeoutError = af_mod.AirflowTimeoutError
AirflowExecutionMismatchError = af_mod.AirflowExecutionMismatchError


class TestAirflowClient(unittest.TestCase):
    """Test suite for AirflowClient with mocked requests session."""

    def setUp(self):
        self.mock_session = MagicMock(spec=requests.Session)
        self.client = AirflowClient(
            base_url="http://localhost:8080",
            username="admin",
            password="admin",
            timeout=5,
            session=self.mock_session
        )

    def test_deterministic_dag_run_id(self):
        """Verify deterministic run ID format based on execution_id and retry_count."""
        run_id_0 = build_dag_run_id(10461, retry_count=0)
        run_id_1 = build_dag_run_id(10461, retry_count=1)
        self.assertEqual(run_id_0, "supplysense_exec_10461_attempt_0")
        self.assertEqual(run_id_1, "supplysense_exec_10461_attempt_1")

    def test_successful_dag_trigger(self):
        """Verify successful DAG run trigger returns structured metadata."""
        mock_resp = MagicMock()
        mock_resp.status_code = 201
        mock_resp.json.return_value = {
            "dag_id": "supplysense_demand_forecast",
            "dag_run_id": "supplysense_exec_10461_attempt_0",
            "state": "queued",
            "execution_date": "2026-10-15T09:00:00Z"
        }
        self.mock_session.post.return_value = mock_resp

        result = self.client.trigger_dag_run(
            dag_id="supplysense_demand_forecast",
            execution_id=10461,
            pipeline_name="Demand Forecast Pipeline",
            retry_count=0
        )

        self.assertEqual(result["dag_id"], "supplysense_demand_forecast")
        self.assertEqual(result["dag_run_id"], "supplysense_exec_10461_attempt_0")
        self.assertEqual(result["state"], "queued")
        self.assertFalse(result["is_reused"])

    def test_duplicate_conflict_response_with_matching_conf(self):
        """Verify HTTP 409 Conflict triggers lookup and reuses matching DAG run."""
        post_resp = MagicMock()
        post_resp.status_code = 409

        get_resp = MagicMock()
        get_resp.status_code = 200
        get_resp.json.return_value = {
            "dag_id": "supplysense_demand_forecast",
            "dag_run_id": "supplysense_exec_10461_attempt_0",
            "state": "running",
            "execution_date": "2026-10-15T09:00:00Z",
            "conf": {
                "execution_id": 10461,
                "pipeline_name": "Demand Forecast Pipeline"
            }
        }

        self.mock_session.post.return_value = post_resp
        self.mock_session.get.return_value = get_resp

        result = self.client.trigger_dag_run(
            dag_id="supplysense_demand_forecast",
            execution_id=10461,
            pipeline_name="Demand Forecast Pipeline",
            retry_count=0
        )

        self.assertEqual(result["dag_run_id"], "supplysense_exec_10461_attempt_0")
        self.assertTrue(result["is_reused"])
        self.assertEqual(result["state"], "running")

    def test_conflict_response_with_mismatched_conf_raises_error(self):
        """Verify HTTP 409 Conflict with mismatched conf raises AirflowExecutionMismatchError."""
        post_resp = MagicMock()
        post_resp.status_code = 409

        get_resp = MagicMock()
        get_resp.status_code = 200
        get_resp.json.return_value = {
            "dag_id": "supplysense_demand_forecast",
            "dag_run_id": "supplysense_exec_10461_attempt_0",
            "conf": {
                "execution_id": 99999,  # Mismatched execution_id
                "pipeline_name": "Demand Forecast Pipeline"
            }
        }

        self.mock_session.post.return_value = post_resp
        self.mock_session.get.return_value = get_resp

        with self.assertRaises(AirflowExecutionMismatchError):
            self.client.trigger_dag_run(
                dag_id="supplysense_demand_forecast",
                execution_id=10461,
                pipeline_name="Demand Forecast Pipeline",
                retry_count=0
            )

    def test_timeout_followed_by_successful_lookup(self):
        """Verify request timeout where run was created succeeds via idempotent lookup."""
        self.mock_session.post.side_effect = requests.Timeout("Socket timed out")

        get_resp = MagicMock()
        get_resp.status_code = 200
        get_resp.json.return_value = {
            "dag_id": "supplysense_demand_forecast",
            "dag_run_id": "supplysense_exec_10461_attempt_0",
            "state": "running",
            "conf": {
                "execution_id": 10461,
                "pipeline_name": "Demand Forecast Pipeline"
            }
        }
        self.mock_session.get.return_value = get_resp

        result = self.client.trigger_dag_run(
            dag_id="supplysense_demand_forecast",
            execution_id=10461,
            pipeline_name="Demand Forecast Pipeline",
            retry_count=0
        )

        self.assertEqual(result["dag_run_id"], "supplysense_exec_10461_attempt_0")
        self.assertTrue(result["is_reused"])

    def test_timeout_when_no_dag_run_created_raises_timeout_error(self):
        """Verify request timeout where run was NOT created raises AirflowTimeoutError."""
        self.mock_session.post.side_effect = requests.Timeout("Socket timed out")

        get_resp = MagicMock()
        get_resp.status_code = 404  # Run was not created
        self.mock_session.get.return_value = get_resp

        with self.assertRaises(AirflowTimeoutError):
            self.client.trigger_dag_run(
                dag_id="supplysense_demand_forecast",
                execution_id=10461,
                pipeline_name="Demand Forecast Pipeline",
                retry_count=0
            )

    def test_authentication_failure_raises_error(self):
        """Verify HTTP 401/403 raises AirflowAuthenticationError."""
        mock_resp = MagicMock()
        mock_resp.status_code = 401
        self.mock_session.post.return_value = mock_resp

        with self.assertRaises(AirflowAuthenticationError):
            self.client.trigger_dag_run(
                dag_id="supplysense_demand_forecast",
                execution_id=10461,
                pipeline_name="Demand Forecast Pipeline"
            )

    def test_connection_error_raises_error(self):
        """Verify connection failure raises AirflowConnectionError."""
        self.mock_session.post.side_effect = requests.ConnectionError("Connection refused")

        with self.assertRaises(AirflowConnectionError):
            self.client.trigger_dag_run(
                dag_id="supplysense_demand_forecast",
                execution_id=10461,
                pipeline_name="Demand Forecast Pipeline"
            )

    def test_malformed_json_response_raises_error(self):
        """Verify non-JSON response on 201 raises AirflowClientError."""
        mock_resp = MagicMock()
        mock_resp.status_code = 201
        mock_resp.json.side_effect = ValueError("Invalid JSON")
        mock_resp.text = "Internal HTML Error"
        self.mock_session.post.return_value = mock_resp

        with self.assertRaises(AirflowClientError):
            self.client.trigger_dag_run(
                dag_id="supplysense_demand_forecast",
                execution_id=10461,
                pipeline_name="Demand Forecast Pipeline"
            )


if __name__ == "__main__":
    unittest.main()
