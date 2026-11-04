"""
SupplySense AI - Unit Tests for Safe Live Capstone Demo Runner (Phase F2)

Verifies:
  1. Preflight failure prevents mutations and exits safely.
  2. Execution #36 safety audit validates COMPLETED status, 770 rows, and 9 artifacts.
  3. Execution #36 safety audit detects tampered state and aborts.
  4. BLOCK scenario correctly validates incomplete dependencies.
  5. TRIGGER scenario creates a new execution (never #36) and dispatches via Airflow.
  6. Airflow timeout is handled cleanly and raises TimeoutError.
  7. Post-run verification is strictly scoped to the newly generated execution ID.
  8. Secrets and credentials are not printed in preflight or logs.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import io
from scripts.demo_capstone_live import (
    run_preflight,
    audit_benchmark_execution_36,
    verify_starting_dataset_states,
    demonstrate_block_scenario,
    demonstrate_trigger_scenario,
    monitor_airflow_dag_run,
    verify_post_execution,
    main,
)


class TestDemoCapstoneLive(unittest.TestCase):
    """Test suite for Safe Live Capstone Demonstration Runner."""

    @patch("scripts.demo_capstone_live.requests.get")
    @patch("scripts.demo_capstone_live.get_connection")
    def test_preflight_success(self, mock_get_conn, mock_requests_get):
        """Verify preflight returns True when all services are healthy."""
        # Mock MySQL
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = {"cnt": 50}
        mock_conn.cursor.return_value = mock_cur
        mock_get_conn.return_value = mock_conn

        # Mock HTTP responses for Observability, Airflow Health, and Airflow DAG
        resp_obs = MagicMock()
        resp_obs.status_code = 200
        resp_obs.json.return_value = {"status": "healthy", "database": "connected"}

        resp_af_health = MagicMock()
        resp_af_health.status_code = 200
        resp_af_health.json.return_value = {"metadatabase": {"status": "healthy"}}

        resp_af_dag = MagicMock()
        resp_af_dag.status_code = 200
        resp_af_dag.json.return_value = {"is_paused": False}

        mock_requests_get.side_effect = [resp_obs, resp_af_health, resp_af_dag]

        result = run_preflight(
            airflow_base_url="http://mock-airflow:8080",
            airflow_user="admin",
            airflow_password="mock_password",
            dag_id="supplysense_demand_forecast"
        )
        self.assertTrue(result)

    @patch("scripts.demo_capstone_live.requests.get")
    @patch("scripts.demo_capstone_live.get_connection")
    def test_preflight_failure_prevents_mutations(self, mock_get_conn, mock_requests_get):
        """Verify preflight returns False when a service is unreachable."""
        # Mock MySQL success
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = {"cnt": 50}
        mock_conn.cursor.return_value = mock_cur
        mock_get_conn.return_value = mock_conn

        # Mock Observability API failure
        resp_obs = MagicMock()
        resp_obs.status_code = 503
        mock_requests_get.return_value = resp_obs

        result = run_preflight()
        self.assertFalse(result)

    @patch("scripts.demo_capstone_live.os.listdir")
    @patch("scripts.demo_capstone_live.os.path.isdir")
    @patch("scripts.demo_capstone_live.get_connection")
    def test_audit_execution_36_success(self, mock_get_conn, mock_isdir, mock_listdir):
        """Verify safety audit passes when execution #36 has 770 rows and 9 artifacts."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        # Row for execution #36
        mock_cur.fetchone.side_effect = [
            {"execution_id": 36, "status": "COMPLETED", "airflow_run_id": "supplysense_exec_36_attempt_0"},
            {"cnt": 770}
        ]
        mock_conn.cursor.return_value = mock_cur
        mock_get_conn.return_value = mock_conn

        mock_isdir.return_value = True
        mock_listdir.return_value = [f"file_{i}.json" for i in range(9)]

        self.assertTrue(audit_benchmark_execution_36())

    @patch("scripts.demo_capstone_live.get_connection")
    def test_audit_execution_36_fails_if_tampered(self, mock_get_conn):
        """Verify safety audit fails if execution #36 is not COMPLETED or missing rows."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchone.side_effect = [
            {"execution_id": 36, "status": "FAILED", "airflow_run_id": "supplysense_exec_36_attempt_0"},
        ]
        mock_conn.cursor.return_value = mock_cur
        mock_get_conn.return_value = mock_conn

        self.assertFalse(audit_benchmark_execution_36())

    @patch("scripts.demo_capstone_live.process_event")
    @patch("scripts.demo_capstone_live.process_cdc_event")
    def test_block_scenario_verification(self, mock_cdc, mock_engine):
        """Verify BLOCK scenario processes event and asserts BLOCK decision."""
        mock_cdc.return_value = {
            "event_id": 901,
            "dataset_name": "Orders",
            "dataset_version": 205
        }
        mock_engine.return_value = ("BLOCK", "1 of 3 required datasets are READY.")

        mock_conn = MagicMock()
        decision, reason, event_id = demonstrate_block_scenario(mock_conn)

        self.assertEqual(decision, "BLOCK")
        self.assertEqual(event_id, 901)
        mock_engine.assert_called_once()

    @patch("scripts.demo_capstone_live.process_event")
    @patch("scripts.demo_capstone_live.process_cdc_event")
    def test_trigger_scenario_creates_new_execution_not_36(self, mock_cdc, mock_engine):
        """Verify TRIGGER scenario produces a new execution ID strictly greater than 36."""
        mock_cdc.side_effect = [
            {"event_id": 902, "dataset_name": "Products", "dataset_version": 114},
            {"event_id": 903, "dataset_name": "Sellers", "dataset_version": 70}
        ]
        mock_engine.return_value = ("TRIGGER", "All required datasets are READY.")

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        # Mock created execution row
        mock_cur.fetchone.return_value = {
            "execution_id": 37,
            "pipeline_name": "Demand Forecast Pipeline",
            "triggering_event_id": 903,
            "status": "RUNNING",
            "orchestrator": "AIRFLOW",
            "airflow_dag_id": "supplysense_demand_forecast",
            "airflow_run_id": "supplysense_exec_37_attempt_0"
        }
        mock_conn.cursor.return_value = mock_cur

        new_exec_id, dag_run_id = demonstrate_trigger_scenario(mock_conn)

        self.assertEqual(new_exec_id, 37)
        self.assertNotEqual(new_exec_id, 36)
        self.assertEqual(dag_run_id, "supplysense_exec_37_attempt_0")

    def test_monitor_airflow_timeout_handling(self):
        """Verify Airflow monitoring raises TimeoutError on elapsed timeout."""
        mock_client = MagicMock()
        mock_client.get_dag_run.return_value = {"state": "running"}
        mock_client.session.get.return_value.status_code = 200
        mock_client.session.get.return_value.json.return_value = {"task_instances": []}

        with self.assertRaises(TimeoutError):
            monitor_airflow_dag_run(
                client=mock_client,
                dag_id="supplysense_demand_forecast",
                dag_run_id="supplysense_exec_37_attempt_0",
                timeout_seconds=1
            )

    @patch("scripts.demo_capstone_live.requests.get")
    @patch("scripts.demo_capstone_live.os.listdir")
    @patch("scripts.demo_capstone_live.os.path.isdir")
    @patch("scripts.demo_capstone_live.get_connection")
    def test_post_execution_verification_scoped_to_new_id(self, mock_get_conn, mock_isdir, mock_listdir, mock_http):
        """Verify post-execution verification checks the target execution ID and checks #36."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchone.side_effect = [
            # Target execution row
            {"execution_id": 37, "status": "COMPLETED", "orchestrator": "AIRFLOW", "output_location": "loc"},
            # Target forecast rows
            {"row_count": 770, "cat_count": 70, "avg_demand": 27.5, "model_name": "XGBoost", "model_version": "v1"},
            # Execution 36 integrity checks
            {"status": "COMPLETED"},
            {"cnt": 770}
        ]
        mock_conn.cursor.return_value = mock_cur
        mock_get_conn.return_value = mock_conn

        mock_isdir.return_value = True
        mock_listdir.return_value = [f"f_{i}" for i in range(9)]
        mock_http.return_value.status_code = 200

        result = verify_post_execution(37)
        self.assertTrue(result)

    @patch("scripts.demo_capstone_live.requests.get")
    @patch("scripts.demo_capstone_live.get_connection")
    def test_secret_values_never_printed(self, mock_get_conn, mock_requests_get):
        """Verify secrets and passwords are never printed to stdout in preflight."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = {"cnt": 50}
        mock_conn.cursor.return_value = mock_cur
        mock_get_conn.return_value = mock_conn

        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"status": "healthy", "metadatabase": {"status": "healthy"}, "is_paused": False}
        mock_requests_get.return_value = resp

        captured_output = io.StringIO()
        sys_stdout = sys.stdout
        try:
            sys.stdout = captured_output
            run_preflight(
                airflow_base_url="http://localhost:18080",
                airflow_user="admin",
                airflow_password="SUPER_SECRET_PASSWORD_12345",
                dag_id="supplysense_demand_forecast"
            )
        finally:
            sys.stdout = sys_stdout

        output_str = captured_output.getvalue()
        self.assertNotIn("SUPER_SECRET_PASSWORD_12345", output_str)
        self.assertNotIn("mock_password", output_str)

    def test_starting_state_guard_passes_when_all_waiting(self):
        """Verify starting-state guard passes when Orders, Products, and Sellers are all WAITING."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [
            {"dataset_name": "Orders", "status": "WAITING", "current_version": 204},
            {"dataset_name": "Products", "status": "WAITING", "current_version": 113},
            {"dataset_name": "Sellers", "status": "WAITING", "current_version": 69},
        ]
        mock_conn.cursor.return_value = mock_cur

        result = verify_starting_dataset_states(mock_conn)
        self.assertTrue(result)
        mock_cur.close.assert_called_once()

    def test_starting_state_guard_aborts_when_orders_ready(self):
        """Verify starting-state guard aborts when Orders is READY."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [
            {"dataset_name": "Orders", "status": "READY", "current_version": 204},
            {"dataset_name": "Products", "status": "WAITING", "current_version": 113},
            {"dataset_name": "Sellers", "status": "WAITING", "current_version": 69},
        ]
        mock_conn.cursor.return_value = mock_cur

        result = verify_starting_dataset_states(mock_conn)
        self.assertFalse(result)

    def test_starting_state_guard_aborts_when_products_ready(self):
        """Verify starting-state guard aborts when Products is READY."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [
            {"dataset_name": "Orders", "status": "WAITING", "current_version": 204},
            {"dataset_name": "Products", "status": "READY", "current_version": 113},
            {"dataset_name": "Sellers", "status": "WAITING", "current_version": 69},
        ]
        mock_conn.cursor.return_value = mock_cur

        result = verify_starting_dataset_states(mock_conn)
        self.assertFalse(result)

    def test_starting_state_guard_aborts_when_sellers_ready(self):
        """Verify starting-state guard aborts when Sellers is READY."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [
            {"dataset_name": "Orders", "status": "WAITING", "current_version": 204},
            {"dataset_name": "Products", "status": "WAITING", "current_version": 113},
            {"dataset_name": "Sellers", "status": "READY", "current_version": 69},
        ]
        mock_conn.cursor.return_value = mock_cur

        result = verify_starting_dataset_states(mock_conn)
        self.assertFalse(result)

    def test_starting_state_guard_aborts_when_dataset_missing(self):
        """Verify starting-state guard aborts when a required dataset record is missing."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [
            {"dataset_name": "Orders", "status": "WAITING", "current_version": 204},
            {"dataset_name": "Products", "status": "WAITING", "current_version": 113},
        ]
        mock_conn.cursor.return_value = mock_cur

        result = verify_starting_dataset_states(mock_conn)
        self.assertFalse(result)

    def test_starting_state_guard_aborts_on_db_query_error(self):
        """Verify starting-state guard aborts when database query raises an exception."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.execute.side_effect = Exception("DB connection timeout")
        mock_conn.cursor.return_value = mock_cur

        result = verify_starting_dataset_states(mock_conn)
        self.assertFalse(result)

    @patch("scripts.demo_capstone_live.demonstrate_block_scenario")
    @patch("scripts.demo_capstone_live.verify_starting_dataset_states")
    @patch("scripts.demo_capstone_live.audit_benchmark_execution_36")
    @patch("scripts.demo_capstone_live.run_preflight")
    def test_main_check_only_stops_without_mutation_when_precondition_checked(
        self, mock_preflight, mock_audit, mock_guard, mock_block
    ):
        """Verify --check-only runs precondition guard and exits 0 without invoking demo scenarios."""
        mock_preflight.return_value = True
        mock_audit.return_value = True
        mock_guard.return_value = True

        with patch("sys.argv", ["demo_capstone_live.py", "--check-only"]):
            with self.assertRaises(SystemExit) as cm:
                main()
            self.assertEqual(cm.exception.code, 0)

        mock_preflight.assert_called_once()
        mock_audit.assert_called_once()
        mock_guard.assert_called_once()
        mock_block.assert_not_called()

    @patch("scripts.demo_capstone_live.demonstrate_block_scenario")
    @patch("scripts.demo_capstone_live.verify_starting_dataset_states")
    @patch("scripts.demo_capstone_live.audit_benchmark_execution_36")
    @patch("scripts.demo_capstone_live.run_preflight")
    def test_main_aborts_when_starting_state_guard_fails(
        self, mock_preflight, mock_audit, mock_guard, mock_block
    ):
        """Verify main aborts with exit code 1 if starting-state guard fails."""
        mock_preflight.return_value = True
        mock_audit.return_value = True
        mock_guard.return_value = False

        with patch("sys.argv", ["demo_capstone_live.py"]):
            with self.assertRaises(SystemExit) as cm:
                main()
            self.assertEqual(cm.exception.code, 1)

        mock_guard.assert_called_once()
        mock_block.assert_not_called()


if __name__ == "__main__":
    unittest.main()
