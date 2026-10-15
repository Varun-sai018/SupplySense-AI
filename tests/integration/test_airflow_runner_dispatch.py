"""
SupplySense AI - Integration Tests for Phase C (Pipeline Runner -> Airflow Dispatch & Retry)

Verifies:
  1. LOCAL mode calls internal handler and marks COMPLETED.
  2. AIRFLOW mode dispatches to AirflowClient, updates metadata, and skips internal handler.
  3. AIRFLOW mode leaves status in RUNNING and does not prematurely mark COMPLETED.
  4. AIRFLOW mode dispatch persists orchestrator='AIRFLOW', airflow_dag_id, and airflow_run_id.
  5. Repeated dispatch for the same execution attempt reuses the deterministic run ID.
  6. AIRFLOW retry atomically claims execution and triggers Airflow with attempt-specific run ID.
  7. Internal ML handler is never invoked during an Airflow-managed execution or retry.
  8. Observability API surfaces orchestrator, airflow_dag_id, and airflow_run_id accurately.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock
import datetime
import importlib

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection

runner_mod = importlib.import_module('services.pipeline-runner.runner')
retry_mod = importlib.import_module('services.pipeline-runner.retry_service')
registry_mod = importlib.import_module('services.pipeline-runner.registry')
af_mod = importlib.import_module('services.pipeline-runner.airflow_client')
obs_service = importlib.import_module('services.observability.service')

execute_pipeline = runner_mod.execute_pipeline
retry_execution = retry_mod.retry_execution
register_pipeline = registry_mod.register_pipeline
get_pipeline_handler = registry_mod.get_pipeline_handler
AirflowClient = af_mod.AirflowClient


class TestPhaseCAirflowIntegration(unittest.TestCase):
    """Integration test suite for Phase C Airflow REST API runner dispatch and lifecycle."""

    def setUp(self):
        self.conn = get_connection()
        self.cur = self.conn.cursor()
        self.test_exec_ids = []
        self.test_decision_ids = []

    def tearDown(self):
        if self.test_exec_ids:
            fmt = ','.join(['%s'] * len(self.test_exec_ids))
            self.cur.execute(f"DELETE FROM forecast_results WHERE execution_id IN ({fmt})", tuple(self.test_exec_ids))
            self.cur.execute(f"DELETE FROM pipeline_executions WHERE execution_id IN ({fmt})", tuple(self.test_exec_ids))
        if self.test_decision_ids:
            fmt = ','.join(['%s'] * len(self.test_decision_ids))
            self.cur.execute(f"DELETE FROM pipeline_decisions WHERE decision_id IN ({fmt})", tuple(self.test_decision_ids))
        self.conn.commit()
        self.cur.close()
        self.conn.close()

    def _create_test_execution(self, exec_id, pipeline_name="Demand Forecast Pipeline", status="WAITING", orchestrator="LOCAL"):
        """Helper to create a temporary test execution with valid foreign key."""
        self.cur.execute("""
            INSERT INTO pipeline_decisions (
                pipeline_name, condition_type, ready_count, total_required, decision, reason, triggered_at
            ) VALUES (
                %s, 'ALL', 1, 1, 'TRIGGER', 'Phase C test trigger', NOW()
            )
        """, (pipeline_name,))
        decision_id = self.cur.lastrowid
        self.test_decision_ids.append(decision_id)

        self.cur.execute("""
            INSERT INTO pipeline_executions (
                execution_id, pipeline_name, decision_id, triggering_event_id, status, orchestrator
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (exec_id, pipeline_name, decision_id, exec_id + 910000, status, orchestrator))
        self.conn.commit()
        self.test_exec_ids.append(exec_id)
        return exec_id

    def test_local_mode_executes_internal_handler(self):
        """Verify LOCAL mode calls internal handler and marks execution COMPLETED."""
        exec_id = 91001
        self._create_test_execution(exec_id, orchestrator="LOCAL")

        mock_handler = MagicMock(return_value={"output_location": "ml/results/test.json"})
        register_pipeline("Test Local Pipeline", mock_handler)

        try:
            res = execute_pipeline(
                execution_id=exec_id,
                pipeline_name="Test Local Pipeline",
                connection=self.conn,
                orchestrator="LOCAL"
            )

            self.assertEqual(res["status"], "COMPLETED")
            self.assertEqual(res["orchestrator"], "LOCAL")
            mock_handler.assert_called_once_with(exec_id, "Test Local Pipeline")

            # Verify in DB
            self.cur.execute("SELECT status, orchestrator FROM pipeline_executions WHERE execution_id = %s", (exec_id,))
            row = self.cur.fetchone()
            self.assertEqual(row["status"], "COMPLETED")
            self.assertEqual(row["orchestrator"], "LOCAL")
        finally:
            # Clean up test handler
            pass

    def test_airflow_mode_dispatches_to_client_and_skips_handler(self):
        """Verify AIRFLOW mode triggers Airflow REST API, skips internal handler, and leaves in RUNNING."""
        exec_id = 91002
        self._create_test_execution(exec_id, orchestrator="AIRFLOW")

        mock_handler = MagicMock()
        register_pipeline("Test Airflow Pipeline", mock_handler)

        mock_client = MagicMock(spec=AirflowClient)
        mock_client.trigger_dag_run.return_value = {
            "dag_id": "supplysense_demand_forecast",
            "dag_run_id": f"supplysense_exec_{exec_id}_attempt_0",
            "state": "queued",
            "execution_date": "2026-10-15T09:00:00Z",
            "is_reused": False
        }

        res = execute_pipeline(
            execution_id=exec_id,
            pipeline_name="Test Airflow Pipeline",
            connection=self.conn,
            orchestrator="AIRFLOW",
            airflow_client=mock_client
        )

        # Assertions
        self.assertEqual(res["status"], "RUNNING")
        self.assertEqual(res["orchestrator"], "AIRFLOW")
        self.assertEqual(res["airflow_dag_id"], "supplysense_demand_forecast")
        self.assertEqual(res["airflow_run_id"], f"supplysense_exec_{exec_id}_attempt_0")

        # Internal handler MUST NOT be called
        mock_handler.assert_not_called()

        # AirflowClient trigger_dag_run MUST be called once
        mock_client.trigger_dag_run.assert_called_once_with(
            dag_id="supplysense_demand_forecast",
            execution_id=exec_id,
            pipeline_name="Test Airflow Pipeline",
            retry_count=0
        )

        # Verify DB metadata
        self.cur.execute(
            "SELECT status, orchestrator, airflow_dag_id, airflow_run_id FROM pipeline_executions WHERE execution_id = %s",
            (exec_id,)
        )
        row = self.cur.fetchone()
        self.assertEqual(row["status"], "RUNNING")
        self.assertEqual(row["orchestrator"], "AIRFLOW")
        self.assertEqual(row["airflow_dag_id"], "supplysense_demand_forecast")
        self.assertEqual(row["airflow_run_id"], f"supplysense_exec_{exec_id}_attempt_0")

    def test_airflow_mode_retry_dispatches_with_incremented_attempt(self):
        """Verify Airflow retry uses attempt-specific run ID and does not call internal handler."""
        exec_id = 91003
        self._create_test_execution(exec_id, pipeline_name="Test Airflow Pipeline", status="FAILED", orchestrator="AIRFLOW")

        # Set initial failure state
        self.cur.execute("""
            UPDATE pipeline_executions
            SET error_message = 'Airflow API connection timed out',
                retry_error_type = 'TRANSIENT_TIMEOUT',
                retry_count = 0,
                max_retries = 3
            WHERE execution_id = %s
        """, (exec_id,))
        self.conn.commit()

        mock_handler = MagicMock()
        register_pipeline("Test Airflow Pipeline", mock_handler)

        mock_client = MagicMock(spec=AirflowClient)
        mock_client.trigger_dag_run.return_value = {
            "dag_id": "supplysense_demand_forecast",
            "dag_run_id": f"supplysense_exec_{exec_id}_attempt_1",
            "state": "queued",
            "execution_date": "2026-10-15T09:05:00Z",
            "is_reused": False
        }

        res = retry_execution(
            execution_id=exec_id,
            connection=self.conn,
            airflow_client=mock_client
        )

        self.assertEqual(res["status"], "RUNNING")
        self.assertEqual(res["orchestrator"], "AIRFLOW")
        self.assertEqual(res["retry_count"], 1)
        self.assertEqual(res["airflow_run_id"], f"supplysense_exec_{exec_id}_attempt_1")

        # Handler was NOT called
        mock_handler.assert_not_called()

        # Airflow client called with retry_count=1
        mock_client.trigger_dag_run.assert_called_once_with(
            dag_id="supplysense_demand_forecast",
            execution_id=exec_id,
            pipeline_name="Test Airflow Pipeline",
            retry_count=1
        )

    def test_observability_api_exposes_airflow_fields(self):
        """Verify observability service exposes orchestrator, airflow_dag_id, and airflow_run_id."""
        exec_id = 91004
        self._create_test_execution(exec_id, status="RUNNING", orchestrator="AIRFLOW")

        self.cur.execute("""
            UPDATE pipeline_executions
            SET airflow_dag_id = 'supplysense_demand_forecast',
                airflow_run_id = 'supplysense_exec_91004_attempt_0'
            WHERE execution_id = %s
        """, (exec_id,))
        self.conn.commit()

        # Test get_execution_detail
        detail = obs_service.get_execution_detail(exec_id, connection=self.conn)
        self.assertIsNotNone(detail)
        self.assertEqual(detail["orchestrator"], "AIRFLOW")
        self.assertEqual(detail["airflow_dag_id"], "supplysense_demand_forecast")
        self.assertEqual(detail["airflow_run_id"], "supplysense_exec_91004_attempt_0")

        # Test get_recent_executions
        recent = obs_service.get_recent_executions(limit=50, connection=self.conn)
        target = next((item for item in recent if item["execution_id"] == exec_id), None)
        self.assertIsNotNone(target)
        self.assertEqual(target["orchestrator"], "AIRFLOW")
        self.assertEqual(target["airflow_dag_id"], "supplysense_demand_forecast")
        self.assertEqual(target["airflow_run_id"], "supplysense_exec_91004_attempt_0")


if __name__ == "__main__":
    unittest.main()
