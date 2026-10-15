"""
SupplySense AI - Airflow DAG Safety and Execution-ID Validation Tests.

Verifies:
  1. DAG requires explicit dag_run.conf.execution_id and raises ValueError if missing.
  2. Non-numeric or non-positive execution_id is rejected.
  3. Non-existent execution_id raises ValueError to prevent attaching to uninitialized executions.
  4. Valid existing execution_id updates pipeline_executions status to RUNNING and succeeds.
  5. Distinct execution IDs receive isolated artifact directories.
  6. Forecast persistence is idempotent on repeated execution.
"""

import os
import sys
import unittest
import shutil
from unittest.mock import MagicMock

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from dags.supplysense_demand_forecast import (
    task_validate_input_datasets,
    task_finalize_execution_telemetry,
    handle_dag_failure
)
import importlib
repo_mod = importlib.import_module('services.pipeline-runner.forecast_repository')
save_forecast_results = repo_mod.save_forecast_results


class MockDagRun:
    def __init__(self, conf=None, run_id=None):
        self.conf = conf or {}
        self.run_id = run_id


class MockTaskInstance:
    def __init__(self):
        self.xcom_data = {}

    def xcom_push(self, key, value):
        self.xcom_data[key] = value

    def xcom_pull(self, task_ids=None, key=None):
        return self.xcom_data.get(key)


class TestAirflowDagSafety(unittest.TestCase):
    """Integration test suite for Airflow DAG execution safety and validation."""

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
            for eid in self.test_exec_ids:
                edir = os.path.join(REPO_ROOT, "ml", "results", "executions", str(eid))
                if os.path.exists(edir):
                    shutil.rmtree(edir, ignore_errors=True)
        if self.test_decision_ids:
            fmt = ','.join(['%s'] * len(self.test_decision_ids))
            self.cur.execute(f"DELETE FROM pipeline_decisions WHERE decision_id IN ({fmt})", tuple(self.test_decision_ids))
        self.conn.commit()
        self.cur.close()
        self.conn.close()

    def _create_test_execution(self, exec_id, pipeline_name="Demand Forecast Pipeline"):
        """Helper to create a temporary test execution with valid foreign key."""
        self.cur.execute("""
            INSERT INTO pipeline_decisions (
                pipeline_name, condition_type, ready_count, total_required, decision, reason, triggered_at
            ) VALUES (
                %s, 'ALL', 1, 1, 'TRIGGER', 'DAG safety test trigger', NOW()
            )
        """, (pipeline_name,))
        decision_id = self.cur.lastrowid
        self.test_decision_ids.append(decision_id)

        self.cur.execute("""
            INSERT INTO pipeline_executions (
                execution_id, pipeline_name, decision_id, triggering_event_id, status
            ) VALUES (%s, %s, %s, %s, 'WAITING')
        """, (exec_id, pipeline_name, decision_id, exec_id + 800000))
        self.conn.commit()
        self.test_exec_ids.append(exec_id)

    def test_missing_execution_id_raises_error(self):
        """Verify that a DAG run without execution_id raises ValueError."""
        ti = MockTaskInstance()
        context = {"dag_run": MockDagRun(conf={}), "ti": ti}

        with self.assertRaises(ValueError) as ctx:
            task_validate_input_datasets(**context)

        self.assertIn("Missing required 'execution_id'", str(ctx.exception))

    def test_invalid_execution_id_raises_error(self):
        """Verify that non-numeric or non-positive execution_id raises ValueError."""
        ti = MockTaskInstance()

        for invalid_id in [-1, 0, "not_an_int"]:
            with self.subTest(invalid_id=invalid_id):
                context = {"dag_run": MockDagRun(conf={"execution_id": invalid_id}), "ti": ti}
                with self.assertRaises(ValueError) as ctx:
                    task_validate_input_datasets(**context)
                self.assertTrue(
                    "positive integer" in str(ctx.exception) or "Invalid execution_id" in str(ctx.exception)
                )

    def test_nonexistent_execution_id_raises_error(self):
        """Verify that an execution_id not present in pipeline_executions is rejected."""
        ti = MockTaskInstance()
        non_existent_id = 99999999
        context = {"dag_run": MockDagRun(conf={"execution_id": non_existent_id}), "ti": ti}

        with self.assertRaises(ValueError) as ctx:
            task_validate_input_datasets(**context)

        self.assertIn("does not exist in pipeline_executions table", str(ctx.exception))

    def test_valid_execution_id_updates_status_and_isolates_dir(self):
        """Verify valid execution_id updates status to RUNNING and isolates artifact directory."""
        test_id = 98881
        self._create_test_execution(test_id)

        ti = MockTaskInstance()
        context = {"dag_run": MockDagRun(conf={"execution_id": test_id}), "ti": ti}

        result = task_validate_input_datasets(**context)

        self.assertEqual(result["status"], "VALIDATED")
        self.assertEqual(result["execution_id"], test_id)
        self.assertTrue(result["exec_dir"].endswith(str(test_id)))

        # Verify XCom pushes
        self.assertEqual(ti.xcom_data.get("execution_id"), test_id)
        self.assertTrue(ti.xcom_data.get("exec_dir").endswith(str(test_id)))

        # Verify DB status updated to RUNNING
        self.cur.execute("SELECT status, started_at FROM pipeline_executions WHERE execution_id = %s", (test_id,))
        row = self.cur.fetchone()
        status = row["status"] if isinstance(row, dict) else row[0]
        self.assertEqual(status, "RUNNING")

    def test_concurrent_execution_dir_isolation(self):
        """Verify two distinct execution IDs receive distinct artifact directories."""
        id_a, id_b = 98882, 98883
        self._create_test_execution(id_a)
        self._create_test_execution(id_b)

        ti_a = MockTaskInstance()
        ti_b = MockTaskInstance()

        res_a = task_validate_input_datasets(**{"dag_run": MockDagRun(conf={"execution_id": id_a}), "ti": ti_a})
        res_b = task_validate_input_datasets(**{"dag_run": MockDagRun(conf={"execution_id": id_b}), "ti": ti_b})

        self.assertNotEqual(res_a["exec_dir"], res_b["exec_dir"])
        self.assertTrue(res_a["exec_dir"].endswith(str(id_a)))
        self.assertTrue(res_b["exec_dir"].endswith(str(id_b)))

    def test_forecast_persistence_idempotency(self):
        """Verify that persisting predictions twice with same execution_id is idempotent."""
        test_id = 98884
        self._create_test_execution(test_id)

        predictions_data = [
            {"product_category": "telefonia", "forecast_week": "2026-03-01", "predicted_demand": 42.5},
            {"product_category": "beleza_saude", "forecast_week": "2026-03-01", "predicted_demand": 78.0},
        ]

        saved_first = save_forecast_results(
            execution_id=test_id,
            pipeline_name="Demand Forecast Pipeline",
            predictions=predictions_data,
            model_name="XGBoost",
            model_version="xgboost-v1"
        )
        self.assertEqual(saved_first, 2)

        # Re-save identical rows (simulating duplicate/retry persistence)
        saved_second = save_forecast_results(
            execution_id=test_id,
            pipeline_name="Demand Forecast Pipeline",
            predictions=predictions_data,
            model_name="XGBoost",
            model_version="xgboost-v1"
        )
        self.assertEqual(saved_second, 2)

        # Count total rows for this execution in DB
        self.cur.execute("SELECT COUNT(*) as cnt FROM forecast_results WHERE execution_id = %s", (test_id,))
        count_row = self.cur.fetchone()
        total_rows = count_row["cnt"] if isinstance(count_row, dict) else count_row[0]
        self.assertEqual(total_rows, 2)

    def test_late_attempt_completion_or_failure_does_not_overwrite_newer_attempt(self):
        """7. Verify late callbacks from an older attempt (attempt_0) cannot overwrite attempt_1 state."""
        test_id = 98886
        self._create_test_execution(test_id)

        # Set execution to RUNNING with attempt_1 as active Airflow run
        active_run_id = f"supplysense_exec_{test_id}_attempt_1"
        stale_run_id = f"supplysense_exec_{test_id}_attempt_0"

        self.cur.execute("""
            UPDATE pipeline_executions
            SET status = 'RUNNING',
                orchestrator = 'AIRFLOW',
                airflow_dag_id = 'supplysense_demand_forecast',
                airflow_run_id = %s
            WHERE execution_id = %s
        """, (active_run_id, test_id))
        self.conn.commit()

        # Simulate older attempt (attempt_0) failure callback firing late
        stale_fail_context = {
            "dag_run": MockDagRun(conf={"execution_id": test_id}, run_id=stale_run_id),
            "exception": RuntimeError("Attempt 0 died")
        }
        handle_dag_failure(stale_fail_context)

        # Execution MUST remain RUNNING with no error message from attempt 0
        self.cur.execute("SELECT status, error_message, airflow_run_id FROM pipeline_executions WHERE execution_id = %s", (test_id,))
        row = self.cur.fetchone()
        self.assertEqual(row["status"], "RUNNING")
        self.assertIsNone(row["error_message"])
        self.assertEqual(row["airflow_run_id"], active_run_id)

        # Simulate older attempt (attempt_0) completion firing late
        exec_dir = os.path.join(REPO_ROOT, "ml", "results", "executions", str(test_id))
        os.makedirs(exec_dir, exist_ok=True)
        with open(os.path.join(exec_dir, "model_comparison.json"), "w") as f:
            f.write("{}")

        ti_stale = MockTaskInstance()
        ti_stale.xcom_push("execution_id", test_id)
        ti_stale.xcom_push("exec_dir", exec_dir)
        ti_stale.xcom_push("forecasts_saved", 0)

        stale_complete_context = {
            "dag_run": MockDagRun(conf={"execution_id": test_id}, run_id=stale_run_id),
            "ti": ti_stale
        }
        task_finalize_execution_telemetry(**stale_complete_context)

        # Execution MUST still remain RUNNING because run_id didn't match
        self.cur.execute("SELECT status, airflow_run_id FROM pipeline_executions WHERE execution_id = %s", (test_id,))
        row = self.cur.fetchone()
        self.assertEqual(row["status"], "RUNNING")

        # Now simulate ACTIVE attempt (attempt_1) completing
        ti_active = MockTaskInstance()
        ti_active.xcom_push("execution_id", test_id)
        ti_active.xcom_push("exec_dir", exec_dir)
        ti_active.xcom_push("forecasts_saved", 0)

        active_complete_context = {
            "dag_run": MockDagRun(conf={"execution_id": test_id}, run_id=active_run_id),
            "ti": ti_active
        }
        task_finalize_execution_telemetry(**active_complete_context)

        # Execution MUST now transition to COMPLETED (commit to refresh repeatable-read snapshot)
        self.conn.commit()
        self.cur.execute("SELECT status, output_location, airflow_run_id FROM pipeline_executions WHERE execution_id = %s", (test_id,))
        row = self.cur.fetchone()
        self.assertEqual(row["status"], "COMPLETED")
        self.assertIn("model_comparison.json", row["output_location"])
        self.assertEqual(row["airflow_run_id"], active_run_id)

    def test_retry_handoff_delayed_attempt0_and_trigger_failure(self):
        """8. Simulate delayed Attempt 0 callback during retry handoff and verify trigger failure recovery."""
        test_id = 98887
        self._create_test_execution(test_id)

        # 1. Attempt 0 is associated with the execution in FAILED status
        stale_run_id = f"supplysense_exec_{test_id}_attempt_0"
        new_run_id = f"supplysense_exec_{test_id}_attempt_1"

        self.cur.execute("""
            UPDATE pipeline_executions
            SET status = 'FAILED',
                orchestrator = 'AIRFLOW',
                airflow_dag_id = 'supplysense_demand_forecast',
                airflow_run_id = %s,
                retry_count = 0,
                max_retries = 3,
                error_message = 'Transient network timeout'
            WHERE execution_id = %s
        """, (stale_run_id, test_id))
        self.conn.commit()

        # 2. Simulate Retry Attempt 1 being claimed
        retry_mod = importlib.import_module('services.pipeline-runner.retry_service')
        retry_execution_fn = retry_mod.retry_execution
        airflow_mod = importlib.import_module('services.pipeline-runner.airflow_client')
        AirflowConnectionError = airflow_mod.AirflowConnectionError

        exec_dir = os.path.join(REPO_ROOT, "ml", "results", "executions", str(test_id))
        os.makedirs(exec_dir, exist_ok=True)
        with open(os.path.join(exec_dir, "model_comparison.json"), "w") as f:
            f.write("{}")

        ti_stale = MockTaskInstance()
        ti_stale.xcom_push("execution_id", test_id)
        ti_stale.xcom_push("exec_dir", exec_dir)
        ti_stale.xcom_push("forecasts_saved", 0)

        # Mock client whose trigger_dag_run simulates Attempt 0 firing during dispatch
        mock_client = MagicMock()
        mock_client.get_dag_run.return_value = {"state": "failed"}

        def simulate_delayed_attempt0_callback(*args, **kwargs):
            # Attempt 0 fires late while attempt 1 is being triggered
            stale_fail_ctx = {
                "dag_run": MockDagRun(conf={"execution_id": test_id}, run_id=stale_run_id),
                "exception": RuntimeError("Attempt 0 zombie failure")
            }
            handle_dag_failure(stale_fail_ctx)

            stale_complete_ctx = {
                "dag_run": MockDagRun(conf={"execution_id": test_id}, run_id=stale_run_id),
                "ti": ti_stale
            }
            task_finalize_execution_telemetry(**stale_complete_ctx)

            return {
                "dag_id": "supplysense_demand_forecast",
                "dag_run_id": new_run_id,
                "is_reused": False
            }

        mock_client.trigger_dag_run.side_effect = simulate_delayed_attempt0_callback

        result = retry_execution_fn(test_id, connection=self.conn, airflow_client=mock_client)

        self.assertEqual(result["status"], "RUNNING")
        self.assertEqual(result["retry_count"], 1)
        self.assertEqual(result["airflow_run_id"], new_run_id)

        # 3. Verify attempt 0 CANNOT corrupt attempt 1 state
        self.cur.execute("""
            SELECT status, retry_count, airflow_run_id, error_message, completed_at
            FROM pipeline_executions
            WHERE execution_id = %s
        """, (test_id,))
        row = self.cur.fetchone()
        self.assertEqual(row["status"], "RUNNING")  # NOT COMPLETED or FAILED from attempt 0
        self.assertEqual(row["retry_count"], 1)
        self.assertEqual(row["airflow_run_id"], new_run_id)
        self.assertIsNone(row["error_message"])
        self.assertIsNone(row["completed_at"])

        # 4. Now test trigger failure during retry handoff
        test_id_fail = 98888
        self._create_test_execution(test_id_fail)
        self.cur.execute("""
            UPDATE pipeline_executions
            SET status = 'FAILED',
                orchestrator = 'AIRFLOW',
                airflow_dag_id = 'supplysense_demand_forecast',
                airflow_run_id = 'supplysense_exec_98888_attempt_0',
                retry_count = 0,
                max_retries = 3,
                error_message = 'Transient network timeout'
            WHERE execution_id = %s
        """, (test_id_fail,))
        self.conn.commit()

        mock_client_fail = MagicMock()
        mock_client_fail.get_dag_run.return_value = {"state": "failed"}
        mock_client_fail.trigger_dag_run.side_effect = AirflowConnectionError("Airflow connection refused")

        result_fail = retry_execution_fn(test_id_fail, connection=self.conn, airflow_client=mock_client_fail)

        self.assertEqual(result_fail["status"], "FAILED")
        self.assertEqual(result_fail["retry_count"], 1)
        self.assertIsNotNone(result_fail["next_retry_at"])

        # Execution must NOT remain RUNNING
        self.cur.execute("""
            SELECT status, retry_count, next_retry_at, error_message
            FROM pipeline_executions
            WHERE execution_id = %s
        """, (test_id_fail,))
        fail_row = self.cur.fetchone()
        self.assertEqual(fail_row["status"], "FAILED")
        self.assertEqual(fail_row["retry_count"], 1)
        self.assertIsNotNone(fail_row["next_retry_at"])
        self.assertIn("Airflow connection refused", fail_row["error_message"])


if __name__ == "__main__":
    unittest.main()
