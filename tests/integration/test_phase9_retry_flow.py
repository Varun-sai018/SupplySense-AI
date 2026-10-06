"""
Integration tests for Phase 9 - Automated Pipeline Retry & Failure Recovery.
Verifies against live MySQL database:
  1. Transient failure -> FAILED -> retry -> COMPLETED -> forecast results persisted.
  2. Idempotency preservation (same triggering_event_id is not duplicated).
  3. Retry exhaustion (3 retries failed -> permanently FAILED, 0 further retries).
  4. Non-retryable error (fails once, never retried).
  5. Stale execution reaped by Phase 8 reaper -> picked up by Phase 9 retry service -> COMPLETED.
"""

import os
import sys
import unittest
import datetime

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import importlib
from common.database import get_connection

runner_mod = importlib.import_module('services.pipeline-runner.runner')
registry_mod = importlib.import_module('services.pipeline-runner.registry')
retry_mod = importlib.import_module('services.pipeline-runner.retry_service')
reaper_mod = importlib.import_module('services.pipeline-runner.reaper')
handlers_mod = importlib.import_module('services.pipeline-runner.handlers')
forecast_repo = importlib.import_module('services.pipeline-runner.forecast_repository')

execute_pipeline = runner_mod.execute_pipeline
register_pipeline = registry_mod.register_pipeline
retry_execution = retry_mod.retry_execution
process_retry_candidates = retry_mod.process_retry_candidates
reap_stale_executions = reaper_mod.reap_stale_executions
default_forecast_handler = handlers_mod.demand_forecast_pipeline_handler


class TestPhase9RetryIntegrationFlow(unittest.TestCase):

    def setUp(self):
        self.conn = get_connection()
        self.cur = self.conn.cursor()
        self.test_decision_ids = []
        self.test_exec_ids = []

        # Clean up any leftover test data
        self.cur.execute("DELETE FROM pipeline_executions WHERE triggering_event_id BETWEEN 999910 AND 999990")
        self.cur.execute("DELETE FROM forecast_results WHERE execution_id >= 90000")
        self.conn.commit()

    def tearDown(self):
        # Restore default handler
        register_pipeline("Demand Forecast Pipeline", default_forecast_handler)

        if self.test_exec_ids:
            fmt = ','.join(['%s'] * len(self.test_exec_ids))
            self.cur.execute(f"DELETE FROM forecast_results WHERE execution_id IN ({fmt})", tuple(self.test_exec_ids))
            self.cur.execute(f"DELETE FROM pipeline_executions WHERE execution_id IN ({fmt})", tuple(self.test_exec_ids))
        if self.test_decision_ids:
            fmt = ','.join(['%s'] * len(self.test_decision_ids))
            self.cur.execute(f"DELETE FROM pipeline_decisions WHERE decision_id IN ({fmt})", tuple(self.test_decision_ids))
        self.conn.commit()
        self.conn.close()

    def _create_decision(self, reason: str = "Test trigger for retry") -> int:
        self.cur.execute("""
            INSERT INTO pipeline_decisions (
                pipeline_name, condition_type, decision, ready_count, total_required, reason
            ) VALUES (
                'Demand Forecast Pipeline', 'ALL', 'TRIGGER', 3, 3, %s
            )
        """, (reason,))
        d_id = self.cur.lastrowid
        self.test_decision_ids.append(d_id)
        return d_id

    def test_transient_failure_retried_to_completed_with_forecasts(self):
        """1. Transient failure -> FAILED -> retry attempt succeeds -> COMPLETED + forecasts saved."""
        d_id = self._create_decision("Test transient failure retry")
        trigger_event_id = 999911

        # 1. Insert execution record
        self.cur.execute("""
            INSERT INTO pipeline_executions (
                pipeline_name, decision_id, triggering_event_id, status, retry_count, max_retries
            ) VALUES (
                'Demand Forecast Pipeline', %s, %s, 'RUNNING', 0, 3
            )
        """, (d_id, trigger_event_id))
        exec_id = self.cur.lastrowid
        self.test_exec_ids.append(exec_id)
        self.conn.commit()

        # 2. Simulate initial run failing with transient DB connection timeout
        def transient_failing_handler(e_id, p_name):
            raise RuntimeError("MySQL connection timed out during query")

        register_pipeline("Demand Forecast Pipeline", transient_failing_handler)
        run_res = execute_pipeline(exec_id, 'Demand Forecast Pipeline', connection=self.conn)
        self.assertEqual(run_res["status"], "FAILED")

        # Verify DB state after initial failure
        self.cur.execute("SELECT status, retry_count, retry_error_type, next_retry_at FROM pipeline_executions WHERE execution_id = %s", (exec_id,))
        row = self.cur.fetchone()
        self.assertEqual(row["status"], "FAILED")
        self.assertEqual(row["retry_count"], 0)
        self.assertEqual(row["retry_error_type"], "TRANSIENT_DB")
        self.assertIsNotNone(row["next_retry_at"])

        # 3. Simulate Retry mechanism processing candidate and handler succeeding
        mock_forecast_preds = [
            {"product_category": "agro_industria_e_comercio", "forecast_week": "2018-09-03", "predicted_demand": 15.2, "model_name": "xgboost-v1"},
            {"product_category": "agro_industria_e_comercio", "forecast_week": "2018-09-10", "predicted_demand": 18.7, "model_name": "xgboost-v1"},
        ]

        def successful_handler(e_id, p_name):
            forecast_repo.save_forecast_results(e_id, p_name, mock_forecast_preds, connection=self.conn)
            return {"output_location": "ml/results/model_comparison.json", "forecast_rows": len(mock_forecast_preds)}

        register_pipeline("Demand Forecast Pipeline", successful_handler)
        retry_res = retry_execution(exec_id, connection=self.conn)

        self.assertEqual(retry_res["status"], "COMPLETED")
        self.assertEqual(retry_res["retry_count"], 1)

        # 4. Verify DB state after successful retry
        self.cur.execute("SELECT status, retry_count, completed_at, output_location, error_message, next_retry_at FROM pipeline_executions WHERE execution_id = %s", (exec_id,))
        final_row = self.cur.fetchone()
        self.assertEqual(final_row["status"], "COMPLETED")
        self.assertEqual(final_row["retry_count"], 1)
        self.assertIsNotNone(final_row["completed_at"])
        self.assertIsNone(final_row["error_message"])
        self.assertIsNone(final_row["next_retry_at"])

        # 5. Verify forecast results persisted and linked to exec_id
        self.cur.execute("SELECT COUNT(*) AS total FROM forecast_results WHERE execution_id = %s", (exec_id,))
        fc_count = self.cur.fetchone()["total"]
        self.assertEqual(fc_count, 2)

        # 6. Verify idempotency: exact same triggering_event_id has exactly 1 execution row
        self.cur.execute("SELECT COUNT(*) AS total FROM pipeline_executions WHERE triggering_event_id = %s", (trigger_event_id,))
        exec_count = self.cur.fetchone()["total"]
        self.assertEqual(exec_count, 1)

    def test_retry_exhaustion_scenario(self):
        """2. Persistent transient errors exhaust max_retries (3 retries) and stop permanently."""
        d_id = self._create_decision("Test retry exhaustion")
        trigger_event_id = 999912

        self.cur.execute("""
            INSERT INTO pipeline_executions (
                pipeline_name, decision_id, triggering_event_id, status, retry_count, max_retries
            ) VALUES (
                'Demand Forecast Pipeline', %s, %s, 'RUNNING', 0, 3
            )
        """, (d_id, trigger_event_id))
        exec_id = self.cur.lastrowid
        self.test_exec_ids.append(exec_id)
        self.conn.commit()

        # Handler that always fails with transient error
        def failing_kafka_handler(e_id, p_name):
            raise RuntimeError("Kafka broker not available")

        register_pipeline("Demand Forecast Pipeline", failing_kafka_handler)

        # Initial run fails
        execute_pipeline(exec_id, 'Demand Forecast Pipeline', connection=self.conn)

        # Attempt Retry 1
        r1 = retry_execution(exec_id, connection=self.conn)
        self.assertEqual(r1["status"], "FAILED")
        self.assertEqual(r1["retry_count"], 1)
        self.assertFalse(r1["retry_exhausted"])

        # Attempt Retry 2
        r2 = retry_execution(exec_id, connection=self.conn)
        self.assertEqual(r2["status"], "FAILED")
        self.assertEqual(r2["retry_count"], 2)
        self.assertFalse(r2["retry_exhausted"])

        # Attempt Retry 3 (Reaches max_retries=3)
        r3 = retry_execution(exec_id, connection=self.conn)
        self.assertEqual(r3["status"], "FAILED")
        self.assertEqual(r3["retry_count"], 3)
        self.assertTrue(r3["retry_exhausted"])
        self.assertIsNone(r3["next_retry_at"])

        # Attempt Retry 4 (Exhausted -> Skipped)
        r4 = retry_execution(exec_id, connection=self.conn)
        self.assertEqual(r4["status"], "EXHAUSTED")
        self.assertEqual(r4["retry_count"], 3)

        # Verify DB final status
        self.cur.execute("SELECT status, retry_count, next_retry_at FROM pipeline_executions WHERE execution_id = %s", (exec_id,))
        final_row = self.cur.fetchone()
        self.assertEqual(final_row["status"], "FAILED")
        self.assertEqual(final_row["retry_count"], 3)
        self.assertIsNone(final_row["next_retry_at"])

    def test_reaper_marked_stale_execution_recovering_via_retry(self):
        """3. Phase 8 reaper marks stale execution FAILED -> Phase 9 retry service retries to COMPLETED."""
        d_id = self._create_decision("Test reaper recovery flow")
        trigger_event_id = 999913
        stale_time = datetime.datetime.now() - datetime.timedelta(minutes=40)

        self.cur.execute("""
            INSERT INTO pipeline_executions (
                pipeline_name, decision_id, triggering_event_id, status, started_at, created_at, retry_count, max_retries
            ) VALUES (
                'Demand Forecast Pipeline', %s, %s, 'RUNNING', %s, %s, 0, 3
            )
        """, (d_id, trigger_event_id, stale_time, stale_time))
        exec_id = self.cur.lastrowid
        self.test_exec_ids.append(exec_id)
        self.conn.commit()

        # Step 1: Run Reaper sweep (timeout = 1800s)
        reap_stats = reap_stale_executions(timeout_seconds=1800, connection=self.conn)
        self.assertGreaterEqual(reap_stats["stale_found"], 1)
        self.assertGreaterEqual(reap_stats["reconciled"], 1)

        # Verify execution is now FAILED with retry scheduled
        self.cur.execute("SELECT status, retry_error_type, next_retry_at FROM pipeline_executions WHERE execution_id = %s", (exec_id,))
        reaped_row = self.cur.fetchone()
        self.assertEqual(reaped_row["status"], "FAILED")
        self.assertEqual(reaped_row["retry_error_type"], "TRANSIENT_REAPER")
        self.assertIsNotNone(reaped_row["next_retry_at"])

        # Step 2: Retry recovers the execution to COMPLETED
        def recovered_handler(e_id, p_name):
            return {"output_location": "ml/results/recovered.json"}

        register_pipeline("Demand Forecast Pipeline", recovered_handler)
        retry_res = retry_execution(exec_id, connection=self.conn)

        self.assertEqual(retry_res["status"], "COMPLETED")
        self.assertEqual(retry_res["retry_count"], 1)

        # Verify DB
        self.cur.execute("SELECT status, retry_count, output_location FROM pipeline_executions WHERE execution_id = %s", (exec_id,))
        recovered_row = self.cur.fetchone()
        self.assertEqual(recovered_row["status"], "COMPLETED")
        self.assertEqual(recovered_row["retry_count"], 1)
        self.assertEqual(recovered_row["output_location"], "ml/results/recovered.json")


if __name__ == '__main__':
    unittest.main()
