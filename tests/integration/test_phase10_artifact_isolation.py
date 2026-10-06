"""
Integration tests for Phase 10 - Execution-Scoped Artifacts & Result Isolation.

Verifies end-to-end against MySQL:
  1. Successive/concurrent executions write to distinct directories:
     ml/results/executions/<execution_id_A>/
     ml/results/executions/<execution_id_B>/
  2. No cross-execution contamination of metrics, predictions, or model files.
  3. Output location in pipeline_executions table correctly links to execution-scoped comparison.
  4. Failure retry reuses the existing execution directory cleanly with atomic overwrites.
  5. Forecast predictions in forecast_results point to the respective execution_id.
"""

import os
import sys
import json
import shutil
import unittest
import importlib

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection

runner_mod = importlib.import_module('services.pipeline-runner.runner')
registry_mod = importlib.import_module('services.pipeline-runner.registry')
retry_mod = importlib.import_module('services.pipeline-runner.retry_service')
handlers_mod = importlib.import_module('services.pipeline-runner.handlers')

execute_pipeline = runner_mod.execute_pipeline
register_pipeline = registry_mod.register_pipeline
retry_execution = retry_mod.retry_execution
default_forecast_handler = handlers_mod.demand_forecast_pipeline_handler


class TestPhase10ArtifactIsolationIntegration(unittest.TestCase):
    """Integration test suite for Phase 10 execution artifact isolation."""

    def setUp(self):
        self.conn = get_connection()
        self.cur = self.conn.cursor()
        self.test_exec_ids = []
        self.test_decision_ids = []
        self.created_dirs = []

        # Clean up any leftover test data
        self.cur.execute("DELETE FROM pipeline_executions WHERE triggering_event_id BETWEEN 999980 AND 999999")
        self.conn.commit()

    def tearDown(self):
        register_pipeline("Demand Forecast Pipeline", default_forecast_handler)

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

        # Clean up artifact directories
        for d in self.created_dirs:
            if os.path.exists(d):
                shutil.rmtree(d, ignore_errors=True)

    def _create_decision(self, reason: str = "Test decision for Phase 10") -> int:
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

    def test_multi_execution_artifact_isolation(self):
        """Verify two pipeline executions produce isolated artifacts with proper metadata."""
        # 1. Setup execution 1
        d_id_1 = self._create_decision("Test multi-exec A")
        event_id_1 = 999981
        self.cur.execute("""
            INSERT INTO pipeline_executions (
                pipeline_name, decision_id, triggering_event_id, status
            ) VALUES (
                'Demand Forecast Pipeline', %s, %s, 'RUNNING'
            )
        """, (d_id_1, event_id_1))
        exec_id_1 = self.cur.lastrowid
        self.test_exec_ids.append(exec_id_1)
        self.conn.commit()

        # Run Execution 1
        res_1 = execute_pipeline(exec_id_1, "Demand Forecast Pipeline", connection=self.conn)
        self.assertEqual(res_1["status"], "COMPLETED")
        exec_dir_1 = os.path.join(REPO_ROOT, 'ml', 'results', 'executions', str(exec_id_1))
        self.created_dirs.append(exec_dir_1)

        # 2. Setup execution 2
        d_id_2 = self._create_decision("Test multi-exec B")
        event_id_2 = 999982
        self.cur.execute("""
            INSERT INTO pipeline_executions (
                pipeline_name, decision_id, triggering_event_id, status
            ) VALUES (
                'Demand Forecast Pipeline', %s, %s, 'RUNNING'
            )
        """, (d_id_2, event_id_2))
        exec_id_2 = self.cur.lastrowid
        self.test_exec_ids.append(exec_id_2)
        self.conn.commit()

        # Run Execution 2
        res_2 = execute_pipeline(exec_id_2, "Demand Forecast Pipeline", connection=self.conn)
        self.assertEqual(res_2["status"], "COMPLETED")
        exec_dir_2 = os.path.join(REPO_ROOT, 'ml', 'results', 'executions', str(exec_id_2))
        self.created_dirs.append(exec_dir_2)

        # Assert executions are distinct
        self.assertNotEqual(exec_id_1, exec_id_2)
        self.assertNotEqual(exec_dir_1, exec_dir_2)

        # Verify Execution 1 artifacts
        self.assertTrue(os.path.isdir(exec_dir_1))
        comp_file_1 = os.path.join(exec_dir_1, 'model_comparison.json')
        self.assertTrue(os.path.isfile(comp_file_1))
        with open(comp_file_1, 'r', encoding='utf-8') as f:
            comp_1 = json.load(f)
        self.assertEqual(comp_1["metadata"]["execution_id"], exec_id_1)

        # Verify Execution 2 artifacts
        self.assertTrue(os.path.isdir(exec_dir_2))
        comp_file_2 = os.path.join(exec_dir_2, 'model_comparison.json')
        self.assertTrue(os.path.isfile(comp_file_2))
        with open(comp_file_2, 'r', encoding='utf-8') as f:
            comp_2 = json.load(f)
        self.assertEqual(comp_2["metadata"]["execution_id"], exec_id_2)

        # Verify DB output_locations
        self.cur.execute("SELECT execution_id, output_location FROM pipeline_executions WHERE execution_id IN (%s, %s)", (exec_id_1, exec_id_2))
        rows = {r["execution_id"]: r["output_location"] for r in self.cur.fetchall()}
        self.assertEqual(rows[exec_id_1], f"ml/results/executions/{exec_id_1}/model_comparison.json")
        self.assertEqual(rows[exec_id_2], f"ml/results/executions/{exec_id_2}/model_comparison.json")

        # Verify forecast_results persistence
        self.cur.execute("SELECT COUNT(*) as cnt FROM forecast_results WHERE execution_id = %s", (exec_id_1,))
        self.assertGreater(self.cur.fetchone()["cnt"], 0)
        self.cur.execute("SELECT COUNT(*) as cnt FROM forecast_results WHERE execution_id = %s", (exec_id_2,))
        self.assertGreater(self.cur.fetchone()["cnt"], 0)

    def test_retry_artifact_isolation_and_atomic_overwrite(self):
        """Verify retry reuses and cleanly overwrites execution-specific artifacts."""
        d_id = self._create_decision("Test retry overwrite")
        event_id = 999983
        self.cur.execute("""
            INSERT INTO pipeline_executions (
                pipeline_name, decision_id, triggering_event_id, status, retry_count, max_retries
            ) VALUES (
                'Demand Forecast Pipeline', %s, %s, 'RUNNING', 0, 3
            )
        """, (d_id, event_id))
        exec_id = self.cur.lastrowid
        self.test_exec_ids.append(exec_id)
        self.conn.commit()

        # Mock handler to fail first attempt, write partial file
        attempts = 0
        def flaking_handler(e_id, p_name):
            nonlocal attempts
            attempts += 1
            exec_dir = os.path.join(REPO_ROOT, 'ml', 'results', 'executions', str(e_id))
            os.makedirs(exec_dir, exist_ok=True)
            if attempts == 1:
                # Partial/corrupt write during failure
                with open(os.path.join(exec_dir, 'model_comparison.json'), 'w') as f:
                    f.write('{"corrupted": true}')
                raise RuntimeError("MySQL connection timed out during execution")
            return default_forecast_handler(e_id, p_name)

        register_pipeline("Demand Forecast Pipeline", flaking_handler)

        res_initial = execute_pipeline(exec_id, "Demand Forecast Pipeline", connection=self.conn)
        exec_dir = os.path.join(REPO_ROOT, 'ml', 'results', 'executions', str(exec_id))
        self.created_dirs.append(exec_dir)

        self.assertEqual(res_initial["status"], "FAILED")

        self.cur.execute("SELECT retry_count FROM pipeline_executions WHERE execution_id = %s", (exec_id,))
        self.assertEqual(self.cur.fetchone()["retry_count"], 0)

        # Now retry
        retry_res = retry_execution(exec_id, connection=self.conn)
        self.assertEqual(retry_res["status"], "COMPLETED")
        self.assertEqual(retry_res["retry_count"], 1)

        # Verify the partial corrupted file was cleanly replaced by valid artifact
        comp_file = os.path.join(exec_dir, 'model_comparison.json')
        self.assertTrue(os.path.isfile(comp_file))
        with open(comp_file, 'r', encoding='utf-8') as f:
            comp_data = json.load(f)
        self.assertEqual(comp_data["metadata"]["execution_id"], exec_id)
        self.assertIn("validation", comp_data)
        self.assertIn("test", comp_data)


if __name__ == '__main__':
    unittest.main()
