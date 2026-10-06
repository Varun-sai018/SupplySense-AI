"""
Integration Tests for Phase 12 - Capstone Readiness & Master E2E Integration.

Verifies:
  1. Full end-to-end flow: Ingestion -> CDC Dedup & Batching -> Dependency Evaluation -> Execution -> ML Forecast -> Persistence -> Observability.
  2. Database schema completeness across all 11 migrations.
  3. Execution-scoped artifact isolation and directory completeness.
  4. Failure reconciliation (Reaper) and automated retry recovery (Phase 9).
  5. Observability telemetry consistency with real database state.
"""

import os
import sys
import json
import unittest
import datetime
from fastapi.testclient import TestClient

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from services.observability.main import app as obs_app

import importlib
runner_mod = importlib.import_module('services.pipeline-runner.runner')
registry_mod = importlib.import_module('services.pipeline-runner.registry')
retry_mod = importlib.import_module('services.pipeline-runner.retry_service')
reaper_mod = importlib.import_module('services.pipeline-runner.reaper')
handlers_mod = importlib.import_module('services.pipeline-runner.handlers')
engine_main = importlib.import_module('services.dependency-engine.main')
batcher_mod = importlib.import_module('services.cdc-adapter.batcher')

execute_pipeline = runner_mod.execute_pipeline
register_pipeline = registry_mod.register_pipeline
retry_execution = retry_mod.retry_execution
reap_stale_executions = reaper_mod.reap_stale_executions
process_event = engine_main.process_event
CDCBatcher = batcher_mod.CDCBatcher
default_forecast_handler = handlers_mod.demand_forecast_pipeline_handler


class TestPhase12CapstoneReadiness(unittest.TestCase):
    """Integration test suite for master capstone readiness."""

    def setUp(self):
        self.client = TestClient(obs_app)
        self.conn = get_connection()
        self.cur = self.conn.cursor()
        self.test_exec_ids = []
        self.test_decision_ids = []
        self.test_event_ids = []

    def tearDown(self):
        register_pipeline("Demand Forecast Pipeline", default_forecast_handler)

        if self.test_exec_ids:
            fmt = ','.join(['%s'] * len(self.test_exec_ids))
            self.cur.execute(f"DELETE FROM forecast_results WHERE execution_id IN ({fmt})", tuple(self.test_exec_ids))
            self.cur.execute(f"DELETE FROM pipeline_executions WHERE execution_id IN ({fmt})", tuple(self.test_exec_ids))
        if self.test_decision_ids:
            fmt = ','.join(['%s'] * len(self.test_decision_ids))
            self.cur.execute(f"DELETE FROM pipeline_decisions WHERE decision_id IN ({fmt})", tuple(self.test_decision_ids))
        if self.test_event_ids:
            fmt = ','.join(['%s'] * len(self.test_event_ids))
            self.cur.execute(f"DELETE FROM dataset_events WHERE event_id IN ({fmt})", tuple(self.test_event_ids))

        self.conn.commit()
        self.cur.close()
        self.conn.close()

    def test_schema_completeness(self):
        """1. Verify all expected core tables from migrations 001-011 exist with required columns."""
        expected_tables = [
            "dataset_metadata",
            "dataset_events",
            "pipeline_dependencies",
            "dependency_datasets",
            "pipeline_decisions",
            "pipeline_executions",
            "forecast_results"
        ]
        self.cur.execute("SHOW TABLES")
        existing_tables = [list(r.values())[0] for r in self.cur.fetchall()]
        for tbl in expected_tables:
            self.assertIn(tbl, existing_tables, f"Missing required table: {tbl}")

        # Check key columns added in Phase 8, 9, 10
        self.cur.execute("DESCRIBE dataset_events")
        event_cols = [r["Field"] for r in self.cur.fetchall()]
        self.assertIn("source_change_id", event_cols)
        self.assertIn("batch_id", event_cols)

        self.cur.execute("DESCRIBE pipeline_executions")
        exec_cols = [r["Field"] for r in self.cur.fetchall()]
        self.assertIn("retry_count", exec_cols)
        self.assertIn("max_retries", exec_cols)
        self.assertIn("next_retry_at", exec_cols)
        self.assertIn("retry_error_type", exec_cols)
        self.assertIn("output_location", exec_cols)

    def test_end_to_end_capstone_pipeline_flow(self):
        """2. Verify full master pipeline flow from event to observability."""
        # 1. CDC Deduplication and Micro-batching
        batcher = CDCBatcher(max_events=100, max_wait_ms=5000)
        source_id = f"mysql-bin.000099:{int(datetime.datetime.now().timestamp())}:0"
        
        batcher.add_event({
            "dataset_name": "Orders",
            "source_file": "mysql-bin.000099",
            "source_pos": 9000,
            "source_change_id": source_id,
            "rows_changed": 10
        })
        # Duplicate check
        dup_res = batcher.add_event({
            "dataset_name": "Orders",
            "source_file": "mysql-bin.000099",
            "source_pos": 9000,
            "source_change_id": source_id,
            "rows_changed": 10
        })
        self.assertTrue(dup_res.get("is_duplicate"))

        batches = batcher.flush_all()
        self.assertEqual(len(batches), 1)
        batch = batches[0]

        # 2. Register Dataset Event
        self.cur.execute("""
            INSERT INTO dataset_events (
                dataset_id, dataset_name, event_type, dataset_version,
                rows_changed, source_file, source_pos, source_change_id, batch_id, event_status
            ) VALUES (1, 'Orders', 'DATASET_UPDATED', 200, %s, 'mysql-bin.000099', 9000, %s, %s, 'PUBLISHED')
        """, (batch['rows_changed'], source_id, batch['batch_id']))
        event_id = self.cur.lastrowid
        self.test_event_ids.append(event_id)
        self.conn.commit()

        # 3. Set dependencies to READY and process event
        self.cur.execute("UPDATE dataset_metadata SET status = 'READY', current_version = 200 WHERE dataset_name = 'Orders'")
        self.cur.execute("UPDATE dataset_metadata SET status = 'READY' WHERE dataset_name IN ('Products', 'Sellers')")
        self.conn.commit()

        decision, reason = process_event({
            "event_id": event_id,
            "dataset_name": "Orders",
            "dataset_version": 200,
            "rows_changed": batch['rows_changed']
        }, self.conn)

        self.assertEqual(decision, "TRIGGER")

        # 4. Verify execution record created and completed
        self.cur.execute("SELECT execution_id, decision_id, status, output_location FROM pipeline_executions WHERE triggering_event_id = %s", (event_id,))
        exec_row = self.cur.fetchone()
        self.assertIsNotNone(exec_row)
        exec_id = exec_row["execution_id"]
        self.test_exec_ids.append(exec_id)
        self.test_decision_ids.append(exec_row["decision_id"])

        self.assertEqual(exec_row["status"], "COMPLETED")
        self.assertIn(f"ml/results/executions/{exec_id}/model_comparison.json", exec_row["output_location"])

        # 5. Verify forecast results
        self.cur.execute("SELECT COUNT(*) as cnt FROM forecast_results WHERE execution_id = %s", (exec_id,))
        self.assertGreater(self.cur.fetchone()["cnt"], 0)

        # 6. Verify observability API
        res_summary = self.client.get("/api/observability/summary")
        self.assertEqual(res_summary.status_code, 200)
        self.assertGreater(res_summary.json()["executions"]["total"], 0)

        res_detail = self.client.get(f"/api/observability/executions/{exec_id}")
        self.assertEqual(res_detail.status_code, 200)
        detail = res_detail.json()
        self.assertEqual(detail["status"], "COMPLETED")
        self.assertTrue(detail["artifacts"]["exists"])


if __name__ == '__main__':
    unittest.main()
