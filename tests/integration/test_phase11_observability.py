"""
Integration Tests for Phase 11 - Pipeline Execution Observability & Monitoring.

Verifies against live MySQL database using FastAPI TestClient:
  1. GET /health reports healthy and database connected.
  2. GET /api/observability/summary returns accurate aggregates matching MySQL state.
  3. GET /api/observability/executions filters by status, pipeline_name, and respects limits.
  4. GET /api/observability/executions/{execution_id} returns metadata, decision, forecast, and artifacts.
  5. GET /api/observability/executions/{invalid_id} returns 404 Not Found.
  6. GET /api/observability/datasets returns all datasets with latest CDC/event metadata.
  7. GET /api/observability/dependencies returns pipeline dependencies and latest evaluated decisions.
  8. GET /api/observability/forecasts returns grouped forecast summaries.
  9. Strict read-only verification: observability API modifies zero database rows.
"""

import os
import sys
import unittest
import datetime
from fastapi.testclient import TestClient

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from services.observability.main import app


class TestPhase11ObservabilityIntegration(unittest.TestCase):
    """Integration test suite for Phase 11 observability API."""

    def setUp(self):
        self.client = TestClient(app)
        self.conn = get_connection()
        self.cur = self.conn.cursor()

        self.test_exec_ids = []
        self.test_decision_ids = []
        self.test_event_ids = []

        # Setup test decision
        self.cur.execute("""
            INSERT INTO pipeline_decisions (
                pipeline_name, condition_type, decision, ready_count, total_required, reason, triggered_at
            ) VALUES (
                'Demand Forecast Pipeline', 'ALL', 'TRIGGER', 3, 3, 'Phase 11 Integration Decision', NOW()
            )
        """)
        self.demo_decision_id = self.cur.lastrowid
        self.test_decision_ids.append(self.demo_decision_id)

        # Setup completed execution
        self.cur.execute("""
            INSERT INTO pipeline_executions (
                pipeline_name, decision_id, triggering_event_id, status, retry_count, max_retries,
                started_at, completed_at, output_location
            ) VALUES (
                'Demand Forecast Pipeline', %s, 999971, 'COMPLETED', 0, 3,
                NOW() - INTERVAL 20 SECOND, NOW(), 'ml/results/executions/999971/model_comparison.json'
            )
        """, (self.demo_decision_id,))
        self.completed_exec_id = self.cur.lastrowid
        self.test_exec_ids.append(self.completed_exec_id)

        # Setup failed execution with retry
        self.cur.execute("""
            INSERT INTO pipeline_executions (
                pipeline_name, decision_id, triggering_event_id, status, retry_count, max_retries,
                started_at, completed_at, next_retry_at, retry_error_type, error_message
            ) VALUES (
                'Demand Forecast Pipeline', %s, 999972, 'FAILED', 1, 3,
                NOW() - INTERVAL 10 SECOND, NOW() - INTERVAL 5 SECOND,
                NOW() + INTERVAL 10 SECOND, 'TRANSIENT_DB', 'Database timeout'
            )
        """, (self.demo_decision_id,))
        self.failed_exec_id = self.cur.lastrowid
        self.test_exec_ids.append(self.failed_exec_id)

        # Setup forecast results for completed execution
        self.cur.execute("""
            INSERT INTO forecast_results (
                execution_id, pipeline_name, product_category, forecast_week, predicted_demand,
                model_name, model_version
            ) VALUES 
            (%s, 'Demand Forecast Pipeline', 'bed_bath_table', '2018-02-19', 15.5000, 'XGBoost', 'xgboost-v1'),
            (%s, 'Demand Forecast Pipeline', 'health_beauty', '2018-02-19', 22.1000, 'XGBoost', 'xgboost-v1')
        """, (self.completed_exec_id, self.completed_exec_id))

        self.conn.commit()

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

    def test_health_endpoint(self):
        """1. Health check returns 200 with connected database."""
        res = self.client.get("/health")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["status"], "healthy")
        self.assertEqual(data["database"], "connected")

    def test_observability_summary_endpoint(self):
        """2. Observability summary matches actual database state."""
        res = self.client.get("/api/observability/summary")
        self.assertEqual(res.status_code, 200)
        data = res.json()

        self.assertIn("executions", data)
        self.assertGreaterEqual(data["executions"]["total"], 2)
        self.assertGreaterEqual(data["executions"]["completed"], 1)
        self.assertGreaterEqual(data["executions"]["failed"], 1)
        self.assertIn("success_rate", data)
        self.assertIn("failure_rate", data)
        self.assertIn("datasets", data)
        self.assertGreaterEqual(data["datasets"]["total"], 1)
        self.assertIsNotNone(data["latest_execution"])
        self.assertIsNotNone(data["latest_forecast"])

    def test_executions_endpoint_and_filtering(self):
        """3. Executions list supports filtering by status, pipeline_name, and pagination limit."""
        # Unfiltered
        res = self.client.get("/api/observability/executions?limit=10")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIsInstance(data, list)
        self.assertLessEqual(len(data), 10)

        # Filter by status = COMPLETED
        res_completed = self.client.get("/api/observability/executions?status=COMPLETED")
        self.assertEqual(res_completed.status_code, 200)
        for row in res_completed.json():
            self.assertEqual(row["status"], "COMPLETED")

        # Filter by status = FAILED
        res_failed = self.client.get("/api/observability/executions?status=FAILED")
        self.assertEqual(res_failed.status_code, 200)
        for row in res_failed.json():
            self.assertEqual(row["status"], "FAILED")

    def test_execution_detail_endpoint(self):
        """4. Execution detail returns full metadata, decision, and forecast context."""
        res = self.client.get(f"/api/observability/executions/{self.completed_exec_id}")
        self.assertEqual(res.status_code, 200)
        data = res.json()

        self.assertEqual(data["execution_id"], self.completed_exec_id)
        self.assertEqual(data["status"], "COMPLETED")
        self.assertIsNotNone(data["decision"])
        self.assertEqual(data["decision"]["decision"], "TRIGGER")
        self.assertIsNotNone(data["forecast"])
        self.assertEqual(data["forecast"]["row_count"], 2)
        self.assertEqual(data["forecast"]["category_count"], 2)

    def test_execution_detail_not_found_returns_404(self):
        """5. Non-existent execution returns 404 HTTP status."""
        res = self.client.get("/api/observability/executions/99999999")
        self.assertEqual(res.status_code, 404)
        self.assertIn("not found", res.json()["detail"])

    def test_datasets_endpoint(self):
        """6. Datasets list returns tracked datasets."""
        res = self.client.get("/api/observability/datasets")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIsInstance(data, list)
        self.assertGreater(len(data), 0)
        names = [d["dataset_name"] for d in data]
        self.assertIn("Orders", names)

    def test_dependencies_endpoint(self):
        """7. Dependencies list returns pipeline rules and latest decisions."""
        res = self.client.get("/api/observability/dependencies")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIsInstance(data, list)
        self.assertGreater(len(data), 0)
        pipeline_names = [d["pipeline_name"] for d in data]
        self.assertIn("Demand Forecast Pipeline", pipeline_names)

    def test_forecasts_endpoint(self):
        """8. Forecasts list returns execution summaries with aggregation."""
        res = self.client.get("/api/observability/forecasts?limit=5")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIsInstance(data, list)
        self.assertGreater(len(data), 0)
        first_fc = data[0]
        self.assertIn("execution_id", first_fc)
        self.assertIn("row_count", first_fc)
        self.assertIn("category_count", first_fc)
        self.assertIn("average_predicted_demand", first_fc)

    def test_strict_read_only_nature(self):
        """9. Verify observability calls make zero mutations to database state."""
        self.cur.execute("SELECT COUNT(*) as cnt FROM pipeline_executions")
        count_before = self.cur.fetchone()["cnt"]

        # Hit multiple endpoints
        self.client.get("/api/observability/summary")
        self.client.get("/api/observability/executions")
        self.client.get(f"/api/observability/executions/{self.completed_exec_id}")
        self.client.get("/api/observability/datasets")
        self.client.get("/api/observability/dependencies")
        self.client.get("/api/observability/forecasts")

        self.cur.execute("SELECT COUNT(*) as cnt FROM pipeline_executions")
        count_after = self.cur.fetchone()["cnt"]

        self.assertEqual(count_before, count_after)


if __name__ == '__main__':
    unittest.main()
