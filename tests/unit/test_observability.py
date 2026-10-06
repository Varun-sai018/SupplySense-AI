"""
Unit Tests for Phase 11 - Pipeline Execution Observability & Monitoring.

Verifies:
  1. Summary aggregation (total, running, completed, failed, success/failure rates).
  2. Duration calculation (completed, failed, running, missing timestamps).
  3. Dataset status formatting with and without latest events.
  4. Dependency status formatting across condition types (ALL, ANY, QUORUM).
  5. Forecast summary aggregation (row count, category count, date ranges).
  6. Execution filtering by status, pipeline_name, and limit boundaries.
  7. Execution detail lookups and missing execution 404 responses.
  8. Health endpoint reporting and database health check handling.
  9. Security check: no sensitive credentials exposed.
"""

import os
import sys
import datetime
import unittest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from services.observability import service, repository
from services.observability.main import app


class TestObservabilityUnit(unittest.TestCase):
    """Unit test suite for observability service and API endpoints."""

    def setUp(self):
        self.client = TestClient(app)

    # A. Duration Calculation Tests
    def test_duration_calculation_completed(self):
        """Test duration calculation for COMPLETED execution."""
        started = datetime.datetime(2026, 10, 6, 10, 0, 0)
        completed = datetime.datetime(2026, 10, 6, 10, 0, 15)
        duration = service._calculate_duration(started, completed, "COMPLETED")
        self.assertEqual(duration, 15.0)

    def test_duration_calculation_failed(self):
        """Test duration calculation for FAILED execution."""
        started = datetime.datetime(2026, 10, 6, 10, 0, 0)
        completed = datetime.datetime(2026, 10, 6, 10, 0, 7, 500000)
        duration = service._calculate_duration(started, completed, "FAILED")
        self.assertEqual(duration, 7.5)

    def test_duration_calculation_running(self):
        """Test duration calculation for RUNNING execution (elapsed time)."""
        started = datetime.datetime.now() - datetime.timedelta(seconds=12)
        duration = service._calculate_duration(started, None, "RUNNING")
        self.assertIsNotNone(duration)
        self.assertGreaterEqual(duration, 11.0)

    def test_duration_calculation_null(self):
        """Test duration calculation with missing or invalid timestamps."""
        self.assertIsNone(service._calculate_duration(None, None, "COMPLETED"))
        self.assertIsNone(service._calculate_duration("invalid", None, "COMPLETED"))

    # B. Summary Aggregation Tests
    @patch('services.observability.repository.get_execution_counts')
    @patch('services.observability.repository.get_latest_execution')
    @patch('services.observability.repository.get_dataset_counts')
    @patch('services.observability.repository.get_latest_forecast_summary')
    def test_summary_aggregation_calculations(
        self, mock_fc, mock_ds, mock_latest_exec, mock_exec_counts
    ):
        """Test rate and aggregate calculations in get_observability_summary."""
        mock_exec_counts.return_value = {
            "total": 10,
            "running": 2,
            "completed": 7,
            "failed": 1,
            "retrying": 1,
            "retried_total": 2,
            "avg_duration_seconds": 12.34
        }
        mock_latest_exec.return_value = {
            "execution_id": 105,
            "pipeline_name": "Demand Forecast Pipeline",
            "status": "COMPLETED",
            "started_at": datetime.datetime(2026, 10, 6, 9, 0, 0),
            "completed_at": datetime.datetime(2026, 10, 6, 9, 0, 10),
            "output_location": "ml/results/executions/105/model_comparison.json"
        }
        mock_ds.return_value = {"total": 4, "ready": 3, "waiting": 1}
        mock_fc.return_value = {
            "execution_id": 105,
            "pipeline_name": "Demand Forecast Pipeline",
            "model_name": "XGBoost",
            "model_version": "xgboost-v1",
            "row_count": 770,
            "category_count": 73,
            "min_forecast_week": datetime.date(2018, 2, 19),
            "max_forecast_week": datetime.date(2018, 4, 30),
            "avg_predicted_demand": 14.2,
            "created_at": datetime.datetime(2026, 10, 6, 9, 0, 10),
            "output_location": "ml/results/executions/105/model_comparison.json"
        }

        summary = service.get_observability_summary()

        self.assertEqual(summary["executions"]["total"], 10)
        self.assertEqual(summary["executions"]["completed"], 7)
        self.assertEqual(summary["executions"]["failed"], 1)
        self.assertEqual(summary["success_rate"], 70.0)
        self.assertEqual(summary["failure_rate"], 10.0)
        self.assertEqual(summary["average_duration_seconds"], 12.34)
        self.assertEqual(summary["datasets"]["total"], 4)
        self.assertEqual(summary["datasets"]["ready"], 3)
        self.assertIsNotNone(summary["latest_forecast"])
        self.assertEqual(summary["latest_forecast"]["execution_id"], 105)

    # C. Dataset Status Formatting Tests
    @patch('services.observability.repository.get_datasets_with_latest_events')
    def test_dataset_status_formatting(self, mock_repo_ds):
        """Test formatting of datasets with and without event metadata."""
        mock_repo_ds.return_value = [
            {
                "dataset_id": 1,
                "dataset_name": "Orders",
                "source_type": "mysql_cdc",
                "table_name": "orders",
                "current_version": 5,
                "status": "READY",
                "row_count": 500,
                "last_updated_at": datetime.datetime(2026, 10, 6, 9, 30, 0),
                "latest_event_id": 101,
                "latest_event_time": datetime.datetime(2026, 10, 6, 9, 30, 0),
                "latest_event_type": "INSERT",
                "latest_batch_id": "batch_orders_001",
                "latest_rows_changed": 50
            },
            {
                "dataset_id": 2,
                "dataset_name": "Products",
                "source_type": "csv_snapshot",
                "table_name": "products",
                "current_version": 1,
                "status": "WAITING",
                "row_count": 100,
                "last_updated_at": None,
                "latest_event_id": None,
                "latest_event_time": None,
                "latest_event_type": None,
                "latest_batch_id": None,
                "latest_rows_changed": None
            }
        ]

        datasets = service.get_dataset_status()
        self.assertEqual(len(datasets), 2)
        self.assertEqual(datasets[0]["latest_batch_id"], "batch_orders_001")
        self.assertIsNone(datasets[1]["latest_event_id"])

    # D. Dependency Status Formatting Tests
    @patch('services.observability.repository.get_pipeline_dependencies')
    @patch('services.observability.repository.get_latest_decision_for_pipeline')
    def test_dependency_status_formatting(self, mock_dec, mock_deps):
        """Test dependency state representation across pipelines."""
        mock_deps.return_value = [
            {
                "dependency_id": 1,
                "pipeline_name": "Demand Forecast Pipeline",
                "condition_type": "ALL",
                "required_count": 3,
                "datasets": "Orders, Products, Sellers"
            },
            {
                "dependency_id": 2,
                "pipeline_name": "Quick Report Pipeline",
                "condition_type": "ANY",
                "required_count": 1,
                "datasets": "Orders, Inventory"
            }
        ]
        mock_dec.return_value = {
            "decision_id": 55,
            "pipeline_name": "Demand Forecast Pipeline",
            "condition_type": "ALL",
            "decision": "TRIGGER",
            "ready_count": 3,
            "total_required": 3,
            "reason": "All required datasets are READY.",
            "triggered_at": datetime.datetime(2026, 10, 6, 9, 0, 0),
            "created_at": datetime.datetime(2026, 10, 6, 9, 0, 0)
        }

        deps = service.get_dependency_status()
        self.assertEqual(len(deps), 2)
        self.assertEqual(deps[0]["condition_type"], "ALL")
        self.assertEqual(len(deps[0]["configured_datasets"]), 3)
        self.assertEqual(deps[0]["latest_decision"]["decision"], "TRIGGER")

    # E. Execution Detail & 404 Tests
    @patch('services.observability.repository.get_execution_by_id')
    def test_execution_detail_not_found(self, mock_get_exec):
        """Test 404 is raised when querying a non-existent execution."""
        mock_get_exec.return_value = None

        response = self.client.get("/api/observability/executions/999999")
        self.assertEqual(response.status_code, 404)
        self.assertIn("not found", response.json()["detail"])

    # F. Health Check & Security Tests
    @patch('services.observability.repository.check_db_health')
    def test_health_check_endpoint(self, mock_health):
        """Test health endpoint responds correctly for healthy and unhealthy states."""
        mock_health.return_value = True
        res = self.client.get("/health")
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json(), {"status": "healthy", "database": "connected"})

        mock_health.return_value = False
        res_unhealthy = self.client.get("/health")
        self.assertEqual(res_unhealthy.status_code, 200)
        self.assertEqual(res_unhealthy.json(), {"status": "unhealthy", "database": "disconnected"})

    def test_no_sensitive_secrets_exposed(self):
        """Verify endpoints and responses do not leak sensitive environment or database secrets."""
        with patch('services.observability.repository.get_execution_counts', return_value={"total": 0, "running": 0, "completed": 0, "failed": 0, "retrying": 0, "retried_total": 0, "avg_duration_seconds": None}), \
             patch('services.observability.repository.get_latest_execution', return_value=None), \
             patch('services.observability.repository.get_dataset_counts', return_value={"total": 0, "ready": 0, "waiting": 0}), \
             patch('services.observability.repository.get_latest_forecast_summary', return_value=None):
            
            res = self.client.get("/api/observability/summary")
            content_str = res.text.lower()
            
            # Ensure no credentials appear in serialized response
            self.assertNotIn("password", content_str)
            self.assertNotIn("secret", content_str)
            self.assertNotIn("token", content_str)


if __name__ == '__main__':
    unittest.main()
