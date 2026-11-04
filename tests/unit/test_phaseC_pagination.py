"""
Unit tests for Phase C: Data Filtering & Pagination.
Validates scope filtering (OPERATIONAL, SIMULATED, ALL), SQL pagination, count consistency,
and envelope schema ({ items, total_count, limit, offset }) for both Executions and Datasets APIs.
"""

import unittest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

from services.observability.main import app
from services.observability import repository
from services.observability import service


class TestExecutionsScopingAndPagination(unittest.TestCase):
    """Unit tests for repository and service layer executions pagination and scoping."""

    def test_repository_get_executions_operational_scope(self):
        """Repository get_executions with scope='OPERATIONAL' adds NOT LIKE %s filter."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cur

        repository.get_executions(scope="OPERATIONAL", connection=mock_conn)
        sql = mock_cur.execute.call_args[0][0]
        params = mock_cur.execute.call_args[0][1]
        self.assertIn("e.pipeline_name NOT LIKE %s", sql)
        self.assertIn("SIM-%", params)

    def test_repository_get_executions_simulated_scope(self):
        """Repository get_executions with scope='SIMULATED' adds LIKE %s filter."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cur

        repository.get_executions(scope="SIMULATED", connection=mock_conn)
        sql = mock_cur.execute.call_args[0][0]
        params = mock_cur.execute.call_args[0][1]
        self.assertIn("e.pipeline_name LIKE %s", sql)
        self.assertIn("SIM-%", params)

    def test_repository_get_executions_all_scope(self):
        """Repository get_executions with scope='ALL' contains no SIM prefix filter."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cur

        repository.get_executions(scope="ALL", connection=mock_conn)
        sql = mock_cur.execute.call_args[0][0]
        params = mock_cur.execute.call_args[0][1]
        self.assertNotIn("SIM-%", params)

    def test_repository_count_executions_matches_where_clause(self):
        """Repository count_executions applies the identical WHERE clauses as get_executions."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = {"total": 42}
        mock_conn.cursor.return_value = mock_cur

        cnt = repository.count_executions(status="COMPLETED", scope="OPERATIONAL", connection=mock_conn)
        self.assertEqual(cnt, 42)
        sql = mock_cur.execute.call_args[0][0]
        params = mock_cur.execute.call_args[0][1]
        self.assertIn("SELECT COUNT(*) AS total FROM pipeline_executions", sql)
        self.assertIn("e.status = %s", sql)
        self.assertIn("e.pipeline_name NOT LIKE %s", sql)
        self.assertIn("SIM-%", params)

    def test_service_get_recent_executions_returns_envelope(self):
        """Service get_recent_executions returns envelope with items, total_count, limit, offset."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [
            {
                "execution_id": 101,
                "pipeline_name": "Inventory Reorder Pipeline",
                "decision_id": 10,
                "triggering_event_id": 5,
                "status": "COMPLETED",
                "orchestrator": "LOCAL",
                "airflow_dag_id": None,
                "airflow_run_id": None,
                "retry_count": 0,
                "max_retries": 3,
                "started_at": None,
                "completed_at": None,
                "next_retry_at": None,
                "retry_error_type": None,
                "output_location": None,
                "error_message": None,
                "created_at": None,
                "action_id": None
            }
        ]
        mock_cur.fetchone.return_value = {"total": 123}
        mock_conn.cursor.return_value = mock_cur

        result = service.get_recent_executions(
            scope="OPERATIONAL",
            limit=10,
            offset=20,
            connection=mock_conn
        )
        self.assertIsInstance(result, dict)
        self.assertIn("items", result)
        self.assertIn("total_count", result)
        self.assertIn("limit", result)
        self.assertIn("offset", result)
        self.assertEqual(result["total_count"], 123)
        self.assertEqual(result["limit"], 10)
        self.assertEqual(result["offset"], 20)
        self.assertEqual(len(result["items"]), 1)


class TestDatasetsScopingAndPagination(unittest.TestCase):
    """Unit tests for repository and service layer datasets pagination and scoping."""

    def test_repository_get_datasets_operational_scope(self):
        """Repository get_datasets_with_latest_events with scope='OPERATIONAL' excludes SIMULATED."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cur

        repository.get_datasets_with_latest_events(scope="OPERATIONAL", connection=mock_conn)
        sql = mock_cur.execute.call_args[0][0]
        params = mock_cur.execute.call_args[0][1]
        self.assertIn("m.source_type != %s", sql)
        self.assertIn("SIMULATED", params)

    def test_repository_get_datasets_simulated_scope(self):
        """Repository get_datasets_with_latest_events with scope='SIMULATED' requires SIMULATED."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cur

        repository.get_datasets_with_latest_events(scope="SIMULATED", connection=mock_conn)
        sql = mock_cur.execute.call_args[0][0]
        params = mock_cur.execute.call_args[0][1]
        self.assertIn("m.source_type = %s", sql)
        self.assertIn("SIMULATED", params)

    def test_repository_get_datasets_all_scope(self):
        """Repository get_datasets_with_latest_events with scope='ALL' applies no source_type filter in WHERE."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cur

        repository.get_datasets_with_latest_events(scope="ALL", connection=mock_conn)
        sql = mock_cur.execute.call_args[0][0]
        self.assertNotIn("AND m.source_type", sql)

    def test_repository_count_datasets_where_clause(self):
        """Repository count_datasets applies identical clauses to dataset_metadata."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = {"total": 16}
        mock_conn.cursor.return_value = mock_cur

        cnt = repository.count_datasets(scope="OPERATIONAL", connection=mock_conn)
        self.assertEqual(cnt, 16)
        sql = mock_cur.execute.call_args[0][0]
        params = mock_cur.execute.call_args[0][1]
        self.assertIn("SELECT COUNT(*) AS total FROM dataset_metadata", sql)
        self.assertIn("m.source_type != %s", sql)
        self.assertIn("SIMULATED", params)

    def test_service_get_dataset_status_paginated_returns_envelope(self):
        """Service get_dataset_status_paginated returns envelope with items and counts."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [
            {
                "dataset_id": 1,
                "dataset_name": "Orders",
                "source_type": "CDC",
                "table_name": "olist_orders",
                "current_version": 1,
                "status": "READY",
                "row_count": 99441,
                "last_updated_at": None,
                "latest_event_id": 10,
                "latest_event_time": None,
                "latest_event_type": "DATASET_READY",
                "latest_batch_id": "b1",
                "latest_rows_changed": 100
            }
        ]
        mock_cur.fetchone.return_value = {"total": 5}
        mock_conn.cursor.return_value = mock_cur

        result = service.get_dataset_status_paginated(
            scope="OPERATIONAL",
            limit=5,
            offset=0,
            connection=mock_conn
        )
        self.assertIsInstance(result, dict)
        self.assertIn("items", result)
        self.assertEqual(result["total_count"], 5)
        self.assertEqual(result["limit"], 5)
        self.assertEqual(result["offset"], 0)
        self.assertEqual(len(result["items"]), 1)


class TestObservabilityAPIEndpoints(unittest.TestCase):
    """FastAPI TestClient tests for /api/observability/executions and /api/observability/datasets."""

    def setUp(self):
        self.client = TestClient(app, raise_server_exceptions=False)

    @patch("services.observability.service.get_recent_executions")
    def test_executions_default_scope_is_operational(self, mock_svc):
        """GET /api/observability/executions defaults to scope=OPERATIONAL."""
        mock_svc.return_value = {"items": [], "total_count": 0, "limit": 20, "offset": 0}
        resp = self.client.get("/api/observability/executions")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(mock_svc.call_args.kwargs.get("scope"), "OPERATIONAL")

    @patch("services.observability.service.get_recent_executions")
    def test_executions_scope_param_forwarded(self, mock_svc):
        """GET /api/observability/executions forwards scope parameter."""
        mock_svc.return_value = {"items": [], "total_count": 0, "limit": 20, "offset": 0}
        resp = self.client.get("/api/observability/executions?scope=SIMULATED")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(mock_svc.call_args.kwargs.get("scope"), "SIMULATED")

    def test_executions_invalid_scope_returns_400(self):
        """GET /api/observability/executions with invalid scope returns HTTP 400."""
        resp = self.client.get("/api/observability/executions?scope=INVALID_SCOPE")
        self.assertEqual(resp.status_code, 400)
        self.assertIn("Invalid scope", resp.json().get("detail", ""))

    @patch("services.observability.service.get_dataset_status_paginated")
    def test_datasets_default_scope_is_all(self, mock_svc):
        """GET /api/observability/datasets defaults to scope=ALL."""
        mock_svc.return_value = {"items": [], "total_count": 0, "limit": 20, "offset": 0}
        resp = self.client.get("/api/observability/datasets")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(mock_svc.call_args.kwargs.get("scope"), "ALL")

    @patch("services.observability.service.get_dataset_status_paginated")
    def test_datasets_scope_operational(self, mock_svc):
        """GET /api/observability/datasets forwards scope=OPERATIONAL."""
        mock_svc.return_value = {"items": [], "total_count": 0, "limit": 20, "offset": 0}
        resp = self.client.get("/api/observability/datasets?scope=OPERATIONAL")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(mock_svc.call_args.kwargs.get("scope"), "OPERATIONAL")

    def test_datasets_invalid_scope_returns_400(self):
        """GET /api/observability/datasets with invalid scope returns HTTP 400."""
        resp = self.client.get("/api/observability/datasets?scope=UNKNOWN")
        self.assertEqual(resp.status_code, 400)
        self.assertIn("Invalid scope", resp.json().get("detail", ""))

    @patch("services.observability.service.get_dataset_status_paginated")
    def test_datasets_pagination_params_forwarded(self, mock_svc):
        """GET /api/observability/datasets forwards limit and offset."""
        mock_svc.return_value = {"items": [], "total_count": 0, "limit": 10, "offset": 5}
        resp = self.client.get("/api/observability/datasets?limit=10&offset=5")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(mock_svc.call_args.kwargs.get("limit"), 10)
        self.assertEqual(mock_svc.call_args.kwargs.get("offset"), 5)


if __name__ == "__main__":
    unittest.main()
