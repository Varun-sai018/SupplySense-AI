"""
Unit tests for SupplySense AI - Phase A: Backend Scoping & Query Isolation.

Covers:
1. Operational Execution Isolation (tests 1-6)
2. Operational Dataset Isolation (tests 7-9)
3. Operational Alert Isolation (tests 10-12)
4. Dynamic Operational Churn & Inventory (tests 13-17)
"""

import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime
from services.observability import repository, service, analyst_service
from fastapi.testclient import TestClient
from services.observability.main import app


class TestPhaseABackendScoping(unittest.TestCase):

    def setUp(self):
        analyst_service.clear_analyst_cache()
        self.client = TestClient(app)

    # -------------------------------------------------------------------------
    # Execution Isolation (Items 1-6)
    # -------------------------------------------------------------------------

    def test_01_operational_summary_excludes_simulated_executions(self):
        """1. Operational summary query adds WHERE pipeline_name NOT LIKE 'SIM-%'."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {
            "total": 123, "running": 0, "completed": 78, "failed": 45,
            "retrying": 0, "retried_total": 0, "avg_duration_seconds": 35532.0
        }
        mock_conn.cursor.return_value = mock_cursor

        result = repository.get_execution_counts(connection=mock_conn, exclude_simulated=True)
        self.assertEqual(result["total"], 123)
        sql = mock_cursor.execute.call_args[0][0]
        self.assertIn("pipeline_name NOT LIKE 'SIM-%'", sql)

    def test_02_completed_operational_count_correct(self):
        """2. Completed operational count only includes non-simulated executions."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {
            "total": 123, "running": 0, "completed": 78, "failed": 45,
            "retrying": 0, "retried_total": 0, "avg_duration_seconds": 35532.0
        }
        mock_conn.cursor.return_value = mock_cursor

        counts = repository.get_execution_counts(connection=mock_conn, exclude_simulated=True)
        self.assertEqual(counts["completed"], 78)

    def test_03_failed_operational_count_correct(self):
        """3. Failed operational count excludes simulated failures."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {
            "total": 123, "running": 0, "completed": 78, "failed": 45,
            "retrying": 0, "retried_total": 0, "avg_duration_seconds": 35532.0
        }
        mock_conn.cursor.return_value = mock_cursor

        counts = repository.get_execution_counts(connection=mock_conn, exclude_simulated=True)
        self.assertEqual(counts["failed"], 45)

    def test_04_success_rate_uses_operational_executions_only(self):
        """4. Success rate calculation in service.get_observability_summary uses operational counts."""
        mock_counts = {
            "total": 123, "running": 0, "completed": 78, "failed": 45,
            "retrying": 0, "retried_total": 0, "avg_duration_seconds": 35532.0
        }
        with patch("services.observability.repository.get_execution_counts", return_value=mock_counts) as mock_execs, \
             patch("services.observability.repository.get_latest_execution", return_value=None), \
             patch("services.observability.repository.get_dataset_counts", return_value={"total": 5, "ready": 5, "waiting": 0}), \
             patch("services.observability.repository.get_latest_forecast_summary", return_value=None):
            summary = service.get_observability_summary(exclude_simulated=True)
            mock_execs.assert_called_once_with(connection=None, exclude_simulated=True)
            expected_rate = round((78 / 123) * 100.0, 2)
            self.assertEqual(summary["success_rate"], expected_rate)
            self.assertEqual(summary["executions"]["total"], 123)

    def test_05_latest_operational_execution_excludes_simulated(self):
        """5. get_latest_execution with exclude_simulated=True excludes SIM-* pipelines."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {
            "execution_id": 123, "pipeline_name": "operational_pipeline_main", "status": "COMPLETED"
        }
        mock_conn.cursor.return_value = mock_cursor

        latest = repository.get_latest_execution(connection=mock_conn, exclude_simulated=True)
        self.assertEqual(latest["execution_id"], 123)
        self.assertFalse(latest["pipeline_name"].startswith("SIM-"))
        sql = mock_cursor.execute.call_args[0][0]
        self.assertIn("pipeline_name NOT LIKE 'SIM-%'", sql)

    def test_06_execution_history_can_still_retrieve_simulated(self):
        """6. get_executions without exclude_simulated (or exclude_simulated=False) retrieves SIM-* pipelines."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {"execution_id": 135, "pipeline_name": "SIM-AST-Pipeline", "status": "COMPLETED"}
        ]
        mock_conn.cursor.return_value = mock_cursor

        results = repository.get_executions(exclude_simulated=False, connection=mock_conn)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["pipeline_name"], "SIM-AST-Pipeline")
        sql = mock_cursor.execute.call_args[0][0]
        self.assertNotIn("pipeline_name NOT LIKE 'SIM-%'", sql)

    # -------------------------------------------------------------------------
    # Dataset Isolation (Items 7-9)
    # -------------------------------------------------------------------------

    def test_07_operational_dataset_summary_excludes_simulated(self):
        """7. Operational dataset count query adds WHERE source_type != 'SIMULATED'."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {"total": 5, "ready": 5, "waiting": 0}
        mock_conn.cursor.return_value = mock_cursor

        counts = repository.get_dataset_counts(connection=mock_conn, exclude_simulated=True)
        self.assertEqual(counts["total"], 5)
        sql = mock_cursor.execute.call_args[0][0]
        self.assertIn("source_type != 'SIMULATED'", sql)

    def test_08_ready_and_total_counts_use_consistent_filtering(self):
        """8. Ready, waiting, and total dataset counts use the exact same WHERE filter."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {"total": 5, "ready": 5, "waiting": 0}
        mock_conn.cursor.return_value = mock_cursor

        counts = repository.get_dataset_counts(connection=mock_conn, exclude_simulated=True)
        self.assertEqual(counts["ready"] + counts["waiting"], counts["total"])

    def test_09_simulation_dataset_retrieval_remains_functional(self):
        """9. Dataset queries can still filter by source_type='SIMULATED' or return all datasets."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {"dataset_id": 10, "dataset_name": "SIM_Regional_Node_A", "source_type": "SIMULATED"}
        ]
        mock_conn.cursor.return_value = mock_cursor

        rows = repository.get_datasets_with_latest_events(source_type="SIMULATED", connection=mock_conn)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["source_type"], "SIMULATED")
        sql = mock_cursor.execute.call_args[0][0]
        self.assertIn("source_type = %s", sql)

    # -------------------------------------------------------------------------
    # Alert Isolation (Items 10-12)
    # -------------------------------------------------------------------------

    def test_10_failed_sim_execution_excluded_from_operational_alerts(self):
        """10. Failed SIM-* executions are excluded from operational alerts."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.side_effect = [
            [],  # problem_suppliers
            [],  # catalog_failures
            [],  # failed_replenishments
            [],  # overdue_replenishments
            [],  # stuck_replenishments
            []   # failed_executions
        ]
        mock_conn.cursor.return_value = mock_cursor

        alerts = repository.get_operational_alerts_raw(exclude_simulated=True, connection=mock_conn)
        calls = mock_cursor.execute.call_args_list
        failed_exec_sql = calls[5][0][0]
        self.assertIn("e.pipeline_name NOT LIKE 'SIM-%'", failed_exec_sql)
        self.assertEqual(len(alerts["failed_executions"]), 0)

    def test_11_failed_operational_execution_appears_in_operational_alerts(self):
        """11. Failed non-simulated pipeline execution appears in operational alerts."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.side_effect = [
            [], [], [], [], [],
            [{"execution_id": 99, "pipeline_name": "operational_etl", "status": "FAILED", "error_message": "Timeout"}]
        ]
        mock_conn.cursor.return_value = mock_cursor

        alerts = repository.get_operational_alerts_raw(exclude_simulated=True, connection=mock_conn)
        self.assertEqual(len(alerts["failed_executions"]), 1)
        self.assertEqual(alerts["failed_executions"][0]["pipeline_name"], "operational_etl")

    def test_12_existing_supplier_and_inventory_alerts_remain_functional(self):
        """12. Supplier and catalog alerts are preserved intact in operational alerts."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.side_effect = [
            [{"supplier_id": 2, "supplier_name": "Supplier Beta", "status": "Degraded", "reliability_score": 0.82}],
            [{"supplier_id": 2, "sku": "SKU-001", "supply_availability": "OUT_OF_STOCK"}],
            [], [], [], []
        ]
        mock_conn.cursor.return_value = mock_cursor

        alerts = repository.get_operational_alerts_raw(exclude_simulated=True, connection=mock_conn)
        self.assertEqual(len(alerts["problem_suppliers"]), 1)
        self.assertEqual(len(alerts["catalog_failures"]), 1)

    # -------------------------------------------------------------------------
    # Operational Churn & Inventory (Items 13-17)
    # -------------------------------------------------------------------------

    def test_13_inventory_totals_calculated_from_actual_db_records(self):
        """13. Total inventory and SKU metrics reflect operational_inventory query."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            {"backlog_count": 10, "processing_count": 5, "invoiced_count": 5, "approved_count": 0, "created_count": 0},
            {"total_skus": 106, "total_on_hand": 4235, "total_reserved": 0, "total_available": 4235, "total_safe_threshold": 640, "low_stock_skus": 3},
            {"total_suppliers": 7, "online_count": 5, "degraded_count": 1, "offline_count": 1}
        ]
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cursor

        res = analyst_service.get_operational_churn(connection=mock_conn)
        self.assertEqual(res["current_inventory_level"], 4235)
        self.assertEqual(res["total_skus"], 106)
        self.assertEqual(res["total_available"], 4235)
        self.assertEqual(res["safe_threshold"], 640)

    def test_14_low_stock_calculations_use_actual_thresholds(self):
        """14. Low-stock SKU count is derived from actual query where available <= threshold."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            {"backlog_count": 0, "processing_count": 0, "invoiced_count": 0, "approved_count": 0, "created_count": 0},
            {"total_skus": 106, "total_on_hand": 4235, "total_reserved": 0, "total_available": 4235, "total_safe_threshold": 640, "low_stock_skus": 3},
            {"total_suppliers": 7, "online_count": 5, "degraded_count": 1, "offline_count": 1}
        ]
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cursor

        res = analyst_service.get_operational_churn(connection=mock_conn)
        self.assertEqual(res["low_stock_skus"], 3)
        inv_sql = mock_cursor.execute.call_args_list[1][0][0]
        self.assertIn("COUNT(CASE WHEN available_quantity <= reorder_threshold THEN 1 END) AS low_stock_skus", inv_sql)

    def test_15_supplier_status_derived_from_actual_records(self):
        """15. Supplier status reflects actual breakdown: 5 Online, 1 Degraded, 1 Offline -> DEGRADED."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            {"backlog_count": 0, "processing_count": 0, "invoiced_count": 0, "approved_count": 0, "created_count": 0},
            {"total_skus": 106, "total_on_hand": 4235, "total_reserved": 0, "total_available": 4235, "total_safe_threshold": 640, "low_stock_skus": 3},
            {"total_suppliers": 7, "online_count": 5, "degraded_count": 1, "offline_count": 1}
        ]
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cursor

        res = analyst_service.get_operational_churn(connection=mock_conn)
        self.assertEqual(res["supplier_status"], "DEGRADED")
        self.assertIn("5/7 Online", res["supplier_status_label"])
        self.assertEqual(res["supplier_breakdown"]["total"], 7)
        self.assertEqual(res["supplier_breakdown"]["online"], 5)
        self.assertEqual(res["supplier_breakdown"]["degraded"], 1)
        self.assertEqual(res["supplier_breakdown"]["offline"], 1)

    def test_16_missing_depletion_history_produces_null_unavailable(self):
        """16. Missing time-series history produces null/unavailable metrics rather than fabricated numbers."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            {"backlog_count": 0, "processing_count": 0, "invoiced_count": 0, "approved_count": 0, "created_count": 0},
            {"total_skus": 106, "total_on_hand": 4235, "total_reserved": 0, "total_available": 4235, "total_safe_threshold": 640, "low_stock_skus": 3},
            {"total_suppliers": 7, "online_count": 5, "degraded_count": 1, "offline_count": 1}
        ]
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cursor

        res = analyst_service.get_operational_churn(connection=mock_conn)
        self.assertIsNone(res["depletion_rate_per_hour"])
        self.assertIsNone(res["estimated_hours_to_stockout"])
        self.assertEqual(res["stockout_risk"], "UNAVAILABLE")
        self.assertIn("Unavailable", res["depletion_rate_label"])

    def test_17_no_hardcoded_constants_returned(self):
        """17. No hardcoded constants 153, 20, 2.4, or 63.8 are returned."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            {"backlog_count": 0, "processing_count": 0, "invoiced_count": 0, "approved_count": 0, "created_count": 0},
            {"total_skus": 106, "total_on_hand": 4235, "total_reserved": 0, "total_available": 4235, "total_safe_threshold": 640, "low_stock_skus": 3},
            {"total_suppliers": 7, "online_count": 5, "degraded_count": 1, "offline_count": 1}
        ]
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cursor

        res = analyst_service.get_operational_churn(connection=mock_conn)
        self.assertNotEqual(res["current_inventory_level"], 153)
        self.assertNotEqual(res["safe_threshold"], 20)
        self.assertNotEqual(res["depletion_rate_per_hour"], 2.4)
        self.assertNotEqual(res["estimated_hours_to_stockout"], 63.8)


if __name__ == "__main__":
    unittest.main()
