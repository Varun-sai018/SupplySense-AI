"""
Unit tests for Forecast Repository (Phase 6).

Tests forecast prediction batch insertion, query helpers, error handling,
and data normalization without requiring a live database connection.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
import datetime

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import importlib
forecast_repo = importlib.import_module('services.pipeline-runner.forecast_repository')

save_forecast_results = forecast_repo.save_forecast_results
get_forecasts_by_execution_id = forecast_repo.get_forecasts_by_execution_id
get_forecasts_by_category = forecast_repo.get_forecasts_by_category
get_forecasts_by_week = forecast_repo.get_forecasts_by_week
get_recent_forecasts = forecast_repo.get_recent_forecasts
get_forecast_summary_by_execution = forecast_repo.get_forecast_summary_by_execution


class TestForecastRepository(unittest.TestCase):

    def setUp(self):
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value = self.mock_cursor

    def test_save_forecast_results_from_dataframe(self):
        """1. Saving DataFrame predictions performs batch insert with correct arguments."""
        df = pd.DataFrame([
            {"product_category": "bed_bath_table", "week_start_date": "2018-06-04", "predicted_demand": 25.4},
            {"product_category": "health_beauty", "week_start_date": "2018-06-04", "predicted_demand": 38.1},
        ])

        count = save_forecast_results(
            execution_id=10,
            pipeline_name="Demand Forecast Pipeline",
            predictions=df,
            model_name="XGBoost",
            model_version="xgboost-v1",
            connection=self.mock_conn
        )

        self.assertEqual(count, 2)
        self.mock_cursor.executemany.assert_called_once()
        args = self.mock_cursor.executemany.call_args[0]
        query = args[0]
        records = args[1]

        self.assertIn("INSERT INTO forecast_results", query)
        self.assertIn("ON DUPLICATE KEY UPDATE", query)
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0], (10, "Demand Forecast Pipeline", "bed_bath_table", "2018-06-04", 25.4, "XGBoost", "xgboost-v1"))
        self.mock_conn.commit.assert_called_once()

    def test_save_forecast_results_from_list_of_dicts_with_alias_columns(self):
        """2. Saving list of dicts with alias keys normalizes columns and saves properly."""
        rows = [
            {"category": "sports_leisure", "forecast_week": "2018-07-02", "forecast": 12.5},
            {"category": "electronics", "forecast_week": "2018-07-02", "forecast": 45.0},
        ]

        count = save_forecast_results(
            execution_id=11,
            pipeline_name="Demand Forecast Pipeline",
            predictions=rows,
            connection=self.mock_conn
        )

        self.assertEqual(count, 2)
        self.mock_cursor.executemany.assert_called_once()
        records = self.mock_cursor.executemany.call_args[0][1]
        self.assertEqual(records[0][2], "sports_leisure")
        self.assertEqual(records[0][3], "2018-07-02")
        self.assertEqual(records[0][4], 12.5)

    def test_save_forecast_results_empty_returns_zero(self):
        """3. Empty DataFrame or list returns 0 without calling database cursor."""
        count_df = save_forecast_results(12, "Test Pipeline", pd.DataFrame(), connection=self.mock_conn)
        count_list = save_forecast_results(12, "Test Pipeline", [], connection=self.mock_conn)

        self.assertEqual(count_df, 0)
        self.assertEqual(count_list, 0)
        self.mock_cursor.executemany.assert_not_called()

    def test_save_forecast_results_missing_required_columns_raises_error(self):
        """4. Missing required column raises ValueError."""
        invalid_df = pd.DataFrame([{"product_category": "cat1", "predicted_demand": 10.0}])

        with self.assertRaises(ValueError) as ctx:
            save_forecast_results(13, "Demand Forecast Pipeline", invalid_df, connection=self.mock_conn)
        self.assertIn("forecast_week", str(ctx.exception))

    def test_save_forecast_results_nonexistent_file_raises_filenotfound(self):
        """5. Nonexistent CSV path string raises FileNotFoundError."""
        with self.assertRaises(FileNotFoundError):
            save_forecast_results(14, "Demand Forecast Pipeline", "nonexistent/preds.csv", connection=self.mock_conn)

    def test_save_forecast_results_db_error_triggers_rollback(self):
        """6. Database execution exception triggers rollback and re-raises exception."""
        df = pd.DataFrame([{"product_category": "cat1", "forecast_week": "2018-06-04", "predicted_demand": 10.0}])
        self.mock_cursor.executemany.side_effect = Exception("MySQL connection timed out")

        with self.assertRaises(Exception) as ctx:
            save_forecast_results(15, "Demand Forecast Pipeline", df, connection=self.mock_conn)
        self.assertIn("MySQL connection timed out", str(ctx.exception))
        self.mock_conn.rollback.assert_called_once()

    def test_get_forecasts_by_execution_id(self):
        """7. get_forecasts_by_execution_id executes expected SELECT query."""
        mock_records = [{"forecast_id": 1, "product_category": "auto", "predicted_demand": 15.0}]
        self.mock_cursor.fetchall.return_value = mock_records

        res = get_forecasts_by_execution_id(101, connection=self.mock_conn)
        self.assertEqual(res, mock_records)
        self.mock_cursor.execute.assert_called_once()
        self.assertIn("WHERE execution_id = %s", self.mock_cursor.execute.call_args[0][0])
        self.assertEqual(self.mock_cursor.execute.call_args[0][1], (101,))

    def test_get_forecasts_by_category(self):
        """8. get_forecasts_by_category queries by category with limit."""
        mock_records = [{"forecast_id": 2, "product_category": "watches_gifts"}]
        self.mock_cursor.fetchall.return_value = mock_records

        res = get_forecasts_by_category("watches_gifts", limit=20, connection=self.mock_conn)
        self.assertEqual(res, mock_records)
        self.assertIn("WHERE product_category = %s", self.mock_cursor.execute.call_args[0][0])
        self.assertEqual(self.mock_cursor.execute.call_args[0][1], ("watches_gifts", 20))

    def test_get_forecasts_by_week(self):
        """9. get_forecasts_by_week formats date and queries by forecast_week."""
        mock_records = [{"forecast_id": 3, "forecast_week": "2018-06-11"}]
        self.mock_cursor.fetchall.return_value = mock_records

        d = datetime.date(2018, 6, 11)
        res = get_forecasts_by_week(d, limit=30, connection=self.mock_conn)
        self.assertEqual(res, mock_records)
        self.assertIn("WHERE forecast_week = %s", self.mock_cursor.execute.call_args[0][0])
        self.assertEqual(self.mock_cursor.execute.call_args[0][1], ("2018-06-11", 30))

    def test_get_recent_forecasts(self):
        """10. get_recent_forecasts queries recent records ordered by created_at DESC."""
        mock_records = [{"forecast_id": 4}]
        self.mock_cursor.fetchall.return_value = mock_records

        res = get_recent_forecasts(limit=15, connection=self.mock_conn)
        self.assertEqual(res, mock_records)
        self.assertIn("ORDER BY created_at DESC", self.mock_cursor.execute.call_args[0][0])
        self.assertEqual(self.mock_cursor.execute.call_args[0][1], (15,))

    def test_get_forecast_summary_by_execution(self):
        """11. get_forecast_summary_by_execution returns aggregate dictionary."""
        mock_summary = {
            "total_forecasts": 770,
            "distinct_categories": 70,
            "min_week": datetime.date(2018, 5, 28),
            "max_week": datetime.date(2018, 8, 27),
            "avg_predicted_demand": 14.52,
            "model_name": "XGBoost",
            "model_version": "xgboost-v1"
        }
        self.mock_cursor.fetchone.return_value = mock_summary

        res = get_forecast_summary_by_execution(101, connection=self.mock_conn)
        self.assertEqual(res["total_forecasts"], 770)
        self.assertEqual(res["distinct_categories"], 70)
        self.assertEqual(res["model_name"], "XGBoost")


if __name__ == '__main__':
    unittest.main()
