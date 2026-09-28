"""
Phase 6 Integration Test Suite: Downstream Forecast Results Persistence in MySQL.

Tests:
  1. Live MySQL persistence of forecast predictions linked by execution_id to pipeline_executions.
  2. Query helpers: get_forecasts_by_execution_id, get_forecasts_by_category, get_forecasts_by_week, get_recent_forecasts, get_forecast_summary_by_execution.
  3. Idempotency: Duplicate insertions update existing records without constraint violations.
  4. Foreign key constraint & CASCADE delete: Deleting a pipeline_execution deletes related forecast_results.
  5. End-to-end dependency trigger execution flow: TRIGGER -> Pipeline execution -> Forecast results persisted in MySQL.
"""

import os
import sys
import unittest
import datetime
import pandas as pd
import importlib

# Ensure repo root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection

forecast_repo = importlib.import_module('services.pipeline-runner.forecast_repository')
save_forecast_results = forecast_repo.save_forecast_results
get_forecasts_by_execution_id = forecast_repo.get_forecasts_by_execution_id
get_forecasts_by_category = forecast_repo.get_forecasts_by_category
get_forecasts_by_week = forecast_repo.get_forecasts_by_week
get_recent_forecasts = forecast_repo.get_recent_forecasts
get_forecast_summary_by_execution = forecast_repo.get_forecast_summary_by_execution

dep_engine = importlib.import_module('services.dependency-engine.main')


class TestPhase6ForecastPersistence(unittest.TestCase):

    def setUp(self):
        self.conn = get_connection()
        self.cursor = self.conn.cursor()

        # Clean up test records in range 9700-9799
        self.cursor.execute("DELETE FROM forecast_results WHERE execution_id BETWEEN 9700 AND 9799")
        self.cursor.execute("DELETE FROM pipeline_executions WHERE triggering_event_id BETWEEN 9700 AND 9799 OR execution_id BETWEEN 9700 AND 9799")
        self.cursor.execute("DELETE FROM pipeline_decisions WHERE decision_id BETWEEN 9700 AND 9799")
        self.cursor.execute("DELETE FROM dataset_events WHERE event_id BETWEEN 9700 AND 9799")
        
        # Reset dataset status
        self.cursor.execute("UPDATE dataset_metadata SET status = 'WAITING' WHERE dataset_name IN ('Orders', 'Products', 'Sellers')")
        self.cursor.execute("UPDATE pipeline_dependencies SET condition_type = 'ALL', required_count = NULL WHERE pipeline_name = 'Demand Forecast Pipeline'")
        self.conn.commit()

        # Seed a valid parent decision and execution record for direct repository tests
        self.cursor.execute("""
            INSERT INTO pipeline_decisions (decision_id, pipeline_name, condition_type, decision, ready_count, total_required, reason)
            VALUES (9701, 'Demand Forecast Pipeline', 'ALL', 'TRIGGER', 3, 3, 'Test seed')
            ON DUPLICATE KEY UPDATE decision = VALUES(decision)
        """)
        self.cursor.execute("""
            INSERT INTO pipeline_executions (execution_id, pipeline_name, decision_id, triggering_event_id, status)
            VALUES (9701, 'Demand Forecast Pipeline', 9701, 9701, 'RUNNING')
            ON DUPLICATE KEY UPDATE status = VALUES(status)
        """)
        self.conn.commit()

    def tearDown(self):
        if self.conn:
            self.cursor.execute("DELETE FROM forecast_results WHERE execution_id BETWEEN 9700 AND 9799")
            self.cursor.execute("DELETE FROM pipeline_executions WHERE triggering_event_id BETWEEN 9700 AND 9799 OR execution_id BETWEEN 9700 AND 9799")
            self.cursor.execute("DELETE FROM pipeline_decisions WHERE decision_id BETWEEN 9700 AND 9799")
            self.cursor.execute("DELETE FROM dataset_events WHERE event_id BETWEEN 9700 AND 9799")
            self.cursor.execute("UPDATE dataset_metadata SET status = 'WAITING' WHERE dataset_name IN ('Orders', 'Products', 'Sellers')")
            self.conn.commit()
            self.conn.close()

    def test_save_and_query_forecast_results_in_mysql(self):
        """1. Insert forecast rows into MySQL and verify query functions."""
        sample_df = pd.DataFrame([
            {"product_category": "bed_bath_table", "week_start_date": "2018-06-04", "predicted_demand": 24.50},
            {"product_category": "health_beauty", "week_start_date": "2018-06-04", "predicted_demand": 31.75},
            {"product_category": "bed_bath_table", "week_start_date": "2018-06-11", "predicted_demand": 26.10},
        ])

        saved = save_forecast_results(
            execution_id=9701,
            pipeline_name="Demand Forecast Pipeline",
            predictions=sample_df,
            model_name="XGBoost",
            model_version="xgboost-v1",
            connection=self.conn
        )
        self.assertEqual(saved, 3)

        # 1. Query by execution_id
        exec_forecasts = get_forecasts_by_execution_id(9701, connection=self.conn)
        self.assertEqual(len(exec_forecasts), 3)
        self.assertEqual(exec_forecasts[0]["product_category"], "bed_bath_table")
        self.assertEqual(float(exec_forecasts[0]["predicted_demand"]), 24.50)
        self.assertEqual(exec_forecasts[0]["model_name"], "XGBoost")
        self.assertEqual(exec_forecasts[0]["model_version"], "xgboost-v1")

        # 2. Query by category
        cat_forecasts = get_forecasts_by_category("bed_bath_table", limit=10, connection=self.conn)
        self.assertGreaterEqual(len(cat_forecasts), 2)

        # 3. Query by week
        week_forecasts = get_forecasts_by_week("2018-06-04", limit=10, connection=self.conn)
        self.assertGreaterEqual(len(week_forecasts), 2)

        # 4. Query summary
        summary = get_forecast_summary_by_execution(9701, connection=self.conn)
        self.assertEqual(summary["total_forecasts"], 3)
        self.assertEqual(summary["distinct_categories"], 2)
        self.assertEqual(summary["model_name"], "XGBoost")
        self.assertEqual(summary["model_version"], "xgboost-v1")

    def test_forecast_results_idempotency_upsert(self):
        """2. Saving forecast rows with updated predictions updates records idempotently."""
        initial_df = pd.DataFrame([
            {"product_category": "sports_leisure", "week_start_date": "2018-07-02", "predicted_demand": 15.00}
        ])
        save_forecast_results(9701, "Demand Forecast Pipeline", initial_df, connection=self.conn)

        # Update with new predicted demand for same execution, category, and week
        updated_df = pd.DataFrame([
            {"product_category": "sports_leisure", "week_start_date": "2018-07-02", "predicted_demand": 19.75}
        ])
        save_forecast_results(9701, "Demand Forecast Pipeline", updated_df, connection=self.conn)

        forecasts = get_forecasts_by_execution_id(9701, connection=self.conn)
        # Must only have 1 row for sports_leisure, not duplicate
        sports_rows = [f for f in forecasts if f["product_category"] == "sports_leisure"]
        self.assertEqual(len(sports_rows), 1)
        self.assertEqual(float(sports_rows[0]["predicted_demand"]), 19.75)

    def test_forecast_results_foreign_key_cascade_delete(self):
        """3. Deleting a pipeline_execution cascades and deletes its forecast_results."""
        df = pd.DataFrame([
            {"product_category": "electronics", "week_start_date": "2018-06-18", "predicted_demand": 42.00}
        ])
        save_forecast_results(9701, "Demand Forecast Pipeline", df, connection=self.conn)

        # Verify row exists
        self.cursor.execute("SELECT COUNT(*) AS count FROM forecast_results WHERE execution_id = 9701")
        self.assertGreater(self.cursor.fetchone()["count"], 0)

        # Delete parent pipeline_executions record
        self.cursor.execute("DELETE FROM pipeline_executions WHERE execution_id = 9701")
        self.conn.commit()

        # Verify forecast_results rows were cascade-deleted
        self.cursor.execute("SELECT COUNT(*) AS count FROM forecast_results WHERE execution_id = 9701")
        self.assertEqual(self.cursor.fetchone()["count"], 0)

    def test_e2e_trigger_flow_persists_forecast_results(self):
        """4. Full event flow: Trigger -> Pipeline Execution -> Forecasts Persisted in MySQL."""
        # Set all datasets to READY
        self.cursor.execute("UPDATE dataset_metadata SET status = 'READY' WHERE dataset_name IN ('Orders', 'Products', 'Sellers')")
        self.conn.commit()

        # Dispatch triggering event
        event = {"event_id": 9702, "dataset_name": "Sellers", "dataset_version": 1, "rows_changed": 50}
        decision, reason = dep_engine.process_event(event, self.conn)

        self.assertEqual(decision, "TRIGGER")

        # Find the created execution
        self.cursor.execute("SELECT execution_id, status, output_location FROM pipeline_executions WHERE triggering_event_id = 9702")
        exec_record = self.cursor.fetchone()
        self.assertIsNotNone(exec_record)
        self.assertEqual(exec_record["status"], "COMPLETED")
        self.assertIn(".json", exec_record["output_location"])

        execution_id = exec_record["execution_id"]

        # Verify forecast_results rows exist for this execution
        forecasts = get_forecasts_by_execution_id(execution_id, connection=self.conn)
        self.assertGreater(len(forecasts), 0)

        # Check sample forecast values
        sample = forecasts[0]
        self.assertEqual(sample["execution_id"], execution_id)
        self.assertEqual(sample["pipeline_name"], "Demand Forecast Pipeline")
        self.assertEqual(sample["model_name"], "XGBoost")
        self.assertEqual(sample["model_version"], "xgboost-v1")
        self.assertIsNotNone(sample["forecast_week"])
        self.assertIsNotNone(sample["predicted_demand"])


if __name__ == '__main__':
    unittest.main()
