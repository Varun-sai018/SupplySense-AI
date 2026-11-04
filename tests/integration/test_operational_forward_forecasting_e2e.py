"""
SupplySense AI - Integration Test: End-to-End Operational Forward Demand Forecasting

Tests full chain: Order Ingestion -> Feature Generation -> Forward XGBoost Inference:
  1. Ingests batch of operational orders.
  2. Runs Operational Feature Pipeline to populate operational_feature_store.
  3. Executes Operational Demand Forecast Pipeline via Pipeline Runner.
  4. Verifies 7-day and 14-day forward predictions in operational_forecast_results.
  5. Verifies safety invariants and cleans up test data.
"""

import os
import sys
import unittest
import uuid
import datetime

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from services.observability.repository import ingest_operational_orders
from services.feature_pipeline.pipeline import run_operational_feature_pipeline
from services.forecasting.pipeline import run_operational_forecast_pipeline
from services.forecasting.repository import get_operational_forecasts


class TestOperationalForwardForecastingE2E(unittest.TestCase):

    def setUp(self):
        self.conn = get_connection()
        self.test_batch_id = f"e2e_fc_{uuid.uuid4().hex[:12]}"

        # Pick a valid catalog SKU
        cur = self.conn.cursor()
        cur.execute("""
            SELECT product_id, product_category_name
            FROM olist_products
            WHERE product_category_name IS NOT NULL AND product_category_name != ''
            LIMIT 1
        """)
        row = cur.fetchone()
        self.test_sku = row["product_id"]
        self.test_category = row["product_category_name"]

        # Create prerequisite decision and execution records to satisfy foreign keys
        cur.execute("""
            INSERT INTO pipeline_decisions (
                pipeline_name, condition_type, decision, reason, triggered_at
            ) VALUES (
                'Operational Demand Feature Pipeline', 'ALL', 'TRIGGER', 'E2E Test Trigger', NOW()
            )
        """)
        self.dec_id = cur.lastrowid

        cur.execute("""
            INSERT INTO pipeline_executions (
                execution_id, pipeline_name, decision_id, triggering_event_id, status
            ) VALUES (
                88801, 'Operational Demand Feature Pipeline', %s, 99901, 'RUNNING'
            )
            ON DUPLICATE KEY UPDATE status = 'RUNNING'
        """, (self.dec_id,))

        cur.execute("""
            INSERT INTO pipeline_executions (
                execution_id, pipeline_name, decision_id, triggering_event_id, status
            ) VALUES (
                88802, 'Operational Demand Forecast Pipeline', %s, 99902, 'RUNNING'
            )
            ON DUPLICATE KEY UPDATE status = 'RUNNING'
        """, (self.dec_id,))

        self.conn.commit()
        cur.close()

    def tearDown(self):
        cur = self.conn.cursor()
        cur.execute("DELETE FROM operational_forecast_results WHERE product_category = %s", (self.test_category,))
        cur.execute("DELETE FROM operational_feature_store WHERE product_category = %s", (self.test_category,))
        cur.execute("DELETE FROM operational_demand_weekly WHERE product_category = %s", (self.test_category,))
        cur.execute("DELETE FROM operational_orders WHERE idempotency_key LIKE %s", (f"{self.test_batch_id}%",))
        cur.execute("DELETE FROM dataset_events WHERE source_change_id = %s", (self.test_batch_id,))
        cur.execute("DELETE FROM pipeline_executions WHERE execution_id IN (88801, 88802)")
        cur.execute("DELETE FROM pipeline_decisions WHERE decision_id = %s", (self.dec_id,))
        self.conn.commit()
        cur.close()
        self.conn.close()

    def test_e2e_forward_forecasting_flow(self):
        # 1. Ingest operational orders
        orders = [
            {"sku": self.test_sku, "quantity": 12, "unit_price": 40.0},
            {"sku": self.test_sku, "quantity": 18, "unit_price": 40.0},
        ]
        ingest_res = ingest_operational_orders(
            orders=orders,
            idempotency_key=self.test_batch_id,
            source="SIMULATED",
            connection=self.conn
        )
        self.assertEqual(ingest_res["status"], "INGESTED")

        # 2. Run Feature Pipeline
        feat_res = run_operational_feature_pipeline(
            execution_id=88801,
            pipeline_name="Operational Demand Feature Pipeline",
            connection=self.conn
        )
        self.assertEqual(feat_res["status"], "COMPLETED")

        # 3. Run Forward Forecast Pipeline
        fc_res = run_operational_forecast_pipeline(
            execution_id=88802,
            pipeline_name="Operational Demand Forecast Pipeline",
            connection=self.conn,
            horizons=["7_DAYS", "14_DAYS"]
        )
        self.assertEqual(fc_res["status"], "COMPLETED")
        self.assertGreater(fc_res["predictions_count"], 0)

        # 4. Verify predictions in operational_forecast_results table
        forecasts = get_operational_forecasts(
            product_category=self.test_category,
            execution_id=88802,
            connection=self.conn
        )
        self.assertEqual(len(forecasts), 2)
        h_types = {f["forecast_horizon"] for f in forecasts}
        self.assertEqual(h_types, {"7_DAYS", "14_DAYS"})

        for f in forecasts:
            self.assertEqual(f["data_provenance"], "OPERATIONAL")
            self.assertEqual(f["model_name"], "XGBoost")
            self.assertEqual(f["model_version"], "xgboost-v1")
            self.assertGreaterEqual(float(f["predicted_demand"]), 0.0)

        # 5. Verify database safety invariants
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) AS total FROM olist_orders")
        self.assertEqual(cur.fetchone()["total"], 99441)
        cur.execute("SELECT COUNT(*) AS total FROM operational_inventory")
        self.assertEqual(cur.fetchone()["total"], 106)
        cur.close()


if __name__ == '__main__':
    unittest.main()
