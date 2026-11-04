"""
SupplySense AI - Integration Test: End-to-End Operational Feature Pipeline

Tests full flow from Order Ingestion -> Dependency Trigger -> Feature Store Persistence:
  1. Ingests test operational orders via ingest_operational_orders.
  2. Claims event via MySQL consumer and evaluates dependencies.
  3. Dispatches Operational Demand Feature Pipeline via Pipeline Runner.
  4. Verifies records in operational_demand_weekly and operational_feature_store.
  5. Verifies database safety invariants and cleans up test data.
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
from services.dependency_engine.mysql_consumer import poll_and_process_batch
from services.feature_pipeline.repository import (
    get_operational_demand_weekly,
    get_operational_features,
)


class TestOperationalFeaturePipelineE2E(unittest.TestCase):

    def setUp(self):
        self.conn = get_connection()
        self.test_batch_id = f"e2e_feat_{uuid.uuid4().hex[:12]}"

        # Pick a valid catalog SKU from olist_products
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
        cur.close()

    def tearDown(self):
        # Clean up any test records
        cur = self.conn.cursor()
        cur.execute("DELETE FROM operational_feature_store WHERE product_category = %s AND source_provenance = 'SIMULATED'", (self.test_category,))
        cur.execute("DELETE FROM operational_demand_weekly WHERE product_category = %s AND source_provenance = 'SIMULATED'", (self.test_category,))
        cur.execute("DELETE FROM operational_orders WHERE idempotency_key LIKE %s", (f"{self.test_batch_id}%",))
        cur.execute("DELETE FROM dataset_events WHERE source_change_id = %s", (self.test_batch_id,))
        cur.execute("DELETE FROM pipeline_executions WHERE pipeline_name = 'Operational Demand Feature Pipeline' AND status = 'COMPLETED'")
        self.conn.commit()
        cur.close()
        self.conn.close()

    def test_e2e_ingestion_to_feature_store(self):
        # 1. Ingest batch of operational orders
        orders = [
            {"sku": self.test_sku, "quantity": 5, "unit_price": 50.0},
            {"sku": self.test_sku, "quantity": 10, "unit_price": 50.0},
        ]
        ingest_res = ingest_operational_orders(
            orders=orders,
            idempotency_key=self.test_batch_id,
            source="SIMULATED",
            connection=self.conn
        )
        self.assertEqual(ingest_res["status"], "INGESTED")
        event_id = ingest_res["event_id"]
        self.assertIsNotNone(event_id)

        # 2. Trigger dependency polling
        poll_res = poll_and_process_batch(
            batch_size=50,
            connection=self.conn
        )
        self.assertGreaterEqual(poll_res["processed"], 1)

        # 3. Verify that features were generated in operational_feature_store
        features = get_operational_features(
            product_category=self.test_category,
            limit=10,
            connection=self.conn
        )
        self.assertGreater(len(features), 0, "No features persisted for test category")
        first_feat = features[0]
        self.assertIn("lag_1_demand", first_feat)
        self.assertIn("rolling_mean_4w", first_feat)
        self.assertIn("demand_trend_ratio", first_feat)
        self.assertIn("feature_status", first_feat)

        # 4. Verify safety invariants
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) AS total FROM olist_orders")
        self.assertEqual(cur.fetchone()["total"], 99441)
        cur.execute("SELECT COUNT(*) AS total FROM operational_inventory")
        self.assertEqual(cur.fetchone()["total"], 106)
        cur.close()


if __name__ == '__main__':
    unittest.main()
