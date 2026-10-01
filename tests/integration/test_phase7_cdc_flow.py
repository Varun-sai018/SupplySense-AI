"""
Phase 7 Integration Test Suite: Debezium CDC Pipeline & Change Event Routing.

Tests:
  1. Debezium MySQL Connector registration and live health check.
  2. End-to-end CDC event normalization, database metadata updates, and Kafka routing.
  3. Change-triggered downstream Dependency Engine orchestration and execution flow.
"""

import os
import sys
import unittest
import json
import time
import importlib

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from config import settings

cdc_adapter_mod = importlib.import_module('services.cdc-adapter.adapter')
register_conn_mod = importlib.import_module('services.cdc-adapter.register_connector')
dep_engine = importlib.import_module('services.dependency-engine.main')

normalize_cdc_record = cdc_adapter_mod.normalize_cdc_record
process_cdc_event = cdc_adapter_mod.process_cdc_event
register_or_update_connector = register_conn_mod.register_or_update_connector
get_connector_status = register_conn_mod.get_connector_status
wait_for_connector_running = register_conn_mod.wait_for_connector_running


class TestPhase7CDCFlow(unittest.TestCase):

    def setUp(self):
        self.conn = get_connection()
        self.cursor = self.conn.cursor()

        # Clean test records in range 9600-9699
        self.cursor.execute("DELETE FROM forecast_results WHERE execution_id BETWEEN 9600 AND 9699")
        self.cursor.execute("DELETE FROM pipeline_executions WHERE triggering_event_id BETWEEN 9600 AND 9699 OR execution_id BETWEEN 9600 AND 9699")
        self.cursor.execute("DELETE FROM pipeline_decisions WHERE decision_id BETWEEN 9600 AND 9699")
        self.cursor.execute("DELETE FROM dataset_events WHERE event_id BETWEEN 9600 AND 9699")
        
        # Reset dependency datasets to WAITING
        self.cursor.execute("UPDATE dataset_metadata SET status = 'WAITING' WHERE dataset_name IN ('Orders', 'Products', 'Sellers')")
        self.cursor.execute("UPDATE pipeline_dependencies SET condition_type = 'ALL', required_count = NULL WHERE pipeline_name = 'Demand Forecast Pipeline'")
        self.conn.commit()

    def tearDown(self):
        if self.conn:
            self.cursor.execute("DELETE FROM forecast_results WHERE execution_id BETWEEN 9600 AND 9699")
            self.cursor.execute("DELETE FROM pipeline_executions WHERE triggering_event_id BETWEEN 9600 AND 9699 OR execution_id BETWEEN 9600 AND 9699")
            self.cursor.execute("DELETE FROM pipeline_decisions WHERE decision_id BETWEEN 9600 AND 9699")
            self.cursor.execute("DELETE FROM dataset_events WHERE event_id BETWEEN 9600 AND 9699")
            self.cursor.execute("UPDATE dataset_metadata SET status = 'WAITING' WHERE dataset_name IN ('Orders', 'Products', 'Sellers')")
            self.conn.commit()
            self.conn.close()

    def test_debezium_connector_registration_and_health(self):
        """1. Register Debezium MySQL connector and verify RUNNING state."""
        res = register_or_update_connector()
        self.assertIsNotNone(res)

        is_running = wait_for_connector_running(timeout=20)
        self.assertTrue(is_running, "Debezium connector failed to transition to RUNNING.")

        status = get_connector_status()
        self.assertEqual(status.get("connector", {}).get("state"), "RUNNING")
        tasks = status.get("tasks", [])
        self.assertGreater(len(tasks), 0)
        self.assertEqual(tasks[0].get("state"), "RUNNING")

    def test_cdc_event_processing_and_metadata_ready(self):
        """2. Process CDC event: metadata updates to READY and event is published."""
        sample_cdc_payload = {
            "before": None,
            "after": {
                "order_id": "test_cdc_ord_001",
                "customer_id": "test_cust_001",
                "order_status": "invoiced"
            },
            "source": {
                "version": "2.7.3.Final",
                "connector": "mysql",
                "name": "supplysense_cdc",
                "ts_ms": int(time.time() * 1000),
                "db": "supplysense",
                "table": "olist_orders"
            },
            "op": "c",
            "ts_ms": int(time.time() * 1000)
        }

        normalized = normalize_cdc_record(sample_cdc_payload)
        self.assertIsNotNone(normalized)
        self.assertEqual(normalized["dataset_name"], "Orders")

        result = process_cdc_event(normalized, connection=self.conn)
        self.assertEqual(result["status"], "PROCESSED")
        self.assertEqual(result["dataset_name"], "Orders")
        self.assertTrue(result["published_to_kafka"])
        event_id = result["event_id"]

        # Verify dataset_metadata status is READY
        self.cursor.execute("SELECT status, current_version FROM dataset_metadata WHERE dataset_name = 'Orders'")
        meta = self.cursor.fetchone()
        self.assertEqual(meta["status"], "READY")
        self.assertEqual(meta["current_version"], result["dataset_version"])

        # Verify dataset_events status is PUBLISHED
        self.cursor.execute("SELECT event_status, dataset_name FROM dataset_events WHERE event_id = %s", (event_id,))
        event_row = self.cursor.fetchone()
        self.assertIsNotNone(event_row)
        self.assertEqual(event_row["event_status"], "PUBLISHED")
        self.assertEqual(event_row["dataset_name"], "Orders")

    def test_cdc_events_trigger_dependency_and_pipeline_execution(self):
        """3. Process 3 CDC events (Orders, Products, Sellers) -> Trigger downstream forecast pipeline."""
        # 1. Orders CDC event
        cdc_orders = normalize_cdc_record({
            "after": {"order_id": "ord_cdc_1"},
            "source": {"table": "olist_orders"},
            "op": "c"
        })
        res_orders = process_cdc_event(cdc_orders, connection=self.conn)
        self.cursor.execute("SELECT * FROM dataset_events WHERE event_id = %s", (res_orders["event_id"],))
        event_orders = self.cursor.fetchone()
        dec_1, reason_1 = dep_engine.process_event(event_orders, self.conn)
        self.assertEqual(dec_1, "BLOCK")

        # 2. Products CDC event
        cdc_products = normalize_cdc_record({
            "after": {"product_id": "prod_cdc_1"},
            "source": {"table": "olist_products"},
            "op": "c"
        })
        res_products = process_cdc_event(cdc_products, connection=self.conn)
        self.cursor.execute("SELECT * FROM dataset_events WHERE event_id = %s", (res_products["event_id"],))
        event_products = self.cursor.fetchone()
        dec_2, reason_2 = dep_engine.process_event(event_products, self.conn)
        self.assertEqual(dec_2, "BLOCK")

        # 3. Sellers CDC event -> satisfies ALL condition -> TRIGGER
        cdc_sellers = normalize_cdc_record({
            "after": {"seller_id": "sell_cdc_1"},
            "source": {"table": "olist_sellers"},
            "op": "c"
        })
        res_sellers = process_cdc_event(cdc_sellers, connection=self.conn)
        self.cursor.execute("SELECT * FROM dataset_events WHERE event_id = %s", (res_sellers["event_id"],))
        event_sellers = self.cursor.fetchone()
        dec_3, reason_3 = dep_engine.process_event(event_sellers, self.conn)
        self.assertEqual(dec_3, "TRIGGER")

        # Verify pipeline execution was created and COMPLETED
        self.cursor.execute("""
            SELECT execution_id, pipeline_name, status, output_location
            FROM pipeline_executions
            WHERE triggering_event_id = %s
        """, (event_sellers["event_id"],))
        exec_record = self.cursor.fetchone()
        self.assertIsNotNone(exec_record)
        self.assertEqual(exec_record["status"], "COMPLETED")
        self.assertIsNotNone(exec_record["output_location"])

        # Verify forecast_results table populated
        self.cursor.execute("""
            SELECT COUNT(*) AS count
            FROM forecast_results
            WHERE execution_id = %s
        """, (exec_record["execution_id"],))
        forecast_count = self.cursor.fetchone()["count"]
        self.assertGreater(forecast_count, 0)


if __name__ == '__main__':
    unittest.main()
