"""
Unit tests for Debezium CDC Adapter (Phase 7).

Tests CDC payload normalization, operation handling (insert, update, delete),
table-to-dataset mapping, and event publishing logic.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch
import json

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import importlib
cdc_adapter_mod = importlib.import_module('services.cdc-adapter.adapter')
register_conn_mod = importlib.import_module('services.cdc-adapter.register_connector')

normalize_cdc_record = cdc_adapter_mod.normalize_cdc_record
process_cdc_event = cdc_adapter_mod.process_cdc_event
TABLE_TO_DATASET_MAP = cdc_adapter_mod.TABLE_TO_DATASET_MAP
OP_CREATE = cdc_adapter_mod.OP_CREATE
OP_UPDATE = cdc_adapter_mod.OP_UPDATE
OP_DELETE = cdc_adapter_mod.OP_DELETE
get_connector_payload = register_conn_mod.get_connector_payload


class TestCDCAdapter(unittest.TestCase):

    def setUp(self):
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value = self.mock_cursor

    def test_table_to_dataset_mapping(self):
        """1. Verify expected table-to-dataset mapping dictionary."""
        self.assertEqual(TABLE_TO_DATASET_MAP["olist_orders"], "Orders")
        self.assertEqual(TABLE_TO_DATASET_MAP["olist_products"], "Products")
        self.assertEqual(TABLE_TO_DATASET_MAP["olist_sellers"], "Sellers")
        self.assertEqual(TABLE_TO_DATASET_MAP["olist_order_items"], "Order Items")

    def test_normalize_cdc_record_insert(self):
        """2. Normalize standard Debezium insert event (op: 'c')."""
        payload = {
            "before": None,
            "after": {
                "order_id": "ord_12345",
                "customer_id": "cust_999",
                "order_status": "delivered"
            },
            "source": {
                "version": "2.7.3.Final",
                "connector": "mysql",
                "name": "supplysense_cdc",
                "ts_ms": 1727581000000,
                "db": "supplysense",
                "table": "olist_orders"
            },
            "op": OP_CREATE,
            "ts_ms": 1727581000500
        }

        result = normalize_cdc_record(payload)
        self.assertIsNotNone(result)
        self.assertEqual(result["table_name"], "olist_orders")
        self.assertEqual(result["dataset_name"], "Orders")
        self.assertEqual(result["op"], "c")
        self.assertEqual(result["after"]["order_id"], "ord_12345")
        self.assertIsNone(result["before"])

    def test_normalize_cdc_record_update(self):
        """3. Normalize Debezium update event (op: 'u')."""
        payload = {
            "before": {"product_id": "prod_1", "product_category_name": "auto"},
            "after": {"product_id": "prod_1", "product_category_name": "automotivo"},
            "source": {"table": "olist_products", "db": "supplysense"},
            "op": OP_UPDATE,
            "ts_ms": 1727582000000
        }

        result = normalize_cdc_record(payload)
        self.assertIsNotNone(result)
        self.assertEqual(result["table_name"], "olist_products")
        self.assertEqual(result["dataset_name"], "Products")
        self.assertEqual(result["op"], "u")
        self.assertEqual(result["after"]["product_category_name"], "automotivo")
        self.assertEqual(result["before"]["product_category_name"], "auto")

    def test_normalize_cdc_record_delete(self):
        """4. Normalize Debezium delete event (op: 'd')."""
        payload = {
            "before": {"seller_id": "sell_99"},
            "after": None,
            "source": {"table": "olist_sellers"},
            "op": OP_DELETE
        }

        result = normalize_cdc_record(payload)
        self.assertIsNotNone(result)
        self.assertEqual(result["table_name"], "olist_sellers")
        self.assertEqual(result["dataset_name"], "Sellers")
        self.assertEqual(result["op"], "d")
        self.assertIsNone(result["after"])

    def test_normalize_cdc_record_fallback_topic_parsing(self):
        """5. Normalize event when table name is parsed from topic name."""
        payload = {
            "after": {"order_item_id": 1},
            "op": "c"
        }
        topic = "supplysense_cdc.supplysense.olist_order_items"

        result = normalize_cdc_record(payload, topic=topic)
        self.assertIsNotNone(result)
        self.assertEqual(result["table_name"], "olist_order_items")
        self.assertEqual(result["dataset_name"], "Order Items")

    def test_normalize_cdc_record_unmapped_table_returns_none(self):
        """6. Unmapped tables return None."""
        payload = {"source": {"table": "unknown_table"}, "op": "c"}
        self.assertIsNone(normalize_cdc_record(payload))

    def test_normalize_cdc_record_invalid_input(self):
        """7. Malformed string or non-dict input returns None."""
        self.assertIsNone(normalize_cdc_record("invalid-json-string"))
        self.assertIsNone(normalize_cdc_record(12345))

    def test_process_cdc_event_success_flow(self):
        """8. process_cdc_event updates metadata, inserts event, and publishes to Kafka."""
        self.mock_cursor.fetchone.return_value = {
            "dataset_id": 1,
            "current_version": 10,
            "row_count": 1000,
            "status": "WAITING"
        }
        self.mock_cursor.lastrowid = 888

        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_meta = MagicMock()
        mock_meta.partition = 0
        mock_meta.offset = 42
        mock_future.get.return_value = mock_meta
        mock_producer.send.return_value = mock_future

        normalized = {
            "table_name": "olist_orders",
            "dataset_name": "Orders",
            "op": "c",
            "after": {"order_id": "1"},
            "before": None
        }

        res = process_cdc_event(
            normalized_cdc=normalized,
            connection=self.mock_conn,
            kafka_producer=mock_producer
        )

        self.assertEqual(res["status"], "PROCESSED")
        self.assertEqual(res["event_id"], 888)
        self.assertEqual(res["dataset_version"], 11)
        self.assertTrue(res["published_to_kafka"])

        # Check metadata update to READY
        self.mock_cursor.execute.assert_any_call(
            unittest.mock.ANY,
            (11, 1001, unittest.mock.ANY, 1)
        )
        # Check Kafka send
        mock_producer.send.assert_called_once()
        sent_topic = mock_producer.send.call_args[0][0]
        sent_value = mock_producer.send.call_args[1]["value"]
        self.assertEqual(sent_topic, "dataset-events")
        self.assertEqual(sent_value["dataset_name"], "Orders")
        self.assertEqual(sent_value["dataset_version"], 11)
        self.assertEqual(sent_value["source"], "DEBEZIUM_CDC")

    def test_process_cdc_event_db_error_triggers_rollback(self):
        """9. Database exception inside process_cdc_event triggers connection.rollback()."""
        self.mock_cursor.execute.side_effect = Exception("DB Connection Lost")

        normalized = {
            "table_name": "olist_products",
            "dataset_name": "Products",
            "op": "c"
        }

        with self.assertRaises(Exception):
            process_cdc_event(normalized, connection=self.mock_conn)
        self.mock_conn.rollback.assert_called_once()

    def test_get_connector_payload_structure(self):
        """10. Verify connector config payload generation."""
        payload = get_connector_payload()
        self.assertEqual(payload["name"], "supplysense-mysql-cdc")
        config = payload["config"]
        self.assertEqual(config["connector.class"], "io.debezium.connector.mysql.MySqlConnector")
        self.assertEqual(config["topic.prefix"], "supplysense_cdc")
        self.assertIn("olist_orders", config["table.include.list"])

    def test_wait_for_pipeline_execution_success_after_running(self):
        """11. Verify wait_for_pipeline_execution transitions from RUNNING to COMPLETED."""
        demo_script = importlib.import_module('scripts.demo_phase7_cdc')
        wait_for_pipeline_execution = demo_script.wait_for_pipeline_execution

        # First returns RUNNING, second returns COMPLETED
        self.mock_cursor.fetchone.side_effect = [
            {"execution_id": 999, "status": "RUNNING", "pipeline_name": "Demand Forecast Pipeline"},
            {"execution_id": 999, "status": "COMPLETED", "pipeline_name": "Demand Forecast Pipeline", "output_location": "data/processed/forecasts.csv"}
        ]

        with patch('time.sleep', return_value=None):
            result = wait_for_pipeline_execution(triggering_event_id=123, conn=self.mock_conn, timeout=10, poll_interval=1)
            self.assertEqual(result["status"], "COMPLETED")
            self.assertEqual(result["execution_id"], 999)

    def test_wait_for_pipeline_execution_failed_raises_runtime_error(self):
        """12. Verify wait_for_pipeline_execution raises RuntimeError on FAILED status."""
        demo_script = importlib.import_module('scripts.demo_phase7_cdc')
        wait_for_pipeline_execution = demo_script.wait_for_pipeline_execution

        self.mock_cursor.fetchone.return_value = {
            "execution_id": 999,
            "status": "FAILED",
            "error_message": "XGBoost training convergence error"
        }

        with self.assertRaises(RuntimeError) as ctx:
            wait_for_pipeline_execution(triggering_event_id=123, conn=self.mock_conn, timeout=10, poll_interval=1)
        self.assertIn("FAILED: XGBoost training convergence error", str(ctx.exception))

    def test_wait_for_pipeline_execution_timeout_raises_timeout_error(self):
        """13. Verify wait_for_pipeline_execution raises TimeoutError when timeout expires."""
        demo_script = importlib.import_module('scripts.demo_phase7_cdc')
        wait_for_pipeline_execution = demo_script.wait_for_pipeline_execution

        self.mock_cursor.fetchone.return_value = {
            "execution_id": 999,
            "status": "RUNNING"
        }

        with patch('time.time', side_effect=[0, 0, 15, 20]):
            with patch('time.sleep', return_value=None):
                with self.assertRaises(TimeoutError) as ctx:
                    wait_for_pipeline_execution(triggering_event_id=123, conn=self.mock_conn, timeout=10, poll_interval=1)
                self.assertIn("Timed out after 10s", str(ctx.exception))

    # --- Phase 8 Task 8.3 Deduplication & Idempotency Tests ---

    def test_generate_source_change_id_gtid(self):
        """14. generate_source_change_id prioritizes GTID when available."""
        generate_source_change_id = cdc_adapter_mod.generate_source_change_id
        source = {"gtid": "3a0b1c2d-0001:105", "file": "mysql-bin.000001", "pos": 100, "row": 2}
        change_id = generate_source_change_id(source)
        self.assertEqual(change_id, "gtid:3a0b1c2d-0001:105:2")

    def test_generate_source_change_id_file_pos_row(self):
        """15. generate_source_change_id constructs standard file:pos:row format."""
        generate_source_change_id = cdc_adapter_mod.generate_source_change_id
        source = {"file": "mysql-bin.000005", "pos": 54321, "row": 0}
        change_id = generate_source_change_id(source)
        self.assertEqual(change_id, "mysql-bin.000005:54321:0")

    def test_generate_source_change_id_deterministic_replay(self):
        """16. Exactly the same Debezium event generates the exact same source_change_id."""
        generate_source_change_id = cdc_adapter_mod.generate_source_change_id
        payload = {
            "source": {"file": "mysql-bin.000002", "pos": 9999, "row": 1, "table": "olist_orders"}
        }
        id1 = generate_source_change_id(payload["source"])
        id2 = generate_source_change_id(payload["source"])
        self.assertEqual(id1, id2)

    def test_generate_source_change_id_different_coords(self):
        """17. Different source coordinates produce distinct identifiers."""
        generate_source_change_id = cdc_adapter_mod.generate_source_change_id
        s1 = {"file": "mysql-bin.000001", "pos": 100, "row": 0}
        s2 = {"file": "mysql-bin.000001", "pos": 101, "row": 0}
        s3 = {"file": "mysql-bin.000002", "pos": 100, "row": 0}
        self.assertNotEqual(generate_source_change_id(s1), generate_source_change_id(s2))
        self.assertNotEqual(generate_source_change_id(s1), generate_source_change_id(s3))

    def test_generate_source_change_id_fallback(self):
        """18. Fallback to table:ts_ms:server_id when binlog coordinates are absent."""
        generate_source_change_id = cdc_adapter_mod.generate_source_change_id
        source = {"table": "olist_orders", "ts_ms": 1727589000000, "server_id": 1}
        change_id = generate_source_change_id(source, table_name="olist_orders")
        self.assertEqual(change_id, "olist_orders:1727589000000:1")

    def test_process_cdc_event_duplicate_skipped(self):
        """19. process_cdc_event detects duplicate source_change_id and skips without Kafka publish."""
        mock_producer = MagicMock()
        # Mock finding existing event on deduplication query
        self.mock_cursor.fetchone.return_value = {
            "event_id": 777,
            "dataset_id": 1,
            "dataset_name": "Orders",
            "dataset_version": 5,
            "event_status": "PUBLISHED"
        }

        normalized = {
            "table_name": "olist_orders",
            "dataset_name": "Orders",
            "op": "c",
            "source_file": "mysql-bin.000001",
            "source_pos": 100,
            "source_change_id": "mysql-bin.000001:100:0"
        }

        result = process_cdc_event(
            normalized_cdc=normalized,
            connection=self.mock_conn,
            kafka_producer=mock_producer
        )

        self.assertEqual(result["status"], "DUPLICATE_SKIPPED")
        self.assertTrue(result["is_duplicate"])
        self.assertEqual(result["event_id"], 777)
        self.assertFalse(result["published_to_kafka"])
        mock_producer.send.assert_not_called()

    def test_process_cdc_event_race_condition_integrity_error_handled(self):
        """20. process_cdc_event handles database race condition duplicate via IntegrityError safely."""
        mock_producer = MagicMock()
        # Step 0 check returns None (no existing record found yet)
        # Step 1 dataset metadata returns valid dataset
        # Step 2 insert raises pymysql.err.IntegrityError (concurrent insert)
        # Step 3 post-rollback select returns the race winner record
        import pymysql
        self.mock_cursor.fetchone.side_effect = [
            None, # Deduplication check
            {"dataset_id": 1, "current_version": 10, "row_count": 500, "status": "READY"}, # metadata
            {"event_id": 999, "dataset_id": 1, "dataset_name": "Orders", "dataset_version": 11} # dup fetch
        ]
        self.mock_cursor.execute.side_effect = [
            None, # Dedup select
            None, # Metadata select
            pymysql.err.IntegrityError(1062, "Duplicate entry 'mysql-bin.000001:100:0' for key 'idx_unique_source_change'"),
            None  # Fetch dup
        ]

        normalized = {
            "table_name": "olist_orders",
            "dataset_name": "Orders",
            "op": "c",
            "source_file": "mysql-bin.000001",
            "source_pos": 100,
            "source_change_id": "mysql-bin.000001:100:0"
        }

        result = process_cdc_event(
            normalized_cdc=normalized,
            connection=self.mock_conn,
            kafka_producer=mock_producer
        )

        self.assertEqual(result["status"], "DUPLICATE_SKIPPED")
        self.assertTrue(result["is_duplicate"])
        self.assertEqual(result["event_id"], 999)
        self.assertFalse(result["published_to_kafka"])
        self.mock_conn.rollback.assert_called()


if __name__ == '__main__':
    unittest.main()

