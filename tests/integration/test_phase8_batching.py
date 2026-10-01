"""
Phase 8 Integration Test Suite: CDC Micro-Batching, Replay Deduplication & Scale (Task 8.4).
"""

import os
import sys
import unittest
import time
from unittest.mock import MagicMock, patch

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import importlib
from common.database import get_connection

batcher_mod = importlib.import_module('services.cdc-adapter.batcher')
adapter_mod = importlib.import_module('services.cdc-adapter.adapter')
dep_engine = importlib.import_module('services.dependency-engine.main')

CDCBatcher = batcher_mod.CDCBatcher
normalize_cdc_record = adapter_mod.normalize_cdc_record
process_cdc_event = adapter_mod.process_cdc_event


class TestPhase8Batching(unittest.TestCase):

    def setUp(self):
        self.conn = get_connection()
        self.cursor = self.conn.cursor()

        # Clean test records
        self.cursor.execute("DELETE FROM forecast_results WHERE execution_id BETWEEN 9600 AND 9699")
        self.cursor.execute("DELETE FROM pipeline_executions WHERE triggering_event_id BETWEEN 9600 AND 9699 OR execution_id BETWEEN 9600 AND 9699")
        self.cursor.execute("DELETE FROM pipeline_decisions WHERE decision_id BETWEEN 9600 AND 9699")
        self.cursor.execute("DELETE FROM dataset_events WHERE source_change_id LIKE 'test_batch_%' OR batch_id LIKE 'batch_orders_test_%'")

        self.cursor.execute("UPDATE dataset_metadata SET status = 'WAITING' WHERE dataset_name IN ('Orders', 'Products', 'Sellers')")
        self.cursor.execute("UPDATE pipeline_dependencies SET condition_type = 'ALL', required_count = NULL WHERE pipeline_name = 'Demand Forecast Pipeline'")
        self.conn.commit()

    def tearDown(self):
        if self.conn:
            self.cursor.execute("DELETE FROM forecast_results WHERE execution_id BETWEEN 9600 AND 9699")
            self.cursor.execute("DELETE FROM pipeline_executions WHERE triggering_event_id BETWEEN 9600 AND 9699 OR execution_id BETWEEN 9600 AND 9699")
            self.cursor.execute("DELETE FROM pipeline_decisions WHERE decision_id BETWEEN 9600 AND 9699")
            self.cursor.execute("DELETE FROM dataset_events WHERE source_change_id LIKE 'test_batch_%' OR batch_id LIKE 'batch_orders_test_%'")
            self.cursor.execute("UPDATE dataset_metadata SET status = 'WAITING' WHERE dataset_name IN ('Orders', 'Products', 'Sellers')")
            self.conn.commit()
            self.conn.close()

    def test_100_cdc_events_consolidate_to_one_batch_and_replay_deduplication(self):
        """1. 100 CDC messages produce exactly 1 dataset event, and replay produces 0 additional events."""
        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_future.get.return_value = MagicMock(partition=0, offset=42)
        mock_producer.send.return_value = mock_future

        batcher = CDCBatcher(
            max_events=100,
            max_wait_ms=5000,
            flush_callback=lambda batch, connection=None: process_cdc_event(batch, connection=connection, kafka_producer=mock_producer),
            connection=self.conn
        )

        # Get initial counts and version
        self.cursor.execute("SELECT COUNT(*) AS total FROM dataset_events")
        initial_event_count = self.cursor.fetchone()["total"]

        self.cursor.execute("SELECT current_version FROM dataset_metadata WHERE dataset_name = 'Orders'")
        initial_version = self.cursor.fetchone()["current_version"]

        # 1. Ingest 100 CDC events
        ts = int(time.time() * 1000)
        cdc_messages = []
        for i in range(100):
            raw = {
                "before": None,
                "after": {"order_id": f"batch_ord_{i}", "order_status": "invoiced"},
                "source": {
                    "file": "mysql-bin.000009",
                    "pos": 10000 + i,
                    "row": 0,
                    "table": "olist_orders",
                    "db": "supplysense"
                },
                "op": "c",
                "ts_ms": ts + i
            }
            norm = normalize_cdc_record(raw)
            cdc_messages.append(norm)

        flush_result = None
        for msg in cdc_messages:
            res = batcher.add_event(msg, connection=self.conn)
            if res:
                flush_result = res

        # Verify batch flush occurred on the 100th event
        self.assertIsNotNone(flush_result)
        self.assertEqual(flush_result["status"], "PROCESSED")
        self.assertEqual(flush_result["rows_changed"], 100)
        self.assertIsNotNone(flush_result["batch_id"])

        # Exactly 1 new row in dataset_events
        self.cursor.execute("SELECT COUNT(*) AS total FROM dataset_events")
        after_batch_count = self.cursor.fetchone()["total"]
        self.assertEqual(after_batch_count, initial_event_count + 1)

        # Verify dataset_metadata incremented version by +1 (not +100)
        self.cursor.execute("SELECT current_version, status FROM dataset_metadata WHERE dataset_name = 'Orders'")
        meta_row = self.cursor.fetchone()
        self.assertEqual(meta_row["current_version"], initial_version + 1)
        self.assertEqual(meta_row["status"], "READY")

        # Verify Kafka producer sent exactly 1 message
        mock_producer.send.assert_called_once()
        kafka_payload = mock_producer.send.call_args[1]["value"]
        self.assertEqual(kafka_payload["rows_changed"], 100)
        self.assertEqual(kafka_payload["dataset_version"], initial_version + 1)

        # 2. Downstream Dependency Evaluation (1 event -> 1 evaluation)
        self.cursor.execute("SELECT * FROM dataset_events WHERE event_id = %s", (flush_result["event_id"],))
        event_record = self.cursor.fetchone()
        dec, reason = dep_engine.process_event(event_record, self.conn)
        self.assertEqual(dec, "BLOCK") # Since Products and Sellers are WAITING

        # 3. REPLAY TEST: Replay the exact same 100 CDC messages
        mock_producer.reset_mock()
        for msg in cdc_messages:
            rep_res = batcher.add_event(msg, connection=self.conn)
            # Each replay should be identified as DUPLICATE_SKIPPED
            self.assertIsNotNone(rep_res)
            self.assertEqual(rep_res["status"], "DUPLICATE_SKIPPED")
            self.assertTrue(rep_res["is_duplicate"])

        # Flush any pending (should be empty)
        remaining = batcher.flush_all(connection=self.conn)
        self.assertEqual(remaining, [])

        # Verify ZERO additional dataset_events created on replay (+0)
        self.cursor.execute("SELECT COUNT(*) AS total FROM dataset_events")
        after_replay_count = self.cursor.fetchone()["total"]
        self.assertEqual(after_replay_count, after_batch_count)

        # Verify ZERO additional Kafka messages published on replay
        mock_producer.send.assert_not_called()

        # Verify dataset_version remained unchanged
        self.cursor.execute("SELECT current_version FROM dataset_metadata WHERE dataset_name = 'Orders'")
        self.assertEqual(self.cursor.fetchone()["current_version"], initial_version + 1)

    def test_500_cdc_events_scale_consolidation(self):
        """2. Scale Performance Test: 500 CDC events consolidate to 1 dataset event with rows_changed=500."""
        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_future.get.return_value = MagicMock(partition=0, offset=99)
        mock_producer.send.return_value = mock_future

        batcher = CDCBatcher(
            max_events=500,
            max_wait_ms=10000,
            flush_callback=lambda batch, connection=None: process_cdc_event(batch, connection=connection, kafka_producer=mock_producer),
            connection=self.conn
        )

        self.cursor.execute("SELECT current_version FROM dataset_metadata WHERE dataset_name = 'Products'")
        initial_version = self.cursor.fetchone()["current_version"]

        ts = int(time.time() * 1000)
        flush_result = None
        for i in range(500):
            raw = {
                "before": None,
                "after": {"product_id": f"scale_prod_{i}", "product_category_name": "auto"},
                "source": {
                    "file": "mysql-bin.000010",
                    "pos": 50000 + i,
                    "row": 0,
                    "table": "olist_products",
                    "db": "supplysense"
                },
                "op": "c",
                "ts_ms": ts + i
            }
            norm = normalize_cdc_record(raw)
            res = batcher.add_event(norm, connection=self.conn)
            if res:
                flush_result = res

        self.assertIsNotNone(flush_result)
        self.assertEqual(flush_result["status"], "PROCESSED")
        self.assertEqual(flush_result["rows_changed"], 500)
        self.assertEqual(flush_result["dataset_name"], "Products")
        self.assertEqual(flush_result["dataset_version"], initial_version + 1)

        # Verify exactly 1 Kafka publication
        mock_producer.send.assert_called_once()
        msg = mock_producer.send.call_args[1]["value"]
        self.assertEqual(msg["rows_changed"], 500)


if __name__ == '__main__':
    unittest.main()
