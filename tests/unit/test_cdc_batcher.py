"""
Unit tests for CDC Micro-Batcher (Task 8.4).
"""

import os
import sys
import time
import unittest
from unittest.mock import MagicMock, patch

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import importlib
batcher_mod = importlib.import_module('services.cdc-adapter.batcher')
adapter_mod = importlib.import_module('services.cdc-adapter.adapter')
CDCBatcher = batcher_mod.CDCBatcher
process_cdc_event = adapter_mod.process_cdc_event


class TestCDCBatcher(unittest.TestCase):

    def setUp(self):
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value = self.mock_cursor

    def test_single_event_creates_batch_on_flush(self):
        """1. Single event creates a valid batch structure when flushed."""
        batcher = CDCBatcher(max_events=10, max_wait_ms=5000)
        event = {
            "dataset_name": "Orders",
            "table_name": "olist_orders",
            "op": "c",
            "source_file": "mysql-bin.000001",
            "source_pos": 100,
            "source_change_id": "mysql-bin.000001:100:0"
        }

        # Adding 1 event should not auto-flush since max_events=10
        res = batcher.add_event(event)
        self.assertIsNone(res)
        self.assertEqual(batcher.get_pending_count("Orders"), 1)

        # Explicit flush
        batch = batcher.flush_dataset("Orders")
        self.assertIsNotNone(batch)
        self.assertEqual(batch["dataset_name"], "Orders")
        self.assertEqual(batch["rows_changed"], 1)
        self.assertTrue(batch["is_batch"])
        self.assertIsNotNone(batch["batch_id"])
        self.assertEqual(batcher.get_pending_count("Orders"), 0)

    def test_multiple_events_consolidated_into_one_batch(self):
        """2. 100 events for the same dataset are consolidated into 1 batch with rows_changed=100."""
        batcher = CDCBatcher(max_events=100, max_wait_ms=5000)
        flushed_batch = None

        for i in range(100):
            event = {
                "dataset_name": "Orders",
                "table_name": "olist_orders",
                "op": "c",
                "source_file": "mysql-bin.000001",
                "source_pos": 100 + i,
                "source_change_id": f"mysql-bin.000001:{100 + i}:0"
            }
            res = batcher.add_event(event)
            if res:
                flushed_batch = res

        self.assertIsNotNone(flushed_batch)
        self.assertEqual(flushed_batch["dataset_name"], "Orders")
        self.assertEqual(flushed_batch["rows_changed"], 100)
        self.assertTrue(flushed_batch["is_batch"])
        self.assertEqual(batcher.get_pending_count("Orders"), 0)

    def test_different_datasets_remain_separate(self):
        """3. Events for different datasets buffer in distinct dataset queues."""
        batcher = CDCBatcher(max_events=50, max_wait_ms=5000)

        for i in range(10):
            batcher.add_event({
                "dataset_name": "Orders",
                "source_change_id": f"ord_{i}"
            })
        for i in range(5):
            batcher.add_event({
                "dataset_name": "Products",
                "source_change_id": f"prod_{i}"
            })
        for i in range(3):
            batcher.add_event({
                "dataset_name": "Sellers",
                "source_change_id": f"sell_{i}"
            })

        self.assertEqual(batcher.get_pending_count("Orders"), 10)
        self.assertEqual(batcher.get_pending_count("Products"), 5)
        self.assertEqual(batcher.get_pending_count("Sellers"), 3)
        self.assertEqual(batcher.get_pending_count(), 18)

        batches = batcher.flush_all()
        self.assertEqual(len(batches), 3)
        datasets_flushed = {b["dataset_name"]: b["rows_changed"] for b in batches}
        self.assertEqual(datasets_flushed["Orders"], 10)
        self.assertEqual(datasets_flushed["Products"], 5)
        self.assertEqual(datasets_flushed["Sellers"], 3)

    def test_max_events_threshold_triggers_flush(self):
        """4. Maximum event threshold immediately triggers a flush."""
        callback_mock = MagicMock(return_value={"status": "PROCESSED"})
        batcher = CDCBatcher(max_events=5, max_wait_ms=10000, flush_callback=callback_mock)

        for i in range(4):
            res = batcher.add_event({"dataset_name": "Orders", "source_change_id": f"id_{i}"})
            self.assertIsNone(res)

        # 5th event reaches max_events=5 -> triggers flush callback
        res_5 = batcher.add_event({"dataset_name": "Orders", "source_change_id": "id_4"})
        self.assertIsNotNone(res_5)
        callback_mock.assert_called_once()
        batch_arg = callback_mock.call_args[0][0]
        self.assertEqual(batch_arg["rows_changed"], 5)
        self.assertEqual(batch_arg["dataset_name"], "Orders")

    def test_max_wait_time_triggers_flush(self):
        """5. Maximum wait time timeout flushes pending events."""
        callback_mock = MagicMock(return_value={"status": "PROCESSED"})
        batcher = CDCBatcher(max_events=50, max_wait_ms=50, flush_callback=callback_mock)

        batcher.add_event({"dataset_name": "Orders", "source_change_id": "id_1"})
        self.assertEqual(batcher.get_pending_count("Orders"), 1)

        # Before timeout
        flushes_before = batcher.check_timeouts()
        self.assertEqual(len(flushes_before), 0)

        # Wait past max_wait_ms (50ms)
        time.sleep(0.08)
        flushes_after = batcher.check_timeouts()
        self.assertEqual(len(flushes_after), 1)
        self.assertEqual(batcher.get_pending_count("Orders"), 0)

    def test_batch_id_generated_and_unique_per_batch(self):
        """6. Every emitted batch receives a unique, traceable batch_id."""
        batcher = CDCBatcher(max_events=10)
        batcher.add_event({"dataset_name": "Orders", "source_change_id": "c1"})
        b1 = batcher.flush_dataset("Orders")

        batcher.add_event({"dataset_name": "Orders", "source_change_id": "c2"})
        b2 = batcher.flush_dataset("Orders")

        self.assertTrue(b1["batch_id"].startswith("batch_orders_"))
        self.assertTrue(b2["batch_id"].startswith("batch_orders_"))
        self.assertNotEqual(b1["batch_id"], b2["batch_id"])

    def test_duplicate_source_change_id_does_not_increase_batch_count(self):
        """7. Duplicate source_change_id is rejected and does not increase batch count."""
        batcher = CDCBatcher(max_events=10)

        # First delivery of id_1
        batcher.add_event({"dataset_name": "Orders", "source_change_id": "id_1"})
        self.assertEqual(batcher.get_pending_count("Orders"), 1)

        # Replay of id_1
        res = batcher.add_event({"dataset_name": "Orders", "source_change_id": "id_1"})
        self.assertIsNotNone(res)
        self.assertEqual(res["status"], "DUPLICATE_SKIPPED")
        self.assertTrue(res["is_duplicate"])

        # Count should remain 1
        self.assertEqual(batcher.get_pending_count("Orders"), 1)

    def test_duplicate_source_change_id_does_not_increase_rows_changed(self):
        """8. Duplicate events do not increase rows_changed when batch flushes."""
        batcher = CDCBatcher(max_events=10)
        batcher.add_event({"dataset_name": "Orders", "source_change_id": "id_1"})
        batcher.add_event({"dataset_name": "Orders", "source_change_id": "id_2"})
        # Replay id_1 and id_2
        batcher.add_event({"dataset_name": "Orders", "source_change_id": "id_1"})
        batcher.add_event({"dataset_name": "Orders", "source_change_id": "id_2"})

        batch = batcher.flush_dataset("Orders")
        self.assertEqual(batch["rows_changed"], 2)

    def test_empty_batch_does_nothing(self):
        """9. Flushing empty dataset returns None and does not emit events."""
        batcher = CDCBatcher()
        self.assertIsNone(batcher.flush_dataset("NonExistent"))
        self.assertEqual(batcher.flush_all(), [])

    def test_multiple_sequential_batches_receive_different_batch_ids(self):
        """10. Sequential flushes generate distinct batch IDs."""
        batcher = CDCBatcher()
        batcher.add_event({"dataset_name": "Orders", "source_change_id": "s1"})
        b1 = batcher.flush_dataset("Orders")

        batcher.add_event({"dataset_name": "Orders", "source_change_id": "s2"})
        b2 = batcher.flush_dataset("Orders")

        self.assertNotEqual(b1["batch_id"], b2["batch_id"])

    def test_dataset_version_increments_once_per_batch(self):
        """11. Processing a consolidated batch increments dataset version exactly once."""
        self.mock_cursor.fetchone.return_value = {
            "dataset_id": 1,
            "current_version": 20,
            "row_count": 5000,
            "status": "WAITING"
        }
        self.mock_cursor.lastrowid = 1001

        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_meta = MagicMock()
        mock_meta.partition = 0
        mock_meta.offset = 77
        mock_future.get.return_value = mock_meta
        mock_producer.send.return_value = mock_future

        batch_payload = {
            "dataset_name": "Orders",
            "table_name": "olist_orders",
            "op": "c",
            "batch_id": "batch_orders_test_001",
            "rows_changed": 100,
            "net_row_delta": 100,
            "is_batch": True,
            "source_change_id": None
        }

        res = process_cdc_event(
            batch_payload,
            connection=self.mock_conn,
            kafka_producer=mock_producer
        )

        self.assertEqual(res["status"], "PROCESSED")
        self.assertEqual(res["dataset_version"], 21) # Version incremented by +1, not +100
        self.assertEqual(res["rows_changed"], 100)
        self.assertEqual(res["batch_id"], "batch_orders_test_001")

        # Verify SQL update for metadata set version=21
        self.mock_cursor.execute.assert_any_call(
            unittest.mock.ANY,
            (21, 5100, unittest.mock.ANY, 1)
        )

        # Verify Kafka published 1 message with version 21 and rows_changed 100
        mock_producer.send.assert_called_once()
        sent_msg = mock_producer.send.call_args[1]["value"]
        self.assertEqual(sent_msg["dataset_version"], 21)
        self.assertEqual(sent_msg["rows_changed"], 100)
        self.assertEqual(sent_msg["batch_id"], "batch_orders_test_001")

    def test_non_cdc_events_continue_to_work(self):
        """12. Non-CDC events (without batch_id or source_change_id) process normally."""
        self.mock_cursor.fetchone.return_value = {
            "dataset_id": 2,
            "current_version": 5,
            "row_count": 100,
            "status": "WAITING"
        }
        self.mock_cursor.lastrowid = 1002

        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_future.get.return_value = MagicMock(partition=0, offset=1)
        mock_producer.send.return_value = mock_future

        non_cdc_payload = {
            "dataset_name": "Products",
            "table_name": "olist_products",
            "op": "c",
            "source_change_id": None,
            "batch_id": None
        }

        res = process_cdc_event(
            non_cdc_payload,
            connection=self.mock_conn,
            kafka_producer=mock_producer
        )

        self.assertEqual(res["status"], "PROCESSED")
        self.assertEqual(res["dataset_version"], 6)
        self.assertEqual(res["rows_changed"], 1)
        self.assertIsNone(res["batch_id"])


if __name__ == '__main__':
    unittest.main()
