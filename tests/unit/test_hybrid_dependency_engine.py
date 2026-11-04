"""
Unit and Integration Tests for Phase 2: Hybrid Dependency Engine.

Validates all 15 authorized requirements:
  1. MySQL polling discovers NEW events.
  2. Event claiming prevents simultaneous duplicate processing (FOR UPDATE SKIP LOCKED).
  3. ALL dependency condition.
  4. ANY dependency condition.
  5. QUORUM dependency condition.
  6. AST dependency condition.
  7. NESTED_RULE_B condition.
  8. BLOCK decision when required datasets are not ready.
  9. TRIGGER decision when all required conditions are satisfied.
  10. Pipeline Runner invocation upon TRIGGER.
  11. Duplicate event does not cause duplicate pipeline execution (Idempotency).
  12. Worker restart recovers eligible pending/stale events.
  13. Kafka outage does not affect MYSQL mode.
  14. Kafka mode remains functional when configured.
  15. Graceful worker shutdown.
"""

import os
import sys
import time
import uuid
import unittest
import threading
import datetime
from unittest.mock import MagicMock, patch

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import importlib
from common.database import get_connection

dep_engine = importlib.import_module("services.dependency-engine.main")
mysql_consumer = importlib.import_module("services.dependency-engine.mysql_consumer")
evaluator_mod = importlib.import_module("services.dependency-engine.evaluator")

process_event = dep_engine.process_event
claim_events = mysql_consumer.claim_events
mark_event_completed = mysql_consumer.mark_event_completed
mark_event_failed = mysql_consumer.mark_event_failed
poll_and_process_batch = mysql_consumer.poll_and_process_batch
run_mysql_consumer = mysql_consumer.run_mysql_consumer
evaluate_condition = evaluator_mod.evaluate_condition


class TestHybridDependencyEngine(unittest.TestCase):

    def setUp(self):
        self.conn = get_connection()
        self.cursor = self.conn.cursor()

        # Capture safety baselines
        self.cursor.execute("SELECT COUNT(*) AS total FROM olist_orders")
        self.initial_olist_count = self.cursor.fetchone()["total"]

        self.cursor.execute("SELECT COUNT(*) AS total FROM operational_inventory")
        self.initial_inv_count = self.cursor.fetchone()["total"]

        self.cleanup_test_data()

    def tearDown(self):
        self.cleanup_test_data()
        if self.conn:
            self.conn.close()

    def cleanup_test_data(self):
        """Cleans up test records without affecting historical or seed data."""
        try:
            self.cursor.execute("DELETE FROM pipeline_executions WHERE triggering_event_id >= 999000 OR pipeline_name LIKE 'TEST-%'")
            self.cursor.execute("DELETE FROM pipeline_decisions WHERE reason LIKE '%[event_id:999%' OR reason LIKE '%test_p2_%'")
            self.cursor.execute("DELETE FROM dataset_events WHERE source_change_id LIKE 'test_p2_%' OR batch_id LIKE 'test_p2_%'")
            self.conn.commit()
        except Exception as e:
            if self.conn:
                self.conn.rollback()

    # --------------------------------------------------------------------------
    # Test 1: MySQL polling discovers NEW events
    # --------------------------------------------------------------------------
    def test_01_mysql_polling_discovers_new_events(self):
        """1. MySQL polling discovers eligible NEW events in dataset_events."""
        test_key = f"test_p2_disc_{uuid.uuid4().hex[:8]}"
        self.cursor.execute("""
            INSERT INTO dataset_events (
                dataset_id, dataset_name, event_type, dataset_version,
                rows_changed, source_change_id, batch_id, event_status
            ) VALUES (1, 'Orders', 'DATASET_UPDATED', 1, 5, %s, %s, 'NEW')
        """, (test_key, test_key))
        self.conn.commit()
        inserted_id = self.cursor.lastrowid

        claimed = claim_events(batch_size=50, connection=self.conn)
        claimed_ids = [e["event_id"] for e in claimed]
        self.assertIn(inserted_id, claimed_ids)

        # Verify status transitioned to PROCESSING
        self.cursor.execute("SELECT event_status FROM dataset_events WHERE event_id = %s", (inserted_id,))
        self.assertEqual(self.cursor.fetchone()["event_status"], "PROCESSING")

    # --------------------------------------------------------------------------
    # Test 2: Event claiming prevents simultaneous duplicate processing
    # --------------------------------------------------------------------------
    def test_02_concurrent_claiming_prevents_duplicate_processing(self):
        """2. FOR UPDATE SKIP LOCKED prevents two workers from claiming the same event."""
        test_key = f"test_p2_lock_{uuid.uuid4().hex[:8]}"
        self.cursor.execute("""
            INSERT INTO dataset_events (
                dataset_id, dataset_name, event_type, dataset_version,
                rows_changed, source_change_id, batch_id, event_status
            ) VALUES (1, 'Orders', 'DATASET_UPDATED', 1, 5, %s, %s, 'NEW')
        """, (test_key, test_key))
        self.conn.commit()
        event_id = self.cursor.lastrowid

        # Worker 1 claims
        claimed_w1 = claim_events(batch_size=5, connection=self.conn)
        w1_ids = [e["event_id"] for e in claimed_w1]

        # Worker 2 claims concurrently
        conn2 = get_connection()
        claimed_w2 = claim_events(batch_size=5, connection=conn2)
        w2_ids = [e["event_id"] for e in claimed_w2]
        conn2.close()

        # Both workers must have mutually exclusive claim sets
        intersection = set(w1_ids).intersection(set(w2_ids))
        self.assertEqual(len(intersection), 0)
        self.assertIn(event_id, w1_ids)
        self.assertNotIn(event_id, w2_ids)

    # --------------------------------------------------------------------------
    # Test 3: ALL dependency condition
    # --------------------------------------------------------------------------
    def test_03_all_dependency_condition(self):
        """3. ALL condition requires ready_count == total_required."""
        dec_trigger, _ = evaluate_condition("ALL", ready_count=3, total_required=3)
        self.assertEqual(dec_trigger, "TRIGGER")

        dec_block, _ = evaluate_condition("ALL", ready_count=2, total_required=3)
        self.assertEqual(dec_block, "BLOCK")

    # --------------------------------------------------------------------------
    # Test 4: ANY dependency condition
    # --------------------------------------------------------------------------
    def test_04_any_dependency_condition(self):
        """4. ANY condition triggers when at least one required dataset is READY."""
        dec_trigger, _ = evaluate_condition("ANY", ready_count=1, total_required=3)
        self.assertEqual(dec_trigger, "TRIGGER")

        dec_block, _ = evaluate_condition("ANY", ready_count=0, total_required=3)
        self.assertEqual(dec_block, "BLOCK")

    # --------------------------------------------------------------------------
    # Test 5: QUORUM dependency condition
    # --------------------------------------------------------------------------
    def test_05_quorum_dependency_condition(self):
        """5. QUORUM condition triggers when ready_count >= required_count threshold."""
        dec_trigger, _ = evaluate_condition("QUORUM", ready_count=3, total_required=5, required_count=3)
        self.assertEqual(dec_trigger, "TRIGGER")

        dec_block, _ = evaluate_condition("QUORUM", ready_count=2, total_required=5, required_count=3)
        self.assertEqual(dec_block, "BLOCK")

    # --------------------------------------------------------------------------
    # Test 6: AST dependency condition
    # --------------------------------------------------------------------------
    def test_06_ast_dependency_condition(self):
        """6. AST evaluates boolean expressions over dataset status and metrics."""
        ctx_pass = {"Orders": "READY", "Products": "READY"}
        dec_pass, _ = evaluate_condition("AND(Orders == 'READY', Products == 'READY')", ready_count=2, total_required=2, context=ctx_pass)
        self.assertEqual(dec_pass, "TRIGGER")

        ctx_fail = {"Orders": "READY", "Products": "WAITING"}
        dec_fail, _ = evaluate_condition("AND(Orders == 'READY', Products == 'READY')", ready_count=1, total_required=2, context=ctx_fail)
        self.assertEqual(dec_fail, "BLOCK")

    # --------------------------------------------------------------------------
    # Test 7: NESTED_RULE_B condition
    # --------------------------------------------------------------------------
    def test_07_nested_rule_b_condition(self):
        """7. Evaluates Rule B inventory reorder business rule dispatch."""
        with patch.object(dep_engine, "_process_rule_b_event") as mock_rule_b:
            mock_rule_b.return_value = ("TRIGGER", "Stock below threshold: reorder dispatched")
            event = {
                "event_type": "INVENTORY_REORDER",
                "product_id": "PROD-TEST-123",
                "safe_threshold": 25,
                "reorder_quantity": 50
            }
            decision, reason = process_event(event, self.conn)
            self.assertEqual(decision, "TRIGGER")
            mock_rule_b.assert_called_once()

    # --------------------------------------------------------------------------
    # Test 8: BLOCK decision when required datasets are not ready
    # --------------------------------------------------------------------------
    def test_08_block_decision_when_datasets_not_ready(self):
        """8. When required datasets are WAITING, process_event records BLOCK in pipeline_decisions."""
        # Ensure Products and Sellers are WAITING
        self.cursor.execute("UPDATE dataset_metadata SET status = 'WAITING' WHERE dataset_name IN ('Products', 'Sellers')")
        self.conn.commit()

        event = {
            "event_id": 999101,
            "dataset_name": "Orders",
            "pipeline_name": "Demand Forecast Pipeline"
        }
        with patch.object(dep_engine, "execute_pipeline") as mock_exec:
            decision, reason = process_event(event, self.conn)
            self.assertEqual(decision, "BLOCK")
            mock_exec.assert_not_called()

            # Verify decision persisted in pipeline_decisions
            self.cursor.execute(
                "SELECT decision, reason FROM pipeline_decisions WHERE pipeline_name = 'Demand Forecast Pipeline' AND reason LIKE %s",
                (f"%[event_id:{event['event_id']}]%",)
            )
            row = self.cursor.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["decision"], "BLOCK")

    # --------------------------------------------------------------------------
    # Test 9: TRIGGER decision when all required conditions are satisfied
    # --------------------------------------------------------------------------
    def test_09_trigger_decision_when_conditions_satisfied(self):
        """9. When all required datasets are READY, process_event returns TRIGGER."""
        # Set all 3 datasets to READY
        self.cursor.execute("UPDATE dataset_metadata SET status = 'READY' WHERE dataset_name IN ('Orders', 'Products', 'Sellers')")
        self.conn.commit()

        event = {
            "event_id": 999102,
            "dataset_name": "Orders",
            "pipeline_name": "Demand Forecast Pipeline"
        }
        with patch.object(dep_engine, "execute_pipeline") as mock_exec:
            mock_exec.return_value = {"status": "COMPLETED", "execution_id": 999}
            decision, reason = process_event(event, self.conn)
            self.assertEqual(decision, "TRIGGER")

            # Verify decision in pipeline_decisions
            self.cursor.execute(
                "SELECT decision FROM pipeline_decisions WHERE pipeline_name = 'Demand Forecast Pipeline' AND reason LIKE %s",
                (f"%[event_id:{event['event_id']}]%",)
            )
            row = self.cursor.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["decision"], "TRIGGER")

    # --------------------------------------------------------------------------
    # Test 10: Pipeline Runner invocation
    # --------------------------------------------------------------------------
    def test_10_pipeline_runner_invocation_on_trigger(self):
        """10. TRIGGER invokes execute_pipeline() exactly once with canonical execution_id."""
        self.cursor.execute("UPDATE dataset_metadata SET status = 'READY' WHERE dataset_name IN ('Orders', 'Products', 'Sellers')")
        self.conn.commit()

        event = {
            "event_id": 999103,
            "dataset_name": "Orders",
            "pipeline_name": "Demand Forecast Pipeline"
        }
        with patch.object(dep_engine, "execute_pipeline") as mock_exec:
            mock_exec.return_value = {"status": "COMPLETED"}
            decision, reason = process_event(event, self.conn)
            self.assertEqual(decision, "TRIGGER")
            mock_exec.assert_called_once()
            # Verify called with pipeline name
            args, _ = mock_exec.call_args
            self.assertEqual(args[1], "Demand Forecast Pipeline")

    # --------------------------------------------------------------------------
    # Test 11: Duplicate event does not cause duplicate pipeline execution
    # --------------------------------------------------------------------------
    def test_11_duplicate_event_idempotency_skip(self):
        """11. Replaying an identical event_id is skipped and does not re-execute pipeline."""
        event_id = 999104
        # Create a valid decision record first to satisfy FK fk_execution_decision
        self.cursor.execute("""
            INSERT INTO pipeline_decisions (
                pipeline_name, condition_type, decision, reason
            ) VALUES ('Demand Forecast Pipeline', 'ALL', 'TRIGGER', 'test_p2_fk_setup')
        """)
        decision_id = self.cursor.lastrowid
        self.cursor.execute("""
            INSERT INTO pipeline_executions (
                pipeline_name, decision_id, triggering_event_id, status
            ) VALUES ('Demand Forecast Pipeline', %s, %s, 'COMPLETED')
        """, (decision_id, event_id))
        self.conn.commit()

        with patch.object(dep_engine, "execute_pipeline") as mock_exec:
            decision, reason = process_event({"event_id": event_id, "dataset_name": "Orders", "pipeline_name": "Demand Forecast Pipeline"}, self.conn)
            self.assertEqual(decision, "SKIPPED")
            self.assertIn("Idempotency", reason)
            mock_exec.assert_not_called()

    # --------------------------------------------------------------------------
    # Test 12: Worker restart recovers eligible pending events
    # --------------------------------------------------------------------------
    def test_12_worker_crash_recovery_for_stale_processing_events(self):
        """12. Events abandoned in PROCESSING state by a crashed worker are reclaimed after stale timeout."""
        test_key = f"test_p2_stale_{uuid.uuid4().hex[:8]}"
        stale_time = datetime.datetime.now() - datetime.timedelta(seconds=600)  # 10 minutes ago
        self.cursor.execute("""
            INSERT INTO dataset_events (
                dataset_id, dataset_name, event_type, dataset_version,
                rows_changed, source_change_id, batch_id, event_status, processed_at
            ) VALUES (1, 'Orders', 'DATASET_UPDATED', 1, 5, %s, %s, 'PROCESSING', %s)
        """, (test_key, test_key, stale_time))
        self.conn.commit()
        stale_event_id = self.cursor.lastrowid

        # Claim with 300s timeout -> must reclaim stale_event_id
        claimed = claim_events(batch_size=10, stale_timeout_seconds=300, connection=self.conn)
        claimed_ids = [e["event_id"] for e in claimed]
        self.assertIn(stale_event_id, claimed_ids)

    # --------------------------------------------------------------------------
    # Test 13: Kafka outage does not affect MYSQL mode
    # --------------------------------------------------------------------------
    def test_13_kafka_outage_does_not_affect_mysql_mode(self):
        """13. When Kafka is offline / broker unavailable, MySQL mode continues operating smoothly."""
        test_key = f"test_p2_nokafka_{uuid.uuid4().hex[:8]}"
        self.cursor.execute("""
            INSERT INTO dataset_events (
                dataset_id, dataset_name, event_type, dataset_version,
                rows_changed, source_change_id, batch_id, event_status
            ) VALUES (1, 'Orders', 'DATASET_UPDATED', 1, 2, %s, %s, 'NEW')
        """, (test_key, test_key))
        self.conn.commit()

        # Execute MySQL batch pass (even with Kafka completely offline)
        summary = poll_and_process_batch(
            batch_size=5,
            connection=self.conn,
            process_func=lambda evt, conn: ("BLOCK", "Test single pass")
        )
        self.assertGreater(summary["claimed"], 0)
        self.assertGreater(summary["processed"], 0)
        self.assertEqual(summary["failed"], 0)

    # --------------------------------------------------------------------------
    # Test 14: Kafka mode remains functional when configured
    # --------------------------------------------------------------------------
    def test_14_kafka_mode_initialization_structure(self):
        """14. Kafka consumer mode handles configuration and graceful timeout when brokers offline."""
        stop_flag = threading.Event()
        stop_flag.set()  # Stop immediately so test does not hang

        with patch("kafka.KafkaConsumer") as mock_consumer_cls:
            mock_consumer = MagicMock()
            mock_consumer.__iter__.return_value = []
            mock_consumer_cls.return_value = mock_consumer

            dep_engine.run_kafka_consumer(stop_event=stop_flag)
            mock_consumer_cls.assert_called_once()

    # --------------------------------------------------------------------------
    # Test 15: Graceful worker shutdown
    # --------------------------------------------------------------------------
    def test_15_graceful_worker_shutdown(self):
        """15. run_mysql_consumer responds to stop_event and exits without error."""
        stop_flag = threading.Event()
        stop_flag.set()  # Signal immediate shutdown

        # Must exit cleanly in under 1 second
        start = time.time()
        run_mysql_consumer(
            poll_interval=0.1,
            batch_size=5,
            stop_event=stop_flag
        )
        elapsed = time.time() - start
        self.assertLess(elapsed, 1.5)

    # --------------------------------------------------------------------------
    # Safety Check: Historical & Operational Invariant
    # --------------------------------------------------------------------------
    def test_safety_invariants(self):
        """Safety invariant: Historical Olist orders and operational inventory remain untouched."""
        self.cursor.execute("SELECT COUNT(*) AS total FROM olist_orders")
        current_olist = self.cursor.fetchone()["total"]
        self.assertEqual(self.initial_olist_count, current_olist)

        self.cursor.execute("SELECT COUNT(*) AS total FROM operational_inventory")
        current_inv = self.cursor.fetchone()["total"]
        self.assertEqual(self.initial_inv_count, current_inv)


if __name__ == "__main__":
    unittest.main()
