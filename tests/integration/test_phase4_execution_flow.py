"""
Phase 4 Integration Test Suite: Actual Pipeline Execution Lifecycle

Tests the full end-to-end flow with MySQL:
  1. BLOCK decision produces no pipeline execution.
  2. TRIGGER decision produces an execution record that transitions to COMPLETED.
  3. COMPLETED execution records output_location and completed_at.
  4. Duplicate triggering event is idempotently skipped without creating a second execution.
  5. Dependency cycle resets correctly to WAITING after TRIGGER.
"""

import os
import sys
import unittest
import importlib

# Ensure repo root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
dep_engine = importlib.import_module('services.dependency-engine.main')


class TestPhase4ExecutionIntegrationFlow(unittest.TestCase):

    def setUp(self):
        self.conn = get_connection()
        self.cursor = self.conn.cursor()

        # Clean up test events in range 9800-9899
        self.cursor.execute("DELETE FROM pipeline_executions WHERE triggering_event_id BETWEEN 9800 AND 9899")
        self._set_condition("ALL", None)
        self.cursor.execute("UPDATE dataset_metadata SET status = 'WAITING' WHERE dataset_name IN ('Orders', 'Products', 'Sellers')")
        self.conn.commit()

    def tearDown(self):
        if self.conn:
            self.cursor.execute("DELETE FROM pipeline_executions WHERE triggering_event_id BETWEEN 9800 AND 9899")
            self._set_condition("ALL", None)
            self.cursor.execute("UPDATE dataset_metadata SET status = 'WAITING' WHERE dataset_name IN ('Orders', 'Products', 'Sellers')")
            self.conn.commit()
            self.conn.close()

    def _set_condition(self, condition_type, required_count=None):
        self.cursor.execute(
            """
            UPDATE pipeline_dependencies 
            SET condition_type = %s, required_count = %s 
            WHERE pipeline_name = 'Demand Forecast Pipeline'
            """,
            (condition_type, required_count)
        )
        self.conn.commit()

    def _get_execution_record(self, event_id):
        self.cursor.execute("""
            SELECT execution_id, pipeline_name, status, started_at, completed_at, output_location, error_message
            FROM pipeline_executions
            WHERE triggering_event_id = %s
        """, (event_id,))
        return self.cursor.fetchone()

    def _get_execution_count(self, event_id):
        self.cursor.execute("SELECT COUNT(*) as count FROM pipeline_executions WHERE triggering_event_id = %s", (event_id,))
        res = self.cursor.fetchone()
        return res['count'] if isinstance(res, dict) else res[0]

    def _get_dataset_statuses(self):
        self.cursor.execute("SELECT dataset_name, status FROM dataset_metadata WHERE dataset_name IN ('Orders', 'Products', 'Sellers')")
        rows = self.cursor.fetchall()
        return {r['dataset_name']: r['status'] for r in rows}

    def test_phase4_block_does_not_execute_pipeline(self):
        """Scenario A: Incomplete dependencies -> BLOCK -> No execution record created."""
        self._set_condition("ALL", None)
        # Orders is READY, Products & Sellers are WAITING
        self.cursor.execute("UPDATE dataset_metadata SET status = 'READY' WHERE dataset_name = 'Orders'")
        self.cursor.execute("UPDATE dataset_metadata SET status = 'WAITING' WHERE dataset_name IN ('Products', 'Sellers')")
        self.conn.commit()

        event = {"event_id": 9801, "dataset_name": "Orders", "dataset_version": 1, "rows_changed": 100}
        decision, reason = dep_engine.process_event(event, self.conn)

        self.assertEqual(decision, "BLOCK")
        self.assertEqual(self._get_execution_count(9801), 0)

    def test_phase4_trigger_executes_pipeline_to_completed(self):
        """Scenario B: All dependencies READY -> TRIGGER -> Execution created & COMPLETED with output_location."""
        self._set_condition("ALL", None)
        # Orders, Products, Sellers all READY
        self.cursor.execute("UPDATE dataset_metadata SET status = 'READY' WHERE dataset_name IN ('Orders', 'Products', 'Sellers')")
        self.conn.commit()

        event = {"event_id": 9802, "dataset_name": "Sellers", "dataset_version": 2, "rows_changed": 50}
        decision, reason = dep_engine.process_event(event, self.conn)

        self.assertEqual(decision, "TRIGGER")
        self.assertEqual(self._get_execution_count(9802), 1)

        # Inspect execution record
        record = self._get_execution_record(9802)
        self.assertIsNotNone(record)
        self.assertEqual(record["pipeline_name"], "Demand Forecast Pipeline")
        self.assertEqual(record["status"], "COMPLETED")
        self.assertIsNotNone(record["started_at"])
        self.assertIsNotNone(record["completed_at"])
        self.assertIsNotNone(record["output_location"])
        self.assertTrue(any(k in record["output_location"] for k in ["model_comparison.json", "baseline_metrics.json"]))
        self.assertIsNone(record["error_message"])

        # Verify dependency cycle reset to WAITING
        statuses = self._get_dataset_statuses()
        self.assertEqual(statuses["Orders"], "WAITING")
        self.assertEqual(statuses["Products"], "WAITING")
        self.assertEqual(statuses["Sellers"], "WAITING")

    def test_phase4_idempotent_duplicate_event_handling(self):
        """Scenario C: Duplicate event does not launch duplicate execution or overwrite COMPLETED status."""
        self._set_condition("ALL", None)
        self.cursor.execute("UPDATE dataset_metadata SET status = 'READY' WHERE dataset_name IN ('Orders', 'Products', 'Sellers')")
        self.conn.commit()

        event = {"event_id": 9803, "dataset_name": "Sellers", "dataset_version": 3, "rows_changed": 50}
        
        # 1. First trigger
        decision1, _ = dep_engine.process_event(event, self.conn)
        self.assertEqual(decision1, "TRIGGER")
        self.assertEqual(self._get_execution_count(9803), 1)
        record1 = self._get_execution_record(9803)
        self.assertEqual(record1["status"], "COMPLETED")

        # 2. Duplicate trigger with same event
        decision2, reason2 = dep_engine.process_event(event, self.conn)
        self.assertEqual(decision2, "SKIPPED")
        self.assertIn("already triggered", reason2)
        self.assertEqual(self._get_execution_count(9803), 1)

        # Verify record remains unchanged
        record2 = self._get_execution_record(9803)
        self.assertEqual(record1["execution_id"], record2["execution_id"])
        self.assertEqual(record2["status"], "COMPLETED")


if __name__ == '__main__':
    unittest.main()
