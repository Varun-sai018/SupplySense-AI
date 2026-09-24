import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from common.database import get_connection
import importlib
dep_engine = importlib.import_module('services.dependency-engine.main')

class TestObjective1IntegrationFlow(unittest.TestCase):
    def setUp(self):
        self.conn = get_connection()
        self.cursor = self.conn.cursor()
        
        # Clean up test events from pipeline_executions
        self.cursor.execute("DELETE FROM pipeline_executions WHERE triggering_event_id BETWEEN 9900 AND 9999")
        # Ensure default condition is ALL
        self._set_condition("ALL", None)
        # Override statuses for Orders/Products/Sellers
        self.cursor.execute("UPDATE dataset_metadata SET status = 'WAITING' WHERE dataset_name IN ('Orders', 'Products', 'Sellers')")
        self.conn.commit()

    def tearDown(self):
        if self.conn:
            self.cursor.execute("DELETE FROM pipeline_executions WHERE triggering_event_id BETWEEN 9900 AND 9999")
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

    def _get_execution_count(self, event_id):
        self.cursor.execute("SELECT COUNT(*) as count FROM pipeline_executions WHERE triggering_event_id = %s", (event_id,))
        res = self.cursor.fetchone()
        return res['count'] if isinstance(res, dict) else res[0]

    def _get_decision(self):
        self.cursor.execute("SELECT decision FROM pipeline_decisions ORDER BY decision_id DESC LIMIT 1")
        res = self.cursor.fetchone()
        return res['decision'] if isinstance(res, dict) else res[0]

    def _get_dataset_statuses(self):
        self.cursor.execute("SELECT dataset_name, status FROM dataset_metadata WHERE dataset_name IN ('Orders', 'Products', 'Sellers')")
        rows = self.cursor.fetchall()
        return {r['dataset_name']: r['status'] for r in rows}

    def test_obj1_008_009_010_011_012_integration_flow(self):
        """End-to-end integration flow: 2/3 BLOCK -> 3/3 TRIGGER -> reset to WAITING -> Idempotency."""
        # 1. Start with none ready. Fire event for Orders.
        event1 = {"event_id": 9991, "dataset_name": "Orders", "dataset_version": 2, "rows_changed": 100}
        self.cursor.execute("UPDATE dataset_metadata SET status = 'READY' WHERE dataset_name = 'Orders'")
        dep_engine.process_event(event1, self.conn)
        
        # Should be BLOCK (OBJ1-009)
        self.assertEqual(self._get_decision(), "BLOCK")
        self.assertEqual(self._get_execution_count(9991), 0)

        # 2. Fire event for Products.
        event2 = {"event_id": 9992, "dataset_name": "Products", "dataset_version": 2, "rows_changed": 100}
        self.cursor.execute("UPDATE dataset_metadata SET status = 'READY' WHERE dataset_name = 'Products'")
        dep_engine.process_event(event2, self.conn)
        
        # Should be BLOCK still
        self.assertEqual(self._get_decision(), "BLOCK")
        self.assertEqual(self._get_execution_count(9992), 0)

        # 3. Fire event for Sellers. (All 3 now ready)
        event3 = {"event_id": 9993, "dataset_name": "Sellers", "dataset_version": 2, "rows_changed": 100}
        self.cursor.execute("UPDATE dataset_metadata SET status = 'READY' WHERE dataset_name = 'Sellers'")
        dep_engine.process_event(event3, self.conn)
        
        # Should be TRIGGER (OBJ1-008 & OBJ1-011 re-evaluation)
        self.assertEqual(self._get_decision(), "TRIGGER")
        self.assertEqual(self._get_execution_count(9993), 1)

        # After TRIGGER, datasets must be reset to WAITING
        statuses = self._get_dataset_statuses()
        self.assertEqual(statuses["Orders"], "WAITING")
        self.assertEqual(statuses["Products"], "WAITING")
        self.assertEqual(statuses["Sellers"], "WAITING")

        # 4. Duplicate Event (Idempotency - OBJ1-010)
        dep_engine.process_event(event3, self.conn)
        
        # Count should still be 1! (Should not execute again for same event_id)
        self.assertEqual(self._get_execution_count(9993), 1)

    def test_all_condition_lifecycle_and_reset(self):
        """ALL: 2/3 -> BLOCK, 3/3 -> TRIGGER, after TRIGGER -> reset to WAITING, new event required."""
        self._set_condition("ALL", None)

        # 2/3 Ready (Orders, Products)
        self.cursor.execute("UPDATE dataset_metadata SET status = 'READY' WHERE dataset_name IN ('Orders', 'Products')")
        self.cursor.execute("UPDATE dataset_metadata SET status = 'WAITING' WHERE dataset_name = 'Sellers'")
        self.conn.commit()

        event1 = {"event_id": 9911, "dataset_name": "Orders", "dataset_version": 3, "rows_changed": 50}
        decision, _ = dep_engine.process_event(event1, self.conn)
        self.assertEqual(decision, "BLOCK")
        self.assertEqual(self._get_execution_count(9911), 0)

        # 3/3 Ready (Sellers becomes READY)
        self.cursor.execute("UPDATE dataset_metadata SET status = 'READY' WHERE dataset_name = 'Sellers'")
        self.conn.commit()

        event2 = {"event_id": 9912, "dataset_name": "Sellers", "dataset_version": 3, "rows_changed": 50}
        decision, _ = dep_engine.process_event(event2, self.conn)
        self.assertEqual(decision, "TRIGGER")
        self.assertEqual(self._get_execution_count(9912), 1)

        # Verify datasets reset to WAITING after execution record created
        statuses = self._get_dataset_statuses()
        self.assertEqual(statuses["Orders"], "WAITING")
        self.assertEqual(statuses["Products"], "WAITING")
        self.assertEqual(statuses["Sellers"], "WAITING")

        # Next cycle: New event for Orders marks only Orders READY (1/3) -> BLOCK
        self.cursor.execute("UPDATE dataset_metadata SET status = 'READY' WHERE dataset_name = 'Orders'")
        self.conn.commit()

        event3 = {"event_id": 9913, "dataset_name": "Orders", "dataset_version": 4, "rows_changed": 50}
        decision, _ = dep_engine.process_event(event3, self.conn)
        self.assertEqual(decision, "BLOCK")
        self.assertEqual(self._get_execution_count(9913), 0)

    def test_any_condition_lifecycle_and_reset(self):
        """ANY: 0/3 -> BLOCK, 1/3 -> TRIGGER, after TRIGGER -> reset to WAITING."""
        self._set_condition("ANY", None)

        # 0/3 Ready
        event1 = {"event_id": 9921, "dataset_name": "Orders", "dataset_version": 1, "rows_changed": 10}
        decision, _ = dep_engine.process_event(event1, self.conn)
        self.assertEqual(decision, "BLOCK")
        self.assertEqual(self._get_execution_count(9921), 0)

        # 1/3 Ready (Orders becomes READY)
        self.cursor.execute("UPDATE dataset_metadata SET status = 'READY' WHERE dataset_name = 'Orders'")
        self.conn.commit()

        event2 = {"event_id": 9922, "dataset_name": "Orders", "dataset_version": 2, "rows_changed": 20}
        decision, _ = dep_engine.process_event(event2, self.conn)
        self.assertEqual(decision, "TRIGGER")
        self.assertEqual(self._get_execution_count(9922), 1)

        # Verify datasets reset to WAITING
        statuses = self._get_dataset_statuses()
        self.assertEqual(statuses["Orders"], "WAITING")
        self.assertEqual(statuses["Products"], "WAITING")
        self.assertEqual(statuses["Sellers"], "WAITING")

        # Next cycle: with all WAITING, another event without READY status -> BLOCK
        event3 = {"event_id": 9923, "dataset_name": "Products", "dataset_version": 2, "rows_changed": 20}
        decision, _ = dep_engine.process_event(event3, self.conn)
        self.assertEqual(decision, "BLOCK")
        self.assertEqual(self._get_execution_count(9923), 0)

    def test_quorum_condition_lifecycle_and_reset(self):
        """QUORUM (quorum 2): 1/3 -> BLOCK, 2/3 -> TRIGGER, after TRIGGER -> reset to WAITING."""
        self._set_condition("QUORUM", 2)

        # 1/3 Ready (Orders only)
        self.cursor.execute("UPDATE dataset_metadata SET status = 'READY' WHERE dataset_name = 'Orders'")
        self.conn.commit()

        event1 = {"event_id": 9931, "dataset_name": "Orders", "dataset_version": 1, "rows_changed": 15}
        decision, _ = dep_engine.process_event(event1, self.conn)
        self.assertEqual(decision, "BLOCK")
        self.assertEqual(self._get_execution_count(9931), 0)

        # 2/3 Ready (Products also becomes READY)
        self.cursor.execute("UPDATE dataset_metadata SET status = 'READY' WHERE dataset_name = 'Products'")
        self.conn.commit()

        event2 = {"event_id": 9932, "dataset_name": "Products", "dataset_version": 1, "rows_changed": 25}
        decision, _ = dep_engine.process_event(event2, self.conn)
        self.assertEqual(decision, "TRIGGER")
        self.assertEqual(self._get_execution_count(9932), 1)

        # Verify datasets reset to WAITING
        statuses = self._get_dataset_statuses()
        self.assertEqual(statuses["Orders"], "WAITING")
        self.assertEqual(statuses["Products"], "WAITING")
        self.assertEqual(statuses["Sellers"], "WAITING")

        # Next cycle: 1/3 becomes READY -> BLOCK (below quorum 2)
        self.cursor.execute("UPDATE dataset_metadata SET status = 'READY' WHERE dataset_name = 'Sellers'")
        self.conn.commit()

        event3 = {"event_id": 9933, "dataset_name": "Sellers", "dataset_version": 1, "rows_changed": 5}
        decision, _ = dep_engine.process_event(event3, self.conn)
        self.assertEqual(decision, "BLOCK")
        self.assertEqual(self._get_execution_count(9933), 0)

    def test_idempotency_duplicate_event(self):
        """Duplicate triggering event must not create duplicate execution records."""
        self._set_condition("ALL", None)

        self.cursor.execute("UPDATE dataset_metadata SET status = 'READY' WHERE dataset_name IN ('Orders', 'Products', 'Sellers')")
        self.conn.commit()

        event = {"event_id": 9941, "dataset_name": "Sellers", "dataset_version": 5, "rows_changed": 100}
        decision, _ = dep_engine.process_event(event, self.conn)
        self.assertEqual(decision, "TRIGGER")
        self.assertEqual(self._get_execution_count(9941), 1)

        # Re-send same event
        decision2, reason2 = dep_engine.process_event(event, self.conn)
        self.assertEqual(decision2, "SKIPPED")
        self.assertIn("already triggered", reason2)
        self.assertEqual(self._get_execution_count(9941), 1)

if __name__ == '__main__':
    unittest.main()
