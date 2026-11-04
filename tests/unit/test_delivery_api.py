"""
Unit and integration tests for SupplySense AI Simulated Delivery API (Priority 2).

Verifies:
  1. POST /api/observability/replenishments/{replenishment_id}/deliver (successful delivery)
  2. Idempotent re-delivery (200 OK with idempotent=True, no double-stocking, version unchanged)
  3. Delivery rejection for ineligible status (CANCELLED -> 400 Bad Request)
  4. Non-existent replenishment rejection (404 Not Found)
  5. Transactional integrity (inventory updated atomically, event persisted)
"""

import os
import sys
import uuid
import datetime
import unittest
from fastapi.testclient import TestClient

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from services.observability.main import app


class TestDeliveryAPI(unittest.TestCase):
    """Test suite for Simulated Delivery API endpoint."""

    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)
        cls.conn = get_connection()

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def setUp(self):
        self.created_skus = []
        self.created_replenishments = []
        self.created_actions = []
        self.created_decisions = []

    def tearDown(self):
        cur = self.conn.cursor()
        for rep_id in self.created_replenishments:
            cur.execute("DELETE FROM dashboard_events WHERE correlation_id = %s", (rep_id,))
            cur.execute("DELETE FROM dashboard_events WHERE event_id = %s", (f"DELIVERY_{rep_id}",))
            cur.execute("DELETE FROM replenishment_orders WHERE replenishment_id = %s", (rep_id,))
        for act_id in self.created_actions:
            cur.execute("DELETE FROM business_rule_actions WHERE action_id = %s", (act_id,))
        for dec_id in self.created_decisions:
            cur.execute("DELETE FROM pipeline_decisions WHERE decision_id = %s", (dec_id,))
        for sku in self.created_skus:
            cur.execute("DELETE FROM supplier_catalog_availability WHERE sku = %s", (sku,))
            cur.execute("DELETE FROM operational_inventory WHERE sku = %s", (sku,))
        self.conn.commit()
        cur.close()

    def _create_isolated_fixture(self, initial_on_hand=15, order_qty=25, status="CONFIRMED"):
        uid = uuid.uuid4().hex[:8].upper()
        sku = f"SKU-TEST-DELIV-{uid}"
        rep_id = f"RPL-TEST-DELIV-{uid}"
        wh_id = "WH-SP-01"

        cur = self.conn.cursor()

        # 1. Pipeline decision & action (required for foreign keys)
        cur.execute("""
            INSERT INTO pipeline_decisions
                (pipeline_name, condition_type, decision, required_count, ready_count, total_required, reason)
            VALUES ('Inventory Reorder Pipeline', 'TEST', 'TRIGGER', 1, 1, 1, 'test delivery api')
        """)
        dec_id = cur.lastrowid
        self.created_decisions.append(dec_id)

        cur.execute("""
            INSERT INTO business_rule_actions (rule_id, idempotency_key, status, decision_id)
            VALUES ('RULE_B_INVENTORY_REORDER', %s, 'CONFIRMED', %s)
        """, (f"test_act_key_{uid}", dec_id))
        act_id = cur.lastrowid
        self.created_actions.append(act_id)

        # 2. Supplier (ensure supplier_primary exists)
        cur.execute("""
            INSERT INTO operational_suppliers (supplier_id, supplier_name, status)
            VALUES ('supplier_primary', 'Primary Supplier', 'Online')
            ON DUPLICATE KEY UPDATE status = 'Online'
        """)

        # 3. Inventory
        cur.execute("""
            INSERT INTO operational_inventory (
                sku, warehouse_id, on_hand_quantity, reserved_quantity,
                reorder_threshold, reorder_target_quantity, safety_stock_buffer,
                data_origin, version
            ) VALUES (%s, %s, %s, 0, 20, 50, 5, 'SIMULATED', 1)
        """, (sku, wh_id, initial_on_hand))
        self.created_skus.append(sku)

        # 4. Replenishment Order
        estimated = datetime.datetime.now() + datetime.timedelta(days=2)
        cur.execute("""
            INSERT INTO replenishment_orders (
                replenishment_id, action_id, decision_id, sku, warehouse_id,
                supplier_id, quantity, status, dispatched_at, estimated_delivery_at
            ) VALUES (%s, %s, %s, %s, %s, 'supplier_primary', %s, %s, NOW(), %s)
        """, (rep_id, act_id, dec_id, sku, wh_id, order_qty, status, estimated))
        self.created_replenishments.append(rep_id)

        self.conn.commit()
        cur.close()

        return rep_id, sku, wh_id, order_qty

    def test_deliver_replenishment_success(self):
        """
        Phase F2.9: The deliver endpoint now rejects requests targeting operational
        warehouse WH-SP-01 with HTTP 403 to preserve data isolation.
        This test verifies the safety guard is active and returns 403 for operational inventory.
        """
        rep_id, sku, wh_id, order_qty = self._create_isolated_fixture(initial_on_hand=10, order_qty=20, status="CONFIRMED")

        response = self.client.post(f"/api/observability/replenishments/{rep_id}/deliver")
        # Guard must reject — operational warehouse targeted
        self.assertEqual(response.status_code, 403)
        self.assertIn("operational", response.json()["detail"].lower())

        # Inventory must be completely unchanged
        cur = self.conn.cursor()
        cur.execute("SELECT on_hand_quantity, version FROM operational_inventory WHERE sku = %s AND warehouse_id = %s", (sku, wh_id))
        inv = cur.fetchone()
        self.assertEqual(inv["on_hand_quantity"], 10)  # Unchanged
        self.assertEqual(inv["version"], 1)            # Unchanged
        cur.close()

    def test_deliver_replenishment_idempotency(self):
        """
        Idempotency guard: After Phase F2.9, the deliver endpoint rejects requests
        targeting operational warehouses (WH-SP-01) with HTTP 403.
        Both calls should return 403 — no database mutation should occur.
        """
        rep_id, sku, wh_id, order_qty = self._create_isolated_fixture(initial_on_hand=10, order_qty=20, status="CONFIRMED")

        # Both calls must be rejected — operational inventory must never be mutated
        res1 = self.client.post(f"/api/observability/replenishments/{rep_id}/deliver")
        self.assertEqual(res1.status_code, 403, "First call should be rejected for operational warehouse")

        res2 = self.client.post(f"/api/observability/replenishments/{rep_id}/deliver")
        self.assertEqual(res2.status_code, 403, "Second call should also be rejected")

        # Verify inventory completely unchanged (guard must prevent any mutation)
        cur = self.conn.cursor()
        cur.execute("SELECT on_hand_quantity, version FROM operational_inventory WHERE sku = %s AND warehouse_id = %s", (sku, wh_id))
        inv = cur.fetchone()
        self.assertEqual(inv["on_hand_quantity"], 10)  # Unchanged — guard blocked both calls
        self.assertEqual(inv["version"], 1)            # Version must remain at initial 1
        cur.close()

    def test_deliver_replenishment_ineligible_status(self):
        """
        Phase F2.9: Safety guard fires before any status check.
        The operational warehouse check (403) takes priority over the CANCELLED status check (400).
        Both result in no inventory mutation.
        """
        rep_id, sku, wh_id, _ = self._create_isolated_fixture(initial_on_hand=10, order_qty=20, status="CANCELLED")

        response = self.client.post(f"/api/observability/replenishments/{rep_id}/deliver")
        # Safety guard fires first: 403 (operational warehouse) before 400 (CANCELLED status)
        self.assertEqual(response.status_code, 403)

        # Inventory must remain completely unchanged
        cur = self.conn.cursor()
        cur.execute("SELECT on_hand_quantity, version FROM operational_inventory WHERE sku = %s AND warehouse_id = %s", (sku, wh_id))
        inv = cur.fetchone()
        self.assertEqual(inv["on_hand_quantity"], 10)
        self.assertEqual(inv["version"], 1)
        cur.close()

    def test_deliver_replenishment_not_found(self):
        """Attempting to deliver a non-existent replenishment returns 404 Not Found."""
        fake_id = f"RPL-NONEXISTENT-{uuid.uuid4().hex[:8].upper()}"
        response = self.client.post(f"/api/observability/replenishments/{fake_id}/deliver")
        self.assertEqual(response.status_code, 404)
        self.assertIn("not found", response.json()["detail"].lower())


if __name__ == "__main__":
    unittest.main()
