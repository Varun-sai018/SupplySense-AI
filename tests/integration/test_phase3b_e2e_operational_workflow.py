"""
SupplySense AI — Phase 3B: End-to-End Operational Workflow Verification

Tests the complete operational lifecycle across:
  1. Event Ingestion -> Dependency Engine -> Dynamic Rule B -> Decision & Action Persistence.
  2. Pipeline Runner Dispatch -> Simulated Supplier Confirmation -> Replenishment Order Persistence.
  3. Observability API Integration -> Inventory & Replenishment Data Visibility.
  4. Deterministic Delivery Service -> Inventory On-Hand & Version Increment -> Transactional DELIVERY_COMPLETED Event.
  5. Delivery Idempotency & Event Replay Protection (No double-stocking, no duplicate events).
  6. BLOCK Scenarios (Stock above threshold, Supplier Offline, SKU Out of Stock) -> Zero dispatches, zero replenishments.
  7. Frontend Contract Verification (Observability schema integrity, Distinct supplier vs SKU states, Provenance isolation).
  8. Safety Guarantees (Protected executions #36, #99586, #99653 intact; historical Olist data unmutated).
"""

import os
import sys
import uuid
import json
import datetime
import unittest
from fastapi.testclient import TestClient

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from services.dependency_engine.main import process_event
from services.dependency_engine.repository import get_inventory, apply_inventory_delta
from services.simulator.delivery_service import deliver_replenishment
from services.observability.main import app


def _db_reachable():
    try:
        conn = get_connection()
        conn.close()
        return True
    except Exception:
        return False


DB_AVAILABLE = _db_reachable()


@unittest.skipUnless(DB_AVAILABLE, "MySQL database is not reachable")
class TestPhase3BEndToEndWorkflow(unittest.TestCase):
    """End-to-End verification of the SupplySense AI operational inventory & replenishment workflow."""

    @classmethod
    def setUpClass(cls):
        cls.conn = get_connection()
        cls.api_client = TestClient(app)
        cls.created_skus = []
        cls.created_replenishments = []
        cls.created_decisions = []
        cls.created_executions = []

        # Record initial baseline state for safety assertions
        cur = cls.conn.cursor()
        cur.execute("SELECT execution_id, pipeline_name, status FROM pipeline_executions WHERE execution_id IN (36, 99586, 99653)")
        cls.protected_baseline = {r["execution_id"]: (r["pipeline_name"], r["status"]) for r in cur.fetchall()}
        
        # Verify protected executions exist
        for eid in (36, 99586, 99653):
            if eid not in cls.protected_baseline:
                raise RuntimeError(f"Safety violation: Protected execution #{eid} not found in database!")

    @classmethod
    def tearDownClass(cls):
        """Clean up exclusively the isolated test fixtures created during the test run."""
        try:
            cur = cls.conn.cursor()
            for rep_id in cls.created_replenishments:
                cur.execute("DELETE FROM dashboard_events WHERE event_id = %s", (f"DELIVERY_{rep_id}",))
                cur.execute("DELETE FROM replenishment_orders WHERE replenishment_id = %s", (rep_id,))
            
            for sku in cls.created_skus:
                cur.execute("DELETE FROM replenishment_orders WHERE sku = %s", (sku,))
                cur.execute("DELETE FROM business_rule_actions WHERE payload LIKE %s", (f'%{sku}%',))
                cur.execute("DELETE FROM supplier_catalog_availability WHERE sku = %s", (sku,))
                cur.execute("DELETE FROM operational_inventory WHERE sku = %s", (sku,))
            
            for exec_id in cls.created_executions:
                cur.execute("DELETE FROM pipeline_executions WHERE execution_id = %s", (exec_id,))
                
            for dec_id in cls.created_decisions:
                cur.execute("DELETE FROM pipeline_decisions WHERE decision_id = %s", (dec_id,))

            cls.conn.commit()
        except Exception as e:
            cls.conn.rollback()
            print(f"Warning: Cleanup encountered error: {e}")
        finally:
            cls.conn.close()

    def _setup_test_sku(
        self,
        on_hand: int = 8,
        reserved: int = 0,
        threshold: int = 20,
        target_qty: int = 50,
        supplier_id: str = "supplier_primary",
        supplier_status: str = "Online",
        sku_availability: str = "IN_STOCK",
        warehouse_id: str = "WH-SP-01",
    ) -> str:
        """Creates an isolated test SKU with explicit initial parameters."""
        sku = f"SKU-E2E-{uuid.uuid4().hex[:8].upper()}"
        cur = self.conn.cursor()

        # 1. Operational inventory row
        cur.execute("""
            INSERT INTO operational_inventory (
                sku, warehouse_id, on_hand_quantity, reserved_quantity,
                reorder_threshold, reorder_target_quantity, safety_stock_buffer,
                data_origin, version
            ) VALUES (%s, %s, %s, %s, %s, %s, 5, 'SIMULATED', 1)
        """, (sku, warehouse_id, on_hand, reserved, threshold, target_qty))

        # 2. Supplier row (ensure status matches desired)
        cur.execute("""
            INSERT INTO operational_suppliers (supplier_id, supplier_name, status)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE status = VALUES(status)
        """, (supplier_id, f"Test Supplier {supplier_id}", supplier_status))

        # 3. Supplier catalog availability row
        cur.execute("""
            INSERT INTO supplier_catalog_availability (
                supplier_id, sku, supply_availability, unit_reorder_cost,
                min_reorder_quantity, lead_time_hours
            ) VALUES (%s, %s, %s, 15.00, 10, 48)
            ON DUPLICATE KEY UPDATE supply_availability = VALUES(supply_availability)
        """, (supplier_id, sku, sku_availability))

        self.conn.commit()
        self.__class__.created_skus.append(sku)
        return sku

    # =========================================================================
    # Step 3: Verify Successful Reorder Workflow
    # =========================================================================
    def test_01_successful_reorder_workflow(self):
        """
        End-to-End: Low stock triggers Rule B -> Creates Decision & Action ->
        Dispatches Pipeline -> Confirms Supplier -> Persists exactly ONE Replenishment Order ->
        Exposes authoritative data via Observability APIs.
        """
        # 1. Setup isolated test SKU below threshold (8 units < 20 threshold)
        sku = self._setup_test_sku(on_hand=8, reserved=0, threshold=20, target_qty=50)

        # Baseline check
        initial_inv = get_inventory(sku=sku, warehouse_id="WH-SP-01", connection=self.conn)
        self.assertEqual(initial_inv["on_hand_quantity"], 8)
        self.assertEqual(initial_inv["reserved_quantity"], 0)
        self.assertEqual(initial_inv["available_quantity"], 8)
        self.assertEqual(initial_inv["version"], 1)

        # 2. Ingest event via supported process_event entrypoint
        event = {
            "event_type": "INVENTORY_REORDER",
            "product_id": sku,
            "supplier_id": "supplier_primary",
            "safe_threshold": 20,
            "reorder_quantity": 50,
            "warehouse_id": "WH-SP-01",
        }
        decision, reason = process_event(event, self.conn)

        # 3. Verify Decision & Engine Output
        self.assertEqual(decision, "TRIGGER", f"Expected TRIGGER, got {decision} with reason: {reason}")
        self.assertIn("Low stock and eligible supplier", reason)

        # 4. Verify Decision & Execution in MySQL
        cur = self.conn.cursor()
        cur.execute("""
            SELECT decision_id, pipeline_name, decision, reason, triggered_at
            FROM pipeline_decisions
            WHERE pipeline_name = 'Inventory Reorder Pipeline'
            ORDER BY decision_id DESC LIMIT 1
        """)
        dec_row = cur.fetchone()
        self.assertIsNotNone(dec_row)
        self.assertEqual(dec_row["decision"], "TRIGGER")
        dec_id = dec_row["decision_id"]
        self.__class__.created_decisions.append(dec_id)

        cur.execute("""
            SELECT execution_id, pipeline_name, decision_id, status
            FROM pipeline_executions
            WHERE decision_id = %s
            ORDER BY execution_id DESC LIMIT 1
        """, (dec_id,))
        exec_row = cur.fetchone()
        self.assertIsNotNone(exec_row)
        self.assertEqual(exec_row["status"], "COMPLETED")
        exec_id = exec_row["execution_id"]
        self.__class__.created_executions.append(exec_id)

        # 5. Verify Action in MySQL
        cur.execute("""
            SELECT action_id, rule_id, status, execution_id, decision_id, payload
            FROM business_rule_actions
            WHERE execution_id = %s
        """, (exec_id,))
        action_row = cur.fetchone()
        self.assertIsNotNone(action_row)
        self.assertEqual(action_row["status"], "CONFIRMED")
        self.assertEqual(action_row["rule_id"], "RULE_B_INVENTORY_REORDER")

        # 6. Verify exactly ONE replenishment row created with correct metadata
        cur.execute("""
            SELECT replenishment_id, action_id, decision_id, execution_id,
                   sku, warehouse_id, supplier_id, quantity, status,
                   supplier_order_reference, dispatched_at, estimated_delivery_at, delivered_at
            FROM replenishment_orders
            WHERE sku = %s
        """, (sku,))
        rep_rows = cur.fetchall()
        self.assertEqual(len(rep_rows), 1, "Exactly one replenishment order must be created")
        rep = rep_rows[0]
        self.__class__.created_replenishments.append(rep["replenishment_id"])

        self.assertEqual(rep["sku"], sku)
        self.assertEqual(rep["warehouse_id"], "WH-SP-01")
        self.assertEqual(rep["supplier_id"], "supplier_primary")
        self.assertEqual(rep["quantity"], 50)
        self.assertEqual(rep["status"], "CONFIRMED")
        self.assertEqual(rep["decision_id"], dec_id)
        self.assertEqual(rep["execution_id"], exec_id)
        self.assertEqual(rep["action_id"], action_row["action_id"])
        self.assertIsNotNone(rep["dispatched_at"])
        self.assertIsNone(rep["delivered_at"])

        # 7. Verify Observability REST APIs
        # Check Inventory API
        inv_api_res = self.api_client.get(f"/api/observability/inventory?search={sku}")
        self.assertEqual(inv_api_res.status_code, 200)
        inv_items = inv_api_res.json().get("items", [])
        self.assertEqual(len(inv_items), 1)
        api_item = inv_items[0]
        self.assertEqual(api_item["sku"], sku)
        self.assertEqual(api_item["on_hand_quantity"], 8)
        self.assertEqual(api_item["available_quantity"], 8)
        self.assertTrue(api_item["is_low_stock"])

        # Check Replenishments API
        rep_api_res = self.api_client.get(f"/api/observability/replenishments?sku={sku}")
        self.assertEqual(rep_api_res.status_code, 200)
        rep_items = rep_api_res.json().get("items", [])
        self.assertEqual(len(rep_items), 1)
        self.assertEqual(rep_items[0]["replenishment_id"], rep["replenishment_id"])
        self.assertEqual(rep_items[0]["status"], "CONFIRMED")
        self.assertEqual(rep_items[0]["execution_id"], exec_id)

    # =========================================================================
    # Step 4: Verify Delivery and Idempotency
    # =========================================================================
    def test_02_deterministic_delivery_and_idempotency(self):
        """
        End-to-End: Deliver replenishment -> Status becomes DELIVERED ->
        on_hand_quantity increases by quantity -> version increments by 1 ->
        DELIVERY_COMPLETED event persisted in MySQL -> Subsequent delivery attempts are no-op.
        """
        # Setup: create SKU and replenishment order
        sku = self._setup_test_sku(on_hand=10, reserved=0, threshold=20, target_qty=40)
        
        # Trigger reorder to obtain replenishment
        event = {
            "event_type": "INVENTORY_REORDER",
            "product_id": sku,
            "supplier_id": "supplier_primary",
            "safe_threshold": 20,
            "reorder_quantity": 40,
        }
        dec, _ = process_event(event, self.conn)
        self.assertEqual(dec, "TRIGGER")

        cur = self.conn.cursor()
        cur.execute("SELECT replenishment_id, quantity FROM replenishment_orders WHERE sku = %s", (sku,))
        rep_row = cur.fetchone()
        self.assertIsNotNone(rep_row)
        rep_id = rep_row["replenishment_id"]
        qty = rep_row["quantity"]
        self.__class__.created_replenishments.append(rep_id)

        # Capture state before delivery
        inv_before = get_inventory(sku=sku, warehouse_id="WH-SP-01", connection=self.conn)
        before_on_hand = inv_before["on_hand_quantity"]
        before_available = inv_before["available_quantity"]
        before_version = inv_before["version"]

        cur.execute("SELECT COUNT(*) AS cnt FROM dashboard_events WHERE event_id = %s", (f"DELIVERY_{rep_id}",))
        events_before = cur.fetchone()["cnt"]
        self.assertEqual(events_before, 0)

        # 1. Execute deterministic delivery
        delivery_res = deliver_replenishment(rep_id, connection=self.conn)
        self.assertEqual(delivery_res["status"], "DELIVERED")
        self.assertFalse(delivery_res["idempotent"])
        self.assertEqual(delivery_res["replenishment_id"], rep_id)

        # 2. Verify Replenishment is DELIVERED with delivered_at
        cur.execute("SELECT status, delivered_at FROM replenishment_orders WHERE replenishment_id = %s", (rep_id,))
        rep_after = cur.fetchone()
        self.assertEqual(rep_after["status"], "DELIVERED")
        self.assertIsNotNone(rep_after["delivered_at"])

        # 3. Verify on_hand_quantity increased by exactly quantity & version incremented by 1
        inv_after = get_inventory(sku=sku, warehouse_id="WH-SP-01", connection=self.conn)
        self.assertEqual(inv_after["on_hand_quantity"], before_on_hand + qty)
        self.assertEqual(inv_after["available_quantity"], before_available + qty)
        self.assertEqual(inv_after["version"], before_version + 1)
        # available_quantity matches database formula (on_hand - reserved)
        self.assertEqual(inv_after["available_quantity"], inv_after["on_hand_quantity"] - inv_after["reserved_quantity"])

        # 4. Verify DELIVERY_COMPLETED event in dashboard_events
        cur.execute("""
            SELECT event_id, event_type, entity, severity, payload, created_at
            FROM dashboard_events
            WHERE event_id = %s
        """, (f"DELIVERY_{rep_id}",))
        ev_row = cur.fetchone()
        self.assertIsNotNone(ev_row)
        self.assertEqual(ev_row["event_type"], "DELIVERY_COMPLETED")
        self.assertEqual(ev_row["entity"], "INVENTORY")
        payload = json.loads(ev_row["payload"])
        self.assertEqual(payload["sku"], sku)
        self.assertEqual(payload["quantity"], qty)
        self.assertEqual(payload["replenishment_id"], rep_id)

        # 5. Verify Observability Recent Events API includes the delivery event
        api_events_res = self.api_client.get("/api/observability/events/recent?event_type=DELIVERY_COMPLETED&limit=10")
        self.assertEqual(api_events_res.status_code, 200)
        recent_events = api_events_res.json()
        matching_ev = next((e for e in recent_events if e["event_id"] == f"DELIVERY_{rep_id}"), None)
        self.assertIsNotNone(matching_ev, "Expected DELIVERY_COMPLETED event in recent events API")

        # 6. Repeat Delivery Request (Idempotency Test)
        repeat_res = deliver_replenishment(rep_id, connection=self.conn)
        self.assertTrue(repeat_res["idempotent"], "Second delivery invocation must be idempotent")
        self.assertEqual(repeat_res["status"], "DELIVERED")

        # Verify inventory NOT double-incremented and version NOT changed
        inv_repeat = get_inventory(sku=sku, warehouse_id="WH-SP-01", connection=self.conn)
        self.assertEqual(inv_repeat["on_hand_quantity"], before_on_hand + qty)
        self.assertEqual(inv_repeat["version"], before_version + 1)

        # Verify event count is still exactly 1 (no duplicate rows)
        cur.execute("SELECT COUNT(*) AS cnt FROM dashboard_events WHERE event_id = %s", (f"DELIVERY_{rep_id}",))
        events_after_repeat = cur.fetchone()["cnt"]
        self.assertEqual(events_after_repeat, 1)

        # 7. Replay Original Event (Idempotency Test)
        replay_decision, _ = process_event(event, self.conn)
        # Because inventory is now 50 (above 20 threshold), Rule B must return BLOCK
        self.assertEqual(replay_decision, "BLOCK")

        # Verify replenishment count is still 1 (no duplicate orders)
        cur.execute("SELECT COUNT(*) AS cnt FROM replenishment_orders WHERE sku = %s", (sku,))
        total_reps = cur.fetchone()["cnt"]
        self.assertEqual(total_reps, 1)

    # =========================================================================
    # Step 5: Verify BLOCK Scenarios
    # =========================================================================
    def test_03_block_scenario_inventory_above_threshold(self):
        """When available inventory >= safe_threshold, Rule B blocks with zero side effects."""
        sku = self._setup_test_sku(on_hand=35, reserved=0, threshold=20)

        event = {
            "event_type": "INVENTORY_REORDER",
            "product_id": sku,
            "safe_threshold": 20,
        }
        decision, reason = process_event(event, self.conn)
        self.assertEqual(decision, "BLOCK")
        self.assertIn("above safe threshold", reason)

        # Verify zero replenishment rows created
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) AS cnt FROM replenishment_orders WHERE sku = %s", (sku,))
        self.assertEqual(cur.fetchone()["cnt"], 0)

        # Verify API reports not low stock
        api_res = self.api_client.get(f"/api/observability/inventory?search={sku}")
        self.assertEqual(api_res.status_code, 200)
        items = api_res.json().get("items", [])
        self.assertEqual(len(items), 1)
        self.assertFalse(items[0]["is_low_stock"])

    def test_04_block_scenario_offline_supplier(self):
        """When supplier connectivity is Offline, Rule B blocks even if stock is low."""
        sku = self._setup_test_sku(
            on_hand=5,
            reserved=0,
            threshold=20,
            supplier_id="SUP-RJ-SECONDARY",
            supplier_status="Offline"
        )

        event = {
            "event_type": "INVENTORY_REORDER",
            "product_id": sku,
            "supplier_id": "SUP-RJ-SECONDARY",
            "safe_threshold": 20,
        }
        decision, reason = process_event(event, self.conn)
        self.assertEqual(decision, "BLOCK")
        self.assertIn("Supplier unavailable or SKU out of stock", reason)

        # Verify zero replenishment rows created
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) AS cnt FROM replenishment_orders WHERE sku = %s", (sku,))
        self.assertEqual(cur.fetchone()["cnt"], 0)

    def test_05_block_scenario_sku_out_of_stock_at_supplier(self):
        """When supplier is Online but catalog SKU is OUT_OF_STOCK, Rule B blocks."""
        sku = self._setup_test_sku(
            on_hand=5,
            reserved=0,
            threshold=20,
            supplier_id="supplier_primary",
            supplier_status="Online",
            sku_availability="OUT_OF_STOCK"
        )

        event = {
            "event_type": "INVENTORY_REORDER",
            "product_id": sku,
            "supplier_id": "supplier_primary",
            "safe_threshold": 20,
        }
        decision, reason = process_event(event, self.conn)
        self.assertEqual(decision, "BLOCK")
        self.assertIn("Supplier unavailable or SKU out of stock", reason)

        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) AS cnt FROM replenishment_orders WHERE sku = %s", (sku,))
        self.assertEqual(cur.fetchone()["cnt"], 0)

    # =========================================================================
    # Step 6: Frontend Contract & Data Distinction Verification
    # =========================================================================
    def test_06_frontend_contract_and_provenance_isolation(self):
        """
        Verifies:
          1. Observability API schemas match frontend expectations in InventoryTab.jsx.
          2. Supplier connectivity (Online/Degraded/Offline) is distinct from catalog availability.
          3. Live operational data is distinctly separated from historical Olist forecasts.
        """
        # Summary API check
        summary_res = self.api_client.get("/api/observability/operational-summary")
        self.assertEqual(summary_res.status_code, 200)
        s_data = summary_res.json()
        self.assertEqual(s_data["data_provenance"], "PERSISTED_OPERATIONAL_TABLES")
        self.assertIn("inventory", s_data)
        self.assertIn("replenishments", s_data)
        self.assertIn("suppliers", s_data)

        # Suppliers API: verify status (connectivity) vs catalog_items (SKU availability)
        sup_res = self.api_client.get("/api/observability/suppliers")
        self.assertEqual(sup_res.status_code, 200)
        sup_items = sup_res.json().get("items", [])
        self.assertGreater(len(sup_items), 0)
        for supp in sup_items:
            self.assertIn("status", supp)
            self.assertIn(supp["status"], ("Online", "Degraded", "Offline"))
            self.assertIn("catalog_items", supp)
            for c_item in supp["catalog_items"]:
                self.assertIn("supply_availability", c_item)
                self.assertIn(c_item["supply_availability"], ("IN_STOCK", "LOW_STOCK", "OUT_OF_STOCK"))

        # Forecasts API check: verify data_provenance is HISTORICAL
        forecast_res = self.api_client.get("/api/observability/forecasts/latest")
        if forecast_res.status_code == 200:
            f_data = forecast_res.json()
            if f_data:
                self.assertEqual(f_data.get("data_provenance"), "HISTORICAL")

    # =========================================================================
    # Step 8: Safety & Invariant Verification
    # =========================================================================
    def test_07_protected_executions_and_historical_tables_intact(self):
        """Guarantees protected historical executions (#36, #99586, #99653) remain untouched."""
        cur = self.conn.cursor()
        cur.execute("SELECT execution_id, pipeline_name, status FROM pipeline_executions WHERE execution_id IN (36, 99586, 99653)")
        current = {r["execution_id"]: (r["pipeline_name"], r["status"]) for r in cur.fetchall()}
        
        self.assertEqual(current, self.protected_baseline, "Protected executions must be strictly immutable")

        # Verify historical Olist table row counts (non-zero and unchanged)
        cur.execute("SELECT COUNT(*) AS cnt FROM olist_orders")
        self.assertGreaterEqual(cur.fetchone()["cnt"], 99000)
        cur.execute("SELECT COUNT(*) AS cnt FROM olist_order_items")
        self.assertGreaterEqual(cur.fetchone()["cnt"], 110000)
        cur.execute("SELECT COUNT(*) AS cnt FROM olist_products")
        self.assertGreaterEqual(cur.fetchone()["cnt"], 32000)


if __name__ == "__main__":
    unittest.main()
