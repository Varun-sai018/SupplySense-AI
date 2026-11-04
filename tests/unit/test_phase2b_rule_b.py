"""
SupplySense AI — Phase 2B Tests
================================
Covers Rule B, dispatcher integration, replenishment persistence,
delivery service, and regression safety.

Test types:
  - MOCK   : uses unittest.mock; no real MySQL required.
  - MYSQL  : requires a live DB connection; skipped when DB is unavailable.

Each test class declares its type in the class docstring.
"""

import os
import sys
import json
import uuid
import datetime
import unittest
from unittest.mock import MagicMock, patch, call

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import services  # noqa: F401 – registers service packages

from services.dependency_engine.rules.rule_inventory_reorder import (
    evaluate_and_dispatch_inventory_reorder,
    RULE_ID,
    PIPELINE_NAME,
)
from services.dependency_engine.main import _process_rule_b_event, process_event


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_conn(lastrowid=999):
    """Build a minimal mock connection whose cursor supports lastrowid."""
    conn = MagicMock()
    cur = MagicMock()
    cur.lastrowid = lastrowid
    cur.fetchone.return_value = None       # no in-flight replenishment by default
    conn.cursor.return_value = cur
    return conn, cur


def _inventory_rec(available=5, threshold=20):
    return {
        "sku": "SKU-TEST",
        "warehouse_id": "WH-SP-01",
        "on_hand_quantity": available,
        "reserved_quantity": 0,
        "available_quantity": available,
        "reorder_threshold": threshold,
        "reorder_target_quantity": 100,
        "version": 1,
    }


def _supplier_rec(status="Online", availability="IN_STOCK"):
    return {
        "supplier_id": "supplier_primary",
        "supplier_name": "Primary Supplier",
        "status": status,
        "supply_availability": availability,
        "reliability_score": 0.98,
    }


# ---------------------------------------------------------------------------
# 1. Rule B evaluation — MOCK tests
# ---------------------------------------------------------------------------

class TestRuleBEvaluationMock(unittest.TestCase):
    """MOCK — no MySQL required."""

    # --- helpers ---
    RULE_B_MOD = "services.dependency_engine.rules.rule_inventory_reorder"

    def _patch_db(self, inventory=None, supplier=None, in_flight=None,
                  action_by_key=None, action_id=501):
        """Return a patch context that wires all DB lookups."""
        conn, cur = _make_mock_conn(lastrowid=901)
        if in_flight:
            cur.fetchone.return_value = in_flight
        patches = [
            patch(f"{self.RULE_B_MOD}.get_inventory",
                  return_value=inventory or _inventory_rec()),
            patch(f"{self.RULE_B_MOD}.get_supplier_availability",
                  return_value=supplier or _supplier_rec()),
            patch(f"{self.RULE_B_MOD}.get_action_by_key",
                  return_value=action_by_key),
            patch(f"{self.RULE_B_MOD}.record_action_attempt",
                  return_value=action_id),
            patch(f"{self.RULE_B_MOD}.update_action_status"),
            patch(f"{self.RULE_B_MOD}.execute_pipeline",
                  return_value={"status": "COMPLETED", "result": {}}),
        ]
        return conn, patches

    def _apply(self, patches):
        mocks = [p.start() for p in patches]
        self.addCleanup(lambda: [p.stop() for p in patches])
        return mocks

    # 1. Above threshold → BLOCK
    def test_01_above_threshold_no_reorder(self):
        conn, patches = self._patch_db(inventory=_inventory_rec(available=25, threshold=20))
        self._apply(patches)
        res = evaluate_and_dispatch_inventory_reorder(
            product_id="SKU-TEST", connection=conn
        )
        self.assertFalse(res["matched"])
        self.assertEqual(res["decision"], "BLOCK")
        self.assertEqual(res["lifecycle_state"], "BLOCKED")

    # 2. At threshold (== 20) → BLOCK (strict <)
    def test_02_at_threshold_no_reorder(self):
        conn, patches = self._patch_db(inventory=_inventory_rec(available=20, threshold=20))
        self._apply(patches)
        res = evaluate_and_dispatch_inventory_reorder(
            product_id="SKU-TEST", connection=conn
        )
        self.assertFalse(res["matched"])
        self.assertEqual(res["decision"], "BLOCK")

    # 3. Below threshold + Online supplier → TRIGGER
    def test_03_below_threshold_online_supplier_triggers(self):
        conn, patches = self._patch_db(inventory=_inventory_rec(available=5, threshold=20))
        mocks = self._apply(patches)
        exec_mock = mocks[5]
        res = evaluate_and_dispatch_inventory_reorder(
            product_id="SKU-TEST", connection=conn
        )
        self.assertTrue(res["matched"])
        self.assertEqual(res["decision"], "TRIGGER")
        self.assertEqual(res["lifecycle_state"], "CONFIRMED")
        exec_mock.assert_called_once()

    # 4. Offline supplier → BLOCK, no pipeline
    def test_04_offline_supplier_blocks(self):
        conn, patches = self._patch_db(
            inventory=_inventory_rec(available=5, threshold=20),
            supplier=_supplier_rec(status="Offline"),
        )
        mocks = self._apply(patches)
        exec_mock = mocks[5]
        res = evaluate_and_dispatch_inventory_reorder(
            product_id="SKU-TEST", connection=conn
        )
        self.assertFalse(res["matched"])
        self.assertEqual(res["decision"], "BLOCK")
        exec_mock.assert_not_called()

    # 5. SKU OUT_OF_STOCK → BLOCK
    def test_05_sku_out_of_stock_blocks(self):
        conn, patches = self._patch_db(
            inventory=_inventory_rec(available=5, threshold=20),
            supplier=_supplier_rec(status="Online", availability="OUT_OF_STOCK"),
        )
        mocks = self._apply(patches)
        exec_mock = mocks[5]
        res = evaluate_and_dispatch_inventory_reorder(
            product_id="SKU-TEST", connection=conn
        )
        self.assertFalse(res["matched"])
        self.assertEqual(res["decision"], "BLOCK")
        exec_mock.assert_not_called()

    # 6. In-flight replenishment → BLOCK
    def test_06_inflight_replenishment_blocks(self):
        conn, cur = _make_mock_conn()
        # cursor.fetchone returns an in-flight row
        cur.fetchone.return_value = {"replenishment_id": "RPL-EXISTING", "status": "CONFIRMED"}
        patches = [
            patch(f"{self.RULE_B_MOD}.get_inventory", return_value=_inventory_rec(available=5)),
            patch(f"{self.RULE_B_MOD}.get_supplier_availability", return_value=_supplier_rec()),
            patch(f"{self.RULE_B_MOD}.get_action_by_key", return_value=None),
            patch(f"{self.RULE_B_MOD}.execute_pipeline"),
        ]
        mocks = [p.start() for p in patches]
        self.addCleanup(lambda: [p.stop() for p in patches])
        res = evaluate_and_dispatch_inventory_reorder(
            product_id="SKU-TEST", connection=conn
        )
        self.assertFalse(res["matched"])
        self.assertIn("in-flight", res["reasons"][0].lower())
        mocks[-1].assert_not_called()

    # 7. Missing inventory record → BLOCK
    def test_07_missing_inventory_blocks(self):
        conn, _ = _make_mock_conn()
        with patch(f"{self.RULE_B_MOD}.get_inventory", return_value=None):
            with patch(f"{self.RULE_B_MOD}.get_supplier_availability", return_value=_supplier_rec()):
                res = evaluate_and_dispatch_inventory_reorder(
                    product_id="SKU-MISSING", connection=conn
                )
        self.assertFalse(res["matched"])
        self.assertEqual(res["decision"], "BLOCK")

    # 8. Missing supplier → BLOCK
    def test_08_missing_supplier_blocks(self):
        conn, _ = _make_mock_conn()
        with patch(f"{self.RULE_B_MOD}.get_inventory", return_value=_inventory_rec(available=5)):
            with patch(f"{self.RULE_B_MOD}.get_supplier_availability", return_value=None):
                res = evaluate_and_dispatch_inventory_reorder(
                    product_id="SKU-TEST", connection=conn
                )
        self.assertFalse(res["matched"])
        self.assertEqual(res["decision"], "BLOCK")

    # 9. Duplicate action → IDEMPOTENT_SKIPPED, no extra pipeline call
    def test_09_duplicate_action_skipped(self):
        existing = {"action_id": 42, "status": "CONFIRMED", "idempotency_key": "k1"}
        conn, patches = self._patch_db(
            inventory=_inventory_rec(available=5),
            action_by_key=existing,
        )
        mocks = self._apply(patches)
        exec_mock = mocks[5]
        res = evaluate_and_dispatch_inventory_reorder(
            product_id="SKU-TEST", connection=conn
        )
        self.assertEqual(res["lifecycle_state"], "IDEMPOTENT_SKIPPED")
        self.assertEqual(res["decision"], "SKIPPED")
        exec_mock.assert_not_called()

    # 10. BLOCK — no action or execution created
    def test_10_block_creates_no_action(self):
        conn, patches = self._patch_db(inventory=_inventory_rec(available=25))
        mocks = self._apply(patches)
        record_mock = mocks[3]
        res = evaluate_and_dispatch_inventory_reorder(
            product_id="SKU-TEST", connection=conn
        )
        self.assertEqual(res["decision"], "BLOCK")
        record_mock.assert_not_called()

    # 11. Backward-compat: inventory_level / supplier_status overrides work
    def test_11_backward_compat_overrides(self):
        """inventory_level=5, supplier_status='Online' should still trigger."""
        conn, patches = self._patch_db(
            # DB returns no record → fall through to caller override
            inventory=None,
            supplier=None,
        )
        # Override get_inventory to return None to exercise fallback
        patches[0] = patch(f"{self.RULE_B_MOD}.get_inventory", return_value=None)
        patches[1] = patch(f"{self.RULE_B_MOD}.get_supplier_availability", return_value=None)
        mocks = self._apply(patches)
        exec_mock = mocks[5]
        # With both DB lookups returning None AND caller overrides provided
        res = evaluate_and_dispatch_inventory_reorder(
            product_id="SKU-TEST",
            inventory_level=5,
            supplier_status="Online",
            pending_orders=0,
            connection=conn,
        )
        # inventory_level fallback works → low stock → but supplier is None from DB
        # and supplier_status override is "Online" → should TRIGGER
        self.assertEqual(res["decision"], "TRIGGER")
        exec_mock.assert_called_once()


# ---------------------------------------------------------------------------
# 2. Dispatcher tests — MOCK
# ---------------------------------------------------------------------------

class TestDispatcherRuleB(unittest.TestCase):
    """MOCK — verifies _process_rule_b_event call contract."""

    MOD = "services.dependency_engine.main"

    def test_dispatcher_calls_rule_b_correctly(self):
        """Dispatcher passes product_id, supplier_id, safe_threshold, reorder_quantity, orchestrator."""
        event = {
            "event_type": "INVENTORY_REORDER",
            "product_id": "SKU-DISP",
            "supplier_id": "sup-abc",
            "safe_threshold": 30,
            "reorder_quantity": 60,
            "orchestrator": None,
        }
        conn = MagicMock()
        mock_result = {
            "decision": "BLOCK",
            "reasons": ["Inventory above safe threshold – no reorder needed."],
            "matched": False,
            "lifecycle_state": "BLOCKED",
        }
        with patch(
            "services.dependency_engine.main.evaluate_and_dispatch_inventory_reorder",
            return_value=mock_result,
        ) as mock_rule:
            decision, reason = _process_rule_b_event(event, conn)

        mock_rule.assert_called_once_with(
            product_id="SKU-DISP",
            supplier_id="sup-abc",
            safe_threshold=30,
            reorder_quantity=60,
            orchestrator=None,
            connection=conn,
        )
        self.assertEqual(decision, "BLOCK")

    def test_dispatcher_block_no_duplicate_persist(self):
        """BLOCK result must not call _persist_decision a second time."""
        event = {"event_type": "INVENTORY_REORDER", "product_id": "SKU-BLOCK"}
        conn = MagicMock()
        mock_result = {"decision": "BLOCK", "reasons": ["above threshold"], "matched": False}
        with patch(
            "services.dependency_engine.main.evaluate_and_dispatch_inventory_reorder",
            return_value=mock_result,
        ):
            with patch("services.dependency_engine.main._persist_decision") as persist_mock:
                _process_rule_b_event(event, conn)
                persist_mock.assert_not_called()

    def test_process_event_routes_inventory_event(self):
        """process_event routes INVENTORY_REORDER to _process_rule_b_event."""
        event = {"event_type": "INVENTORY_REORDER", "product_id": "SKU-ROUTE"}
        conn = MagicMock()
        with patch("services.dependency_engine.main._process_rule_b_event",
                   return_value=("BLOCK", "above threshold")) as mock_b:
            decision, reason = process_event(event, conn)
        mock_b.assert_called_once_with(event, conn)
        self.assertEqual(decision, "BLOCK")


# ---------------------------------------------------------------------------
# 3. Replenishment handler tests — MOCK
# ---------------------------------------------------------------------------

class TestInventoryReorderHandlerMock(unittest.TestCase):
    """MOCK — verifies replenishment persistence logic."""

    HANDLER_MOD = "services.pipeline_runner.handlers"

    def _build_action_row(self, action_id=1, status="PENDING", decision_id=10):
        payload = json.dumps({
            "product_id": "SKU-H",
            "reorder_quantity": 50,
            "supplier_id": "supplier_primary",
            "warehouse_id": "WH-SP-01",
        })
        return {
            "action_id": action_id,
            "idempotency_key": f"reorder_SKU-H_2026102912",
            "payload": payload,
            "status": status,
            "decision_id": decision_id,
        }

    def _setup_conn(self, action_row, dup_key=False):
        """Return a mock connection that simulates a handler run."""
        import pymysql
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur

        # First fetchone → action row; subsequent → None
        cur.fetchone.side_effect = [action_row, None]

        if dup_key:
            # Simulate duplicate key on replenishment insert
            cur.execute.side_effect = [
                None,  # action query
                None,  # supplier confirmed, artifact dir
                pymysql.err.IntegrityError(1062, "Duplicate"),  # replenishment insert
                None,  # fetch existing replenishment
                None,  # update action status
            ]
            # After dup: fetchone returns an existing replenishment
            cur.fetchone.side_effect = [action_row, {"replenishment_id": "RPL-EXISTING"}]

        return conn, cur

    def test_confirmed_creates_replenishment_row(self):
        """Successful supplier confirmation inserts replenishment_orders row."""
        from services.pipeline_runner.handlers import inventory_reorder_pipeline_handler

        with patch("services.pipeline_runner.handlers.get_connection") as mock_gc:
            conn = MagicMock()
            cur = MagicMock()
            conn.cursor.return_value = cur
            cur.fetchone.return_value = self._build_action_row()
            mock_gc.return_value = conn

            with patch("services.pipeline_runner.handlers.SupplierAPIAdapter") as MockAdap:
                MockAdap.return_value.place_reorder.return_value = {
                    "status": "CONFIRMED",
                    "supplier_order_id": "ORD-MOCK-001",
                }
                with patch("os.makedirs"):
                    with patch("builtins.open", unittest.mock.mock_open()):
                        result = inventory_reorder_pipeline_handler(77, PIPELINE_NAME)

        self.assertEqual(result["status"], "COMPLETED")
        self.assertIn("replenishment_id", result)

        # Verify an INSERT into replenishment_orders was attempted
        insert_calls = [
            str(c) for c in cur.execute.call_args_list
            if "replenishment_orders" in str(c) and "INSERT" in str(c)
        ]
        self.assertTrue(len(insert_calls) >= 1, "Expected INSERT into replenishment_orders")

    def test_failed_supplier_no_replenishment(self):
        """Failed supplier confirmation must not insert a replenishment row."""
        from services.pipeline_runner.handlers import inventory_reorder_pipeline_handler

        with patch("services.pipeline_runner.handlers.get_connection") as mock_gc:
            conn = MagicMock()
            cur = MagicMock()
            conn.cursor.return_value = cur
            cur.fetchone.return_value = self._build_action_row()
            mock_gc.return_value = conn

            with patch("services.pipeline_runner.handlers.SupplierAPIAdapter") as MockAdap:
                MockAdap.return_value.place_reorder.return_value = {
                    "status": "FAILED",
                    "error": "Timeout",
                }
                with self.assertRaises(RuntimeError):
                    inventory_reorder_pipeline_handler(78, PIPELINE_NAME)

        replenish_inserts = [
            c for c in cur.execute.call_args_list
            if "replenishment_orders" in str(c) and "INSERT" in str(c)
        ]
        self.assertEqual(len(replenish_inserts), 0)


# ---------------------------------------------------------------------------
# 4. Delivery service tests — MOCK
# ---------------------------------------------------------------------------

class TestDeliveryServiceMock(unittest.TestCase):
    """MOCK — verifies delivery logic without real MySQL."""

    DS_MOD = "services.simulator.delivery_service"

    def _row(self, status="CONFIRMED", qty=50):
        return {
            "replenishment_id": "RPL-001",
            "sku": "SKU-DEL",
            "warehouse_id": "WH-TEST-01",
            "supplier_id": "supplier_primary",
            "quantity": qty,
            "status": status,
            "action_id": 1,
            "decision_id": 1,
            "execution_id": 1,
            "supplier_order_reference": "ORD-001",
            "dispatched_at": datetime.datetime.now(),
            "estimated_delivery_at": datetime.datetime.now() + datetime.timedelta(days=2),
            "delivered_at": None,
            "pipeline_name": "SIM-REORDER-Pipeline",
            "data_origin": "SIMULATED",
        }

    def _make_conn(self, row):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        cur.fetchone.return_value = row
        return conn, cur

    def test_eligible_delivery_marks_delivered_and_increments_stock(self):
        from services.simulator.delivery_service import deliver_replenishment
        conn, cur = self._make_conn(self._row(status="CONFIRMED", qty=50))

        updated_inv = {
            "on_hand_quantity": 70, "available_quantity": 70,
            "version": 2, "sku": "SKU-DEL", "warehouse_id": "WH-SP-01"
        }
        with patch(f"{self.DS_MOD}.apply_inventory_delta", return_value=updated_inv) as mock_delta:
            with patch(f"{self.DS_MOD}.publish_dashboard_event"):
                result = deliver_replenishment("RPL-001", connection=conn)

        self.assertEqual(result["status"], "DELIVERED")
        self.assertFalse(result["idempotent"])
        mock_delta.assert_called_once_with(
            sku="SKU-DEL", warehouse_id="WH-TEST-01",
            delta_on_hand=50, delta_reserved=0, connection=conn
        )
        conn.commit.assert_called()

    def test_already_delivered_is_idempotent(self):
        from services.simulator.delivery_service import deliver_replenishment
        conn, cur = self._make_conn(self._row(status="DELIVERED"))

        with patch(f"{self.DS_MOD}.apply_inventory_delta") as mock_delta:
            result = deliver_replenishment("RPL-001", connection=conn)

        self.assertTrue(result["idempotent"])
        self.assertEqual(result["status"], "DELIVERED")
        mock_delta.assert_not_called()

    def test_invalid_status_raises_valueerror(self):
        from services.simulator.delivery_service import deliver_replenishment
        conn, cur = self._make_conn(self._row(status="FAILED"))

        with self.assertRaises(ValueError) as ctx:
            deliver_replenishment("RPL-001", connection=conn)
        self.assertIn("FAILED", str(ctx.exception))

    def test_missing_replenishment_raises_valueerror(self):
        from services.simulator.delivery_service import deliver_replenishment
        conn, cur = self._make_conn(None)  # no row found

        with self.assertRaises(ValueError):
            deliver_replenishment("RPL-GHOST", connection=conn)

    def test_inventory_failure_rolls_back(self):
        from services.simulator.delivery_service import deliver_replenishment
        conn, cur = self._make_conn(self._row(status="CONFIRMED", qty=50))

        with patch(f"{self.DS_MOD}.apply_inventory_delta", side_effect=RuntimeError("DB error")):
            with self.assertRaises(RuntimeError):
                deliver_replenishment("RPL-001", connection=conn)

        conn.rollback.assert_called()

    def test_delivery_event_contains_correct_fields(self):
        from services.simulator.delivery_service import deliver_replenishment
        conn, cur = self._make_conn(self._row(status="DISPATCHED", qty=30))
        updated_inv = {"on_hand_quantity": 50, "version": 3}
        published = []

        def capture_event(**kwargs):
            published.append(kwargs)

        with patch(f"{self.DS_MOD}.apply_inventory_delta", return_value=updated_inv):
            with patch(f"{self.DS_MOD}.publish_dashboard_event", side_effect=capture_event):
                deliver_replenishment("RPL-001", connection=conn)

        self.assertEqual(len(published), 1)
        ev = published[0]
        self.assertEqual(ev["event_type"], "DELIVERY_COMPLETED")
        data = ev["data"]
        self.assertEqual(data["sku"], "SKU-DEL")
        self.assertEqual(data["warehouse_id"], "WH-TEST-01")
        self.assertEqual(data["quantity"], 30)
        self.assertIn("delivered_at", data)

    def test_delivery_persists_event_within_transaction(self):
        """Verify publish_dashboard_event is called with connection, commit=False, broadcast=False."""
        from services.simulator.delivery_service import deliver_replenishment
        conn, cur = self._make_conn(self._row(status="CONFIRMED", qty=20))
        updated_inv = {"on_hand_quantity": 40, "version": 2}

        with patch(f"{self.DS_MOD}.apply_inventory_delta", return_value=updated_inv):
            with patch(f"{self.DS_MOD}.publish_dashboard_event") as mock_pub:
                deliver_replenishment("RPL-001", connection=conn)

        mock_pub.assert_called_once()
        kwargs = mock_pub.call_args.kwargs
        self.assertEqual(kwargs.get("connection"), conn)
        self.assertFalse(kwargs.get("commit"))
        self.assertFalse(kwargs.get("broadcast"))
        self.assertEqual(kwargs.get("event_id"), "DELIVERY_RPL-001")

    def test_retry_publish_delivery_event_raises_if_not_delivered(self):
        """retry_publish_delivery_event must reject non-DELIVERED replenishment orders."""
        from services.simulator.delivery_service import retry_publish_delivery_event
        conn, cur = self._make_conn(self._row(status="CONFIRMED", qty=20))

        with self.assertRaises(ValueError) as ctx:
            retry_publish_delivery_event("RPL-001", connection=conn)
        self.assertIn("Status must be 'DELIVERED'", str(ctx.exception))


# ---------------------------------------------------------------------------
# 5. MySQL integration tests (skip if DB unavailable)
# ---------------------------------------------------------------------------

def _db_available():
    """Return True if we can open a real DB connection."""
    try:
        from common.database import get_connection
        c = get_connection()
        c.close()
        return True
    except Exception:
        return False


DB_SKIP = not _db_available()
DB_SKIP_REASON = "Live MySQL database not reachable"


@unittest.skipIf(DB_SKIP, DB_SKIP_REASON)
class TestRuleBMysql(unittest.TestCase):
    """MYSQL — verifies real constraint enforcement and idempotency."""

    @classmethod
    def setUpClass(cls):
        from common.database import get_connection
        cls.conn = get_connection()
        cls._created_skus = []

    @classmethod
    def tearDownClass(cls):
        """Clean up only test-created rows."""
        try:
            cur = cls.conn.cursor()
            for sku in cls._created_skus:
                cur.execute(
                    "DELETE FROM replenishment_orders WHERE sku = %s", (sku,)
                )
                cur.execute(
                    "DELETE FROM business_rule_actions WHERE payload LIKE %s",
                    (f'%{sku}%',)
                )
                cur.execute(
                    "DELETE FROM operational_inventory WHERE sku = %s", (sku,)
                )
                cur.execute(
                    "DELETE FROM supplier_catalog_availability WHERE sku = %s", (sku,)
                )
            cls.conn.commit()
        except Exception:
            cls.conn.rollback()
        finally:
            cls.conn.close()

    def _insert_test_inventory(self, sku, available=5, threshold=20):
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO operational_inventory
                (sku, warehouse_id, on_hand_quantity, reserved_quantity,
                 reorder_threshold, reorder_target_quantity, safety_stock_buffer, data_origin, version)
            VALUES (%s, 'WH-SP-01', %s, 0, %s, 100, 5, 'SIMULATED', 1)
            ON DUPLICATE KEY UPDATE
                on_hand_quantity = VALUES(on_hand_quantity),
                reorder_threshold = VALUES(reorder_threshold)
            """,
            (sku, available, threshold),
        )
        self.conn.commit()
        self.__class__._created_skus.append(sku)

    def _insert_test_supplier(self, sku, status="Online", availability="IN_STOCK"):
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO operational_suppliers (supplier_id, supplier_name, status)
            VALUES ('supplier_primary', 'Primary Supplier', %s)
            ON DUPLICATE KEY UPDATE status = VALUES(status)
            """,
            (status,),
        )
        cur.execute(
            """
            INSERT INTO supplier_catalog_availability
                (supplier_id, sku, supply_availability, unit_reorder_cost, min_reorder_quantity, lead_time_hours)
            VALUES ('supplier_primary', %s, %s, 10.00, 10, 48)
            ON DUPLICATE KEY UPDATE supply_availability = VALUES(supply_availability)
            """,
            (sku, availability),
        )
        self.conn.commit()

    def test_mysql_01_trigger_creates_action_and_decision(self):
        """Low stock + Online supplier → action and decision persisted."""
        sku = f"SKU-M-{uuid.uuid4().hex[:6].upper()}"
        self._insert_test_inventory(sku, available=5, threshold=20)
        self._insert_test_supplier(sku, status="Online", availability="IN_STOCK")

        with patch(
            "services.dependency_engine.rules.rule_inventory_reorder.execute_pipeline",
            return_value={"status": "COMPLETED", "result": {}},
        ):
            res = evaluate_and_dispatch_inventory_reorder(
                product_id=sku, connection=self.conn
            )

        self.assertTrue(res["matched"])
        self.assertEqual(res["decision"], "TRIGGER")
        self.assertIsNotNone(res.get("action", {}).get("action_id"))

    def test_mysql_02_duplicate_event_reuses_action(self):
        """Second identical event within same hour window is IDEMPOTENT_SKIPPED."""
        sku = f"SKU-M-{uuid.uuid4().hex[:6].upper()}"
        self._insert_test_inventory(sku, available=5, threshold=20)
        self._insert_test_supplier(sku)

        with patch(
            "services.dependency_engine.rules.rule_inventory_reorder.execute_pipeline",
            return_value={"status": "COMPLETED", "result": {}},
        ):
            res1 = evaluate_and_dispatch_inventory_reorder(
                product_id=sku, connection=self.conn
            )
            res2 = evaluate_and_dispatch_inventory_reorder(
                product_id=sku, connection=self.conn
            )

        self.assertTrue(res1["matched"])
        self.assertIn(res2["lifecycle_state"], ("IDEMPOTENT_SKIPPED",))

    def test_mysql_03_offline_supplier_blocks(self):
        """Offline supplier → BLOCK with no action."""
        sku = f"SKU-M-{uuid.uuid4().hex[:6].upper()}"
        self._insert_test_inventory(sku, available=5, threshold=20)
        self._insert_test_supplier(sku, status="Offline")

        res = evaluate_and_dispatch_inventory_reorder(
            product_id=sku, connection=self.conn
        )
        self.assertFalse(res["matched"])
        self.assertEqual(res["decision"], "BLOCK")
        self.assertIsNone(res["action"])


@unittest.skipIf(DB_SKIP, DB_SKIP_REASON)
class TestDeliveryServiceMysql(unittest.TestCase):
    """MYSQL — verifies delivery atomicity, version increment, and idempotency."""

    @classmethod
    def setUpClass(cls):
        from common.database import get_connection
        cls.conn = get_connection()

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def setUp(self):
        super().setUp()
        self._created_skus = []
        self._created_replenishments = []
        self._created_action_ids = []
        self._created_decision_ids = []

    def tearDown(self):
        """Per-test cleanup strictly targeting exact records created during this test."""
        try:
            if hasattr(self, "conn") and self.conn:
                self.conn.rollback()
                with self.conn.cursor() as cur:
                    if self._created_replenishments:
                        format_rep = ','.join(['%s'] * len(self._created_replenishments))
                        cur.execute(
                            f"DELETE FROM dashboard_events WHERE correlation_id IN ({format_rep})",
                            tuple(self._created_replenishments)
                        )
                        cur.execute(
                            f"DELETE FROM replenishment_orders WHERE replenishment_id IN ({format_rep})",
                            tuple(self._created_replenishments)
                        )
                    if self._created_action_ids:
                        format_act = ','.join(['%s'] * len(self._created_action_ids))
                        cur.execute(
                            f"DELETE FROM business_rule_actions WHERE action_id IN ({format_act})",
                            tuple(self._created_action_ids)
                        )
                    if self._created_decision_ids:
                        format_dec = ','.join(['%s'] * len(self._created_decision_ids))
                        cur.execute(
                            f"DELETE FROM pipeline_decisions WHERE decision_id IN ({format_dec})",
                            tuple(self._created_decision_ids)
                        )
                    if self._created_skus:
                        format_sku = ','.join(['%s'] * len(self._created_skus))
                        cur.execute(
                            f"DELETE FROM operational_inventory WHERE warehouse_id = 'WH-TEST-01' AND sku IN ({format_sku})",
                            tuple(self._created_skus)
                        )
                self.conn.commit()
        finally:
            super().tearDown()

    def _setup_replenishment(self, quantity=50, status="CONFIRMED"):
        """Insert test inventory and a replenishment row; return replenishment_id."""
        sku = f"SKU-D-{uuid.uuid4().hex[:6].upper()}"
        cur = self.conn.cursor()

        # Inventory
        cur.execute(
            """
            INSERT INTO operational_inventory
                (sku, warehouse_id, on_hand_quantity, reserved_quantity,
                 reorder_threshold, reorder_target_quantity, safety_stock_buffer, data_origin, version)
            VALUES (%s, 'WH-TEST-01', 20, 0, 5, 100, 2, 'SIMULATED', 1)
            """,
            (sku,),
        )

        # Minimal action + decision required by FKs
        cur.execute(
            """
            INSERT INTO pipeline_decisions
                (pipeline_name, condition_type, decision, required_count, ready_count, total_required, reason)
            VALUES ('SIM-REORDER-Pipeline', 'TEST', 'TRIGGER', 1, 1, 1, 'test')
            """
        )
        decision_id = cur.lastrowid

        cur.execute(
            """
            INSERT INTO business_rule_actions (rule_id, idempotency_key, status, decision_id)
            VALUES ('RULE_B_INVENTORY_REORDER', %s, 'CONFIRMED', %s)
            """,
            (f"test_key_{sku}", decision_id),
        )
        action_id = cur.lastrowid

        # Supplier (ensure exists)
        cur.execute(
            """
            INSERT INTO operational_suppliers (supplier_id, supplier_name, status)
            VALUES ('supplier_primary', 'Primary', 'Online')
            ON DUPLICATE KEY UPDATE status = 'Online'
            """
        )

        replenishment_id = f"RPL-{uuid.uuid4().hex[:10].upper()}"
        estimated = datetime.datetime.now() + datetime.timedelta(days=2)
        cur.execute(
            """
            INSERT INTO replenishment_orders
                (replenishment_id, action_id, decision_id, sku, warehouse_id,
                 supplier_id, quantity, status, dispatched_at, estimated_delivery_at)
            VALUES (%s, %s, %s, %s, 'WH-TEST-01', 'supplier_primary', %s, %s, NOW(), %s)
            """,
            (replenishment_id, action_id, decision_id, sku, quantity, status, estimated),
        )
        self.conn.commit()

        self._created_skus.append(sku)
        self._created_decision_ids.append(decision_id)
        self._created_action_ids.append(action_id)
        self._created_replenishments.append(replenishment_id)

        return replenishment_id, sku, quantity

    def test_delivery_increments_stock_once(self):
        """Delivery must increase on_hand_quantity by exactly replenishment quantity."""
        from services.simulator.delivery_service import deliver_replenishment
        from services.dependency_engine.repository import get_inventory

        replenishment_id, sku, qty = self._setup_replenishment(quantity=30)
        before = get_inventory(sku=sku, warehouse_id="WH-TEST-01", connection=self.conn)
        before_qty = before["on_hand_quantity"]
        before_ver = before["version"]

        result = deliver_replenishment(replenishment_id)
        self.conn.commit()

        after = get_inventory(sku=sku, warehouse_id="WH-TEST-01", connection=self.conn)
        self.assertEqual(after["on_hand_quantity"], before_qty + qty)
        self.assertEqual(after["version"], before_ver + 1)
        self.assertFalse(result["idempotent"])

    def test_repeated_delivery_no_double_stock(self):
        """Second delivery call must not increment stock again."""
        from services.simulator.delivery_service import deliver_replenishment
        from services.dependency_engine.repository import get_inventory

        replenishment_id, sku, qty = self._setup_replenishment(quantity=25)
        deliver_replenishment(replenishment_id)

        after_first = get_inventory(sku=sku, warehouse_id="WH-TEST-01", connection=self.conn)
        qty_after_first = after_first["on_hand_quantity"]

        result2 = deliver_replenishment(replenishment_id)
        after_second = get_inventory(sku=sku, warehouse_id="WH-TEST-01", connection=self.conn)

        self.assertTrue(result2["idempotent"])
        self.assertEqual(after_second["on_hand_quantity"], qty_after_first)

    def test_invalid_status_no_inventory_change(self):
        """CANCELLED replenishment must not change stock."""
        from services.simulator.delivery_service import deliver_replenishment
        from services.dependency_engine.repository import get_inventory

        replenishment_id, sku, qty = self._setup_replenishment(quantity=20, status="CANCELLED")
        before = get_inventory(sku=sku, warehouse_id="WH-TEST-01", connection=self.conn)

        with self.assertRaises(ValueError):
            deliver_replenishment(replenishment_id)

        after = get_inventory(sku=sku, warehouse_id="WH-TEST-01", connection=self.conn)
        self.assertEqual(before["on_hand_quantity"], after["on_hand_quantity"])

    def test_delivery_event_persisted_in_same_mysql_transaction(self):
        """Delivery must atomically persist DELIVERY_COMPLETED event into dashboard_events."""
        from services.simulator.delivery_service import deliver_replenishment
        import json

        replenishment_id, sku, qty = self._setup_replenishment(quantity=35)
        result = deliver_replenishment(replenishment_id)
        self.conn.commit()

        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT event_id, event_type, entity, correlation_id, payload
            FROM dashboard_events
            WHERE event_id = %s
            """,
            (f"DELIVERY_{replenishment_id}",),
        )
        event_row = cur.fetchone()
        self.assertIsNotNone(event_row, "DELIVERY_COMPLETED event must exist in dashboard_events")
        self.assertEqual(event_row["event_type"], "DELIVERY_COMPLETED")
        self.assertEqual(event_row["entity"], "INVENTORY")
        self.assertEqual(event_row["correlation_id"], replenishment_id)

        payload = event_row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        self.assertEqual(payload["replenishment_id"], replenishment_id)
        self.assertEqual(payload["sku"], sku)
        self.assertEqual(payload["quantity"], 35)

    def test_transactional_rollback_preserves_inventory_and_persists_no_event(self):
        """Failure during delivery rolls back replenishment, inventory, and event atomically."""
        from services.simulator.delivery_service import deliver_replenishment
        from services.dependency_engine.repository import get_inventory

        replenishment_id, sku, qty = self._setup_replenishment(quantity=40)
        before = get_inventory(sku=sku, warehouse_id="WH-TEST-01", connection=self.conn)
        before_qty = before["on_hand_quantity"]

        with patch("services.simulator.delivery_service.apply_inventory_delta", side_effect=RuntimeError("Simulated DB failure")):
            with self.assertRaises(RuntimeError):
                deliver_replenishment(replenishment_id)

        self.conn.commit()
        # 1. Replenishment status must NOT be DELIVERED
        cur = self.conn.cursor()
        cur.execute("SELECT status FROM replenishment_orders WHERE replenishment_id = %s", (replenishment_id,))
        rep_row = cur.fetchone()
        self.assertEqual(rep_row["status"], "CONFIRMED")

        # 2. Inventory must not be incremented
        after = get_inventory(sku=sku, warehouse_id="WH-TEST-01", connection=self.conn)
        self.assertEqual(after["on_hand_quantity"], before_qty)

        # 3. No DELIVERY_COMPLETED event in dashboard_events
        cur.execute("SELECT COUNT(*) AS cnt FROM dashboard_events WHERE event_id = %s", (f"DELIVERY_{replenishment_id}",))
        cnt = cur.fetchone()["cnt"]
        self.assertEqual(cnt, 0, "No event should be committed when delivery transaction rolls back")

    def test_publisher_retry_does_not_duplicate_inventory_or_event(self):
        """Publisher retry must not alter inventory and must not duplicate event records."""
        from services.simulator.delivery_service import deliver_replenishment, retry_publish_delivery_event
        from services.dependency_engine.repository import get_inventory

        replenishment_id, sku, qty = self._setup_replenishment(quantity=25)
        deliver_replenishment(replenishment_id)
        self.conn.commit()

        after1 = get_inventory(sku=sku, warehouse_id="WH-TEST-01", connection=self.conn)

        # Explicit publisher retry
        retry_event = retry_publish_delivery_event(replenishment_id)
        self.assertIsNotNone(retry_event)
        self.conn.commit()

        after2 = get_inventory(sku=sku, warehouse_id="WH-TEST-01", connection=self.conn)
        self.assertEqual(after2["on_hand_quantity"], after1["on_hand_quantity"], "Retry must not increment stock")

        # Duplicate event check in database
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) AS cnt FROM dashboard_events WHERE event_id = %s", (f"DELIVERY_{replenishment_id}",))
        self.assertEqual(cur.fetchone()["cnt"], 1, "Must contain exactly one event record")

    def test_duplicate_delivery_call_ensures_event_without_duplicate_stock_or_rows(self):
        """Second deliver_replenishment call is idempotent and produces no duplicate event rows."""
        from services.simulator.delivery_service import deliver_replenishment
        from services.dependency_engine.repository import get_inventory

        replenishment_id, sku, qty = self._setup_replenishment(quantity=30)
        res1 = deliver_replenishment(replenishment_id)
        self.conn.commit()
        self.assertFalse(res1["idempotent"])

        res2 = deliver_replenishment(replenishment_id)
        self.conn.commit()
        self.assertTrue(res2["idempotent"])

        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) AS cnt FROM dashboard_events WHERE event_id = %s", (f"DELIVERY_{replenishment_id}",))
        self.assertEqual(cur.fetchone()["cnt"], 1, "Duplicate delivery calls must not duplicate dashboard_events rows")


# ---------------------------------------------------------------------------
# 6. Regression — protected executions
# ---------------------------------------------------------------------------

@unittest.skipIf(DB_SKIP, DB_SKIP_REASON)
class TestProtectedExecutions(unittest.TestCase):
    """MYSQL — confirms protected executions were not modified."""

    @classmethod
    def setUpClass(cls):
        from common.database import get_connection
        cls.conn = get_connection()

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def _get_exec(self, eid):
        cur = self.conn.cursor()
        cur.execute(
            "SELECT execution_id, status FROM pipeline_executions WHERE execution_id = %s",
            (eid,),
        )
        return cur.fetchone()

    def test_execution_36_intact(self):
        row = self._get_exec(36)
        self.assertIsNotNone(row, "Execution #36 must exist")

    def test_execution_99586_intact(self):
        row = self._get_exec(99586)
        self.assertIsNotNone(row, "Execution #99586 must exist")

    def test_execution_99653_intact(self):
        row = self._get_exec(99653)
        self.assertIsNotNone(row, "Execution #99653 must exist")


if __name__ == "__main__":
    unittest.main(verbosity=2)
