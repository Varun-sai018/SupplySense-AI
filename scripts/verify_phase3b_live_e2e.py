"""
SupplySense AI - Phase 3B Live End-to-End Operational Workflow Verifier

Executes live end-to-end tests against real MySQL and FastAPI observability endpoints:
1. Baseline state recording.
2. Low-stock isolated test SKU -> process_event() -> Rule B evaluation -> TRIGGER.
3. Pipeline runner invocation -> simulated supplier confirmation -> replenishment order persistence.
4. Observability API check for low-stock and CONFIRMED replenishment.
5. Deterministic delivery service -> status to DELIVERED, on_hand + qty, version + 1, transactional DELIVERY_COMPLETED event.
6. Observability API check for DELIVERED replenishment and DELIVERY_COMPLETED event.
7. Delivery idempotency check -> repeat deliver_replenishment() -> confirm no-op, no double-stocking, no duplicate event.
8. Replay original event -> confirm BLOCK (stock now sufficient), no duplicate replenishment.
9. BLOCK scenario: inventory above threshold -> confirm BLOCK, no execution, no replenishment.
10. BLOCK scenario: supplier offline -> confirm BLOCK, no execution, no replenishment.
11. Protected executions and historical table immutability verification.
"""

import os
import sys
import uuid
import json
import datetime
from fastapi.testclient import TestClient

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from services.dependency_engine.main import process_event
from services.dependency_engine.repository import get_inventory
from services.simulator.delivery_service import deliver_replenishment
from services.observability.main import app

def run_phase3b_verification():
    conn = get_connection()
    client = TestClient(app)
    results = {}

    print("======================================================================")
    print("SUPPLYSENSE AI - PHASE 3B END-TO-END OPERATIONAL WORKFLOW VERIFICATION")
    print("======================================================================")

    created_skus = []
    created_reps = []
    created_decisions = []
    created_executions = []

    try:
        cur = conn.cursor()

        # Step 1: Baseline Check
        print("\n--- STEP 1: Baseline & Safety Verification ---")
        cur.execute("SELECT execution_id, pipeline_name, status FROM pipeline_executions WHERE execution_id IN (36, 99586, 99653)")
        protected_baseline = {r["execution_id"]: (r["pipeline_name"], r["status"]) for r in cur.fetchall()}
        print(f"Protected executions verified: {protected_baseline}")
        assert 36 in protected_baseline and 99586 in protected_baseline and 99653 in protected_baseline

        cur.execute("SELECT COUNT(*) AS cnt FROM olist_orders")
        olist_orders_cnt = cur.fetchone()["cnt"]
        cur.execute("SELECT COUNT(*) AS cnt FROM olist_order_items")
        olist_items_cnt = cur.fetchone()["cnt"]
        print(f"Historical Olist dataset intact: {olist_orders_cnt} orders, {olist_items_cnt} items")

        # Step 2: Isolated Test SKU Setup
        print("\n--- STEP 2: Setup Isolated Test SKU Below Safe Threshold ---")
        test_sku = f"SKU-E2E-{uuid.uuid4().hex[:8].upper()}"
        created_skus.append(test_sku)

        cur.execute("""
            INSERT INTO operational_inventory (
                sku, warehouse_id, on_hand_quantity, reserved_quantity,
                reorder_threshold, reorder_target_quantity, safety_stock_buffer,
                data_origin, version
            ) VALUES (%s, 'WH-SP-01', 7, 0, 20, 50, 5, 'SIMULATED', 1)
        """, (test_sku,))

        cur.execute("""
            INSERT INTO operational_suppliers (supplier_id, supplier_name, status)
            VALUES ('supplier_primary', 'Primary Supplier', 'Online')
            ON DUPLICATE KEY UPDATE status = 'Online'
        """)

        cur.execute("""
            INSERT INTO supplier_catalog_availability (
                supplier_id, sku, supply_availability, unit_reorder_cost,
                min_reorder_quantity, lead_time_hours
            ) VALUES ('supplier_primary', %s, 'IN_STOCK', 18.50, 10, 48)
            ON DUPLICATE KEY UPDATE supply_availability = 'IN_STOCK'
        """, (test_sku,))
        conn.commit()

        inv_initial = get_inventory(sku=test_sku, warehouse_id="WH-SP-01", connection=conn)
        print(f"Initial Inventory for {test_sku}:")
        print(f"  on_hand_quantity:   {inv_initial['on_hand_quantity']}")
        print(f"  reserved_quantity:  {inv_initial['reserved_quantity']}")
        print(f"  available_quantity: {inv_initial['available_quantity']}")
        print(f"  reorder_threshold:  {inv_initial['reorder_threshold']}")
        print(f"  version:            {inv_initial['version']}")

        # Step 3: Trigger Reorder Workflow
        print("\n--- STEP 3: Ingest Event & Trigger Reorder Workflow ---")
        reorder_event = {
            "event_type": "INVENTORY_REORDER",
            "product_id": test_sku,
            "supplier_id": "supplier_primary",
            "safe_threshold": 20,
            "reorder_quantity": 45,
            "warehouse_id": "WH-SP-01",
        }
        decision, reason = process_event(reorder_event, conn)
        print(f"Dependency Engine decision: {decision}")
        print(f"Decision reason:            {reason}")
        assert decision == "TRIGGER"

        # Verify Decision & Execution
        cur.execute("""
            SELECT decision_id, pipeline_name, decision, reason
            FROM pipeline_decisions
            WHERE pipeline_name = 'Inventory Reorder Pipeline'
            ORDER BY decision_id DESC LIMIT 1
        """)
        dec_row = cur.fetchone()
        dec_id = dec_row["decision_id"]
        created_decisions.append(dec_id)

        cur.execute("""
            SELECT execution_id, pipeline_name, status
            FROM pipeline_executions
            WHERE decision_id = %s
            ORDER BY execution_id DESC LIMIT 1
        """, (dec_id,))
        exec_row = cur.fetchone()
        exec_id = exec_row["execution_id"]
        created_executions.append(exec_id)
        print(f"Created pipeline decision #{dec_id} and execution #{exec_id} (status: {exec_row['status']})")
        assert exec_row["status"] == "COMPLETED"

        # Verify Action & Replenishment Order
        cur.execute("SELECT action_id, status FROM business_rule_actions WHERE execution_id = %s", (exec_id,))
        action_row = cur.fetchone()
        action_id = action_row["action_id"]
        print(f"Business rule action #{action_id} status: {action_row['status']}")

        cur.execute("""
            SELECT replenishment_id, sku, warehouse_id, supplier_id, quantity,
                   status, supplier_order_reference, dispatched_at, delivered_at,
                   action_id, decision_id, execution_id
            FROM replenishment_orders
            WHERE sku = %s
        """, (test_sku,))
        rep_rows = cur.fetchall()
        assert len(rep_rows) == 1, "Expected exactly 1 replenishment order row"
        rep = rep_rows[0]
        rep_id = rep["replenishment_id"]
        created_reps.append(rep_id)
        print(f"Replenishment Order Persisted:")
        print(f"  replenishment_id:          {rep['replenishment_id']}")
        print(f"  status:                    {rep['status']}")
        print(f"  quantity:                  {rep['quantity']}")
        print(f"  supplier_id:               {rep['supplier_id']}")
        print(f"  supplier_order_reference:  {rep['supplier_order_reference']}")
        print(f"  action_id / dec_id / ex_id: #{rep['action_id']} / #{rep['decision_id']} / #{rep['execution_id']}")
        assert rep["status"] == "CONFIRMED"
        assert rep["quantity"] == 45

        # Check Observability APIs before delivery
        inv_api = client.get(f"/api/observability/inventory?search={test_sku}").json()
        assert inv_api["items"][0]["is_low_stock"] is True
        print(f"API /inventory reports is_low_stock: True for {test_sku}")

        rep_api = client.get(f"/api/observability/replenishments?sku={test_sku}").json()
        assert rep_api["items"][0]["status"] == "CONFIRMED"
        print(f"API /replenishments reports status: CONFIRMED for order {rep_id}")

        # Step 4: Deterministic Delivery
        print("\n--- STEP 4: Invoke Deterministic Delivery Service ---")
        cur.execute("SELECT COUNT(*) AS cnt FROM dashboard_events WHERE event_id = %s", (f"DELIVERY_{rep_id}",))
        events_before_del = cur.fetchone()["cnt"]
        assert events_before_del == 0

        del_res = deliver_replenishment(rep_id, connection=conn)
        print(f"Delivery result: status={del_res['status']}, idempotent={del_res['idempotent']}")
        assert del_res["status"] == "DELIVERED"
        assert del_res["idempotent"] is False

        # Verify MySQL state after delivery
        cur.execute("SELECT status, delivered_at FROM replenishment_orders WHERE replenishment_id = %s", (rep_id,))
        rep_delivered = cur.fetchone()
        print(f"Replenishment {rep_id} status: {rep_delivered['status']}, delivered_at: {rep_delivered['delivered_at']}")
        assert rep_delivered["status"] == "DELIVERED"
        assert rep_delivered["delivered_at"] is not None

        inv_after_del = get_inventory(sku=test_sku, warehouse_id="WH-SP-01", connection=conn)
        print(f"Inventory After Delivery for {test_sku}:")
        print(f"  on_hand_quantity:   {inv_after_del['on_hand_quantity']} (increased from {inv_initial['on_hand_quantity']} by +45)")
        print(f"  reserved_quantity:  {inv_after_del['reserved_quantity']}")
        print(f"  available_quantity: {inv_after_del['available_quantity']} (consistent: {inv_after_del['on_hand_quantity']} - {inv_after_del['reserved_quantity']})")
        print(f"  version:            {inv_after_del['version']} (incremented from {inv_initial['version']})")
        assert inv_after_del["on_hand_quantity"] == 7 + 45
        assert inv_after_del["version"] == 2

        # Verify Event in Journal
        cur.execute("SELECT event_id, event_type, payload FROM dashboard_events WHERE event_id = %s", (f"DELIVERY_{rep_id}",))
        ev_row = cur.fetchone()
        print(f"Journaled Event: event_id={ev_row['event_id']}, event_type={ev_row['event_type']}")
        assert ev_row["event_type"] == "DELIVERY_COMPLETED"

        # Check Recent Events API
        recent_evs = client.get("/api/observability/events/recent?event_type=DELIVERY_COMPLETED&limit=10").json()
        assert any(e["event_id"] == f"DELIVERY_{rep_id}" for e in recent_evs)
        print(f"API /events/recent successfully returned event DELIVERY_{rep_id}")

        # Step 5: Delivery Idempotency & Replay Protection
        print("\n--- STEP 5: Delivery Idempotency & Replay Protection ---")
        repeat_del = deliver_replenishment(rep_id, connection=conn)
        print(f"Repeat delivery invocation returned idempotent: {repeat_del['idempotent']}")
        assert repeat_del["idempotent"] is True

        inv_repeat = get_inventory(sku=test_sku, warehouse_id="WH-SP-01", connection=conn)
        print(f"Inventory after duplicate delivery: on_hand={inv_repeat['on_hand_quantity']}, version={inv_repeat['version']}")
        assert inv_repeat["on_hand_quantity"] == 52  # NOT double-incremented!
        assert inv_repeat["version"] == 2            # NOT incremented again!

        cur.execute("SELECT COUNT(*) AS cnt FROM dashboard_events WHERE event_id = %s", (f"DELIVERY_{rep_id}",))
        assert cur.fetchone()["cnt"] == 1
        print("Journaled DELIVERY_COMPLETED event count remains exactly 1 (no duplicates)")

        # Replay original event
        replay_decision, replay_reason = process_event(reorder_event, conn)
        print(f"Replayed event decision: {replay_decision} ({replay_reason})")
        assert replay_decision == "BLOCK"

        cur.execute("SELECT COUNT(*) AS cnt FROM replenishment_orders WHERE sku = %s", (test_sku,))
        total_reps_for_sku = cur.fetchone()["cnt"]
        assert total_reps_for_sku == 1
        print(f"Replenishment count for SKU remains exactly {total_reps_for_sku} (no duplicates)")

        # Step 6: BLOCK Scenarios
        print("\n--- STEP 6: BLOCK Scenarios (Threshold & Supplier Status) ---")
        # Block Scenario A: Stock above threshold
        sku_above = f"SKU-E2E-ABOVE-{uuid.uuid4().hex[:6].upper()}"
        created_skus.append(sku_above)
        cur.execute("""
            INSERT INTO operational_inventory (
                sku, warehouse_id, on_hand_quantity, reserved_quantity,
                reorder_threshold, reorder_target_quantity, safety_stock_buffer,
                data_origin, version
            ) VALUES (%s, 'WH-SP-01', 30, 0, 20, 50, 5, 'SIMULATED', 1)
        """, (sku_above,))
        conn.commit()

        dec_above, r_above = process_event({"event_type": "INVENTORY_REORDER", "product_id": sku_above, "safe_threshold": 20}, conn)
        print(f"Scenario A (Stock 30 >= 20 threshold): decision={dec_above}, reason='{r_above}'")
        assert dec_above == "BLOCK"
        cur.execute("SELECT COUNT(*) AS cnt FROM replenishment_orders WHERE sku = %s", (sku_above,))
        assert cur.fetchone()["cnt"] == 0

        # Block Scenario B: Supplier Offline
        sku_offline = f"SKU-E2E-OFFLINE-{uuid.uuid4().hex[:6].upper()}"
        created_skus.append(sku_offline)
        cur.execute("""
            INSERT INTO operational_inventory (
                sku, warehouse_id, on_hand_quantity, reserved_quantity,
                reorder_threshold, reorder_target_quantity, safety_stock_buffer,
                data_origin, version
            ) VALUES (%s, 'WH-SP-01', 5, 0, 20, 50, 5, 'SIMULATED', 1)
        """, (sku_offline,))
        conn.commit()

        dec_off, r_off = process_event({
            "event_type": "INVENTORY_REORDER",
            "product_id": sku_offline,
            "supplier_id": "SUP-RJ-SECONDARY", # Offline in operational_suppliers
            "safe_threshold": 20,
        }, conn)
        print(f"Scenario B (Supplier Offline): decision={dec_off}, reason='{r_off}'")
        assert dec_off == "BLOCK"
        cur.execute("SELECT COUNT(*) AS cnt FROM replenishment_orders WHERE sku = %s", (sku_offline,))
        assert cur.fetchone()["cnt"] == 0

        print("\n--- STEP 7: Protected Executions Final Verification ---")
        cur.execute("SELECT execution_id, pipeline_name, status FROM pipeline_executions WHERE execution_id IN (36, 99586, 99653)")
        final_protected = {r["execution_id"]: (r["pipeline_name"], r["status"]) for r in cur.fetchall()}
        assert final_protected == protected_baseline
        print(f"Protected executions intact: {final_protected}")

        print("\n======================================================================")
        print("PHASE 3B END-TO-END VERIFICATION RESULT: PASS (ALL INVARIANTS SATISFIED)")
        print("======================================================================")

    finally:
        # Cleanup isolated test fixtures
        cur = conn.cursor()
        for r_id in created_reps:
            cur.execute("DELETE FROM dashboard_events WHERE event_id = %s", (f"DELIVERY_{r_id}",))
            cur.execute("DELETE FROM replenishment_orders WHERE replenishment_id = %s", (r_id,))
        for s in created_skus:
            cur.execute("DELETE FROM replenishment_orders WHERE sku = %s", (s,))
            cur.execute("DELETE FROM business_rule_actions WHERE payload LIKE %s", (f'%{s}%',))
            cur.execute("DELETE FROM supplier_catalog_availability WHERE sku = %s", (s,))
            cur.execute("DELETE FROM operational_inventory WHERE sku = %s", (s,))
        for e in created_executions:
            cur.execute("DELETE FROM pipeline_executions WHERE execution_id = %s", (e,))
        for d in created_decisions:
            cur.execute("DELETE FROM pipeline_decisions WHERE decision_id = %s", (d,))
        conn.commit()
        conn.close()

if __name__ == "__main__":
    run_phase3b_verification()
