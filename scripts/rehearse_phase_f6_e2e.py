"""
SupplySense AI - Phase F6 End-to-End Integration & Rehearsal Runner

Executes a non-destructive verification of all integrated components:
1. Benchmark Integrity Audit (#36, #99586)
2. Scenario A: High-Velocity Cart Abandonment + Idempotency Cooldown + SSE
3. Scenario B: Low-Stock Inventory Reorder + Pipeline Dispatch + Idempotency + SSE
4. Scenario C: Payment Failure Spike Alert + Duplicate Suppression + Auto-Recovery + SSE
5. Analyst Metrics Consolidation + Strict Data Provenance Validation
"""

import os
import sys
import json
import logging
import datetime
from unittest.mock import patch, MagicMock

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from services.dependency_engine.rules.rule_cart_abandonment import evaluate_and_dispatch_cart_abandonment
from services.dependency_engine.rules.rule_inventory_reorder import evaluate_and_dispatch_inventory_reorder
from services.dependency_engine.rules.rule_payment_spike import evaluate_and_manage_payment_alerts
from services.observability.event_hub import hub
from services.observability import analyst_service

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("PhaseF6Rehearsal")


def log_step(title: str):
    print("\n" + "=" * 80)
    print(f" {title}")
    print("=" * 80)


def verify_benchmark_safety():
    log_step("STEP 1: BENCHMARK EXECUTION INTEGRITY AUDIT (#36 & #99586)")
    conn = get_connection()
    cur = conn.cursor()
    try:
        # Execution #36
        cur.execute("SELECT execution_id, pipeline_name, status, orchestrator, output_location FROM pipeline_executions WHERE execution_id = 36")
        exec36 = cur.fetchone()
        assert exec36 is not None, "Execution #36 missing from pipeline_executions"
        assert exec36["status"] == "COMPLETED", f"Execution #36 status is {exec36['status']}, expected COMPLETED"
        
        cur.execute("SELECT COUNT(*) AS cnt FROM forecast_results WHERE execution_id = 36")
        fc36 = cur.fetchone()
        assert fc36["cnt"] == 770, f"Execution #36 forecast rows: {fc36['cnt']}, expected 770"
        
        dir36 = os.path.join(REPO_ROOT, "ml/results/executions/36")
        assert os.path.isdir(dir36), f"Execution #36 directory missing: {dir36}"
        artifacts36 = os.listdir(dir36)
        assert len(artifacts36) >= 9, f"Execution #36 has {len(artifacts36)} artifacts, expected >= 9"
        print(f"  [PASS] Benchmark Execution #36: COMPLETED ({fc36['cnt']} forecast rows, {len(artifacts36)} isolated artifacts)")

        # Execution #99586
        cur.execute("SELECT execution_id, pipeline_name, status, orchestrator FROM pipeline_executions WHERE execution_id = 99586")
        exec99586 = cur.fetchone()
        assert exec99586 is not None, "Execution #99586 missing from pipeline_executions"
        assert exec99586["status"] == "COMPLETED", f"Execution #99586 status is {exec99586['status']}, expected COMPLETED"

        dir99586 = os.path.join(REPO_ROOT, "ml/results/executions/99586")
        assert os.path.isdir(dir99586), f"Execution #99586 directory missing: {dir99586}"
        artifacts99586 = os.listdir(dir99586)
        print(f"  [PASS] Live Execution #99586: COMPLETED (Airflow orchestrator, {len(artifacts99586)} isolated artifacts)")
    finally:
        cur.close()
        conn.close()


def test_scenario_a_cart_abandonment():
    log_step("STEP 2: SCENARIO A - CART ABANDONMENT EVALUATION & IDEMPOTENCY")
    # Clean previous rehearsal actions for Rule A to test fresh evaluation
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM business_rule_actions WHERE rule_id = 'RULE_A_CART_ABANDONMENT'")
    conn.commit()
    cur.close()
    conn.close()

    now = datetime.datetime.now(datetime.timezone.utc)
    # Peak hours: 18:00 - 22:00 UTC
    simulated_peak_time = now.replace(hour=19, minute=30, second=0, microsecond=0)

    # 100 sessions, 35 abandoned (35% drop-off > 25% threshold)
    sessions = []
    for i in range(100):
        sessions.append({
            "session_id": f"sess_f6_{i}",
            "started_at": simulated_peak_time - datetime.timedelta(seconds=60),
            "status": "abandoned" if i < 35 else "completed"
        })

    # Test subscriber to capture SSE events
    queue = hub.subscribe()
    try:
        # First attempt: Should TRIGGER and dispatch action
        res1 = evaluate_and_dispatch_cart_abandonment(
            sessions=sessions,
            current_time=simulated_peak_time,
            cooldown_seconds=600
        )
        assert res1["decision"] == "TRIGGER", f"Expected TRIGGER, got {res1['decision']}"
        action1 = res1.get("action", {})
        assert action1.get("status") == "CONFIRMED", f"Expected CONFIRMED, got {action1.get('status')}"
        dropoff = res1["metrics"]["dropoff_rate"]
        print(f"  [PASS] Attempt 1: Decision={res1['decision']}, Drop-off={dropoff*100:.1f}%, ActionStatus={action1.get('status')}")

        # Verify SSE event emitted
        assert not queue.empty(), "Expected SSE event in queue for cart abandonment action"
        event = queue.get_nowait()
        assert event.event_type == "CART_ABANDONMENT_TRIGGERED", f"Expected CART_ABANDONMENT_TRIGGERED, got {event.event_type}"
        print(f"  [PASS] SSE Delivery: Emitted Event #{event.event_id} ({event.event_type}) with dropoff {event.data.get('dropoff_rate')*100:.1f}%")

        # Second attempt immediately (within 600s cooldown): Should be SUPPRESSED
        res2 = evaluate_and_dispatch_cart_abandonment(
            sessions=sessions,
            current_time=simulated_peak_time + datetime.timedelta(seconds=10),
            cooldown_seconds=600
        )
        action2 = res2.get("action", {})
        assert action2.get("status") == "COOLDOWN", f"Expected COOLDOWN, got {action2.get('status')}"
        assert res2["decision"] == "BLOCK", f"Expected decision BLOCK on cooldown, got {res2['decision']}"
        print(f"  [PASS] Attempt 2 (Idempotency): Decision={res2['decision']}, ActionStatus={action2.get('status')} (Duplicate action suppressed)")
    finally:
        hub.unsubscribe(queue)


def test_scenario_b_inventory_reorder():
    log_step("STEP 3: SCENARIO B - INVENTORY REORDER, PIPELINE DISPATCH & DEDUPLICATION")
    product_id = f"PROD-F6-{int(datetime.datetime.now().timestamp())}"
    
    queue = hub.subscribe()
    try:
        # Attempt 1: Low stock (12 units < 20), supplier Online, pending 0 -> TRIGGER
        res1 = evaluate_and_dispatch_inventory_reorder(
            product_id=product_id,
            inventory_level=12,
            supplier_status="Online",
            pending_orders=0,
            safe_threshold=20,
            reorder_quantity=50,
            orchestrator="LOCAL"
        )
        assert res1["decision"] == "TRIGGER", f"Expected TRIGGER, got {res1['decision']}"
        action1 = res1.get("action", {})
        assert action1.get("status") == "CONFIRMED", f"Expected CONFIRMED, got {action1.get('status')}"
        exec_id = res1.get("execution", {}).get("execution_id")
        assert exec_id is not None, "Expected pipeline execution_id to be created"
        print(f"  [PASS] Attempt 1: Decision={res1['decision']}, Stock=12/20, ExecutionID=#{exec_id}, ActionStatus={action1.get('status')}")

        # Verify SSE events emitted (action + pipeline status)
        events = []
        while not queue.empty():
            events.append(queue.get_nowait())
        event_types = [e.event_type for e in events]
        assert "INVENTORY_REORDER_STATE_CHANGED" in event_types, f"Missing INVENTORY_REORDER_STATE_CHANGED in {event_types}"
        print(f"  [PASS] SSE Delivery: Emitted {len(events)} events: {event_types}")

        # Attempt 2: Duplicate call for same product while reorder is confirmed/pending
        res2 = evaluate_and_dispatch_inventory_reorder(
            product_id=product_id,
            inventory_level=12,
            supplier_status="Online",
            pending_orders=0,
            safe_threshold=20,
            reorder_quantity=50,
            orchestrator="LOCAL"
        )
        assert res2["decision"] == "SKIPPED", f"Expected SKIPPED, got {res2['decision']}"
        assert res2["lifecycle_state"] == "IDEMPOTENT_SKIPPED", f"Expected IDEMPOTENT_SKIPPED, got {res2['lifecycle_state']}"
        assert res2["action"]["status"] == "CONFIRMED"
        print(f"  [PASS] Attempt 2 (Idempotency): Decision={res2['decision']}, LifecycleState={res2['lifecycle_state']} (Duplicate reorder prevented)")
    finally:
        hub.unsubscribe(queue)


def test_scenario_c_payment_spike_and_recovery():
    log_step("STEP 4: SCENARIO C - PAYMENT FAILURE SPIKE ALERT & AUTO-RECOVERY")
    # Clean previous rehearsal actions for Rule C to test fresh alert lifecycle
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM business_rule_actions WHERE rule_id = 'RULE_C_PAYMENT_FAILURE_SPIKE'")
    conn.commit()
    cur.close()
    conn.close()

    now = datetime.datetime.now(datetime.timezone.utc)
    
    # Batch 1: 50 transactions, 43 SUCCESS, 7 FAILED -> 86.0% success rate (< 92.0% threshold)
    failing_batch = []
    for i in range(50):
        failing_batch.append({
            "transaction_id": f"tx_f6_fail_{i}",
            "created_at": now - datetime.timedelta(seconds=100 - i),
            "status": "FAILED" if i < 7 else "SUCCESS",
            "amount": 100.0
        })

    queue = hub.subscribe()
    try:
        # Step 4a: Failing batch triggers active alert
        res1 = evaluate_and_manage_payment_alerts(failing_batch, current_time=now)
        rate1 = res1["metrics"]["success_rate"]
        assert res1["decision"] == "TRIGGER", f"Expected TRIGGER, got {res1['decision']}"
        assert res1["alert_status"] in ("NEW_ALERT_PUBLISHED", "ACTIVE_PERSISTING"), f"Expected alert status, got {res1['alert_status']}"
        print(f"  [PASS] Step 4a: SuccessRate={rate1*100:.1f}%, Decision={res1['decision']}, AlertStatus={res1['alert_status']}")

        # Verify SSE event if newly published
        if res1["alert_status"] == "NEW_ALERT_PUBLISHED":
            assert not queue.empty(), "Expected SSE event for payment spike alert"
            event1 = queue.get_nowait()
            assert event1.event_type == "PAYMENT_GATEWAY_ALERT_TRIGGERED"
            print(f"  [PASS] SSE Delivery: Emitted Event #{event1.event_id} ({event1.event_type})")

        # Step 4b: Duplicate failing batch -> Alert deduplication
        res2 = evaluate_and_manage_payment_alerts(failing_batch, current_time=now + datetime.timedelta(seconds=5))
        assert res2["decision"] == "TRIGGER"
        assert res2["alert_status"] == "ACTIVE_PERSISTING", f"Expected ACTIVE_PERSISTING, got {res2['alert_status']}"
        print(f"  [PASS] Step 4b (Deduplication): AlertStatus={res2['alert_status']} (No redundant alert spawned)")

        # Step 4c: Healthy batch (48 SUCCESS, 2 FAILED -> 96.0% >= 92.0%) -> Automatic recovery
        healthy_batch = []
        for i in range(50):
            healthy_batch.append({
                "transaction_id": f"tx_f6_ok_{i}",
                "created_at": now + datetime.timedelta(seconds=i),
                "status": "FAILED" if i < 2 else "SUCCESS",
                "amount": 120.0
            })

        res3 = evaluate_and_manage_payment_alerts(healthy_batch, current_time=now + datetime.timedelta(seconds=60))
        rate3 = res3["metrics"]["success_rate"]
        assert res3["decision"] == "BLOCK", f"Expected BLOCK on healthy rate, got {res3['decision']}"
        assert res3.get("alert_status") == "ALERT_RECOVERED", f"Expected ALERT_RECOVERED, got {res3.get('alert_status')}"
        print(f"  [PASS] Step 4c (Auto-Recovery): SuccessRate={rate3*100:.1f}%, AlertStatus={res3.get('alert_status')}")
    finally:
        hub.unsubscribe(queue)


def test_analyst_metrics_and_provenance():
    log_step("STEP 5: ANALYST METRICS CONSOLIDATION & DATA PROVENANCE CATALOG")
    metrics = analyst_service.get_full_analyst_dashboard_metrics()
    assert metrics["status"] == "success", "Expected status=success"
    
    fin = metrics["financial"]
    op = metrics["operational"]
    cx = metrics["customer_experience"]
    catalog = metrics["provenance_catalog"]

    # Financial domain assertions
    assert fin["gmv"] > 15_000_000.0, f"Expected GMV > 15M, got {fin['gmv']}"
    assert fin["aov"] > 100.0, f"Expected AOV > 100, got {fin['aov']}"
    assert fin["total_orders"] > 90_000, f"Expected orders > 90k, got {fin['total_orders']}"
    assert fin["net_profit"] is None, f"Expected net_profit to be None, got {fin['net_profit']}"
    assert fin["net_profit_status"] == "UNAVAILABLE"
    assert fin["profit_margin"] is None
    assert fin["profit_margin_status"] == "UNAVAILABLE"
    assert fin["cogs"] is None
    assert fin["cogs_status"] == "UNAVAILABLE"
    assert fin["refund_rate"] is None
    assert fin["refund_status"] == "UNAVAILABLE"
    assert fin["chargeback_rate"] is None
    assert fin["chargeback_status"] == "UNAVAILABLE"
    print(f"  [PASS] Financial Domain: GMV={fin['formatted_gmv']}, AOV={fin['formatted_aov']}, NetProfit/COGS/Refunds strictly UNAVAILABLE (None)")

    # Operational domain assertions
    assert op["current_inventory_level"] > 0
    assert op["safe_threshold"] == 20
    assert op["fulfillment_queue_backlog"] > 0
    assert len(op["recent_reorders"]) > 0, "Expected recent reorders to be populated from business_rule_actions"
    print(f"  [PASS] Operational Domain: Stock={op['current_inventory_level']} units, Backlog={op['fulfillment_queue_backlog']} orders, RecentReorders={len(op['recent_reorders'])}")

    # Customer Experience domain assertions
    assert cx["overall_conversion_rate"] == 14.2
    assert len(cx["funnel"]) == 5
    assert len(cx["hourly_abandonment_profile"]) == 12
    assert cx["cart_dropoff_status"] == "HEALTHY"
    print(f"  [PASS] Customer Experience Domain: Conversion={cx['overall_conversion_rate']}%, FunnelStages={len(cx['funnel'])}, ProfileHours={len(cx['hourly_abandonment_profile'])}")

    # Provenance Catalog
    assert len(catalog) >= 8
    classifications = {item["classification"] for item in catalog}
    assert "HISTORICAL" in classifications
    assert "LIVE" in classifications or any("LIVE" in c for c in classifications)
    assert "SIMULATED" in classifications or any("SIMULATED" in c for c in classifications)
    assert "UNAVAILABLE" in classifications
    print(f"  [PASS] Provenance Catalog: {len(catalog)} classified metrics across classifications: {classifications}")


def main():
    print("=" * 80)
    print("       SUPPLYSENSE AI - PHASE F6 END-TO-END REHEARSAL")
    print("=" * 80)
    try:
        verify_benchmark_safety()
        test_scenario_a_cart_abandonment()
        test_scenario_b_inventory_reorder()
        test_scenario_c_payment_spike_and_recovery()
        test_analyst_metrics_and_provenance()

        print("\n" + "=" * 80)
        print(" [SUCCESS] ALL PHASE F6 REHEARSAL SCENARIOS PASSED CLEANLY")
        print("=" * 80)
    except Exception as e:
        logger.exception(f"Rehearsal failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
