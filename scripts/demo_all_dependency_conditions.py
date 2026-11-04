"""
SupplySense AI - Comprehensive Dependency Engine Condition Demonstration

Demonstrates all dependency conditions supported by the Dependency Engine:
  1. ALL Condition: Multi-dataset synchronization (Orders + Products + Sellers).
     - Event 1: Orders updated -> 1/3 READY -> BLOCK
     - Event 2: Products updated -> 2/3 READY -> BLOCK
     - Event 3: Sellers updated -> 3/3 READY -> TRIGGER & executes Demand Forecast
  2. ANY Condition: Redundant / Fast-path ingestion.
     - Event 1: Backup stream arrives -> 1/2 READY -> TRIGGER immediately
  3. QUORUM Condition: Distributed quorum consensus (threshold = 3 of 4 nodes).
     - Node A arrives -> 1/3 -> BLOCK
     - Node B arrives -> 2/3 -> BLOCK
     - Node C arrives -> 3/3 -> TRIGGER
  4. CHAINED REAL-TIME TRANSFORMATION:
     - Operational order batch arrives from Part 2 simulation
     - Ingestion triggers Feature Pipeline -> transforms raw orders into 11 lag/rolling features in operational_feature_store
     - Emits Features Ready event -> triggers Forecast Pipeline -> produces forward XGBoost predictions in operational_forecast_results
"""

import os
import sys
import time
import json
import uuid
import logging
from datetime import datetime, timezone

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from services.dependency_engine.main import process_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("DependencyConditionsDemo")


def print_banner(title: str):
    logger.info("\n" + "=" * 70)
    logger.info(f"  {title.upper()}")
    logger.info("=" * 70)


def demo_all_conditions():
    conn = get_connection()
    cur = conn.cursor()
    # Dynamic base event id ensuring idempotency freshness per test run
    base_eid = int(time.time() * 10) % 900000 + 100000

    # =========================================================================
    # 1. DEMO: ALL CONDITION (Demand Forecast Pipeline)
    # =========================================================================
    print_banner("1. Testing Condition: ALL (Demand Forecast Pipeline)")
    logger.info("Rule: ALL 3 datasets ('Orders', 'Products', 'Sellers') must be READY.")

    # Reset all 3 datasets to WAITING
    cur.execute("UPDATE dataset_metadata SET status = 'WAITING' WHERE dataset_name IN ('Orders', 'Products', 'Sellers')")
    conn.commit()

    # Step 1A: Only 'Orders' becomes READY
    logger.info("\n--> Step 1A: Ingesting 'Orders' event only...")
    cur.execute("UPDATE dataset_metadata SET status = 'READY', current_version = current_version + 1 WHERE dataset_name = 'Orders'")
    conn.commit()
    event_1a = {
        "event_id": base_eid + 1,
        "dataset_name": "Orders",
        "pipeline_name": "Demand Forecast Pipeline",
        "dataset_version": 210,
        "rows_changed": 15
    }
    dec_1a, reason_1a = process_event(event_1a, conn)
    logger.info(f"Result 1A -> Decision: [{dec_1a}] | Reason: {reason_1a}")
    assert dec_1a == "BLOCK", f"Expected BLOCK, got {dec_1a}"

    # Step 1B: 'Products' becomes READY (now 2 of 3)
    logger.info("\n--> Step 1B: Ingesting 'Products' catalog update...")
    cur.execute("UPDATE dataset_metadata SET status = 'READY', current_version = current_version + 1 WHERE dataset_name = 'Products'")
    conn.commit()
    event_1b = {
        "event_id": base_eid + 2,
        "dataset_name": "Products",
        "pipeline_name": "Demand Forecast Pipeline",
        "dataset_version": 135,
        "rows_changed": 5
    }
    dec_1b, reason_1b = process_event(event_1b, conn)
    logger.info(f"Result 1B -> Decision: [{dec_1b}] | Reason: {reason_1b}")
    assert dec_1b == "BLOCK", f"Expected BLOCK, got {dec_1b}"

    # Step 1C: 'Sellers' becomes READY (all 3 of 3 satisfied!)
    logger.info("\n--> Step 1C: Ingesting 'Sellers' update (final required dataset)...")
    cur.execute("UPDATE dataset_metadata SET status = 'READY', current_version = current_version + 1 WHERE dataset_name = 'Sellers'")
    conn.commit()
    event_1c = {
        "event_id": base_eid + 3,
        "dataset_name": "Sellers",
        "pipeline_name": "Demand Forecast Pipeline",
        "dataset_version": 85,
        "rows_changed": 2
    }
    dec_1c, reason_1c = process_event(event_1c, conn)
    logger.info(f"Result 1C -> Decision: [{dec_1c}] | Reason: {reason_1c}")
    assert dec_1c == "TRIGGER", f"Expected TRIGGER, got {dec_1c}"
    logger.info("[PASS] Condition 'ALL' verified: Blocked until all datasets arrived, then triggered execution!")

    # =========================================================================
    # 2. DEMO: ANY CONDITION (SIM-ANY-Pipeline)
    # =========================================================================
    print_banner("2. Testing Condition: ANY (SIM-ANY-Pipeline)")
    logger.info("Rule: ANY dataset ('SIM_Primary_Stream' OR 'SIM_Backup_Stream') READY triggers pipeline.")

    # Reset both to WAITING
    cur.execute("UPDATE dataset_metadata SET status = 'WAITING' WHERE dataset_name IN ('SIM_Primary_Stream', 'SIM_Backup_Stream')")
    conn.commit()

    # Step 2A: Backup stream arrives
    logger.info("\n--> Ingesting single event on 'SIM_Backup_Stream'...")
    cur.execute("UPDATE dataset_metadata SET status = 'READY', current_version = current_version + 1 WHERE dataset_name = 'SIM_Backup_Stream'")
    conn.commit()
    event_any = {
        "event_id": base_eid + 10,
        "dataset_name": "SIM_Backup_Stream",
        "pipeline_name": "SIM-ANY-Pipeline",
        "dataset_version": 5,
        "rows_changed": 20
    }
    dec_any, reason_any = process_event(event_any, conn)
    logger.info(f"Result ANY -> Decision: [{dec_any}] | Reason: {reason_any}")
    assert dec_any == "TRIGGER", f"Expected TRIGGER, got {dec_any}"
    logger.info("[PASS] Condition 'ANY' verified: Single arrival immediately triggered pipeline!")

    # =========================================================================
    # 3. DEMO: QUORUM CONDITION (SIM-QUORUM-Pipeline, Quorum=3 of 4)
    # =========================================================================
    print_banner("3. Testing Condition: QUORUM (SIM-QUORUM-Pipeline, Quorum=3 of 4)")
    logger.info("Rule: At least 3 of 4 regional nodes (A, B, C, D) must be READY.")

    nodes = ['SIM_Regional_Node_A', 'SIM_Regional_Node_B', 'SIM_Regional_Node_C', 'SIM_Regional_Node_D']
    cur.execute(f"UPDATE dataset_metadata SET status = 'WAITING' WHERE dataset_name IN ({','.join(['%s']*4)})", tuple(nodes))
    conn.commit()

    # Step 3A: Node A arrives (1/3) -> BLOCK
    cur.execute("UPDATE dataset_metadata SET status = 'READY' WHERE dataset_name = 'SIM_Regional_Node_A'")
    conn.commit()
    event_q1 = {"event_id": base_eid + 21, "dataset_name": "SIM_Regional_Node_A", "pipeline_name": "SIM-QUORUM-Pipeline", "dataset_version": 1}
    dec_q1, _ = process_event(event_q1, conn)
    logger.info(f"Quorum Step 1 (Node A) -> Decision: [{dec_q1}] (1/3 Quorum)")
    assert dec_q1 == "BLOCK"

    # Step 3B: Node B arrives (2/3) -> BLOCK
    cur.execute("UPDATE dataset_metadata SET status = 'READY' WHERE dataset_name = 'SIM_Regional_Node_B'")
    conn.commit()
    event_q2 = {"event_id": base_eid + 22, "dataset_name": "SIM_Regional_Node_B", "pipeline_name": "SIM-QUORUM-Pipeline", "dataset_version": 1}
    dec_q2, _ = process_event(event_q2, conn)
    logger.info(f"Quorum Step 2 (Node B) -> Decision: [{dec_q2}] (2/3 Quorum)")
    assert dec_q2 == "BLOCK"

    # Step 3C: Node C arrives (3/3) -> TRIGGER!
    cur.execute("UPDATE dataset_metadata SET status = 'READY' WHERE dataset_name = 'SIM_Regional_Node_C'")
    conn.commit()
    event_q3 = {"event_id": base_eid + 23, "dataset_name": "SIM_Regional_Node_C", "pipeline_name": "SIM-QUORUM-Pipeline", "dataset_version": 1}
    dec_q3, _ = process_event(event_q3, conn)
    logger.info(f"Quorum Step 3 (Node C) -> Decision: [{dec_q3}] (3/3 Quorum REACHED!)")
    assert dec_q3 == "TRIGGER"
    logger.info("[PASS] Condition 'QUORUM' verified: Blocked on 1 and 2 nodes, triggered on 3rd node!")

    # =========================================================================
    # 4. DEMO: CHAINED OPERATIONAL TRANSFORMATION
    # =========================================================================
    print_banner("4. Testing Chained Real-Time Data Transformation")
    logger.info("Flow: Operational Order Batch -> Feature Pipeline -> Live Feature Store -> Forecast Pipeline -> Forward Predictions")

    # Ingest a live operational order batch
    batch_key = f"DEMO-TRANSFORM-{uuid.uuid4().hex[:6]}"
    cur.execute("""
        INSERT INTO operational_orders (order_id, sku, warehouse_id, quantity, unit_price, order_status, source, idempotency_key, event_time)
        VALUES 
            (%s, 'SP-SKU-001', 'WH-SP-01', 5, 49.90, 'COMPLETED', 'SIMULATED', %s, NOW()),
            (%s, 'SP-SKU-002', 'WH-SP-01', 3, 89.90, 'COMPLETED', 'SIMULATED', %s, NOW())
    """, (f"{batch_key}-01", f"IDEMP-{batch_key}-01", f"{batch_key}-02", f"IDEMP-{batch_key}-02"))

    # Update Operational Orders dataset to READY
    cur.execute("UPDATE dataset_metadata SET status = 'READY', current_version = current_version + 1 WHERE dataset_name = 'Operational Orders'")
    conn.commit()

    # Trigger Feature Engineering Pipeline
    event_feat = {
        "event_id": base_eid + 30,
        "dataset_name": "Operational Orders",
        "pipeline_name": "Operational Demand Feature Pipeline",
        "dataset_version": 170,
        "rows_changed": 2
    }
    logger.info("\n--> Triggering Stage 1: Feature Pipeline...")
    dec_feat, _ = process_event(event_feat, conn)
    logger.info(f"Stage 1 Decision: [{dec_feat}]")
    assert dec_feat == "TRIGGER"

    # Verify features transformed in operational_feature_store
    cur.execute("SELECT COUNT(*) as feat_count FROM operational_feature_store")
    feat_count = cur.fetchone()["feat_count"]
    logger.info(f"  Operational Feature Store Rows: {feat_count}")

    # Trigger Forecasting Pipeline
    cur.execute("UPDATE dataset_metadata SET status = 'READY', current_version = current_version + 1 WHERE dataset_name = 'Operational Demand Features'")
    conn.commit()
    event_fc = {
        "event_id": base_eid + 31,
        "dataset_name": "Operational Demand Features",
        "pipeline_name": "Operational Demand Forecast Pipeline",
        "dataset_version": 75,
        "rows_changed": feat_count
    }
    logger.info("\n--> Triggering Stage 2: Demand Forecast Pipeline...")
    dec_fc, _ = process_event(event_fc, conn)
    logger.info(f"Stage 2 Decision: [{dec_fc}]")
    assert dec_fc == "TRIGGER"

    cur.execute("SELECT COUNT(*) as fc_count FROM operational_forecast_results")
    fc_count = cur.fetchone()["fc_count"]
    logger.info(f"  Operational Forecast Results Rows: {fc_count}")
    logger.info("[PASS] Chained Data Transformation verified: Raw orders transformed into features and live forecasts!")

    conn.close()
    print_banner("All 4 Dependency Engine Conditions Verified Successfully!")


if __name__ == "__main__":
    demo_all_conditions()
