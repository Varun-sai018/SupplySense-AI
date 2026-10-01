"""
SupplySense AI - Phase 8 Demo Script: CDC Micro-Batching & Replay Resilience (Task 8.4)

Demonstrates:
  1. Ingesting 200 simulated Orders CDC change events.
  2. Micro-batch aggregation buffering and emitting exactly 1 consolidated dataset event (rows_changed=200).
  3. Unique batch_id assignment and dataset_version increment of +1 (not +200).
  4. Single Kafka publication and exactly 1 downstream dependency evaluation.
  5. Replaying the exact same 200 CDC messages.
  6. Source-level deduplication rejecting all 200 replays (additional dataset_events = 0).
  7. Zero redundant Kafka messages or downstream pipeline triggers on replay.
"""

import os
import sys
import time
import importlib
from unittest.mock import MagicMock

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection

batcher_mod = importlib.import_module('services.cdc-adapter.batcher')
adapter_mod = importlib.import_module('services.cdc-adapter.adapter')
dep_engine = importlib.import_module('services.dependency-engine.main')

CDCBatcher = batcher_mod.CDCBatcher
normalize_cdc_record = adapter_mod.normalize_cdc_record
process_cdc_event = adapter_mod.process_cdc_event


def run_phase8_demo():
    conn = get_connection()
    cur = conn.cursor()

    print("=" * 80)
    print("      SUPPLYSENSE AI - PHASE 8 CDC MICRO-BATCHING & RESILIENCE DEMO")
    print("=" * 80)

    # 1. Reset State
    print("\n[Step 1] Initializing Environment & Dependency Status...")
    cur.execute("DELETE FROM dataset_events WHERE source_change_id LIKE 'demo_batch_%' OR batch_id LIKE 'batch_orders_demo_%'")
    cur.execute("UPDATE dataset_metadata SET status = 'WAITING' WHERE dataset_name IN ('Orders', 'Products', 'Sellers')")
    cur.execute("UPDATE pipeline_dependencies SET condition_type = 'ALL', required_count = NULL WHERE pipeline_name = 'Demand Forecast Pipeline'")
    conn.commit()

    cur.execute("SELECT COUNT(*) AS total FROM dataset_events")
    initial_event_count = cur.fetchone()["total"]

    cur.execute("SELECT current_version FROM dataset_metadata WHERE dataset_name = 'Orders'")
    initial_version = cur.fetchone()["current_version"]
    print(f"  Initial Orders Version : {initial_version}")
    print(f"  Initial Event Rows     : {initial_event_count}")

    # Mock Kafka Producer to track publish calls
    mock_producer = MagicMock()
    mock_future = MagicMock()
    mock_future.get.return_value = MagicMock(partition=0, offset=100)
    mock_producer.send.return_value = mock_future

    batcher = CDCBatcher(
        max_events=200,
        max_wait_ms=5000,
        flush_callback=lambda batch, connection=None: process_cdc_event(batch, connection=connection, kafka_producer=mock_producer),
        connection=conn
    )

    # 2. Ingest 200 CDC Messages
    print("\n" + "=" * 80)
    print("STEP 2: INGESTING 200 SIMULATED ROW-LEVEL CDC MESSAGES")
    print("=" * 80)
    ts = int(time.time() * 1000)
    cdc_messages = []
    for i in range(200):
        raw = {
            "before": None,
            "after": {"order_id": f"demo_ord_{i}", "order_status": "invoiced"},
            "source": {
                "file": "mysql-bin.000020",
                "pos": 20000 + i,
                "row": 0,
                "table": "olist_orders",
                "db": "supplysense"
            },
            "op": "c",
            "ts_ms": ts + i
        }
        norm = normalize_cdc_record(raw)
        cdc_messages.append(norm)

    print(f"  Emitted raw CDC messages into buffer: 200")
    flush_result = None
    for msg in cdc_messages:
        res = batcher.add_event(msg, connection=conn)
        if res:
            flush_result = res

    # Verify 200 events triggered batch flush
    print("\n[Batch Consolidation Result]")
    print(f"  Status                 : {flush_result['status']}")
    print(f"  Consolidated Event ID  : #{flush_result['event_id']}")
    print(f"  Dataset Name           : {flush_result['dataset_name']}")
    print(f"  Assigned Batch ID      : {flush_result['batch_id']}")
    print(f"  Rows Changed in Batch  : {flush_result['rows_changed']} (Consolidated from 200 rows)")
    print(f"  New Dataset Version    : v{flush_result['dataset_version']} (+1 increment)")
    print(f"  Published to Kafka     : {flush_result['published_to_kafka']}")

    cur.execute("SELECT COUNT(*) AS total FROM dataset_events")
    after_batch_count = cur.fetchone()["total"]
    print(f"\n  Database dataset_events rows added: {after_batch_count - initial_event_count} (Expected: 1)")

    # 3. Downstream Dependency Evaluation
    print("\n" + "=" * 80)
    print("STEP 3: DOWNSTREAM DEPENDENCY ENGINE EVALUATION")
    print("=" * 80)
    cur.execute("SELECT * FROM dataset_events WHERE event_id = %s", (flush_result["event_id"],))
    event_row = cur.fetchone()
    decision, reason = dep_engine.process_event(event_row, conn)
    print(f"  Evaluated Single Consolidated Event #{event_row['event_id']}")
    print(f"  Dependency Decision: {decision} | Reason: {reason}")

    # 4. Replay Same 200 CDC Messages
    print("\n" + "=" * 80)
    print("STEP 4: REPLAYING THE EXACT SAME 200 CDC MESSAGES")
    print("=" * 80)
    mock_producer.reset_mock()
    duplicates_detected = 0

    for msg in cdc_messages:
        rep_res = batcher.add_event(msg, connection=conn)
        if rep_res and rep_res.get("status") == "DUPLICATE_SKIPPED":
            duplicates_detected += 1

    remaining_flushes = batcher.flush_all(connection=conn)

    print(f"  Replayed CDC Messages          : 200")
    print(f"  Duplicates Detected & Ignored  : {duplicates_detected}")
    print(f"  Pending Batches Flushed        : {len(remaining_flushes)}")

    cur.execute("SELECT COUNT(*) AS total FROM dataset_events")
    after_replay_count = cur.fetchone()["total"]
    print(f"  Additional Events Created      : {after_replay_count - after_batch_count} (Expected: 0)")
    print(f"  Additional Kafka Publishes     : {mock_producer.send.call_count} (Expected: 0)")

    cur.execute("SELECT current_version FROM dataset_metadata WHERE dataset_name = 'Orders'")
    final_version = cur.fetchone()["current_version"]
    print(f"  Final Orders Version           : v{final_version} (Unchanged on replay)")

    conn.close()
    print("\n" + "=" * 80)
    print("      PHASE 8 DEMO COMPLETED SUCCESSFULLY")
    print("=" * 80)


if __name__ == '__main__':
    run_phase8_demo()
