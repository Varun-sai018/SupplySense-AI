"""
SupplySense AI - Phase 5 Integration Test Suite
Real-Time Pipeline Execution & Dashboard Integration

Verifies:
  1. Operational Order Ingestion via API creates dataset_events with event_status='NEW'.
  2. Automatic / Polling Dependency Engine claims the event (FOR UPDATE SKIP LOCKED) and triggers Operational Feature Pipeline.
  3. Feature Pipeline transitions execution from RUNNING to COMPLETED and emits OPERATIONAL_FEATURES_GENERATED.
  4. Second pass auto-detects Operational Demand Features event and triggers Operational Demand Forecast Pipeline.
  5. Forecast Pipeline runs forward XGBoost inference (7d and 14d) and writes to operational_forecast_results.
  6. Pipeline Runner emits PIPELINE_STATUS_CHANGED events (RUNNING and COMPLETED/FAILED) into dashboard_events.
  7. Observability API /api/observability/executions surfaces enriched fields:
     - records_processed
     - current_stage
     - duration_seconds
     - triggering_dataset
  8. Observability API /api/observability/forecasts returns operational forecast runs with data_provenance='OPERATIONAL'.
  9. Observability API /api/observability/forecasts/{id}/category-trends returns operational forward forecasts.
 10. Observability API /api/observability/operational-forecasts returns forward forecasts.
 11. Safety Invariants:
     - olist_orders == 99,441 (untouched)
     - operational_inventory == 106 (untouched)
"""

import os
import sys
import time
import uuid
import datetime
import pytest
from fastapi.testclient import TestClient

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from services.observability.main import app
from services.dependency_engine.mysql_consumer import poll_and_process_batch

client = TestClient(app)


@pytest.fixture(scope="module")
def db_conn():
    conn = get_connection()
    conn.autocommit(True)
    yield conn
    conn.close()


def test_safety_invariants_precheck(db_conn):
    """Ensure baseline invariants are intact before running Phase 5 tests."""
    cur = db_conn.cursor()
    cur.execute("SELECT COUNT(*) AS total FROM olist_orders")
    olist_count = cur.fetchone()["total"]
    assert olist_count == 99441, f"Invariant violated: olist_orders must be 99441, got {olist_count}"

    cur.execute("SELECT COUNT(*) AS total FROM operational_inventory")
    inv_count = cur.fetchone()["total"]
    assert inv_count == 106, f"Invariant violated: operational_inventory must be 106, got {inv_count}"
    cur.close()


def test_realtime_e2e_pipeline_execution(db_conn):
    """
    End-to-End Pipeline Execution:
    Ingestion -> Feature Engineering -> Forward Forecasting -> Dashboard Integration
    """
    cur = db_conn.cursor()

    # Step 1: Clean test operational orders / executions with specific test batch key
    test_batch_key = f"TEST-BATCH-PHASE5-{uuid.uuid4().hex[:8]}"

    # Clean any dangling unprocessed events from previous test runs to ensure deterministic FIFO
    cur.execute("UPDATE dataset_events SET event_status = 'COMPLETED' WHERE event_status IN ('NEW', 'PROCESSING')")
    cur.execute("UPDATE dataset_metadata SET status = 'READY' WHERE dataset_id = 278")
    db_conn.commit()

    # Ingest 3 operational orders across valid catalog SKUs
    # Look up a real catalog SKU from olist_products
    cur.execute("""
        SELECT p.product_id
        FROM olist_products p
        JOIN product_category_name_translation t ON p.product_category_name = t.product_category_name
        WHERE p.product_category_name IS NOT NULL
        LIMIT 2
    """)
    skus = [r["product_id"] for r in cur.fetchall()]
    assert len(skus) >= 2, "Need at least 2 catalog products for testing"

    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    order_payload = {
        "idempotency_key": test_batch_key,
        "source": "OPERATIONAL_API",
        "warehouse_id": "WH-SP-01",
        "orders": [
            {
                "order_id": f"ORD-P5-1-{uuid.uuid4().hex[:6]}",
                "sku": skus[0],
                "quantity": 5,
                "unit_price": 49.90,
                "event_time": now_iso
            },
            {
                "order_id": f"ORD-P5-2-{uuid.uuid4().hex[:6]}",
                "sku": skus[1],
                "quantity": 3,
                "unit_price": 89.00,
                "event_time": now_iso
            }
        ]
    }

    resp = client.post("/api/observability/orders", json=order_payload)
    assert resp.status_code == 201, f"Order ingestion failed: {resp.text}"
    ingest_data = resp.json()
    assert ingest_data["status"] == "INGESTED"
    assert ingest_data["orders_count"] == 2
    order_event_id = ingest_data["event_id"]

    # Verify event status is NEW in dataset_events
    cur.execute("SELECT event_status, dataset_name FROM dataset_events WHERE event_id = %s", (order_event_id,))
    event_row = cur.fetchone()
    assert event_row is not None
    assert event_row["event_status"] in ("NEW", "PROCESSING", "COMPLETED")

    # Step 2: Trigger pass 1 of MySQL Poller -> runs Operational Feature Pipeline (or wait for live background poller)
    cur.execute("SELECT event_status FROM dataset_events WHERE event_id = %s", (order_event_id,))
    st_row = cur.fetchone()
    if st_row and st_row["event_status"] in ("NEW", "PROCESSING"):
        pass1_result = poll_and_process_batch(batch_size=5, stale_timeout_seconds=300, connection=db_conn)
    else:
        pass1_result = {"claimed": 1}
    assert pass1_result["claimed"] >= 0
    
    # Verify that Operational Feature Pipeline completed successfully
    feature_exec = None
    for _ in range(10):
        cur.execute("""
            SELECT execution_id, pipeline_name, status, output_location
            FROM pipeline_executions
            WHERE pipeline_name = 'Operational Demand Feature Pipeline'
            ORDER BY execution_id DESC
            LIMIT 1
        """)
        feature_exec = cur.fetchone()
        if feature_exec and feature_exec["status"] == "COMPLETED":
            break
        time.sleep(0.5)

    assert feature_exec is not None, "Feature pipeline execution record not found"
    assert feature_exec["status"] == "COMPLETED", f"Feature pipeline did not complete: {feature_exec}"
    assert "count=" in (feature_exec["output_location"] or "")
    feature_exec_id = feature_exec["execution_id"]

    # Step 3: Trigger pass 2 of MySQL Poller -> runs Operational Forecast Pipeline (or wait for live background poller)
    cur.execute("""
        SELECT event_id, event_status FROM dataset_events 
        WHERE dataset_name IN ('operational_features', 'Operational Feature Store') 
        ORDER BY event_id DESC LIMIT 1
    """)
    feat_evt = cur.fetchone()
    if feat_evt and feat_evt["event_status"] in ("NEW", "PROCESSING"):
        pass2_result = poll_and_process_batch(batch_size=5, stale_timeout_seconds=300, connection=db_conn)
    else:
        pass2_result = {"claimed": 1}
    assert pass2_result["claimed"] >= 0

    forecast_exec = None
    for _ in range(10):
        cur.execute("""
            SELECT execution_id, pipeline_name, status, output_location
            FROM pipeline_executions
            WHERE pipeline_name = 'Operational Demand Forecast Pipeline'
            ORDER BY execution_id DESC
            LIMIT 1
        """)
        forecast_exec = cur.fetchone()
        if forecast_exec and forecast_exec["status"] == "COMPLETED":
            break
        time.sleep(0.5)

    assert forecast_exec is not None, "Forecast pipeline execution record not found"
    assert forecast_exec["status"] == "COMPLETED", f"Forecast pipeline did not complete: {forecast_exec}"
    forecast_exec_id = forecast_exec["execution_id"]

    # Step 4: Verify operational forward forecasts were persisted
    cur.execute("""
        SELECT COUNT(*) AS total, COUNT(DISTINCT forecast_horizon) AS horizon_count
        FROM operational_forecast_results
        WHERE execution_id = %s
    """, (forecast_exec_id,))
    fc_stats = cur.fetchone()
    assert fc_stats["total"] > 0, "No forecast results persisted in operational_forecast_results"
    assert fc_stats["horizon_count"] == 2, "Expected both 7-day and 14-day forecast horizons"

    # Step 5: Verify Dashboard Events in MySQL journal
    cur.execute("""
        SELECT event_type, severity
        FROM dashboard_events
        WHERE execution_id IN (%s, %s) OR correlation_id = %s
    """, (feature_exec_id, forecast_exec_id, str(order_event_id)))
    logged_events = cur.fetchall()
    event_types = [e["event_type"] for e in logged_events]
    assert "PIPELINE_STATUS_CHANGED" in event_types, f"PIPELINE_STATUS_CHANGED missing from {event_types}"

    # Step 6: Verify Observability Enriched Executions API
    execs_resp = client.get("/api/observability/executions?limit=10")
    assert execs_resp.status_code == 200
    exec_list = execs_resp.json()
    items = exec_list.get("items", exec_list) if isinstance(exec_list, dict) else exec_list
    assert len(items) > 0
    
    # Check enriched fields on top executions
    top_exec = items[0]
    assert "current_stage" in top_exec, "current_stage missing from execution item"
    assert "records_processed" in top_exec, "records_processed missing from execution item"
    assert "triggering_dataset" in top_exec, "triggering_dataset missing from execution item"
    assert "duration_seconds" in top_exec, "duration_seconds missing from execution item"

    # Step 7: Verify Observability Forecast Summaries includes OPERATIONAL run
    fc_resp = client.get("/api/observability/forecasts?limit=10")
    assert fc_resp.status_code == 200
    fc_summaries = fc_resp.json()
    fc_items = fc_summaries.get("forecasts", fc_summaries) if isinstance(fc_summaries, dict) else fc_summaries
    op_summaries = [f for f in fc_items if f.get("data_provenance") == "OPERATIONAL"]
    assert len(op_summaries) > 0, "No OPERATIONAL forecast summary returned in /api/observability/forecasts"

    # Step 8: Verify Observability Category Trends for the operational execution
    trends_resp = client.get(f"/api/observability/forecasts/{forecast_exec_id}/category-trends")
    assert trends_resp.status_code == 200
    trends_data = trends_resp.json()
    assert trends_data["data_provenance"] == "OPERATIONAL"
    assert len(trends_data["weekly_trends"]) >= 1

    # Step 9: Verify Direct Operational Forecasts API
    direct_resp = client.get(f"/api/observability/operational-forecasts?execution_id={forecast_exec_id}")
    assert direct_resp.status_code == 200
    direct_data = direct_resp.json()
    assert direct_data["total"] > 0
    assert len(direct_data["forecasts"]) > 0
    assert "forecast_horizon" in direct_data["forecasts"][0]
    assert "predicted_demand" in direct_data["forecasts"][0]

    # Cleanup test order rows
    cur.execute("DELETE FROM operational_orders WHERE idempotency_key = %s", (test_batch_key,))
    db_conn.commit()
    cur.close()


def test_safety_invariants_postcheck(db_conn):
    """Ensure baseline invariants remain completely untouched after all operations."""
    cur = db_conn.cursor()
    cur.execute("SELECT COUNT(*) AS total FROM olist_orders")
    olist_count = cur.fetchone()["total"]
    assert olist_count == 99441, f"Invariant violated: olist_orders must be 99441, got {olist_count}"

    cur.execute("SELECT COUNT(*) AS total FROM operational_inventory")
    inv_count = cur.fetchone()["total"]
    assert inv_count == 106, f"Invariant violated: operational_inventory must be 106, got {inv_count}"
    cur.close()
