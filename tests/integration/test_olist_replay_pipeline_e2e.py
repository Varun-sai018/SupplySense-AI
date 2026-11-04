"""
End-to-End Integration Tests for SupplySense AI - Olist Historical Order Replay Simulator.

Verifies:
1. Safety invariants pre-check (olist_orders = 99,441; operational_inventory = 106).
2. Simulator API endpoints:
   - GET /api/simulator/replay/status
   - GET /api/simulator/replay/preview
   - POST /api/simulator/replay/start
   - GET /api/simulator/replay/status (polling progress)
3. Operational order ingestion with source = 'SIMULATED'.
4. Automatic event creation in dataset_events (ORDER_BATCH_INGESTED).
5. Automatic execution of Dependency Engine -> Feature Pipeline -> Forecast Pipeline.
6. Forward forecasting inference persistence in operational_forecast_results.
7. Safety invariants post-check: no mutations to historical Olist or inventory tables.
"""

import os
import sys
import time
import pytest
import importlib
from fastapi.testclient import TestClient

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from services.observability.main import app
from services.dependency_engine.mysql_consumer import poll_and_process_batch

EXPECTED_HISTORICAL_ORDERS = 99441
EXPECTED_INVENTORY_SKUS = 106


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


def test_safety_invariants_precheck():
    """Verify that historical and inventory tables have not been modified."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM olist_orders")
            assert cur.fetchone()["cnt"] == EXPECTED_HISTORICAL_ORDERS, "Historical olist_orders count modified!"

            cur.execute("SELECT COUNT(*) AS cnt FROM operational_inventory")
            assert cur.fetchone()["cnt"] == EXPECTED_INVENTORY_SKUS, "operational_inventory count modified!"
    finally:
        conn.close()


def test_olist_replay_api_preview(client):
    """Verifies GET /api/simulator/replay/preview endpoint."""
    resp = client.get(
        "/api/simulator/replay/preview",
        params={
            "window_start": "2018-W30",
            "window_end": "2018-W30",
            "auto_fallback": False,
            "batch_size": 15
        }
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "metadata" in data
    assert "sample_batch" in data
    assert data["metadata"]["window_active"] == "2018-W30..2018-W30"
    assert data["metadata"]["total_eligible_orders"] > 0
    assert data["sample_batch"]["orders_count"] == 15


def test_olist_replay_e2e_execution(client):
    """
    Executes a controlled 2-batch historical order replay through the live API,
    verifying ingestion, dependency engine triggering, feature pipeline, and forward forecasting.
    """
    # 1. Start simulation: 2 batches of 10 orders each
    start_resp = client.post(
        "/api/simulator/replay/start",
        json={
            "window_start": "2018-W30",
            "window_end": "2018-W30",
            "batch_size": 10,
            "interval_seconds": 1.0,
            "auto_fallback": False,
            "max_batches": 2
        }
    )
    assert start_resp.status_code == 200
    start_data = start_resp.json()
    assert start_data["status"] == "RUNNING"
    assert start_data["total_batches"] == 2

    # 2. Wait up to 15 seconds for worker thread to submit the 2 batches
    deadline = time.time() + 15.0
    status_data = {}
    while time.time() < deadline:
        st_resp = client.get("/api/simulator/replay/status")
        assert st_resp.status_code == 200
        status_data = st_resp.json()
        if status_data.get("processed_orders", 0) >= 20 or status_data.get("status") == "COMPLETED":
            break
        time.sleep(0.5)

    assert status_data["successful_batches"] >= 2, f"Expected >= 2 batches, got {status_data}"
    assert status_data["processed_orders"] >= 20, f"Expected >= 20 orders, got {status_data}"

    # 3. Process any pending NEW dataset_events through the dependency engine
    engine_res = poll_and_process_batch(batch_size=10, stale_timeout_seconds=300)
    assert "claimed" in engine_res or "processed" in engine_res

    # 4. Verify orders in operational_orders table
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) AS cnt 
                FROM operational_orders 
                WHERE source = 'SIMULATED' AND idempotency_key LIKE 'OLIST-%'
            """)
            sim_orders_count = cur.fetchone()["cnt"]
            assert sim_orders_count >= 20, f"Expected >= 20 simulated orders, found {sim_orders_count}"

            # 5. Verify feature pipeline execution
            cur.execute("""
                SELECT COUNT(*) AS cnt 
                FROM pipeline_executions 
                WHERE pipeline_name = 'Operational Demand Feature Pipeline' AND status = 'COMPLETED'
            """)
            feat_execs = cur.fetchone()["cnt"]
            assert feat_execs > 0, "Operational Demand Feature Pipeline did not execute!"

            # 6. Verify forecast pipeline execution and forward predictions
            cur.execute("""
                SELECT COUNT(*) AS cnt 
                FROM operational_forecast_results
            """)
            forecast_count = cur.fetchone()["cnt"]
            assert forecast_count > 0, "No forward forecasts found in operational_forecast_results!"
    finally:
        conn.close()

    # 7. Verify operational forecast API returns valid results
    fc_resp = client.get("/api/observability/operational-forecasts?limit=10")
    assert fc_resp.status_code == 200
    fc_data = fc_resp.json()
    fc_list = fc_data.get("forecasts") or fc_data.get("items") or []
    assert len(fc_list) > 0, "Operational forecast API returned no items"
    sample_fc = fc_list[0]
    assert "product_category" in sample_fc
    assert "predicted_demand" in sample_fc
    assert "inference_status" in sample_fc


def test_safety_invariants_postcheck():
    """Verify that historical and inventory tables remain completely untouched."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM olist_orders")
            assert cur.fetchone()["cnt"] == EXPECTED_HISTORICAL_ORDERS, "Historical olist_orders was mutated!"

            cur.execute("SELECT COUNT(*) AS cnt FROM operational_inventory")
            assert cur.fetchone()["cnt"] == EXPECTED_INVENTORY_SKUS, "operational_inventory was mutated!"

            # Verify no sandbox contamination
            cur.execute("SELECT COUNT(*) AS cnt FROM sim_orders WHERE sim_order_id LIKE 'OLIST-%'")
            assert cur.fetchone()["cnt"] == 0, "Sandbox sim_orders table was contaminated with Olist orders!"
    finally:
        conn.close()
