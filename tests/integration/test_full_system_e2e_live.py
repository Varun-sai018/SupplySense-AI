"""
SupplySense AI - Comprehensive End-to-End Live Verification Test

Tests the entire pipeline workflow against live services:
1. Historical Olist Replay Simulation triggering orders.
2. Order Ingestion into operational_orders (source = 'SIMULATED').
3. Dataset event publishing into dataset_events.
4. Hybrid Dependency Engine event claiming and downstream pipeline resolution.
5. Pipeline Runner execution of Feature Pipeline and Forecast Pipeline.
6. Real XGBoost model forward demand forecasting (7-day and 14-day horizons).
7. Persistence in operational_forecast_results.
8. Live frontend API endpoints (summary, category trends, executions, forecasts, events).
9. Simulator controls (start, pause, resume, stop, preview, status).
10. Database invariants: olist_orders == 99,441; operational_inventory == 106.
"""

import os
import sys
import time
import json
import pytest
import urllib.request
import urllib.parse

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection

BASE_URL = "http://127.0.0.1:8000"
VITE_URL = "http://127.0.0.1:5173"


def http_get(path, timeout=10):
    url = f"{BASE_URL}{path}"
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, json.loads(resp.read().decode())


def http_post(path, payload=None, timeout=10):
    url = f"{BASE_URL}{path}"
    data = json.dumps(payload or {}).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'}, method='POST')
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, json.loads(resp.read().decode())


def test_01_backend_and_frontend_health():
    """Verify backend and frontend servers are healthy and responding."""
    status, body = http_get("/health")
    assert status == 200
    assert body.get("status") == "healthy"
    assert body.get("database") == "connected"

    # Test Vite frontend proxy to backend
    req = urllib.request.Request(f"{VITE_URL}/health")
    with urllib.request.urlopen(req, timeout=5) as resp:
        assert resp.status == 200
        vite_body = json.loads(resp.read().decode())
        assert vite_body.get("status") == "healthy"


def test_02_database_safety_invariants_pre():
    """Verify strict non-destructive safety invariants before execution."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM olist_orders")
            assert cur.fetchone()["cnt"] == 99441, "Safety Invariant Violated: olist_orders must be 99441"
            cur.execute("SELECT COUNT(*) AS cnt FROM operational_inventory")
            assert cur.fetchone()["cnt"] == 106, "Safety Invariant Violated: operational_inventory must be 106"
    finally:
        conn.close()


def test_03_simulator_preview_and_controls():
    """Verify simulator preview, eligible orders count, and status."""
    status, preview = http_get("/api/simulator/replay/preview")
    assert status == 200
    assert "sample_batch" in preview
    assert len(preview["sample_batch"]) > 0

    status, sim_status = http_get("/api/simulator/replay/status")
    assert status == 200
    assert "status" in sim_status
    assert sim_status["status"] in ("IDLE", "COMPLETED", "STOPPED")


def test_04_live_simulation_and_pipeline_execution():
    """
    Triggers a live simulation batch:
    - Simulator sends real Olist order items to POST /api/observability/orders.
    - Dataset event emitted.
    - Background Dependency Engine picks up event and runs Feature + Forecast pipelines.
    - XGBoost model predicts forward forecasts.
    """
    # 1. Start simulation with 2 batches of 10 orders
    status, start_resp = http_post("/api/simulator/replay/start", {
        "batch_size": 10,
        "interval_sec": 0.5,
        "max_batches": 2
    })
    assert status == 200
    assert start_resp.get("status") in ("STARTING", "RUNNING")
    run_id = start_resp.get("run_id")

    # 2. Wait for simulation to finish (up to 15 seconds)
    completed = False
    for _ in range(30):
        time.sleep(0.5)
        st, cur_status = http_get("/api/simulator/replay/status")
        if cur_status.get("status") in ("COMPLETED", "STOPPED"):
            completed = True
            assert cur_status.get("successful_batches", 0) >= 1
            assert cur_status.get("processed_orders", 0) > 0
            break

    assert completed, "Simulation did not complete within timeout"

    # 3. Wait for background Dependency Engine to process dataset event (up to 20 seconds)
    engine_processed = False
    latest_fc_exec_id = None
    for _ in range(40):
        time.sleep(0.5)
        st, execs = http_get("/api/observability/executions?scope=ALL&limit=10")
        if st == 200 and "items" in execs:
            for item in execs["items"]:
                if (
                    item.get("pipeline_name") == "Operational Demand Forecast Pipeline"
                    and item.get("status") == "COMPLETED"
                ):
                    latest_fc_exec_id = item.get("execution_id")
                    engine_processed = True
                    break
        if engine_processed:
            break

    assert engine_processed, "Dependency Engine did not complete Operational Forecast Pipeline"
    assert latest_fc_exec_id is not None


def test_05_verify_database_operational_records():
    """Verify that orders, demand grid, features, and forecasts were created in MySQL."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # 1. Check operational_orders has simulated records
            cur.execute("SELECT COUNT(*) AS cnt FROM operational_orders WHERE source = 'SIMULATED'")
            sim_orders_cnt = cur.fetchone()["cnt"]
            assert sim_orders_cnt > 0, "No simulated orders found in operational_orders"

            # 2. Check operational_demand_weekly has rows
            cur.execute("SELECT COUNT(*) AS cnt FROM operational_demand_weekly")
            demand_cnt = cur.fetchone()["cnt"]
            assert demand_cnt > 0, "No weekly demand aggregated"

            # 3. Check operational_feature_store has rows with 11 features
            cur.execute("""
                SELECT lag_1_demand, lag_2_demand, lag_4_demand, rolling_mean_4w, rolling_std_4w,
                       product_category_enc, quarter, month_of_year, week_of_year
                FROM operational_feature_store
                LIMIT 5
            """)
            feats = cur.fetchall()
            assert len(feats) > 0, "No feature rows in operational_feature_store"
            for f in feats:
                assert f["product_category_enc"] is not None
                assert f["month_of_year"] is not None

            # 4. Check operational_forecast_results has forward predictions
            cur.execute("""
                SELECT operational_forecast_id, execution_id, product_category, step_ahead,
                       forecast_horizon, forecast_week, predicted_demand,
                       model_name, model_version, inference_status, data_provenance
                FROM operational_forecast_results
                ORDER BY operational_forecast_id DESC
                LIMIT 10
            """)
            forecasts = cur.fetchall()
            assert len(forecasts) > 0, "No forecast results in operational_forecast_results"

            sample_fc = forecasts[0]
            assert sample_fc["model_name"] == "XGBoost"
            assert sample_fc["model_version"] == "xgboost-v1"
            assert sample_fc["data_provenance"] == "OPERATIONAL"
            assert sample_fc["predicted_demand"] >= 0.0
            assert sample_fc["step_ahead"] in (1, 2)
            assert sample_fc["forecast_horizon"] in ("7_DAYS", "14_DAYS")
    finally:
        conn.close()


def test_06_frontend_api_endpoints_reflection():
    """
    Verify that frontend dashboard API endpoints reflect the new operational forecasts
    and pipeline executions.
    """
    # 1. Operational summary (inventory, replenishments, suppliers)
    st, summary = http_get("/api/observability/operational-summary")
    assert st == 200
    assert "inventory" in summary
    assert "replenishments" in summary
    assert "suppliers" in summary
    assert summary["inventory"]["total_skus"] == 106

    # 1b. Observability pipeline execution summary
    st, obs_summary = http_get("/api/observability/summary")
    assert st == 200
    assert "executions" in obs_summary
    assert obs_summary["executions"]["completed"] > 0
    assert "latest_execution" in obs_summary

    # 2. Operational forecasts list
    st, fc_list = http_get("/api/observability/operational-forecasts?limit=10")
    assert st == 200
    assert len(fc_list.get("forecasts", [])) > 0
    first_fc = fc_list["forecasts"][0]
    assert "category_english" in first_fc
    assert "predicted_demand" in first_fc

    # 3. Category trends for DemandForecastingWorkspace
    st, exec_list = http_get("/api/observability/forecasts?scope=ALL")
    assert st == 200
    op_exec = next((e for e in exec_list if e.get("pipeline_name") == "Operational Demand Forecast Pipeline"), None)
    assert op_exec is not None, "No operational forecast execution in forecasts list"

    exec_id = op_exec["execution_id"]
    st, trends_data = http_get(f"/api/observability/forecasts/{exec_id}/category-trends")
    assert st == 200
    assert trends_data.get("data_provenance") == "OPERATIONAL"
    assert "weekly_trends" in trends_data
    assert len(trends_data["weekly_trends"]) >= 1, "weekly_trends should have points for Recharts"
    assert "available_categories" in trends_data
    assert len(trends_data["available_categories"]) > 0

    # Verify each trend point has week_label and numeric predictions
    for pt in trends_data["weekly_trends"]:
        assert "week_label" in pt
        assert "forecast_week" in pt


def test_07_simulator_pause_resume_stop_lifecycle():
    """Verify pause, resume, and stop controls."""
    # Start simulator with larger batches so we can pause/stop
    st, _ = http_post("/api/simulator/replay/start", {
        "batch_size": 5,
        "interval_sec": 3.0,
        "max_batches": 10
    })
    assert st == 200

    time.sleep(0.5)
    # Pause
    st, pause_resp = http_post("/api/simulator/replay/pause")
    assert st == 200
    assert pause_resp.get("status") == "PAUSED"

    time.sleep(0.5)
    # Resume
    st, resume_resp = http_post("/api/simulator/replay/resume")
    assert st == 200
    assert resume_resp.get("status") == "RUNNING"

    time.sleep(0.5)
    # Stop
    st, stop_resp = http_post("/api/simulator/replay/stop")
    assert st == 200
    assert stop_resp.get("status") == "STOPPED"


def test_08_database_safety_invariants_post():
    """Verify strict safety invariants remain intact after the complete run."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM olist_orders")
            assert cur.fetchone()["cnt"] == 99441, "Invariant Failed: olist_orders modified!"
            cur.execute("SELECT COUNT(*) AS cnt FROM operational_inventory")
            assert cur.fetchone()["cnt"] == 106, "Invariant Failed: operational_inventory modified!"
    finally:
        conn.close()
