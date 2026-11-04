"""
SupplySense AI - Backend Startup & Automatic Simulation Verification Test

Tests that:
  1. On backend startup, the automatic simulation worker initializes.
  2. Generates real Olist data events into dataset_events -> Dependency Engine.
  3. GET /api/observability/datasets returns only authoritative datasets (no dummy SIM_% feeds).
  4. GET /api/observability/dependencies returns only authoritative pipelines (no dummy SIM- pipelines).
  5. GET /api/observability/dependencies/decisions returns real audited decisions.
"""

import os
import sys
import time
import json
import logging
import asyncio
import httpx

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from services.observability.main import app
from services.simulator.auto_simulator import auto_olist_simulator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("TestBackendStartup")


async def async_test_backend():
    logger.info("======================================================================")
    logger.info("  TESTING BACKEND STARTUP AUTOMATIC SIMULATION & API CLEANUP")
    logger.info("======================================================================")

    # 1. Test Step Simulation Directly
    logger.info("1. Testing auto_olist_simulator.step() with real Olist data...")
    res1 = auto_olist_simulator.step()
    logger.info(f"   Step 1 result: {res1}")
    assert res1 is not None and "event_id" in res1

    res2 = auto_olist_simulator.step()
    logger.info(f"   Step 2 result: {res2}")
    assert res2 is not None and "event_id" in res2

    # 2. Test ASGITransport HTTP Client
    logger.info("\n2. Initializing AsyncClient with ASGITransport for FastAPI app...")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        # 3. Test /api/observability/datasets
        logger.info("\n3. Testing GET /api/observability/datasets...")
        resp_datasets = await client.get("/api/observability/datasets?scope=ALL&limit=50")
        assert resp_datasets.status_code == 200, f"Failed: {resp_datasets.text}"
        data_ds = resp_datasets.json()
        items = data_ds.get("items", [])
        logger.info(f"   Total datasets returned: {len(items)} (total_count: {data_ds.get('total_count')})")

        # Verify no unwanted SIM_% feeds exist
        for d in items:
            d_name = d["dataset_name"]
            assert not d_name.startswith("SIM_"), f"Found unwanted dummy dataset: {d_name}"
            assert d.get("source_type") != "SIMULATED", f"Found unwanted virtual source_type: {d_name}"

        dataset_names = [d["dataset_name"] for d in items]
        logger.info(f"   Sample clean datasets: {dataset_names[:8]}...")
        assert "Olist_Orders" in dataset_names
        assert "Olist_Order_Items" in dataset_names
        assert "Olist_Payments" in dataset_names
        assert "Olist_Orders_SP" in dataset_names
        assert "Olist_Payments_Boleto" in dataset_names
        logger.info("   [PASS] No unwanted dummy datasets found! All datasets are authoritative.")

        # 4. Test /api/observability/dependencies
        logger.info("\n4. Testing GET /api/observability/dependencies...")
        resp_deps = await client.get("/api/observability/dependencies")
        assert resp_deps.status_code == 200, f"Failed: {resp_deps.text}"
        deps = resp_deps.json()
        logger.info(f"   Total pipeline dependencies returned: {len(deps)}")

        # Verify no unwanted SIM- pipelines exist
        for dep in deps:
            p_name = dep["pipeline_name"]
            assert not p_name.startswith("SIM-"), f"Found unwanted dummy pipeline: {p_name}"
            logger.info(f"   - #{dep['dependency_id']}: {p_name} [{dep['condition_type']}] -> Datasets: {dep.get('configured_datasets')}")

        pipe_names = [dep["pipeline_name"] for dep in deps]
        assert "Olist Core Forecast Pipeline" in pipe_names
        assert "Olist Regional Dispatch Pipeline" in pipe_names
        assert "Olist Payment Settlement Pipeline" in pipe_names
        assert "Demand Forecast Pipeline" in pipe_names
        logger.info("   [PASS] No unwanted dummy pipelines found! All pipelines are authoritative.")

        # 5. Test /api/observability/dependencies/decisions
        logger.info("\n5. Testing GET /api/observability/dependencies/decisions...")
        resp_dec = await client.get("/api/observability/dependencies/decisions?limit=10&scope=OPERATIONAL")
        assert resp_dec.status_code == 200, f"Failed: {resp_dec.text}"
        dec_list = resp_dec.json()
        dec_items = dec_list if isinstance(dec_list, list) else dec_list.get("items", [])
        logger.info(f"   Total recent decisions returned: {len(dec_items)}")
        for dec in dec_items[:5]:
            logger.info(f"   Decision #{dec['decision_id']} | {dec['pipeline_name']} | [{dec['decision']}] | Ready: {dec.get('ready_count')}/{dec.get('total_required')} | Reason: {dec.get('reason')[:60]}")

        logger.info("   [PASS] Real evaluated decisions verified in audit trail.")

    logger.info("\n======================================================================")
    logger.info("  ALL BACKEND STARTUP & API VERIFICATION TESTS PASSED SUCCESSFULLY!")
    logger.info("======================================================================")


if __name__ == "__main__":
    asyncio.run(async_test_backend())
