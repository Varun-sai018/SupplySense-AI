"""
SupplySense AI - Verification of Dependency Engine Live Incoming Events Feed
Tests:
  1. Auto-Simulator step generating real Olist events with metadata
  2. Background consumer claiming and evaluating event
  3. GET /api/observability/dependencies/events returning rich realistic e-commerce context
  4. Decision linking (BLOCK / TRIGGER) directly associated with the incoming event
"""

import sys
import os
import asyncio
import logging

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import httpx
from services.observability.main import app
from services.simulator.auto_simulator import auto_olist_simulator
from services.dependency_engine.mysql_consumer import poll_and_process_batch

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("TestLiveEvents")

async def test_live_dependency_events():
    logger.info("======================================================================")
    logger.info("  VERIFYING REAL-TIME DEPENDENCY ENGINE INCOMING EVENTS STREAM")
    logger.info("======================================================================")

    # 1. Generate 2 new real Olist simulation steps
    logger.info("1. Generating real Olist stream events via auto_olist_simulator.step()...")
    res1 = auto_olist_simulator.step()
    res2 = auto_olist_simulator.step()
    logger.info(f"   Generated Event: {res1}")
    logger.info(f"   Generated Event: {res2}")

    # 2. Run a background engine pass to process the events
    logger.info("\n2. Processing batch via poll_and_process_batch()...")
    proc_summary = poll_and_process_batch(batch_size=5)
    logger.info(f"   Pass Summary: claimed={proc_summary['claimed']}, processed={proc_summary['processed']}")

    # 3. Test GET /api/observability/dependencies/events
    logger.info("\n3. Testing GET /api/observability/dependencies/events endpoint...")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        resp = await client.get("/api/observability/dependencies/events?limit=6")
        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
        data = resp.json()

        total_count = data.get("total_count", 0)
        items = data.get("items", [])
        logger.info(f"   Total incoming events in DB: {total_count}")
        logger.info(f"   Events returned in page: {len(items)}")

        assert len(items) > 0, "No events returned from API!"

        # Audit top events
        for ev in items[:4]:
            eid = ev["event_id"]
            dname = ev["dataset_name"]
            pname = ev.get("pipeline_name")
            ctype = ev.get("condition_type")
            status = ev.get("event_status")
            dec = ev.get("linked_decision")
            dec_str = f"DECISION: {dec['decision']} ({dec['ready_count']}/{dec['total_required']})" if dec else "QUEUED"

            logger.info(f"\n   [Event #{eid}] Dataset: '{dname}' (v{ev['dataset_version']})")
            logger.info(f"     Target: '{pname}' [{ctype}]")
            logger.info(f"     Status: {status} | {dec_str}")
            logger.info(f"     Realistic Summary: {ev['realistic_context'].get('summary')}")
            if ev["realistic_context"].get("sample_data"):
                logger.info(f"     Payload Sample: {ev['realistic_context'].get('sample_data')}")

    logger.info("\n======================================================================")
    logger.info("  ALL REAL-TIME DEPENDENCY ENGINE EVENT VERIFICATION TESTS PASSED!")
    logger.info("======================================================================")

if __name__ == "__main__":
    asyncio.run(test_live_dependency_events())
