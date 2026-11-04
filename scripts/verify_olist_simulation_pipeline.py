"""
SupplySense AI - Olist Kafka Streaming & Dependency Pipeline Verification

End-to-End Verification Script:
  1. Validates Part 1 (Historical) and Part 2 (Simulation) dataset splits.
  2. Verifies the trained XGBoost model artifact (Part 1).
  3. Verifies Kafka broker health (localhost:9092, dataset-events topic).
  4. Runs the Dependency Engine Kafka consumer in a background worker thread.
  5. Runs the Olist Kafka Simulator to stream Part 2 events at seconds-based intervals.
  6. Verifies that the Dependency Engine receives events from Kafka, evaluates dependencies,
     and records decisions/executions in MySQL.
"""

import os
import sys
import time
import json
import logging
import threading
from datetime import datetime

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from config import settings
from scripts.simulate_olist_kafka import OlistKafkaSimulator
from services.dependency_engine.main import run_kafka_consumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("OlistVerification")


def verify_pipeline():
    logger.info("======================================================================")
    logger.info("  SUPPLYSENSE AI - OLIST DATA PIPELINE & KAFKA VERIFICATION")
    logger.info("======================================================================")

    # -------------------------------------------------------------------------
    # 1. Verify Dataset Splits
    # -------------------------------------------------------------------------
    hist_orders = os.path.join(REPO_ROOT, "data", "olist_split", "historical", "olist_orders_dataset.csv")
    sim_orders = os.path.join(REPO_ROOT, "data", "olist_split", "simulation", "olist_orders_dataset.csv")
    manifest_file = os.path.join(REPO_ROOT, "data", "olist_split", "split_manifest.json")

    assert os.path.exists(hist_orders), f"Missing Part 1 Historical orders: {hist_orders}"
    assert os.path.exists(sim_orders), f"Missing Part 2 Simulation orders: {sim_orders}"
    assert os.path.exists(manifest_file), f"Missing split manifest: {manifest_file}"

    with open(manifest_file, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    logger.info("[PASS] Part 1 (Historical Data):")
    logger.info(f"       Orders: {manifest['historical']['orders_count']:,} | Date Span: {manifest['historical']['date_range']}")
    logger.info("[PASS] Part 2 (Simulation Stream Data):")
    logger.info(f"       Orders: {manifest['simulation']['orders_count']:,} | Date Span: {manifest['simulation']['date_range']}")

    # -------------------------------------------------------------------------
    # 2. Verify XGBoost Model Artifact
    # -------------------------------------------------------------------------
    model_path = os.path.join(REPO_ROOT, "ml", "models", "xgboost_demand_model.json")
    metrics_path = os.path.join(REPO_ROOT, "ml", "results", "xgboost_metrics.json")
    assert os.path.exists(model_path), f"Missing XGBoost model: {model_path}"
    assert os.path.exists(metrics_path), f"Missing XGBoost metrics: {metrics_path}"

    with open(metrics_path, "r", encoding="utf-8") as f:
        metrics = json.load(f)

    test_metrics = metrics.get("test", {})
    logger.info("[PASS] XGBoost Model Verified (Trained on Historical Data):")
    logger.info(f"       Model: {model_path}")
    logger.info(f"       Test Metrics -> R2: {test_metrics.get('r2')}, MAE: {test_metrics.get('mae')}, RMSE: {test_metrics.get('rmse')}")

    # -------------------------------------------------------------------------
    # 3. Verify Kafka Connectivity
    # -------------------------------------------------------------------------
    from kafka import KafkaAdminClient
    admin = KafkaAdminClient(bootstrap_servers=settings.KAFKA_BROKER, request_timeout_ms=5000)
    topics = admin.list_topics()
    admin.close()
    assert settings.KAFKA_TOPIC_EVENTS in topics, f"Kafka topic '{settings.KAFKA_TOPIC_EVENTS}' not found in {topics}"
    logger.info(f"[PASS] Kafka Broker Online ({settings.KAFKA_BROKER}) | Topic '{settings.KAFKA_TOPIC_EVENTS}' active.")

    # -------------------------------------------------------------------------
    # 4. Start Dependency Engine Consumer Thread
    # -------------------------------------------------------------------------
    stop_event = threading.Event()
    dep_engine_thread = threading.Thread(
        target=run_kafka_consumer,
        kwargs={"stop_event": stop_event},
        name="DependencyEngine-KafkaConsumer",
        daemon=True,
    )
    dep_engine_thread.start()
    logger.info("[PASS] Dependency Engine Kafka consumer started in background thread.")
    time.sleep(1.5)  # Allow consumer group registration

    # Get baseline count of decisions
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) AS cnt FROM pipeline_decisions")
    initial_decisions_cnt = c.fetchone()["cnt"]
    c.execute("SELECT COUNT(*) AS cnt FROM pipeline_executions")
    initial_executions_cnt = c.fetchone()["cnt"]
    conn.close()

    # -------------------------------------------------------------------------
    # 5. Run Olist Kafka Simulator (Stream 3 events, interval 2.0s)
    # -------------------------------------------------------------------------
    logger.info("\n--- Launching Simulation Stream ---")
    interval = 2.0
    num_events = 3
    simulator = OlistKafkaSimulator(
        interval_seconds=interval,
        batch_size=10,
        max_events=num_events,
        target_pipeline="Demand Forecast Pipeline",
        cycle_dependencies=True,
    )
    sim_start_time = time.time()
    simulator.run()
    sim_duration = time.time() - sim_start_time
    logger.info(f"Simulation producer completed in {sim_duration:.2f}s ({num_events} events sent at {interval}s intervals).")

    # Give consumer a moment to process the final message
    logger.info("Waiting 3.0s for consumer to finalize pipeline decisions...")
    time.sleep(3.0)

    # Signal stop to consumer
    stop_event.set()
    dep_engine_thread.join(timeout=3.0)

    # -------------------------------------------------------------------------
    # 6. Verify Processing in MySQL
    # -------------------------------------------------------------------------
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) AS cnt FROM pipeline_decisions")
    new_decisions_cnt = c.fetchone()["cnt"]
    c.execute("SELECT COUNT(*) AS cnt FROM pipeline_executions")
    new_executions_cnt = c.fetchone()["cnt"]

    c.execute("""
        SELECT decision_id, pipeline_name, condition_type, decision, ready_count, total_required, reason, triggered_at
        FROM pipeline_decisions
        ORDER BY decision_id DESC
        LIMIT 5
    """)
    recent_decisions = c.fetchall()

    c.execute("""
        SELECT execution_id, pipeline_name, status, orchestrator, started_at, completed_at
        FROM pipeline_executions
        ORDER BY execution_id DESC
        LIMIT 3
    """)
    recent_executions = c.fetchall()
    conn.close()

    logger.info("======================================================================")
    logger.info("  VERIFICATION AUDIT RESULTS")
    logger.info("======================================================================")
    logger.info(f"Total Pipeline Decisions Added : {new_decisions_cnt - initial_decisions_cnt}")
    logger.info(f"Total Pipeline Executions Added: {new_executions_cnt - initial_executions_cnt}")

    logger.info("\nRecent Pipeline Decisions:")
    for d in recent_decisions:
        logger.info(f"  Decision #{d['decision_id']} | {d['pipeline_name']} | Decision: {d['decision']} | Ready: {d['ready_count']}/{d['total_required']} | Reason: {d['reason'][:60]}")

    logger.info("\nRecent Pipeline Executions:")
    for e in recent_executions:
        logger.info(f"  Execution #{e['execution_id']} | {e['pipeline_name']} | Status: {e['status']} | Orchestrator: {e['orchestrator']}")

    assert (new_decisions_cnt - initial_decisions_cnt) >= 1, "Expected at least 1 new decision processed by Dependency Engine!"

    logger.info("\n>>> ALL VERIFICATION CHECKS PASSED SUCCESSFULLY! <<<")


if __name__ == "__main__":
    verify_pipeline()
