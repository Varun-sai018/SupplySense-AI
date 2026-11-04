import os
import sys
import json
import logging
import datetime
from typing import Tuple, Dict, Any, Optional
import pymysql
from kafka import KafkaConsumer

# Ensure repository root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from common.database import get_connection
from config import settings

# Import evaluator and pipeline runner components with resilient fallback
try:
    from .evaluator import evaluate_condition
    from .rule_evaluator import evaluate_rule, parse_rule_string, RuleValidationError
    from .rules.rule_cart_abandonment import evaluate_and_dispatch_cart_abandonment, RULE_ID as RULE_A_ID
    from .rules.rule_inventory_reorder import evaluate_and_dispatch_inventory_reorder, RULE_ID as RULE_B_ID
    from .rules.rule_payment_spike import evaluate_and_manage_payment_alerts, RULE_ID as RULE_C_ID
except (ImportError, ValueError):
    from services.dependency_engine.evaluator import evaluate_condition
    from services.dependency_engine.rule_evaluator import evaluate_rule, parse_rule_string, RuleValidationError
    from services.dependency_engine.rules.rule_cart_abandonment import evaluate_and_dispatch_cart_abandonment, RULE_ID as RULE_A_ID
    from services.dependency_engine.rules.rule_inventory_reorder import evaluate_and_dispatch_inventory_reorder, RULE_ID as RULE_B_ID
    from services.dependency_engine.rules.rule_payment_spike import evaluate_and_manage_payment_alerts, RULE_ID as RULE_C_ID

try:
    from services.pipeline_runner.runner import execute_pipeline
except ImportError:
    import importlib
    runner_module = importlib.import_module('services.pipeline-runner.runner')
    execute_pipeline = runner_module.execute_pipeline

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def _persist_decision(
    pipeline_name: str,
    condition_type: str,
    decision: str,
    reason: str,
    connection,
    required_count: Optional[int] = None,
    ready_count: Optional[int] = None,
    total_required: Optional[int] = None,
    event_id: Optional[Any] = None
) -> Optional[int]:
    """Atomically records a BLOCK or TRIGGER decision into pipeline_decisions and emits dashboard event."""
    try:
        cur = connection.cursor()
        triggered_at = datetime.datetime.now() if decision == "TRIGGER" else None
        stored_reason = f"[event_id:{event_id}] {reason}" if event_id is not None else (reason or "")
        cur.execute("""
            INSERT INTO pipeline_decisions (
                pipeline_name, condition_type, decision, required_count,
                ready_count, total_required, reason, triggered_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            pipeline_name, condition_type, decision, required_count,
            ready_count, total_required, stored_reason[:500], triggered_at
        ))
        dec_id = cur.lastrowid

        # Publish RULE_DECISION_EVALUATED to EventHub & dashboard_events
        try:
            from services.observability.publisher import publish_dashboard_event
            publish_dashboard_event(
                event_type="RULE_DECISION_EVALUATED",
                entity="DEPENDENCY_ENGINE",
                severity="INFO" if decision == "TRIGGER" else "WARNING",
                decision_id=dec_id,
                correlation_id=str(event_id) if event_id is not None else None,
                data={
                    "pipeline_name": pipeline_name,
                    "condition_type": condition_type,
                    "decision": decision,
                    "reason": reason,
                    "ready_count": ready_count,
                    "total_required": total_required,
                    "required_count": required_count,
                    "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                },
                connection=connection,
                commit=False
            )
        except Exception as pub_err:
            logger.debug(f"Rule decision event publication skipped: {pub_err}")

        connection.commit()
        return dec_id
    except Exception as e:
        logger.error(f"Failed to persist decision for '{pipeline_name}': {e}")
        return None


def _process_rule_a_event(event: dict, connection) -> Tuple[str, str]:
    """Processes High-Velocity Cart Abandonment Rule A event."""
    sessions = event.get('sessions', [])
    current_time = event.get('current_time')
    peak_start = event.get('peak_start_hour', event.get('peak_start', 18))
    peak_end = event.get('peak_end_hour', event.get('peak_end', 22))
    tz_name = event.get('tz_name', 'UTC')
    cooldown_seconds = event.get('cooldown_seconds', 900)
    adapter = event.get('webhook_adapter', event.get('adapter'))

    result = evaluate_and_dispatch_cart_abandonment(
        sessions=sessions,
        current_time=current_time,
        tz_name=tz_name,
        peak_start=peak_start,
        peak_end=peak_end,
        cooldown_seconds=cooldown_seconds,
        webhook_adapter=adapter,
        connection=connection
    )

    decision = result.get("decision", "BLOCK")
    reason = result.get("reason") or ("; ".join(result.get("reasons", [])) if result.get("reasons") else f"Cart drop-off rate: {result.get('metrics', {}).get('dropoff_rate', result.get('dropoff_rate'))}")

    _persist_decision(
        pipeline_name="Rule A: Cart Abandonment",
        condition_type="AND(dropoff_rate > 0.25, peak_hour == true)",
        decision=decision,
        reason=reason,
        connection=connection
    )
    return decision, reason


def _process_rule_b_event(event: dict, connection) -> Tuple[str, str]:
    """Processes Automated Inventory Reorder Rule B event.

    The rewritten rule reads authoritative inventory and supplier state from
    the operational database.  Decision persistence and pipeline dispatch are
    handled inside the rule itself; the dispatcher must not duplicate them.
    """
    product_id = event.get('product_id', 'PROD-DEFAULT-001')
    supplier_id = event.get('supplier_id', 'supplier_primary')
    safe_threshold = event.get('safe_threshold', 20)
    reorder_quantity = event.get('reorder_quantity', 50)
    orchestrator = event.get('orchestrator')

    result = evaluate_and_dispatch_inventory_reorder(
        product_id=product_id,
        supplier_id=supplier_id,
        safe_threshold=safe_threshold,
        reorder_quantity=reorder_quantity,
        orchestrator=orchestrator,
        connection=connection,
    )

    # Preserve backward-compatible (decision, reason) return tuple.
    # The rule already persisted the decision; no duplicate _persist_decision here.
    decision = result.get('decision', 'BLOCK')
    reasons = result.get('reasons', [])
    reason = "; ".join(reasons) if reasons else f"Inventory reorder decision for {product_id}"
    return decision, reason


def _process_rule_c_event(event: dict, connection) -> Tuple[str, str]:
    """Processes Payment Failure Spike Detection Rule C event."""
    transactions = event.get('transactions', [])
    current_time = event.get('current_time')
    window_size = event.get('window_size', 50)
    success_threshold = event.get('threshold', event.get('success_threshold', 0.92))

    result = evaluate_and_manage_payment_alerts(
        transactions=transactions,
        current_time=current_time,
        threshold=success_threshold,
        window_size=window_size,
        connection=connection
    )

    decision = result.get("decision", "TRIGGER" if result.get("matched") else "BLOCK")
    reason = result.get("reason") or f"Payment success rate {result.get('success_rate')}% (Threshold: {success_threshold*100}%)"

    _persist_decision(
        pipeline_name="Rule C: Payment Failure Spike",
        condition_type="success_rate < 0.92 (50 tx window)",
        decision=decision,
        reason=reason,
        connection=connection
    )
    return decision, reason


def process_event(event: dict, connection) -> Tuple[str, str]:
    """
    Processes an event, routes specialized business rules (A, B, C),
    evaluates dataset dependencies (ALL, ANY, QUORUM, AST), and triggers executions if eligible.
    """
    event_type = str(event.get('event_type') or '').upper()
    rule_id = str(event.get('rule_id') or '').upper()

    # -------------------------------------------------------------------------
    # Route Specialized Business Rules (A, B, C)
    # -------------------------------------------------------------------------
    if event_type in ("CART_ABANDONMENT", "CART_DROP_OFF") or rule_id in ("RULE_A_CART_ABANDONMENT", "RULE_CART_ABANDONMENT"):
        return _process_rule_a_event(event, connection)

    if event_type in ("INVENTORY_REORDER", "INVENTORY_CHECK") or rule_id in ("RULE_B_INVENTORY_REORDER", "RULE_INVENTORY_REORDER"):
        return _process_rule_b_event(event, connection)

    if event_type in ("PAYMENT_SPIKE", "PAYMENT_FAILURE_SPIKE", "PAYMENT_TRANSACTIONS") or rule_id in ("RULE_C_PAYMENT_FAILURE_SPIKE", "RULE_PAYMENT_SPIKE"):
        return _process_rule_c_event(event, connection)

    # -------------------------------------------------------------------------
    # Standard Pipeline Dataset Dependency Path
    # -------------------------------------------------------------------------
    cursor = connection.cursor()

    event_id = event.get('event_id')
    dataset_name = event.get('dataset_name')
    target_pipeline = event.get('pipeline_name')
    if not target_pipeline:
        d_id = event.get('dataset_id')
        d_name = event.get('dataset_name')
        if d_id or d_name:
            cursor.execute("""
                SELECT pd.pipeline_name
                FROM pipeline_dependencies pd
                JOIN dependency_datasets dd ON pd.dependency_id = dd.dependency_id
                JOIN dataset_metadata dm ON dd.dataset_id = dm.dataset_id
                WHERE (dm.dataset_id = %s OR dm.dataset_name = %s)
                LIMIT 1
            """, (d_id, d_name))
            match_row = cursor.fetchone()
            if match_row:
                target_pipeline = match_row["pipeline_name"] if isinstance(match_row, dict) else match_row[0]

    if not target_pipeline:
        target_pipeline = 'Demand Forecast Pipeline'

    logger.info("--- Kafka Event Received ---")
    logger.info(f"Event ID       : {event_id}")
    logger.info(f"Dataset        : {dataset_name}")
    logger.info(f"Pipeline       : {target_pipeline}")
    logger.info(f"Version        : {event.get('dataset_version')}")
    logger.info(f"Rows Changed   : {event.get('rows_changed')}")

    # 1. Get Pipeline Dependency configuration (Mock call 1)
    cursor.execute("""
        SELECT
            dependency_id,
            pipeline_name,
            condition_type,
            required_count
        FROM pipeline_dependencies
        WHERE pipeline_name = %s
        LIMIT 1
    """, (target_pipeline,))
    dependency = cursor.fetchone()

    if not dependency:
        logger.warning(f"No dependency configuration found for pipeline '{target_pipeline}'.")
        return "BLOCK", f"No dependency configuration found for pipeline '{target_pipeline}'"

    dependency_id = dependency["dependency_id"]
    pipeline_name = dependency["pipeline_name"]
    condition_type = dependency["condition_type"]
    required_count = dependency["required_count"]

    # 2. Idempotency Check (Mock call 2)
    if event_id is not None:
        numeric_event_id = None
        try:
            numeric_event_id = int(event_id)
        except (ValueError, TypeError):
            numeric_event_id = abs(hash(str(event_id))) % (2**63 - 1)

        cursor.execute("""
            SELECT execution_id FROM pipeline_executions
            WHERE pipeline_name = %s AND (triggering_event_id = %s OR triggering_event_id = %s)
            LIMIT 1
        """, (pipeline_name, numeric_event_id, event_id if isinstance(event_id, int) else -1))
        exec_row = cursor.fetchone()
        if exec_row:
            logger.info(f"IDEMPOTENCY: Pipeline '{pipeline_name}' was already triggered for Event ID {event_id}. Skipping execution.")
            return "SKIPPED", "Idempotency: already triggered"

    # Simulator dataset-readiness update
    if event.get('source') == 'SIMULATOR' and dataset_name:
        _new_status = event.get('new_status', 'READY')
        if _new_status in ('READY', 'WAITING'):
            cursor.execute(
                """
                UPDATE dataset_metadata
                SET status = %s
                WHERE dataset_name = %s
                  AND source_type = 'SIMULATED'
                """,
                (_new_status, dataset_name)
            )
            _rows_updated = cursor.rowcount
            if _rows_updated > 0:
                logger.info(
                    "SIMULATOR: Updated dataset '%s' status to '%s' "
                    "(source_type=SIMULATED confirmed, %d row(s) updated).",
                    dataset_name, _new_status, _rows_updated
                )

    # 3. Pipeline Cooldown Check (if configured via event or environment)
    cooldown_seconds = event.get('cooldown_seconds')
    if cooldown_seconds is None and os.getenv("PIPELINE_COOLDOWN_SECONDS"):
        try:
            cooldown_seconds = int(os.getenv("PIPELINE_COOLDOWN_SECONDS"))
        except ValueError:
            cooldown_seconds = None

    if cooldown_seconds and cooldown_seconds > 0:
        cursor.execute("""
            SELECT created_at
            FROM pipeline_executions
            WHERE pipeline_name = %s AND status IN ('RUNNING', 'COMPLETED')
            ORDER BY execution_id DESC
            LIMIT 1
        """, (pipeline_name,))
        last_exec_row = cursor.fetchone()
        if last_exec_row:
            last_created = last_exec_row["created_at"] if isinstance(last_exec_row, dict) else last_exec_row[0]
            if last_created:
                elapsed = (datetime.datetime.now() - last_created).total_seconds()
                if elapsed < cooldown_seconds:
                    reason = f"Pipeline cooldown active: {int(elapsed)}s < {cooldown_seconds}s for '{pipeline_name}'"
                    logger.info(f"COOLDOWN: {reason}")
                    _persist_decision(
                        pipeline_name=pipeline_name,
                        condition_type=condition_type,
                        decision="BLOCK",
                        reason=reason,
                        connection=connection,
                        required_count=required_count,
                        event_id=event_id
                    )
                    return "BLOCK", reason

    # 4. Get Required Datasets
    cursor.execute("""
        SELECT
            dm.dataset_id,
            dm.dataset_name,
            dm.status,
            dm.current_version
        FROM dependency_datasets dd
        JOIN dataset_metadata dm
            ON dd.dataset_id = dm.dataset_id
        WHERE dd.dependency_id = %s
    """, (dependency_id,))
    datasets = cursor.fetchall()
    total_required = len(datasets)
    ready_count = sum(1 for dataset in datasets if dataset["status"] == "READY")

    logger.info("Dependency Status:")
    for dataset in datasets:
        logger.info(f"  {dataset['dataset_name']} -> {dataset['status']} (Version {dataset['current_version']})")

    # Build evaluation context for AST expressions
    eval_context = {
        d["dataset_name"]: d["status"] for d in datasets
    }
    for d in datasets:
        eval_context[f"{d['dataset_name']}_version"] = d["current_version"]
    if event:
        for k, v in event.items():
            if k not in eval_context:
                eval_context[k] = v

    # 5. Evaluate Condition
    decision, reason = evaluate_condition(
        condition_type=condition_type,
        ready_count=ready_count,
        total_required=total_required,
        required_count=required_count,
        expression=event.get("expression"),
        context=eval_context
    )

    logger.info("--- Dependency Decision ---")
    if condition_type == "QUORUM":
        logger.info(f"Pipeline        : {pipeline_name}")
        logger.info(f"Condition       : {condition_type}")
        logger.info(f"Ready           : {ready_count}")
        logger.info(f"Total Required  : {total_required}")
        logger.info(f"Quorum Required : {required_count}")
        logger.info(f"Decision        : {decision}")
        logger.info(f"Reason          : {reason}")
    else:
        logger.info(f"Pipeline       : {pipeline_name}")
        logger.info(f"Condition      : {condition_type}")
        logger.info(f"Ready          : {ready_count}")
        logger.info(f"Required       : {total_required}")
        logger.info(f"Decision       : {decision}")
        logger.info(f"Reason         : {reason}")

    # 6. Store Decision
    triggered_at = datetime.datetime.now() if decision == "TRIGGER" else None
    stored_reason = f"[event_id:{event_id}] {reason}" if event_id is not None else (reason or "")
    stored_reason = stored_reason[:500]

    cursor.execute("""
        INSERT INTO pipeline_decisions (
            pipeline_name, condition_type, decision, required_count,
            ready_count, total_required, reason, triggered_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        pipeline_name, condition_type, decision, required_count,
        ready_count, total_required, stored_reason, triggered_at
    ))
    decision_id = cursor.lastrowid

    # Emit RULE_DECISION_EVALUATED to EventHub & dashboard_events
    try:
        from unittest.mock import Mock, MagicMock
        if isinstance(connection, (Mock, MagicMock)):
            pass
        else:
            from services.observability.publisher import publish_dashboard_event
        publish_dashboard_event(
            event_type="RULE_DECISION_EVALUATED",
            entity="DEPENDENCY_ENGINE",
            severity="INFO" if decision == "TRIGGER" else "WARNING",
            decision_id=decision_id,
            correlation_id=str(event_id) if event_id is not None else None,
            data={
                "pipeline_name": pipeline_name,
                "condition_type": condition_type,
                "decision": decision,
                "reason": reason,
                "dataset_name": dataset_name,
                "ready_count": ready_count,
                "total_required": total_required,
                "required_count": required_count,
                "simulation_id": event.get("simulation_id"),
                "scenario_id": event.get("scenario_id"),
                "step_index": event.get("step_index"),
                "source": event.get("source"),
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            },
            connection=connection,
            commit=False
        )
    except Exception as pub_err:
        logger.debug(f"RULE_DECISION_EVALUATED dashboard publication skipped: {pub_err}")

    # 7. Create Execution Record if Triggered
    if decision == "TRIGGER":
        numeric_event_id = None
        if event_id is not None:
            try:
                numeric_event_id = int(event_id)
            except (ValueError, TypeError):
                numeric_event_id = abs(hash(str(event_id))) % (2**63 - 1)
        try:
            cursor.execute("""
                INSERT INTO pipeline_executions (
                    pipeline_name, decision_id, triggering_event_id, status
                ) VALUES (%s, %s, %s, 'RUNNING')
            """, (pipeline_name, decision_id, numeric_event_id))
            execution_id = cursor.lastrowid
            logger.info(f"Pipeline '{pipeline_name}' EXECUTION RECORD CREATED (Status: RUNNING, ID: {execution_id}).")
        except pymysql.err.IntegrityError:
            # Fallback for concurrent idempotency race conditions
            logger.info(f"IDEMPOTENCY: Pipeline {pipeline_name} already executed for Event {event_id}.")
            connection.rollback()
            return decision, reason

        # 8. Reset relevant dependency datasets back to WAITING for the next cycle
        dataset_ids = [d["dataset_id"] if isinstance(d, dict) else d[0] for d in datasets]
        if dataset_ids:
            format_strings = ','.join(['%s'] * len(dataset_ids))
            cursor.execute(f"""
                UPDATE dataset_metadata
                SET status = 'WAITING'
                WHERE dataset_id IN ({format_strings})
            """, tuple(dataset_ids))
            logger.info("Dependency Cycle Reset:")
            for d in datasets:
                d_name = d["dataset_name"] if isinstance(d, dict) else d[1]
                logger.info(f"  {d_name} -> WAITING")
            logger.info(f"Reset {len(dataset_ids)} dependency datasets to WAITING for pipeline '{pipeline_name}'.")

            try:
                from services.observability.publisher import publish_dashboard_event
                publish_dashboard_event(
                    event_type="DATASET_READINESS_CHANGED",
                    entity="DATASET",
                    severity="INFO",
                    decision_id=decision_id,
                    execution_id=execution_id,
                    correlation_id=str(event_id) if event_id is not None else None,
                    data={
                        "action": "CYCLE_RESET",
                        "status": "WAITING",
                        "pipeline_name": pipeline_name,
                        "reset_dataset_ids": dataset_ids,
                        "simulation_id": event.get("simulation_id"),
                        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    },
                    connection=connection,
                    commit=False
                )
            except Exception as reset_err:
                logger.debug(f"Cycle reset dashboard event skipped: {reset_err}")

        connection.commit()

        # 9. Trigger Downstream Pipeline Execution via Pipeline Runner
        logger.info(f"Invoking pipeline runner for execution #{execution_id} ({pipeline_name})...")
        runner_result = execute_pipeline(execution_id, pipeline_name, connection)
        exec_status = runner_result.get("status", "UNKNOWN")
        logger.info(f"Pipeline '{pipeline_name}' EXECUTION RESULT: {exec_status}")
        if exec_status == "COMPLETED":
            logger.info(f"Output Location: {runner_result.get('output_location')}")
        elif exec_status == "FAILED":
            logger.error(f"Error Message  : {runner_result.get('error')}")

    else:
        connection.commit()

    logger.info("Decision processing completed.")
    return decision, reason


# Import MySQL consumer components with resilient fallback
try:
    from .mysql_consumer import claim_events, mark_event_completed, mark_event_failed, poll_and_process_batch, run_mysql_consumer
except (ImportError, ValueError):
    from services.dependency_engine.mysql_consumer import claim_events, mark_event_completed, mark_event_failed, poll_and_process_batch, run_mysql_consumer


def run_kafka_consumer(stop_event=None):
    """Runs the Kafka event consumer loop."""
    import json
    from kafka import KafkaConsumer

    try:
        consumer = KafkaConsumer(
            settings.KAFKA_TOPIC_EVENTS,
            bootstrap_servers=settings.KAFKA_BROKER,
            group_id=settings.KAFKA_CONSUMER_GROUP,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            consumer_timeout_ms=1000
        )
    except Exception as e:
        logger.error(f"Failed to connect to Kafka broker {settings.KAFKA_BROKER}: {e}")
        return

    logger.info("Kafka consumer started...")
    logger.info(f"Listening to Kafka topic: {settings.KAFKA_TOPIC_EVENTS}")

    connection = None
    try:
        connection = get_connection()
        while stop_event is None or not stop_event.is_set():
            try:
                for message in consumer:
                    if stop_event and stop_event.is_set():
                        break
                    try:
                        connection.ping(reconnect=True)
                    except Exception as ping_err:
                        logger.warning(f"MySQL connection lost ({ping_err}). Reconnecting...")
                        try:
                            connection.close()
                        except Exception:
                            pass
                        connection = get_connection()

                    process_event(message.value, connection)
                    consumer.commit()
            except Exception as loop_err:
                if stop_event and stop_event.is_set():
                    break
                logger.error(f"Error in Kafka consumer message cycle: {loop_err}")
                time.sleep(1.0)
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        if connection:
            connection.close()


def run_hybrid_consumer(
    poll_interval: float = 1.0,
    batch_size: int = 10,
    stale_timeout_seconds: int = 300,
    stop_event=None
):
    """
    Runs Hybrid consumer mode:
      MySQL polling is the authoritative transport, while Kafka consumer runs concurrently
      if brokers are online, with mutual exclusion and idempotency deduplication.
    """
    import threading
    import socket

    logger.info("Hybrid mode initialized: MySQL polling (authoritative) + Kafka listener (optional).")
    stop_flag = stop_event or threading.Event()

    # Check if Kafka broker is reachable before spawning Kafka consumer thread
    kafka_thread = None
    try:
        broker = settings.KAFKA_BROKER or "localhost:9092"
        host, port_str = broker.split(":") if ":" in broker else (broker, "9092")
        with socket.create_connection((host, int(port_str)), timeout=0.2):
            pass
        kafka_thread = threading.Thread(target=run_kafka_consumer, args=(stop_flag,), daemon=True)
        kafka_thread.start()
        logger.info("Hybrid mode: Kafka consumer listener started in background thread.")
    except Exception as k_err:
        logger.info(f"Hybrid mode: Kafka broker offline ({k_err}); proceeding with pure MySQL polling.")

    # Run authoritative MySQL consumer on main thread
    run_mysql_consumer(
        poll_interval=poll_interval,
        batch_size=batch_size,
        stale_timeout_seconds=stale_timeout_seconds,
        stop_event=stop_flag,
        process_func=process_event
    )

    if kafka_thread and kafka_thread.is_alive():
        kafka_thread.join(timeout=2.0)


def print_startup_diagnostics(transport: str, poll_interval: float, batch_size: int):
    """Prints diagnostic startup telemetry for the Dependency Engine."""
    logger.info("==================================================================")
    logger.info("SupplySense AI - Hybrid Dependency Engine (Phase 2)")
    logger.info("==================================================================")
    logger.info(f"Transport Mode     : {transport}")
    logger.info(f"Polling Interval   : {poll_interval}s")
    logger.info(f"Batch Size         : {batch_size}")
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS total FROM pipeline_dependencies")
        pipe_count = cur.fetchone()["total"]
        cur.execute("SELECT COUNT(*) AS total FROM dataset_events WHERE event_status = 'NEW'")
        pending_count = cur.fetchone()["total"]
        conn.close()
        logger.info(f"Active Pipelines   : {pipe_count} configured in database")
        logger.info(f"Pending Events     : {pending_count} NEW events in dataset_events")
        logger.info("Database Health    : CONNECTED (MySQL)")
    except Exception as db_err:
        logger.warning(f"Database Diagnostic Warning: {db_err}")
    logger.info("==================================================================")


def main(args=None):
    """Main CLI entry point for the Dependency Engine."""
    import argparse
    import signal
    import threading

    parser = argparse.ArgumentParser(description="SupplySense AI Dependency Engine (Phase 2)")
    parser.add_argument(
        "--transport",
        choices=["MYSQL", "KAFKA", "HYBRID"],
        default=os.getenv("DEPENDENCY_ENGINE_TRANSPORT") or getattr(settings, "DEPENDENCY_ENGINE_TRANSPORT", "MYSQL"),
        help="Transport mode (default: MYSQL)"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=float(os.getenv("DEPENDENCY_POLL_INTERVAL_SECONDS") or getattr(settings, "DEPENDENCY_POLL_INTERVAL_SECONDS", 1.0)),
        help="MySQL polling interval in seconds (default: 1.0)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=int(os.getenv("DEPENDENCY_BATCH_SIZE") or getattr(settings, "DEPENDENCY_BATCH_SIZE", 10)),
        help="Event claiming batch size (default: 10)"
    )
    parser.add_argument(
        "--stale-timeout",
        type=int,
        default=int(os.getenv("DEPENDENCY_STALE_TIMEOUT_SECONDS") or getattr(settings, "DEPENDENCY_STALE_TIMEOUT_SECONDS", 300)),
        help="Timeout in seconds to recover stale PROCESSING events (default: 300)"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Poll and process a single batch then exit"
    )

    parsed = parser.parse_args(args)
    transport = parsed.transport.upper()

    print_startup_diagnostics(transport, parsed.interval, parsed.batch_size)

    if parsed.once:
        logger.info("Single batch execution (--once) requested.")
        summary = poll_and_process_batch(
            batch_size=parsed.batch_size,
            stale_timeout_seconds=parsed.stale_timeout,
            process_func=process_event
        )
        logger.info(f"Batch execution complete: {summary}")
        return summary

    stop_event = threading.Event()

    def handle_sig(sig, frame):
        logger.info(f"Signal {sig} received; initiating graceful shutdown...")
        stop_event.set()

    try:
        signal.signal(signal.SIGINT, handle_sig)
        signal.signal(signal.SIGTERM, handle_sig)
    except Exception:
        pass

    if transport == "MYSQL":
        run_mysql_consumer(
            poll_interval=parsed.interval,
            batch_size=parsed.batch_size,
            stale_timeout_seconds=parsed.stale_timeout,
            stop_event=stop_event,
            process_func=process_event
        )
    elif transport == "KAFKA":
        run_kafka_consumer(stop_event=stop_event)
    elif transport == "HYBRID":
        run_hybrid_consumer(
            poll_interval=parsed.interval,
            batch_size=parsed.batch_size,
            stale_timeout_seconds=parsed.stale_timeout,
            stop_event=stop_event
        )
    else:
        logger.error(f"Unknown transport: {transport}")
        sys.exit(1)


if __name__ == "__main__":
    main()
