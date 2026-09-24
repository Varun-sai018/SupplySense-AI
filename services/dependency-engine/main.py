import os
import sys
import json
import logging
import datetime
import pymysql
from kafka import KafkaConsumer

# Ensure repository root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from common.database import get_connection
from config import settings
import importlib
evaluator_module = importlib.import_module('services.dependency-engine.evaluator')
evaluate_condition = evaluator_module.evaluate_condition

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def process_event(event: dict, connection) -> None:
    """Processes a single dataset update event, evaluates dependencies, and triggers executions if necessary."""
    cursor = connection.cursor()
    
    event_id = event.get('event_id')
    dataset_name = event.get('dataset_name')
    
    print("\n===================================")
    print("Received Kafka Event")
    print("===================================")
    print(f"Event ID       : {event_id}")
    print(f"Dataset        : {dataset_name}")
    print(f"Version        : {event.get('dataset_version')}")
    print(f"Rows Changed   : {event.get('rows_changed')}")

    # 1. Get Pipeline Dependency configurations mapping to this dataset.
    # Currently assuming a single 'Demand Forecast Pipeline' as per existing business logic.
    cursor.execute("""
        SELECT
            dependency_id,
            pipeline_name,
            condition_type,
            required_count
        FROM pipeline_dependencies
        WHERE pipeline_name = 'Demand Forecast Pipeline'
        LIMIT 1
    """)
    dependency = cursor.fetchone()

    if not dependency:
        logger.warning("No dependency configuration found.")
        return "BLOCK", "No dependency configuration found"

    dependency_id = dependency["dependency_id"]
    pipeline_name = dependency["pipeline_name"]
    condition_type = dependency["condition_type"]
    required_count = dependency["required_count"]

    # 2. Idempotency Check: Did this exact event already trigger this pipeline?
    cursor.execute("""
        SELECT execution_id FROM pipeline_executions
        WHERE pipeline_name = %s AND triggering_event_id = %s
    """, (pipeline_name, event_id))
    if cursor.fetchone():
        logger.info(f"IDEMPOTENCY: Pipeline {pipeline_name} was already triggered for Event ID {event_id}. Skipping execution.")
        return "SKIPPED", "Idempotency: already triggered"

    # 3. Get Required Datasets
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

    print("\nDependency Status:")
    for dataset in datasets:
        print(f"{dataset['dataset_name']} -> {dataset['status']} (Version {dataset['current_version']})")

    # 4. Evaluate Condition
    decision, reason = evaluate_condition(
        condition_type=condition_type,
        ready_count=ready_count,
        total_required=total_required,
        required_count=required_count
    )

    print("\n===================================")
    print("DEPENDENCY DECISION")
    print("===================================")
    if condition_type == "QUORUM":
        print(f"Pipeline        : {pipeline_name}")
        print(f"Condition       : {condition_type}")
        print(f"Ready           : {ready_count}")
        print(f"Total Required  : {total_required}")
        print(f"Quorum Required : {required_count}")
        print(f"Decision        : {decision}")
        print(f"Reason          : {reason}")
    else:
        print(f"Pipeline       : {pipeline_name}")
        print(f"Condition      : {condition_type}")
        print(f"Ready          : {ready_count}")
        print(f"Required       : {total_required}")
        print(f"Decision       : {decision}")
        print(f"Reason         : {reason}")

    # 5. Store Decision
    triggered_at = datetime.datetime.now() if decision == "TRIGGER" else None
    
    cursor.execute("""
        INSERT INTO pipeline_decisions (
            pipeline_name, condition_type, decision, required_count,
            ready_count, total_required, reason, triggered_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        pipeline_name, condition_type, decision, required_count,
        ready_count, total_required, reason, triggered_at
    ))
    decision_id = cursor.lastrowid
    
    # 6. Create Execution Record if Triggered
    if decision == "TRIGGER":
        try:
            cursor.execute("""
                INSERT INTO pipeline_executions (
                    pipeline_name, decision_id, triggering_event_id, status
                ) VALUES (%s, %s, %s, 'RUNNING')
            """, (pipeline_name, decision_id, event_id))
            print(f"\nPipeline '{pipeline_name}' EXECUTION RECORD CREATED (Status: RUNNING).")
        except pymysql.err.IntegrityError:
            # Fallback for concurrent idempotency race conditions
            logger.info(f"IDEMPOTENCY: Pipeline {pipeline_name} already executed for Event {event_id}.")
            connection.rollback()
            return decision, reason

        # 7. Reset relevant dependency datasets back to WAITING for the next cycle
        dataset_ids = [d["dataset_id"] if isinstance(d, dict) else d[0] for d in datasets]
        if dataset_ids:
            format_strings = ','.join(['%s'] * len(dataset_ids))
            cursor.execute(f"""
                UPDATE dataset_metadata
                SET status = 'WAITING'
                WHERE dataset_id IN ({format_strings})
            """, tuple(dataset_ids))
            print("\nDependency Cycle Reset:")
            for d in datasets:
                d_name = d["dataset_name"] if isinstance(d, dict) else d[1]
                print(f"{d_name} -> WAITING")
            logger.info(f"Reset {len(dataset_ids)} dependency datasets to WAITING for pipeline '{pipeline_name}'.")

    connection.commit()
    print("Decision processing completed.")
    return decision, reason


def main():
    try:
        consumer = KafkaConsumer(
            settings.KAFKA_TOPIC_EVENTS,
            bootstrap_servers=settings.KAFKA_BROKER,
            group_id=settings.KAFKA_CONSUMER_GROUP,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda value: json.loads(value.decode("utf-8"))
        )
    except Exception as e:
        logger.error(f"Failed to connect to Kafka broker {settings.KAFKA_BROKER}: {e}")
        return

    logger.info("Dependency Engine started...")
    logger.info(f"Listening to Kafka topic: {settings.KAFKA_TOPIC_EVENTS}")

    connection = None
    try:
        connection = get_connection()
        for message in consumer:
            try:
                process_event(message.value, connection)
            except Exception as e:
                logger.error(f"Error processing event {message.value.get('event_id')}: {e}")
                connection.rollback()
    except Exception as e:
        logger.error(f"Error during consumer execution: {e}")
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        if connection:
            connection.close()

if __name__ == "__main__":
    main()
