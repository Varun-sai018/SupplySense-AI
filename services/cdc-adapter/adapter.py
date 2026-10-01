"""
SupplySense AI - Debezium CDC Event Adapter

Consumes raw MySQL Change Data Capture (CDC) events emitted by Debezium into Kafka,
normalizes them into the SupplySense event specification, updates dataset metadata in MySQL,
records change history in `dataset_events`, and routes normalized events to the `dataset-events`
topic for downstream dependency evaluation.
"""

import os
import sys
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional, Union
import pymysql
from kafka import KafkaConsumer, KafkaProducer

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from config import settings

logger = logging.getLogger(__name__)

# Mapping from MySQL table names to SupplySense registered dataset names
TABLE_TO_DATASET_MAP = {
    "olist_orders": "Orders",
    "olist_order_items": "Order Items",
    "olist_products": "Products",
    "olist_sellers": "Sellers",
}

# Debezium CDC operation codes
OP_CREATE = "c"
OP_UPDATE = "u"
OP_DELETE = "d"
OP_READ = "r"


def generate_source_change_id(
    source: Optional[Dict[str, Any]],
    table_name: str = "",
    ts_ms: Optional[int] = None
) -> Optional[str]:
    """
    Generates a deterministic unique identifier for a Debezium CDC source change event.

    Precedence:
      1. GTID (if available): gtid:{gtid}:{row}
      2. Binlog File & Pos: {file}:{pos}:{row}
      3. Fallback Coordinate: {table}:{ts_ms}:{server_id}
      4. None if non-CDC / missing metadata.
    """
    if not source or not isinstance(source, dict):
        return None

    row = source.get("row")
    row_val = 0 if row is None else row

    # 1. GTID (if present)
    gtid = source.get("gtid")
    if gtid:
        return f"gtid:{gtid}:{row_val}"

    # 2. Binlog file and position
    file = source.get("file")
    pos = source.get("pos")
    if file is not None and pos is not None:
        return f"{file}:{pos}:{row_val}"

    # 3. Fallback coordinate
    ts = ts_ms or source.get("ts_ms")
    tbl = table_name or source.get("table", "unknown")
    srv = source.get("server_id", 0)
    if ts:
        return f"{tbl}:{ts}:{srv}"

    return None


def normalize_cdc_record(payload: Union[str, dict], topic: str = "") -> Optional[Dict[str, Any]]:
    """
    Normalizes a Debezium CDC change record into a structured dictionary.

    Args:
        payload: The Debezium JSON payload (dict or string).
        topic: The Kafka topic from which the event was received.

    Returns:
        Dict with table_name, dataset_name, op, row_data, ts_ms, source coordinates,
        and source_change_id, or None if invalid/ignored.
    """
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception as e:
            logger.warning(f"Failed to parse CDC JSON string: {e}")
            return None

    if not isinstance(payload, dict):
        return None

    # Handle Debezium envelopes with or without schema wrapper
    record = payload.get("payload", payload) if "payload" in payload and isinstance(payload.get("payload"), dict) else payload

    source = record.get("source", {})
    table_name = source.get("table")

    # Fallback to topic name if source.table is missing
    if not table_name and topic:
        parts = topic.split(".")
        if len(parts) >= 3:
            table_name = parts[-1]

    if not table_name:
        return None

    dataset_name = TABLE_TO_DATASET_MAP.get(table_name)
    if not dataset_name:
        logger.debug(f"Ignoring CDC event for unmapped table: {table_name}")
        return None

    op = record.get("op", OP_CREATE)
    after = record.get("after")
    before = record.get("before")
    ts_ms = record.get("ts_ms") or source.get("ts_ms")

    source_file = source.get("file")
    source_pos = source.get("pos")
    source_change_id = generate_source_change_id(source, table_name=table_name, ts_ms=ts_ms)

    return {
        "table_name": table_name,
        "dataset_name": dataset_name,
        "op": op,
        "after": after,
        "before": before,
        "ts_ms": ts_ms,
        "source": source,
        "source_file": source_file,
        "source_pos": source_pos,
        "source_change_id": source_change_id
    }


def process_cdc_event(
    normalized_cdc: Dict[str, Any],
    connection=None,
    kafka_producer: Optional[KafkaProducer] = None
) -> Dict[str, Any]:
    """
    Persists a normalized CDC event into MySQL `dataset_events`, updates `dataset_metadata`,
    and publishes the standardized event to the `dataset-events` Kafka topic.

    Provides idempotent handling: if a CDC message with the same `source_change_id`
    has already been processed, it is skipped without creating a duplicate event,
    advancing dataset version, or publishing redundant Kafka events.

    Args:
        normalized_cdc: Structured dict from normalize_cdc_record.
        connection: Optional open PyMySQL database connection.
        kafka_producer: Optional KafkaProducer instance.

    Returns:
        Dict with processing outcome, event_id, dataset status, and is_duplicate flag.
    """
    dataset_name = normalized_cdc["dataset_name"]
    table_name = normalized_cdc["table_name"]
    op = normalized_cdc.get("op", OP_CREATE)
    source_file = normalized_cdc.get("source_file")
    source_pos = normalized_cdc.get("source_pos")
    source_change_id = normalized_cdc.get("source_change_id")

    should_close_conn = False
    if connection is None:
        connection = get_connection()
        should_close_conn = True

    should_close_producer = False
    if kafka_producer is None:
        try:
            kafka_producer = KafkaProducer(
                bootstrap_servers=settings.KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            should_close_producer = True
        except Exception as e:
            logger.warning(f"Could not connect KafkaProducer in process_cdc_event: {e}")

    cursor = connection.cursor()
    try:
        # Step 0: Deduplication check if source_change_id is present
        if source_change_id:
            cursor.execute("""
                SELECT event_id, dataset_id, dataset_name, dataset_version, event_status
                FROM dataset_events
                WHERE source_change_id = %s
            """, (source_change_id,))
            existing_event = cursor.fetchone()
            if existing_event:
                logger.info(
                    f"CDC event duplicate ignored: dataset={dataset_name} source_change_id={source_change_id}"
                )
                return {
                    "status": "DUPLICATE_SKIPPED",
                    "event_id": existing_event["event_id"],
                    "dataset_name": dataset_name,
                    "dataset_version": existing_event["dataset_version"],
                    "published_to_kafka": False,
                    "is_duplicate": True
                }

        # Step 1: Fetch or initialize dataset_metadata
        cursor.execute("""
            SELECT dataset_id, current_version, row_count, status
            FROM dataset_metadata
            WHERE dataset_name = %s
            FOR UPDATE
        """, (dataset_name,))
        dataset = cursor.fetchone()

        if not dataset:
            cursor.execute("""
                INSERT INTO dataset_metadata (dataset_name, source_type, table_name, current_version, row_count, status)
                VALUES (%s, 'DEBEZIUM_CDC', %s, 0, 0, 'WAITING')
            """, (dataset_name, table_name))
            connection.commit()
            cursor.execute("""
                SELECT dataset_id, current_version, row_count, status
                FROM dataset_metadata
                WHERE dataset_name = %s
                FOR UPDATE
            """, (dataset_name,))
            dataset = cursor.fetchone()

        dataset_id = dataset["dataset_id"]
        current_version = dataset["current_version"] or 0
        current_rows = dataset["row_count"] or 0

        # Determine row delta based on CDC operation
        if op == OP_DELETE:
            row_delta = -1
        elif op in (OP_CREATE, OP_READ):
            row_delta = 1
        else: # Update
            row_delta = 0

        new_version = current_version + 1
        new_row_count = max(0, current_rows + row_delta)
        event_time = datetime.now()

        # Step 2: Record event in dataset_events with race-condition protection
        try:
            cursor.execute("""
                INSERT INTO dataset_events (
                    dataset_id, dataset_name, event_type, event_time,
                    dataset_version, rows_changed, source_file, source_pos,
                    source_change_id, batch_id, event_status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, 'NEW')
            """, (
                dataset_id,
                dataset_name,
                "DATASET_UPDATED",
                event_time,
                new_version,
                abs(row_delta) if row_delta != 0 else 1,
                source_file,
                source_pos,
                source_change_id
            ))
            event_id = cursor.lastrowid
        except pymysql.err.IntegrityError:
            # Race-condition duplicate insert caught by unique constraint
            connection.rollback()
            logger.info(
                f"CDC event duplicate ignored: dataset={dataset_name} source_change_id={source_change_id}"
            )
            cursor.execute("""
                SELECT event_id, dataset_id, dataset_name, dataset_version
                FROM dataset_events
                WHERE source_change_id = %s
            """, (source_change_id,))
            dup_rec = cursor.fetchone()
            return {
                "status": "DUPLICATE_SKIPPED",
                "event_id": dup_rec["event_id"] if dup_rec else None,
                "dataset_name": dataset_name,
                "dataset_version": dup_rec["dataset_version"] if dup_rec else current_version,
                "published_to_kafka": False,
                "is_duplicate": True
            }

        # Step 3: Update dataset_metadata to READY and advance version
        cursor.execute("""
            UPDATE dataset_metadata
            SET current_version = %s,
                row_count = %s,
                last_updated_at = %s,
                status = 'READY'
            WHERE dataset_id = %s
        """, (new_version, new_row_count, event_time, dataset_id))

        connection.commit()

        logger.info(
            f"CDC event accepted: dataset={dataset_name} source_change_id={source_change_id}"
        )

        # Step 4: Publish standardized message to Kafka `dataset-events`
        published = False
        if kafka_producer:
            kafka_msg = {
                "event_id": event_id,
                "dataset_id": dataset_id,
                "dataset_name": dataset_name,
                "event_type": "DATASET_UPDATED",
                "event_time": str(event_time),
                "dataset_version": new_version,
                "rows_changed": abs(row_delta) if row_delta != 0 else 1,
                "source": "DEBEZIUM_CDC",
                "op": op,
                "source_change_id": source_change_id
            }

            future = kafka_producer.send(settings.KAFKA_TOPIC_EVENTS, value=kafka_msg)
            try:
                record_meta = future.get(timeout=5)
                published = True
                cursor.execute("""
                    UPDATE dataset_events
                    SET event_status = 'PUBLISHED', processed_at = NOW()
                    WHERE event_id = %s
                """, (event_id,))
                connection.commit()
                logger.info(
                    f"CDC Event #{event_id} ({dataset_name} v{new_version}) routed to Kafka "
                    f"topic '{settings.KAFKA_TOPIC_EVENTS}' [partition={record_meta.partition}, offset={record_meta.offset}]."
                )
            except Exception as k_err:
                logger.error(f"Failed to publish CDC event #{event_id} to Kafka: {k_err}")

        return {
            "status": "PROCESSED",
            "event_id": event_id,
            "dataset_name": dataset_name,
            "dataset_version": new_version,
            "published_to_kafka": published,
            "is_duplicate": False
        }

    except Exception as exc:
        logger.error(f"Error processing CDC event for {dataset_name}: {exc}")
        if connection:
            connection.rollback()
        raise
    finally:
        cursor.close()
        if should_close_conn and connection:
            connection.close()
        if should_close_producer and kafka_producer:
            try:
                kafka_producer.flush()
                kafka_producer.close()
            except Exception:
                pass


def run_cdc_adapter(
    cdc_topics=None,
    max_events: Optional[int] = None,
    poll_timeout_ms: int = 1000
):
    """
    Runs the CDC Adapter event consumer loop, reading from Debezium change topics
    and feeding SupplySense's event-conditioned architecture.
    """
    if cdc_topics is None:
        cdc_topics = [
            "supplysense_cdc.supplysense.olist_orders",
            "supplysense_cdc.supplysense.olist_products",
            "supplysense_cdc.supplysense.olist_sellers",
            "supplysense_cdc.supplysense.olist_order_items",
        ]

    logger.info(f"Starting CDC Adapter... Subscribing to topics: {cdc_topics}")

    consumer = KafkaConsumer(
        *cdc_topics,
        bootstrap_servers=settings.KAFKA_BROKER,
        group_id="supplysense-cdc-adapter-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None
    )

    producer = KafkaProducer(
        bootstrap_servers=settings.KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    connection = get_connection()
    events_processed = 0

    try:
        while True:
            records = consumer.poll(timeout_ms=poll_timeout_ms)
            for topic_partition, messages in records.items():
                for msg in messages:
                    if msg.value is None:
                        continue
                    normalized = normalize_cdc_record(msg.value, topic=msg.topic)
                    if normalized:
                        process_cdc_event(normalized, connection=connection, kafka_producer=producer)
                        events_processed += 1
                        if max_events and events_processed >= max_events:
                            logger.info(f"Reached max_events limit ({max_events}). Stopping CDC Adapter.")
                            return events_processed
    except KeyboardInterrupt:
        logger.info("CDC Adapter stopped by user.")
    finally:
        consumer.close()
        producer.flush()
        producer.close()
        connection.close()


if __name__ == '__main__':
    run_cdc_adapter()
