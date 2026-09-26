import base64
import hashlib
import hmac
import json
import logging
from datetime import datetime
import pymysql

from common.database import get_connection
from config import settings

logger = logging.getLogger(__name__)


def verify_square_signature(
    raw_body: bytes,
    signature_header: str,
    signature_key: str,
    notification_url: str
) -> bool:
    """
    Verifies a Square Webhook signature against the raw request body and notification URL.
    
    Square HMAC-SHA256 signature algorithm:
    1. Concatenate notification_url + raw_body (decoded as string).
    2. Compute HMAC-SHA256 using signature_key.
    3. Base64 encode the resulting digest.
    4. Securely compare with signature_header.
    """
    if not signature_header or not signature_key or not notification_url:
        return False

    try:
        data = notification_url + raw_body.decode('utf-8')
        computed_hash = hmac.new(
            signature_key.encode('utf-8'),
            data.encode('utf-8'),
            hashlib.sha256
        ).digest()

        expected_signature = base64.b64encode(computed_hash).decode('utf-8')
        return hmac.compare_digest(expected_signature, signature_header)
    except Exception as e:
        logger.error(f"Error verifying Square signature: {e}")
        return False


def normalize_square_event(payload: dict) -> dict:
    """
    Normalizes a Square webhook payload into the internal SupplySense event format.
    Supports 'inventory.count.updated' events.
    """
    event_type_raw = payload.get("type", "")
    event_id = payload.get("event_id", "")
    created_at = payload.get("created_at", datetime.now().isoformat())

    if event_type_raw != "inventory.count.updated":
        logger.warning(f"Unsupported event type: {event_type_raw}")
        return {
            "supported": False,
            "event_type": event_type_raw,
            "reason": f"Unsupported Square event type: {event_type_raw}"
        }

    data_object = payload.get("data", {}).get("object", {})
    inventory_counts = data_object.get("inventory_counts", [])
    
    rows_changed = len(inventory_counts) if inventory_counts else 1

    return {
        "supported": True,
        "external_event_id": event_id,
        "dataset_name": "Inventory",
        "event_type": "DATA_UPDATED",
        "raw_event_type": event_type_raw,
        "event_time": created_at,
        "rows_changed": rows_changed,
        "inventory_counts": inventory_counts
    }


def persist_and_publish_event(normalized_event: dict, connection=None, kafka_producer=None) -> dict:
    """
    Persists a normalized Square event into MySQL dataset_events, updates dataset_metadata,
    and publishes the event to Kafka topic 'dataset-events'.
    Returns status dict with event_id, status, and details.
    """
    if not normalized_event.get("supported"):
        return {
            "status": "IGNORED",
            "reason": normalized_event.get("reason", "Unsupported event type")
        }

    close_conn = False
    if connection is None:
        connection = get_connection()
        close_conn = True

    try:
        cursor = connection.cursor()

        # 1. Check or create Inventory dataset metadata
        cursor.execute(
            """
            SELECT dataset_id, current_version, row_count
            FROM dataset_metadata
            WHERE dataset_name = 'Inventory'
            """
        )
        dataset = cursor.fetchone()

        if not dataset:
            cursor.execute(
                """
                INSERT INTO dataset_metadata (dataset_name, source_type, table_name, current_version, row_count, status)
                VALUES ('Inventory', 'SQUARE', 'square_inventory', 0, 0, 'WAITING')
                """
            )
            connection.commit()
            cursor.execute(
                """
                SELECT dataset_id, current_version, row_count
                FROM dataset_metadata
                WHERE dataset_name = 'Inventory'
                """
            )
            dataset = cursor.fetchone()

        dataset_id = dataset['dataset_id'] if isinstance(dataset, dict) else dataset[0]
        current_version = dataset['current_version'] if isinstance(dataset, dict) else dataset[1]
        current_row_count = dataset['row_count'] if isinstance(dataset, dict) else dataset[2]

        # 2. Check for duplicate processing using external_event_id or version check
        rows_changed = normalized_event.get("rows_changed", 1)
        new_version = current_version + 1
        new_row_count = current_row_count + rows_changed
        event_time = datetime.now()

        # 3. Update dataset_metadata
        cursor.execute(
            """
            UPDATE dataset_metadata
            SET current_version = %s,
                row_count = %s,
                last_updated_at = %s,
                status = 'READY'
            WHERE dataset_id = %s
            """,
            (new_version, new_row_count, event_time, dataset_id)
        )

        # 4. Insert into dataset_events
        cursor.execute(
            """
            INSERT INTO dataset_events
            (dataset_id, dataset_name, event_type, event_time, dataset_version, rows_changed, event_status)
            VALUES (%s, %s, %s, %s, %s, %s, 'NEW')
            """,
            (dataset_id, "Inventory", "DATA_UPDATED", event_time, new_version, rows_changed)
        )

        event_id = cursor.lastrowid
        connection.commit()

        # 5. Kafka Publish (if producer is provided)
        published = False
        if kafka_producer is not None:

            try:
                kafka_msg = {
                    "event_id": event_id,
                    "dataset_id": dataset_id,
                    "dataset_name": "Inventory",
                    "event_type": "DATA_UPDATED",
                    "event_time": str(event_time),
                    "dataset_version": new_version,
                    "rows_changed": rows_changed
                }
                future = kafka_producer.send(settings.KAFKA_TOPIC_EVENTS, value=kafka_msg)
                future.get(timeout=3)
                
                cursor.execute(
                    """
                    UPDATE dataset_events
                    SET event_status = 'PUBLISHED', processed_at = NOW()
                    WHERE event_id = %s
                    """,
                    (event_id,)
                )
                connection.commit()
                published = True
            except Exception as e:
                logger.warning(f"Could not publish to Kafka immediately: {e}")


        return {
            "status": "PROCESSED",
            "event_id": event_id,
            "dataset_name": "Inventory",
            "dataset_version": new_version,
            "published_to_kafka": published
        }

    except Exception as e:
        logger.error(f"Error persisting Square event: {e}")
        if connection:
            connection.rollback()
        raise e
    finally:
        if close_conn and connection:
            connection.close()
