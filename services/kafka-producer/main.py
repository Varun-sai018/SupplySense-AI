import os
import sys
import json
import logging
from kafka import KafkaProducer

# Ensure repository root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from common.database import get_connection
from config import settings

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting Kafka Producer...")
    
    # -----------------------------
    # Create Kafka Producer
    # -----------------------------
    try:
        producer = KafkaProducer(
            bootstrap_servers=settings.KAFKA_BROKER,
            value_serializer=lambda value: json.dumps(value).encode("utf-8")
        )
    except Exception as e:
        logger.error(f"Failed to connect to Kafka broker {settings.KAFKA_BROKER}: {e}")
        return

    connection = None
    try:
        # -----------------------------
        # Connect to MySQL
        # -----------------------------
        connection = get_connection()
        cursor = connection.cursor()

        # -----------------------------
        # Get NEW Events
        # -----------------------------
        cursor.execute("""
            SELECT
                event_id,
                dataset_id,
                dataset_name,
                event_type,
                event_time,
                dataset_version,
                rows_changed
            FROM dataset_events
            WHERE event_status = 'NEW'
            ORDER BY event_id
        """)

        events = cursor.fetchall()
        logger.info(f"Found {len(events)} NEW event(s)")

        # -----------------------------
        # Publish Events to Kafka
        # -----------------------------
        for event in events:
            message = {
                "event_id": event["event_id"],
                "dataset_id": event["dataset_id"],
                "dataset_name": event["dataset_name"],
                "event_type": event["event_type"],
                "event_time": str(event["event_time"]),
                "dataset_version": event["dataset_version"],
                "rows_changed": event["rows_changed"]
            }

            future = producer.send(
                settings.KAFKA_TOPIC_EVENTS,
                value=message
            )

            # Wait for Kafka acknowledgement
            record_metadata = future.get(timeout=10)

            logger.info(
                f"Published Event {event['event_id']} | "
                f"Dataset: {event['dataset_name']} | "
                f"Version: {event['dataset_version']} | "
                f"Partition: {record_metadata.partition} | "
                f"Offset: {record_metadata.offset}"
            )

            # Mark event as published
            cursor.execute("""
                UPDATE dataset_events
                SET
                    event_status = 'PUBLISHED',
                    processed_at = NOW()
                WHERE event_id = %s
            """, (event["event_id"],))

        # -----------------------------
        # Commit MySQL Changes
        # -----------------------------
        connection.commit()
        logger.info("All NEW events published successfully!")

    except Exception as e:
        logger.error(f"Error during producer execution: {e}")
        if connection:
            connection.rollback()
    finally:
        try:
            producer.flush()
            producer.close()
        except Exception:
            pass
            
        if connection:
            connection.close()


if __name__ == "__main__":
    main()
