"""
SupplySense AI - Olist Kafka Streaming Simulator

Simulates real-time incoming e-commerce data from the Part 2 Olist simulation dataset:
  - Iterates chronologically through the simulation orders (data/olist_split/simulation/).
  - Every N seconds (configurable interval), slices a batch of orders.
  - Generates standardized dataset events.
  - Updates dataset_metadata in MySQL.
  - Publishes events to Apache Kafka (topic: dataset-events) for the Dependency Engine.
"""

import os
import sys
import time
import json
import uuid
import argparse
import logging
from datetime import datetime, timezone
import pandas as pd
from kafka import KafkaProducer

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("OlistKafkaSimulator")

SIMULATION_DIR = os.path.join(REPO_ROOT, "data", "olist_split", "simulation")


class OlistKafkaSimulator:
    def __init__(
        self,
        interval_seconds: float = 3.0,
        batch_size: int = 10,
        max_events: int = 5,
        target_pipeline: str = "Demand Forecast Pipeline",
        cycle_dependencies: bool = True,
    ):
        self.interval_seconds = interval_seconds
        self.batch_size = batch_size
        self.max_events = max_events
        self.target_pipeline = target_pipeline
        self.cycle_dependencies = cycle_dependencies

        self.orders_path = os.path.join(SIMULATION_DIR, "olist_orders_dataset.csv")
        self.items_path = os.path.join(SIMULATION_DIR, "olist_order_items_dataset.csv")

        if not os.path.exists(self.orders_path):
            raise FileNotFoundError(
                f"Simulation dataset not found at {self.orders_path}. Run scripts/split_olist_data.py first."
            )

        logger.info(f"Loading simulation orders from {self.orders_path}...")
        self.df_orders = pd.read_csv(self.orders_path)
        self.df_orders["parsed_ts"] = pd.to_datetime(self.df_orders["order_purchase_timestamp"])
        self.df_orders = self.df_orders.sort_values("parsed_ts").reset_index(drop=True)
        logger.info(f"Loaded {len(self.df_orders):,} chronological simulation orders.")

        logger.info(f"Initializing Kafka Producer connecting to {settings.KAFKA_BROKER}...")
        self.producer = KafkaProducer(
            bootstrap_servers=settings.KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            request_timeout_ms=10000,
        )
        logger.info(f"Kafka Producer initialized for topic: '{settings.KAFKA_TOPIC_EVENTS}'")

    def run(self):
        logger.info("==================================================")
        logger.info("Starting Olist Kafka Real-Time Event Simulation")
        logger.info(f"Interval: {self.interval_seconds}s | Batch Size: {self.batch_size} orders")
        logger.info(f"Max Events: {self.max_events} | Target Pipeline: {self.target_pipeline}")
        logger.info("==================================================")

        total_orders = len(self.df_orders)
        offset = 0
        event_counter = 0

        connection = get_connection()

        try:
            while event_counter < self.max_events and offset < total_orders:
                batch_orders = self.df_orders.iloc[offset : offset + self.batch_size]
                if batch_orders.empty:
                    break

                event_counter += 1
                batch_id = f"SIM-OLIST-B{event_counter:04d}-{uuid.uuid4().hex[:6]}"
                source_change_id = f"sim:{batch_id}"
                event_time_now = datetime.now(timezone.utc)

                # Fetch and advance dataset_metadata for 'Orders'
                cursor = connection.cursor()
                cursor.execute(
                    "SELECT dataset_id, current_version, row_count FROM dataset_metadata WHERE dataset_name = 'Orders' FOR UPDATE"
                )
                meta_row = cursor.fetchone()
                if meta_row:
                    dataset_id = meta_row["dataset_id"]
                    curr_ver = meta_row["current_version"] or 0
                    curr_cnt = meta_row["row_count"] or 0
                else:
                    dataset_id = 1
                    curr_ver = 0
                    curr_cnt = 0

                new_version = curr_ver + 1
                new_row_count = curr_cnt + len(batch_orders)

                # Record in dataset_events table
                cursor.execute(
                    """
                    INSERT INTO dataset_events (
                        dataset_id, dataset_name, event_type, event_time,
                        dataset_version, rows_changed, source_change_id,
                        batch_id, event_status
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'NEW')
                """,
                    (
                        dataset_id,
                        "Orders",
                        "DATASET_UPDATED",
                        event_time_now.strftime("%Y-%m-%d %H:%M:%S"),
                        new_version,
                        len(batch_orders),
                        source_change_id,
                        batch_id,
                    ),
                )
                db_event_id = cursor.lastrowid

                # Update Orders status to READY
                cursor.execute(
                    """
                    UPDATE dataset_metadata
                    SET current_version = %s,
                        row_count = %s,
                        status = 'READY',
                        last_updated_at = NOW()
                    WHERE dataset_name = 'Orders'
                """,
                    (new_version, new_row_count),
                )

                # If cycle_dependencies is enabled, also ensure dependent datasets ('Products', 'Sellers')
                # are marked READY so that the Dependency Engine can satisfy the ALL condition
                if self.cycle_dependencies and event_counter % 2 == 0:
                    cursor.execute(
                        "UPDATE dataset_metadata SET status = 'READY', last_updated_at = NOW() WHERE dataset_name IN ('Products', 'Sellers')"
                    )
                    logger.info("  [Dependency Alignment] Marked 'Products' & 'Sellers' READY to fulfill ALL condition.")

                connection.commit()

                # Construct standardized Kafka event payload
                min_ts = str(batch_orders["parsed_ts"].min())
                max_ts = str(batch_orders["parsed_ts"].max())
                kafka_payload = {
                    "event_id": db_event_id,
                    "dataset_id": dataset_id,
                    "dataset_name": "Orders",
                    "event_type": "DATASET_UPDATED",
                    "event_time": event_time_now.isoformat(),
                    "dataset_version": new_version,
                    "rows_changed": len(batch_orders),
                    "source_change_id": source_change_id,
                    "batch_id": batch_id,
                    "pipeline_name": self.target_pipeline,
                    "simulation": {
                        "batch_index": event_counter,
                        "orders_in_batch": len(batch_orders),
                        "historical_min_ts": min_ts,
                        "historical_max_ts": max_ts,
                    },
                }

                # Publish to Kafka
                future = self.producer.send(settings.KAFKA_TOPIC_EVENTS, value=kafka_payload)
                record_meta = future.get(timeout=10)

                logger.info(
                    f"-> [Event #{event_counter}/{self.max_events}] Published to Kafka '{settings.KAFKA_TOPIC_EVENTS}' "
                    f"[Partition: {record_meta.partition}, Offset: {record_meta.offset}] | "
                    f"Orders: {len(batch_orders)} (Span: {min_ts[:16]} -> {max_ts[:16]}) | Event ID: {db_event_id}"
                )

                offset += self.batch_size

                if event_counter < self.max_events:
                    logger.info(f"   Waiting {self.interval_seconds}s before next simulation event...")
                    time.sleep(self.interval_seconds)

        except KeyboardInterrupt:
            logger.info("Simulation interrupted by user.")
        finally:
            connection.close()
            self.producer.flush()
            self.producer.close()
            logger.info(f"Simulation completed. Total events produced to Kafka: {event_counter}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate Olist dataset stream over Kafka")
    parser.add_argument("--interval", type=float, default=2.0, help="Interval in seconds between events")
    parser.add_argument("--batch-size", type=int, default=10, help="Number of orders per event")
    parser.add_argument("--max-events", type=int, default=3, help="Total number of events to produce")
    parser.add_argument("--pipeline-name", type=str, default="Demand Forecast Pipeline", help="Target pipeline")
    parser.add_argument("--no-deps", action="store_true", help="Do not auto-align dependencies to READY")

    args = parser.parse_args()
    sim = OlistKafkaSimulator(
        interval_seconds=args.interval,
        batch_size=args.batch_size,
        max_events=args.max_events,
        target_pipeline=args.pipeline_name,
        cycle_dependencies=not args.no_deps,
    )
    sim.run()
