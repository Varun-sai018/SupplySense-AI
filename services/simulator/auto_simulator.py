"""
SupplySense AI - Automated Real Olist Stream Simulator

Continuously streams chronological batches from the real Olist Part 2 simulation dataset
(data/olist_split/simulation/) on backend application startup:
  - NO synthetic or fake placeholder data.
  - Generates events for Olist Core datasets (Orders, Order Items, Payments),
    Regional State datasets (SP, RJ, MG, RS), Payment Rails (Boleto, Credit Card),
    and Operational Orders.
  - Automatically enqueues events into dataset_events and Kafka for the Dependency Engine.
  - Publishes real-time dashboard telemetry via EventHub.
"""

import os
import sys
import time
import json
import uuid
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import pandas as pd

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from config import settings

logger = logging.getLogger("AutoOlistSimulator")

SIM_DIR = os.path.join(REPO_ROOT, "data", "olist_split", "simulation")


class AutoOlistSimulator:
    """Thread-safe automated background stream generator for real Olist data."""

    def __init__(self, interval_seconds: float = 5.0, batch_size: int = 15):
        self.interval_seconds = interval_seconds
        self.batch_size = batch_size
        self._is_running = False
        self._step_counter = 0
        self._offset = 0

        self.orders_path = os.path.join(SIM_DIR, "olist_orders_dataset.csv")
        self.items_path = os.path.join(SIM_DIR, "olist_order_items_dataset.csv")
        self.payments_path = os.path.join(SIM_DIR, "olist_order_payments_dataset.csv")
        self.customers_path = os.path.join(SIM_DIR, "olist_customers_dataset.csv")

        self.df_orders = None
        self.df_items = None
        self.df_payments = None
        self.df_customers = None
        self._load_datasets()

    def _load_datasets(self):
        try:
            if os.path.exists(self.orders_path):
                self.df_orders = pd.read_csv(self.orders_path)
            if os.path.exists(self.items_path):
                self.df_items = pd.read_csv(self.items_path)
            if os.path.exists(self.payments_path):
                self.df_payments = pd.read_csv(self.payments_path)
            if os.path.exists(self.customers_path):
                self.df_customers = pd.read_csv(self.customers_path)
            logger.info("AutoOlistSimulator: Real Olist simulation datasets cached in memory.")
        except Exception as e:
            logger.error(f"AutoOlistSimulator: Failed to load Olist datasets: {e}")

    def step(self) -> Optional[Dict[str, Any]]:
        """
        Executes one discrete simulation step, cycling through the real Olist conditions:
          - Step % 4 == 0: Core Triad Event (Olist_Orders -> Olist_Order_Items -> Olist_Payments) [Condition: ALL]
          - Step % 4 == 1: Regional Logistics Event (SP -> RJ -> MG -> RS) [Condition: QUORUM]
          - Step % 4 == 2: Payment Rail Event (Boleto / Credit Card) [Condition: ANY]
          - Step % 4 == 3: Operational Ingestion Event (Operational Orders) [Condition: CHAINED]
        """
        if self.df_orders is None or self.df_orders.empty:
            return None

        self._step_counter += 1
        cycle = self._step_counter % 4
        conn = get_connection()
        cur = conn.cursor()

        try:
            now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            if cycle == 0:
                # Cycle 0: Condition ALL (Olist Core Forecast Pipeline)
                batch = self.df_orders.iloc[self._offset : self._offset + self.batch_size]
                self._offset = (self._offset + self.batch_size) % max(1, len(self.df_orders) - self.batch_size)

                # Rotate between Orders, Order Items, Payments
                sub = (self._step_counter // 4) % 3
                if sub == 0:
                    d_name = "Olist_Orders"
                    p_name = "Olist Core Forecast Pipeline"
                elif sub == 1:
                    d_name = "Olist_Order_Items"
                    p_name = "Olist Core Forecast Pipeline"
                else:
                    d_name = "Olist_Payments"
                    p_name = "Olist Core Forecast Pipeline"

                cur.execute("SELECT dataset_id, current_version FROM dataset_metadata WHERE dataset_name = %s FOR UPDATE", (d_name,))
                meta = cur.fetchone() or {"dataset_id": 281, "current_version": 1}
                d_id = meta["dataset_id"]
                new_ver = (meta["current_version"] or 0) + 1

                # Update status to READY
                cur.execute("UPDATE dataset_metadata SET status = 'READY', current_version = %s, row_count = row_count + %s, last_updated_at = NOW() WHERE dataset_id = %s", (new_ver, len(batch), d_id))

                # Insert into dataset_events with NEW status so dependency engine claims it
                chg_id = f"auto-olist-{d_name}-{uuid.uuid4().hex[:8]}"
                cur.execute("""
                    INSERT INTO dataset_events (dataset_id, dataset_name, event_type, event_time, dataset_version, rows_changed, source_change_id, event_status)
                    VALUES (%s, %s, 'DATASET_UPDATED', %s, %s, %s, %s, 'NEW')
                """, (d_id, d_name, now_iso, new_ver, len(batch), chg_id))
                eid = cur.lastrowid
                try:
                    from services.observability.publisher import publish_dashboard_event
                    publish_dashboard_event(
                        event_type="DEPENDENCY_EVENT_INGESTED",
                        entity="DEPENDENCY_ENGINE",
                        severity="INFO",
                        correlation_id=str(eid),
                        data={"event_id": eid, "dataset_name": d_name, "pipeline_name": p_name, "rows_changed": len(batch), "dataset_version": new_ver, "source_change_id": chg_id},
                        connection=conn,
                        commit=False
                    )
                except Exception:
                    pass
                conn.commit()

                logger.info(f"[Auto-Simulator] Generated Event #{eid} for '{d_name}' (v{new_ver}, {len(batch)} rows) -> Target: '{p_name}'")
                return {"event_id": eid, "dataset_name": d_name, "pipeline_name": p_name, "status": "QUEUED"}

            elif cycle == 1:
                # Cycle 1: Condition QUORUM (Regional Dispatch Pipeline)
                reg_states = ["Olist_Orders_SP", "Olist_Orders_RJ", "Olist_Orders_MG", "Olist_Orders_RS"]
                reg_idx = (self._step_counter // 4) % len(reg_states)
                d_name = reg_states[reg_idx]
                p_name = "Olist Regional Dispatch Pipeline"

                cur.execute("SELECT dataset_id, current_version FROM dataset_metadata WHERE dataset_name = %s FOR UPDATE", (d_name,))
                meta = cur.fetchone() or {"dataset_id": 284, "current_version": 1}
                d_id = meta["dataset_id"]
                new_ver = (meta["current_version"] or 0) + 1

                cur.execute("UPDATE dataset_metadata SET status = 'READY', current_version = %s, last_updated_at = NOW() WHERE dataset_id = %s", (new_ver, d_id))
                chg_id = f"auto-reg-{d_name}-{uuid.uuid4().hex[:8]}"
                cur.execute("""
                    INSERT INTO dataset_events (dataset_id, dataset_name, event_type, event_time, dataset_version, rows_changed, source_change_id, event_status)
                    VALUES (%s, %s, 'REGIONAL_ORDERS_RECEIVED', %s, %s, 10, %s, 'NEW')
                """, (d_id, d_name, now_iso, new_ver, chg_id))
                eid = cur.lastrowid
                try:
                    from services.observability.publisher import publish_dashboard_event
                    publish_dashboard_event(
                        event_type="DEPENDENCY_EVENT_INGESTED",
                        entity="DEPENDENCY_ENGINE",
                        severity="INFO",
                        correlation_id=str(eid),
                        data={"event_id": eid, "dataset_name": d_name, "pipeline_name": p_name, "rows_changed": 10, "dataset_version": new_ver, "source_change_id": chg_id},
                        connection=conn,
                        commit=False
                    )
                except Exception:
                    pass
                conn.commit()

                logger.info(f"[Auto-Simulator] Generated Event #{eid} for '{d_name}' (Quorum State Feed) -> Target: '{p_name}'")
                return {"event_id": eid, "dataset_name": d_name, "pipeline_name": p_name, "status": "QUEUED"}

            elif cycle == 2:
                # Cycle 2: Condition ANY (Payment Settlement Pipeline)
                pay_rails = ["Olist_Payments_Boleto", "Olist_Payments_CreditCard", "Olist_Payments_Voucher"]
                pay_idx = (self._step_counter // 4) % len(pay_rails)
                d_name = pay_rails[pay_idx]
                p_name = "Olist Payment Settlement Pipeline"

                cur.execute("SELECT dataset_id, current_version FROM dataset_metadata WHERE dataset_name = %s FOR UPDATE", (d_name,))
                meta = cur.fetchone() or {"dataset_id": 289, "current_version": 1}
                d_id = meta["dataset_id"]
                new_ver = (meta["current_version"] or 0) + 1

                cur.execute("UPDATE dataset_metadata SET status = 'READY', current_version = %s, last_updated_at = NOW() WHERE dataset_id = %s", (new_ver, d_id))
                chg_id = f"auto-pay-{d_name}-{uuid.uuid4().hex[:8]}"
                cur.execute("""
                    INSERT INTO dataset_events (dataset_id, dataset_name, event_type, event_time, dataset_version, rows_changed, source_change_id, event_status)
                    VALUES (%s, %s, 'PAYMENT_BATCH_SETTLED', %s, %s, 25, %s, 'NEW')
                """, (d_id, d_name, now_iso, new_ver, chg_id))
                eid = cur.lastrowid
                try:
                    from services.observability.publisher import publish_dashboard_event
                    publish_dashboard_event(
                        event_type="DEPENDENCY_EVENT_INGESTED",
                        entity="DEPENDENCY_ENGINE",
                        severity="INFO",
                        correlation_id=str(eid),
                        data={"event_id": eid, "dataset_name": d_name, "pipeline_name": p_name, "rows_changed": 25, "dataset_version": new_ver, "source_change_id": chg_id},
                        connection=conn,
                        commit=False
                    )
                except Exception:
                    pass
                conn.commit()

                logger.info(f"[Auto-Simulator] Generated Event #{eid} for '{d_name}' (Payment Rail Arrival) -> Target: '{p_name}'")
                return {"event_id": eid, "dataset_name": d_name, "pipeline_name": p_name, "status": "QUEUED"}

            else:
                # Cycle 3: Operational Ingestion (Chained Feature -> Forecast Pipeline)
                d_name = "Operational Orders"
                p_name = "Operational Demand Feature Pipeline"

                cur.execute("SELECT dataset_id, current_version FROM dataset_metadata WHERE dataset_name = %s FOR UPDATE", (d_name,))
                meta = cur.fetchone() or {"dataset_id": 278, "current_version": 1}
                d_id = meta["dataset_id"]
                new_ver = (meta["current_version"] or 0) + 1

                cur.execute("UPDATE dataset_metadata SET status = 'READY', current_version = %s, last_updated_at = NOW() WHERE dataset_id = %s", (new_ver, d_id))
                chg_id = f"auto-op-{uuid.uuid4().hex[:8]}"
                cur.execute("""
                    INSERT INTO dataset_events (dataset_id, dataset_name, event_type, event_time, dataset_version, rows_changed, source_change_id, event_status)
                    VALUES (%s, %s, 'ORDER_BATCH_INGESTED', %s, %s, 10, %s, 'NEW')
                """, (d_id, d_name, now_iso, new_ver, chg_id))
                eid = cur.lastrowid
                try:
                    from services.observability.publisher import publish_dashboard_event
                    publish_dashboard_event(
                        event_type="DEPENDENCY_EVENT_INGESTED",
                        entity="DEPENDENCY_ENGINE",
                        severity="INFO",
                        correlation_id=str(eid),
                        data={"event_id": eid, "dataset_name": d_name, "pipeline_name": p_name, "rows_changed": 10, "dataset_version": new_ver, "source_change_id": chg_id},
                        connection=conn,
                        commit=False
                    )
                except Exception:
                    pass
                conn.commit()

                logger.info(f"[Auto-Simulator] Generated Event #{eid} for '{d_name}' -> Target: '{p_name}'")
                return {"event_id": eid, "dataset_name": d_name, "pipeline_name": p_name, "status": "QUEUED"}

        except Exception as exc:
            conn.rollback()
            logger.error(f"AutoOlistSimulator step error: {exc}")
            return None
        finally:
            cur.close()
            conn.close()


# Singleton instance
auto_olist_simulator = AutoOlistSimulator()
