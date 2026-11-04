"""
SupplySense AI - Olist Historical Order Replay Simulator

Controls and executes realistic, time-spaced historical order replay using
the Brazilian E-Commerce Olist datasets (Olist/):
  - Filters out-of-sample orders (e.g. 2018-W36 through 2018-W42, with active window fallback).
  - Excludes canceled and unavailable orders.
  - Groups items by (order_id, product_id) to accurately calculate quantities and unit prices.
  - Maps historical 7-week demand distribution across a recent 8-week operational simulation window.
  - Preserves chronological sequence, original timestamp, and catalog product SKUs.
  - Submits batches (default 25 orders every 5.0 seconds) exclusively to POST /api/observability/orders.
  - Supports start, pause, resume, and stop controls with idempotent resuming.
  - Emits real-time telemetry and dashboard lifecycle events.
"""

import os
import sys
import uuid
import time
import logging
import datetime
import threading
from typing import Dict, Any, List, Optional, Tuple
import pandas as pd

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

logger = logging.getLogger(__name__)


class OlistReplayLoader:
    """Loads, validates, filters, and transforms Olist CSV datasets into operational batches."""

    _cached_orders = None
    _cached_items = None

    def __init__(self, olist_dir: Optional[str] = None):
        self.olist_dir = olist_dir or os.path.join(REPO_ROOT, "Olist")
        self.orders_path = os.path.join(self.olist_dir, "olist_orders_dataset.csv")
        self.items_path = os.path.join(self.olist_dir, "olist_order_items_dataset.csv")
        self.products_path = os.path.join(self.olist_dir, "olist_products_dataset.csv")

    def load_batches(
        self,
        window_start: str = "2018-W36",
        window_end: str = "2018-W42",
        auto_fallback: bool = True,
        batch_size: int = 25,
        simulation_window_weeks: int = 8,
        run_id: Optional[str] = None
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Loads Olist orders and order items, filters by window, maps timestamps,
        and constructs batches matching OperationalOrderBatchRequest.
        """
        if not os.path.isdir(self.olist_dir):
            raise FileNotFoundError(f"Olist dataset directory not found at: {self.olist_dir}")

        run_id = run_id or uuid.uuid4().hex[:8]
        logger.info(f"Loading Olist datasets from {self.olist_dir} for window {window_start}..{window_end}...")

        # 1. Load orders and items (cached in-memory for fast batch simulation)
        if OlistReplayLoader._cached_orders is None:
            orders = pd.read_csv(self.orders_path, low_memory=False)
            orders = orders[~orders["order_status"].isin(["canceled", "unavailable"])].copy()
            orders["ts"] = pd.to_datetime(orders["order_purchase_timestamp"])
            iso_cal = orders["ts"].dt.isocalendar()
            orders["year_week"] = iso_cal["year"].astype(str) + "-W" + iso_cal["week"].astype(str).str.zfill(2)
            OlistReplayLoader._cached_orders = orders

        if OlistReplayLoader._cached_items is None:
            OlistReplayLoader._cached_items = pd.read_csv(self.items_path, low_memory=False)

        eligible_orders = OlistReplayLoader._cached_orders
        df_items = OlistReplayLoader._cached_items

        # 2. Filter orders within target window
        window_orders = eligible_orders[
            (eligible_orders["year_week"] >= window_start) & (eligible_orders["year_week"] <= window_end)
        ].copy()

        active_window_used = f"{window_start}..{window_end}"
        fallback_applied = False

        # In Olist, order data collection ceased in late August 2018 (W35), leaving only 1 non-canceled
        # order in W36-W42. If auto_fallback is enabled and count < batch_size, shift to the 7 active post-holdout weeks.
        if len(window_orders) < batch_size and auto_fallback:
            fallback_start = "2018-W29"
            fallback_end = window_end
            window_orders = eligible_orders[
                (eligible_orders["year_week"] >= fallback_start) & (eligible_orders["year_week"] <= fallback_end)
            ].copy()
            active_window_used = f"{fallback_start}..{fallback_end}"
            fallback_applied = True
            logger.info(
                f"Auto-fallback applied: Expanded window to {active_window_used} "
                f"({len(window_orders):,} eligible orders found)."
            )

        if window_orders.empty:
            raise ValueError(f"No eligible orders found in window {active_window_used}.")

        # 4. Sort chronologically
        window_orders = window_orders.sort_values("ts").reset_index(drop=True)
        order_ids = set(window_orders["order_id"])

        # 5. Filter and group order items by (order_id, product_id)
        matched_items = df_items[df_items["order_id"].isin(order_ids)].copy()
        grouped_items = matched_items.groupby(["order_id", "product_id"]).agg(
            quantity=("order_item_id", "count"),
            unit_price=("price", "mean")
        ).reset_index()
        grouped_items["unit_price"] = grouped_items["unit_price"].round(2)

        # Build order-to-items mapping
        items_by_order = {}
        for oid, pid, qty, price in zip(
            grouped_items["order_id"],
            grouped_items["product_id"],
            grouped_items["quantity"],
            grouped_items["unit_price"]
        ):
            if oid not in items_by_order:
                items_by_order[oid] = []
            items_by_order[oid].append({
                "sku": str(pid).strip(),
                "quantity": int(qty),
                "unit_price": float(price)
            })

        # 6. Timestamp Transformation: Map historical span onto recent 8-week window
        t_min = window_orders["ts"].min()
        t_max = window_orders["ts"].max()
        hist_span_seconds = max(1.0, (t_max - t_min).total_seconds())

        now = datetime.datetime.now(datetime.timezone.utc)
        sim_end = now
        sim_start = sim_end - datetime.timedelta(weeks=simulation_window_weeks)
        sim_span_seconds = (sim_end - sim_start).total_seconds()

        # 7. Construct Batches (batch_size orders per batch)
        batches = []
        batch_idx = 1
        current_batch_orders = []
        total_items_count = 0

        for oid, orig_ts in zip(window_orders["order_id"], window_orders["ts"]):
            # Calculate scaled simulation timestamp
            fraction = (orig_ts - t_min).total_seconds() / hist_span_seconds
            sim_ts = sim_start + datetime.timedelta(seconds=fraction * sim_span_seconds)
            sim_ts_iso = sim_ts.isoformat()

            order_line_items = items_by_order.get(oid, [])
            if not order_line_items:
                continue

            # Add each product line item for this order
            for item_idx, item in enumerate(order_line_items, 1):
                if len(order_line_items) == 1:
                    ord_key = f"sim_{run_id}_{oid}"[:64]
                else:
                    ord_key = f"sim_{run_id}_{oid[:24]}_{item_idx:02d}"[:64]

                current_batch_orders.append({
                    "order_id": ord_key,
                    "sku": item["sku"],
                    "warehouse_id": "WH-SP-01",
                    "quantity": item["quantity"],
                    "unit_price": item["unit_price"],
                    "event_time": sim_ts_iso,
                    "idempotency_key": f"OLIST-{run_id}-{oid}-{item['sku'][:8]}"
                })
                total_items_count += 1

            # Check if current batch has reached batch_size orders
            distinct_orders_in_batch = len(set(o["order_id"] for o in current_batch_orders))
            if distinct_orders_in_batch >= batch_size:
                batch_key = f"OLIST-REPLAY-{run_id}-B{batch_idx:04d}"
                batches.append({
                    "batch_index": batch_idx,
                    "batch_id": batch_key,
                    "idempotency_key": batch_key,
                    "source": "SIMULATED",
                    "warehouse_id": "WH-SP-01",
                    "orders": current_batch_orders,
                    "orders_count": distinct_orders_in_batch,
                    "items_count": len(current_batch_orders),
                    "min_sim_time": current_batch_orders[0]["event_time"],
                    "max_sim_time": current_batch_orders[-1]["event_time"]
                })
                batch_idx += 1
                current_batch_orders = []

        # Remaining orders in final partial batch
        if current_batch_orders:
            distinct_orders_in_batch = len(set(o["order_id"] for o in current_batch_orders))
            batch_key = f"OLIST-REPLAY-{run_id}-B{batch_idx:04d}"
            batches.append({
                "batch_index": batch_idx,
                "batch_id": batch_key,
                "idempotency_key": batch_key,
                "source": "SIMULATED",
                "warehouse_id": "WH-SP-01",
                "orders": current_batch_orders,
                "orders_count": distinct_orders_in_batch,
                "items_count": len(current_batch_orders),
                "min_sim_time": current_batch_orders[0]["event_time"],
                "max_sim_time": current_batch_orders[-1]["event_time"]
            })

        metadata = {
            "run_id": run_id,
            "window_requested": f"{window_start}..{window_end}",
            "window_active": active_window_used,
            "auto_fallback_applied": fallback_applied,
            "total_eligible_orders": len(window_orders),
            "total_line_items": total_items_count,
            "total_batches": len(batches),
            "batch_size": batch_size,
            "historical_min_date": str(t_min),
            "historical_max_date": str(t_max),
            "historical_span_days": round(hist_span_seconds / 86400, 1),
            "simulation_window_weeks": simulation_window_weeks,
            "sim_start_date": sim_start.isoformat(),
            "sim_end_date": sim_end.isoformat()
        }

        logger.info(
            f"Olist Replay Loader prepared {len(batches)} batches ({len(window_orders)} orders, "
            f"{total_items_count} items) across window {active_window_used}."
        )
        return batches, metadata


class OlistReplaySimulator:
    """
    Manages the lifecycle and state machine of the Olist historical order replay.
    Supports thread-safe start, pause, resume, and stop controls.
    """

    def __init__(self, base_url: Optional[str] = None):
        self.base_url = (base_url or os.environ.get("OBSERVABILITY_URL", "http://127.0.0.1:8000")).rstrip("/")
        self.lock = threading.RLock()
        self.pause_event = threading.Event()
        self.stop_event = threading.Event()
        self.worker_thread: Optional[threading.Thread] = None

        # State fields
        self.status = "IDLE"  # IDLE | RUNNING | PAUSED | STOPPED | COMPLETED
        self.run_id: Optional[str] = None
        self.batches: List[Dict[str, Any]] = []
        self.metadata: Dict[str, Any] = {}
        self.current_batch_index = 0
        self.total_batches = 0
        self.total_eligible_orders = 0
        self.processed_orders = 0
        self.successful_batches = 0
        self.failed_batches = 0
        self.interval_seconds = 5.0
        self._http_available: Optional[bool] = None
        self.last_error: Optional[str] = None
        self.last_batch_time: Optional[str] = None
        self.started_at: Optional[str] = None

    def start(
        self,
        batch_size: int = 25,
        interval_seconds: float = 5.0,
        window_start: str = "2018-W36",
        window_end: str = "2018-W42",
        auto_fallback: bool = True,
        max_batches: Optional[int] = None
    ) -> Dict[str, Any]:
        """Starts a new historical replay simulation."""
        with self.lock:
            if self.status == "RUNNING":
                raise RuntimeError("Simulator is already RUNNING. Pause or stop it before starting a new run.")

            # Reset events
            self.pause_event.set()  # Unpaused
            self.stop_event.clear()

            self.run_id = f"replay_{uuid.uuid4().hex[:10]}"
            self.interval_seconds = max(0.5, float(interval_seconds))

            # Load batches from Olist CSVs
            loader = OlistReplayLoader()
            batches, metadata = loader.load_batches(
                window_start=window_start,
                window_end=window_end,
                auto_fallback=auto_fallback,
                batch_size=batch_size,
                run_id=self.run_id
            )

            if max_batches and max_batches > 0:
                batches = batches[:max_batches]
                metadata["total_batches"] = len(batches)
                metadata["total_eligible_orders"] = sum(b["orders_count"] for b in batches)

            self.batches = batches
            self.metadata = metadata
            self.total_batches = len(batches)
            self.total_eligible_orders = metadata["total_eligible_orders"]
            self.current_batch_index = 0
            self.processed_orders = 0
            self.successful_batches = 0
            self.failed_batches = 0
            self.last_error = None
            self._http_available = None
            self.started_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
            self.status = "RUNNING"

            # Spawn background replay worker thread
            self.worker_thread = threading.Thread(
                target=self._replay_loop,
                daemon=True,
                name=f"OlistReplayWorker-{self.run_id}"
            )

        self.worker_thread.start()
        # Emit dashboard event
        self._emit_status_event("STARTED")
        return self.get_status()

    def pause(self) -> Dict[str, Any]:
        """Pauses the active simulation at the current batch index."""
        with self.lock:
            if self.status != "RUNNING":
                return self.get_status()

            self.pause_event.clear()  # Triggers pause wait
            self.status = "PAUSED"
            logger.info(f"Olist Replay PAUSED at batch #{self.current_batch_index + 1}/{self.total_batches}.")

        self._emit_status_event("PAUSED")
        return self.get_status()

    def resume(self) -> Dict[str, Any]:
        """Resumes the paused simulation from the current batch index."""
        with self.lock:
            if self.status != "PAUSED":
                return self.get_status()

            self.status = "RUNNING"
            self.pause_event.set()  # Wakes worker
            logger.info(f"Olist Replay RESUMED from batch #{self.current_batch_index + 1}/{self.total_batches}.")

        self._emit_status_event("RESUMED")
        return self.get_status()

    def stop(self) -> Dict[str, Any]:
        """Stops and terminates the simulation run."""
        with self.lock:
            if self.status in ("STOPPED", "IDLE"):
                return self.get_status()

            self.stop_event.set()
            self.pause_event.set()  # Unblock if paused
            self.status = "STOPPED"
            logger.info(f"Olist Replay STOPPED at batch #{self.current_batch_index + 1}/{self.total_batches}.")
            self._emit_status_event("STOPPED")
            th = self.worker_thread

        if th and th.is_alive() and threading.current_thread() != th:
            th.join(timeout=1.5)

        return self.get_status()

    def get_status(self) -> Dict[str, Any]:
        """Returns comprehensive telemetry on the simulation state and progress."""
        with self.lock:
            remaining = max(0, self.total_eligible_orders - self.processed_orders)
            progress = (
                round((self.current_batch_index / self.total_batches) * 100, 2)
                if self.total_batches > 0
                else 0.0
            )

            return {
                "status": self.status,
                "run_id": self.run_id,
                "total_eligible_orders": self.total_eligible_orders,
                "processed_orders": self.processed_orders,
                "remaining_orders": remaining,
                "current_batch": min(self.current_batch_index + 1, self.total_batches) if self.total_batches > 0 else 0,
                "total_batches": self.total_batches,
                "successful_batches": self.successful_batches,
                "failed_batches": self.failed_batches,
                "progress_pct": progress,
                "interval_seconds": self.interval_seconds,
                "started_at": self.started_at,
                "last_batch_time": self.last_batch_time,
                "last_error": self.last_error,
                "metadata": self.metadata
            }

    def preview(
        self,
        window_start: str = "2018-W36",
        window_end: str = "2018-W42",
        auto_fallback: bool = True,
        batch_size: int = 25
    ) -> Dict[str, Any]:
        """Preview dataset filtering and batch counts without starting execution."""
        loader = OlistReplayLoader()
        batches, metadata = loader.load_batches(
            window_start=window_start,
            window_end=window_end,
            auto_fallback=auto_fallback,
            batch_size=batch_size
        )
        sample_batch = batches[0] if batches else None
        return {
            "metadata": metadata,
            "sample_batch": {
                "batch_id": sample_batch["batch_id"] if sample_batch else None,
                "orders_count": sample_batch["orders_count"] if sample_batch else 0,
                "sample_orders": sample_batch["orders"][:3] if sample_batch else []
            }
        }

    def _replay_loop(self):
        """Worker thread that executes batch submissions at configured intervals."""
        logger.info(f"Olist Replay worker started for run #{self.run_id}. Total batches: {self.total_batches}")

        while self.current_batch_index < self.total_batches:
            # 1. Check for stop request
            if self.stop_event.is_set():
                logger.info("Olist Replay worker detected stop signal. Terminating loop.")
                break

            # 2. Check for pause request (wait until unpaused or stopped)
            while not self.pause_event.is_set():
                if self.stop_event.is_set():
                    break
                time.sleep(0.5)

            if self.stop_event.is_set():
                break

            batch = self.batches[self.current_batch_index]
            batch_id = batch["batch_id"]
            logger.info(
                f"Submitting batch #{self.current_batch_index + 1}/{self.total_batches} "
                f"({batch['orders_count']} orders, {batch['items_count']} items) [idempotency_key={batch_id}]..."
            )

            # 3. Submit batch to operational ingestion API
            success = self._submit_batch(batch)
            now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

            with self.lock:
                self.last_batch_time = now_iso
                if success:
                    self.successful_batches += 1
                    self.processed_orders += batch["orders_count"]
                    self.current_batch_index += 1
                else:
                    self.failed_batches += 1
                    # Skip to next batch on error or retry
                    self.current_batch_index += 1

            # 4. Sleep for configured batch interval (responsive to stop/pause)
            sleep_remaining = self.interval_seconds
            while sleep_remaining > 0 and not self.stop_event.is_set():
                sleep_chunk = min(0.5, sleep_remaining)
                time.sleep(sleep_chunk)
                sleep_remaining -= sleep_chunk

        # Completion handling
        with self.lock:
            if not self.stop_event.is_set() and self.current_batch_index >= self.total_batches:
                self.status = "COMPLETED"
                logger.info(
                    f"Olist Replay COMPLETED: Successfully processed {self.processed_orders} orders "
                    f"across {self.successful_batches} batches."
                )
                self._emit_status_event("COMPLETED")

    def _submit_batch(self, batch: Dict[str, Any]) -> bool:
        """Submits batch payload to the operational ingestion API with resilient fallback."""
        payload = {
            "idempotency_key": batch["idempotency_key"],
            "source": batch["source"],
            "warehouse_id": batch["warehouse_id"],
            "orders": batch["orders"]
        }

        # 1. Attempt HTTP submission via urllib/requests if reachable
        if self._http_available is not False:
            try:
                import urllib.request
                import json

                url = f"{self.base_url}/api/observability/orders"
                data_bytes = json.dumps(payload).encode("utf-8")
                req = urllib.request.Request(
                    url,
                    data=data_bytes,
                    headers={"Content-Type": "application/json"}
                )
                with urllib.request.urlopen(req, timeout=1.0) as resp:
                    if resp.status in (200, 201):
                        self._http_available = True
                        return True
                    else:
                        self.last_error = f"HTTP error {resp.status}"
                        logger.warning(f"Batch submission returned non-200 status: {resp.status}")
            except Exception as http_err:
                self._http_available = False
                logger.debug(f"HTTP batch submission failed, using direct in-process service call: {http_err}")

        # 2. Resilient in-process service fallback (for testing or co-hosted daemon)
        try:
            from services.observability import service as obs_service
            res = obs_service.ingest_operational_orders(
                orders=payload["orders"],
                idempotency_key=payload["idempotency_key"],
                source=payload["source"],
                warehouse_id=payload["warehouse_id"]
            )
            return res.get("status") in ("INGESTED", "ALREADY_PROCESSED") or res.get("idempotent_replay", False)
        except Exception as svc_err:
            self.last_error = str(svc_err)
            logger.error(f"In-process batch ingestion failed: {svc_err}")
            return False

    def _emit_status_event(self, action: str):
        """Emits real-time SIMULATION_STATUS_CHANGED event for SSE dashboard streaming."""
        try:
            from services.observability.publisher import publish_dashboard_event
            publish_dashboard_event(
                event_type="SIMULATION_STATUS_CHANGED",
                entity="SIMULATOR",
                severity="INFO",
                data={
                    "simulator_type": "OLIST_REPLAY",
                    "action": action,
                    "status": self.status,
                    "run_id": self.run_id,
                    "current_batch": self.current_batch_index,
                    "total_batches": self.total_batches,
                    "processed_orders": self.processed_orders,
                    "total_eligible_orders": self.total_eligible_orders,
                    "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
                }
            )
        except Exception as exc:
            logger.debug(f"Failed to publish simulation status event: {exc}")


# Global singleton instance for application use
replay_simulator = OlistReplaySimulator()
