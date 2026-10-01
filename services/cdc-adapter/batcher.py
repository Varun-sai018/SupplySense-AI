"""
SupplySense AI - CDC Micro-Batching Layer (Task 8.4)

Groups normalized CDC events by dataset, prevents duplicate source changes from
entering the batch, and flushes consolidated dataset events when either maximum
event count or maximum wait time is reached.
"""

import time
import uuid
import logging
from typing import Dict, Any, List, Optional, Callable
from collections import defaultdict

from config import settings

logger = logging.getLogger(__name__)


class CDCBatcher:
    """
    In-memory micro-batching buffer for CDC events.
    Groups events by dataset and emits consolidated dataset events upon threshold or timeout.
    """

    def __init__(
        self,
        max_events: Optional[int] = None,
        max_wait_ms: Optional[int] = None,
        flush_callback: Optional[Callable[[Dict[str, Any]], Any]] = None,
        connection=None,
        seen_cache_size: int = 10000
    ):
        self.max_events = max_events if max_events is not None else getattr(settings, 'CDC_BATCH_MAX_EVENTS', 100)
        self.max_wait_ms = max_wait_ms if max_wait_ms is not None else getattr(settings, 'CDC_BATCH_MAX_WAIT_MS', 1000)
        self.flush_callback = flush_callback
        self.connection = connection

        # Buffers: dataset_name -> list of normalized_cdc dicts
        self.buffers: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        # First event arrival timestamp: dataset_name -> float (epoch seconds)
        self.first_event_times: Dict[str, float] = {}

        # In-memory deduplication set for fast lookups
        self.seen_source_change_ids: set = set()
        self.seen_cache_size = seen_cache_size

    def is_duplicate(self, source_change_id: Optional[str], connection=None) -> bool:
        """
        Checks if a source_change_id has already been processed or is pending in buffer.
        """
        if not source_change_id:
            return False

        if source_change_id in self.seen_source_change_ids:
            return True

        # Check DB if connection is available
        conn = connection or self.connection
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("""
                    SELECT event_id FROM dataset_events
                    WHERE source_change_id = %s
                    LIMIT 1
                """, (source_change_id,))
                row = cur.fetchone()
                if row:
                    self._add_to_seen_cache(source_change_id)
                    return True
            except Exception as e:
                logger.warning(f"Error checking duplicate in DB: {e}")

        return False

    def _add_to_seen_cache(self, source_change_id: str):
        if len(self.seen_source_change_ids) >= self.seen_cache_size:
            self.seen_source_change_ids.clear()
        self.seen_source_change_ids.add(source_change_id)

    def add_event(
        self,
        normalized_cdc: Dict[str, Any],
        connection=None
    ) -> Optional[Dict[str, Any]]:
        """
        Adds a normalized CDC event to the appropriate dataset batch buffer.
        If the event triggers a flush (max_events reached), returns the consolidated batch outcome.
        If the event is a duplicate, returns a duplicate skipped outcome without buffering.
        """
        dataset_name = normalized_cdc.get("dataset_name")
        if not dataset_name:
            return None

        source_change_id = normalized_cdc.get("source_change_id")

        # Deduplication check
        if source_change_id and self.is_duplicate(source_change_id, connection=connection):
            logger.info(
                f"CDC event duplicate ignored in batcher: dataset={dataset_name} source_change_id={source_change_id}"
            )
            return {
                "status": "DUPLICATE_SKIPPED",
                "dataset_name": dataset_name,
                "is_duplicate": True,
                "source_change_id": source_change_id
            }

        # Track in seen cache
        if source_change_id:
            self._add_to_seen_cache(source_change_id)

        # Buffer the event
        now = time.time()
        if not self.buffers[dataset_name]:
            self.first_event_times[dataset_name] = now

        self.buffers[dataset_name].append(normalized_cdc)

        # Check max_events threshold
        if len(self.buffers[dataset_name]) >= self.max_events:
            return self.flush_dataset(dataset_name, connection=connection)

        return None

    def check_timeouts(self, connection=None) -> List[Dict[str, Any]]:
        """
        Checks all dataset buffers and flushes any whose wait time has exceeded max_wait_ms.
        """
        results = []
        now = time.time()
        max_wait_sec = self.max_wait_ms / 1000.0

        datasets = list(self.buffers.keys())
        for dataset_name in datasets:
            events = self.buffers.get(dataset_name, [])
            if events:
                first_time = self.first_event_times.get(dataset_name, now)
                if (now - first_time) >= max_wait_sec:
                    res = self.flush_dataset(dataset_name, connection=connection)
                    if res:
                        results.append(res)
        return results

    def flush_dataset(
        self,
        dataset_name: str,
        connection=None
    ) -> Optional[Dict[str, Any]]:
        """
        Flushes the buffered events for a specific dataset into a consolidated dataset event.
        """
        events = self.buffers.pop(dataset_name, [])
        self.first_event_times.pop(dataset_name, None)

        if not events:
            return None

        batch_id = f"batch_{dataset_name.lower()}_{uuid.uuid4().hex[:12]}"
        consolidated_event = self._build_consolidated_event(dataset_name, events, batch_id)

        if self.flush_callback:
            try:
                return self.flush_callback(consolidated_event, connection=connection)
            except TypeError:
                try:
                    return self.flush_callback(consolidated_event, connection)
                except TypeError:
                    return self.flush_callback(consolidated_event)

        return consolidated_event

    def flush_all(self, connection=None) -> List[Dict[str, Any]]:
        """
        Flushes all buffered events across all datasets.
        """
        results = []
        datasets = list(self.buffers.keys())
        for dataset_name in datasets:
            res = self.flush_dataset(dataset_name, connection=connection)
            if res:
                results.append(res)
        return results

    def get_pending_count(self, dataset_name: Optional[str] = None) -> int:
        """Returns total count of currently buffered events."""
        if dataset_name:
            return len(self.buffers.get(dataset_name, []))
        return sum(len(evs) for evs in self.buffers.values())

    def _build_consolidated_event(
        self,
        dataset_name: str,
        events: List[Dict[str, Any]],
        batch_id: str
    ) -> Dict[str, Any]:
        """
        Aggregates multiple CDC events into one consolidated event structure.
        """
        total_rows = len(events)
        last_event = events[-1]
        first_event = events[0]

        net_delta = 0
        for ev in events:
            op = ev.get("op", "c")
            if op == "d":
                net_delta -= 1
            elif op in ("c", "r"):
                net_delta += 1

        source_file = last_event.get("source_file") or first_event.get("source_file")
        source_pos = last_event.get("source_pos") or first_event.get("source_pos")
        source_change_id = last_event.get("source_change_id") if total_rows == 1 else None

        return {
            "dataset_name": dataset_name,
            "table_name": last_event.get("table_name", ""),
            "op": last_event.get("op", "c"),
            "event_type": "DATASET_UPDATED",
            "batch_id": batch_id,
            "rows_changed": total_rows,
            "net_row_delta": net_delta,
            "source_file": source_file,
            "source_pos": source_pos,
            "source_change_id": source_change_id,
            "batched_events_count": total_rows,
            "is_batch": True,
            "source_change_ids": [ev.get("source_change_id") for ev in events if ev.get("source_change_id")]
        }
