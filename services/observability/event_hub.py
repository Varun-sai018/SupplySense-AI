"""
SupplySense AI - Observability Event Hub & Versioned SSE Contract (Module 3)

Provides an asynchronous, memory-bounded pub/sub event hub for Server-Sent Events (SSE).
Includes:
  - Versioned dashboard event envelope (v1.0).
  - Subscriber tracking with bounded queues and disconnect cleanup.
  - Ring-buffered replay for Last-Event-ID reconnection.
  - Safe REST snapshot fallback signal when replay window has elapsed.
  - Heartbeat keep-alive emission to detect stale connections.
  - Thread-safe publishing from both synchronous workers and async handlers.
"""

import os
import sys
import json
import time
import asyncio
import logging
import threading
import datetime
from collections import deque
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

SCHEMA_VERSION = "1.0"
DEFAULT_RING_BUFFER_SIZE = 200
DEFAULT_CLIENT_QUEUE_MAXSIZE = 100
HEARTBEAT_INTERVAL_SECONDS = 15.0

# Supported event types
VALID_EVENT_TYPES = {
    "RULE_DECISION_EVALUATED",
    "CART_ABANDONMENT_TRIGGERED",
    "MARKETING_WEBHOOK_OUTCOME",
    "INVENTORY_REORDER_STATE_CHANGED",
    "DELIVERY_COMPLETED",
    "PAYMENT_GATEWAY_ALERT_TRIGGERED",
    "PAYMENT_GATEWAY_ALERT_RECOVERED",
    "PIPELINE_STATUS_CHANGED",
    "HEARTBEAT",
    "REST_SNAPSHOT_REQUIRED",
    "SYSTEM_NOTIFICATION",
    # Phase 2: Simulation lifecycle and readiness events
    "SIMULATION_STATUS_CHANGED",
    "DATASET_READINESS_CHANGED",
}

VALID_SEVERITIES = {"INFO", "WARNING", "CRITICAL"}


@dataclass
class DashboardEvent:
    """Versioned event contract for live dashboard telemetry stream."""
    event_id: str
    event_type: str
    entity: str
    severity: str = "INFO"
    data: Dict[str, Any] = field(default_factory=dict)
    schema_version: str = SCHEMA_VERSION
    timestamp: str = field(default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).isoformat())
    decision_id: Optional[int] = None
    execution_id: Optional[int] = None
    correlation_id: Optional[str] = None

    def validate(self) -> None:
        """Ensures the event conforms strictly to the versioned contract."""
        if not self.event_id or not isinstance(self.event_id, str):
            raise ValueError("event_id must be a non-empty string")
        if self.event_type not in VALID_EVENT_TYPES:
            raise ValueError(f"Unsupported event_type '{self.event_type}'. Valid: {sorted(VALID_EVENT_TYPES)}")
        if self.severity not in VALID_SEVERITIES:
            raise ValueError(f"Unsupported severity '{self.severity}'. Valid: {sorted(VALID_SEVERITIES)}")
        if not isinstance(self.data, dict):
            raise ValueError("data must be a dictionary")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_sse_format(self) -> str:
        """Formats the event into the SSE text/event-stream specification."""
        self.validate()
        payload = json.dumps(self.to_dict())
        return f"id: {self.event_id}\nevent: {self.event_type}\ndata: {payload}\n\n"


class EventHub:
    """
    Central event hub managing SSE client subscriptions, replay ring buffer,
    and thread-safe broadcasts.
    """

    def __init__(self, buffer_size: int = DEFAULT_RING_BUFFER_SIZE):
        self.buffer_size = buffer_size
        self._ring_buffer: deque[DashboardEvent] = deque(maxlen=buffer_size)
        self._subscribers: Set[asyncio.Queue] = set()
        self._lock = threading.Lock()
        self._sequence: int = 0
        self._main_loop: Optional[asyncio.AbstractEventLoop] = None

    def set_event_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Registers the running asyncio event loop for thread-safe cross-thread scheduling."""
        self._main_loop = loop

    def subscribe(self, maxsize: int = DEFAULT_CLIENT_QUEUE_MAXSIZE) -> asyncio.Queue:
        """Creates and registers a bounded queue for a new SSE client connection."""
        queue: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
        with self._lock:
            self._subscribers.add(queue)
            logger.info(f"SSE client subscribed. Active connections: {len(self._subscribers)}")
        return queue

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        """Unregisters an SSE client queue and cleans up resources."""
        with self._lock:
            self._subscribers.discard(queue)
            logger.info(f"SSE client unsubscribed. Remaining active connections: {len(self._subscribers)}")

    def subscriber_count(self) -> int:
        with self._lock:
            return len(self._subscribers)

    def generate_event_id(self) -> str:
        with self._lock:
            self._sequence += 1
            return f"evt_{int(time.time() * 1000)}_{self._sequence}"

    def get_replay_events(self, last_event_id: str) -> Tuple[bool, List[DashboardEvent]]:
        """
        Retrieves events newer than last_event_id from the ring buffer.
        Returns (is_found, list_of_events).
        If last_event_id has expired or is unknown, returns (False, []).
        """
        with self._lock:
            events_list = list(self._ring_buffer)

        # Check if last_event_id is present
        found_idx = -1
        for idx, ev in enumerate(events_list):
            if ev.event_id == last_event_id:
                found_idx = idx
                break

        if found_idx == -1:
            # Replay window elapsed or invalid ID
            return False, []

        return True, events_list[found_idx + 1:]

    def publish(self, event: DashboardEvent) -> None:
        """
        Publishes an event to the ring buffer and distributes to all active subscriber queues.
        Non-blocking: slow clients whose queues are full drop the oldest message to avoid memory leaks.
        Deduplicates against the ring buffer so re-polled events are never broadcast twice.
        """
        event.validate()

        with self._lock:
            for ev in self._ring_buffer:
                if ev.event_id == event.event_id:
                    return
            self._ring_buffer.append(event)
            subscribers = list(self._subscribers)

        if not subscribers:
            return

        def _distribute():
            for q in subscribers:
                try:
                    if q.full():
                        try:
                            # Drop oldest to avoid blocking event loop
                            q.get_nowait()
                        except (asyncio.QueueEmpty, ValueError):
                            pass
                    q.put_nowait(event)
                except Exception as e:
                    logger.debug(f"Failed to queue event for subscriber: {e}")

        # Check if we are running inside the event loop thread
        try:
            curr_loop = asyncio.get_running_loop()
            if curr_loop is self._main_loop or curr_loop.is_running():
                _distribute()
                return
        except RuntimeError:
            pass

        # We are in a separate thread (e.g. synchronous worker)
        if self._main_loop and self._main_loop.is_running():
            self._main_loop.call_soon_threadsafe(_distribute)
        else:
            _distribute()

    def publish_event(
        self,
        event_type: str,
        entity: str,
        severity: str = "INFO",
        data: Optional[Dict[str, Any]] = None,
        decision_id: Optional[int] = None,
        execution_id: Optional[int] = None,
        correlation_id: Optional[str] = None
    ) -> DashboardEvent:
        """Convenience method to construct, validate, and broadcast a DashboardEvent."""
        event = DashboardEvent(
            event_id=self.generate_event_id(),
            event_type=event_type,
            entity=entity,
            severity=severity,
            data=data or {},
            schema_version=SCHEMA_VERSION,
            timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            decision_id=decision_id,
            execution_id=execution_id,
            correlation_id=correlation_id
        )
        self.publish(event)
        return event

    def format_heartbeat(self) -> str:
        """Formats an SSE heartbeat / keep-alive comment."""
        return f": keep-alive {datetime.datetime.now(datetime.timezone.utc).isoformat()}\n\n"

    def format_snapshot_required(self) -> str:
        """Formats the REST_SNAPSHOT_REQUIRED event instructing client to re-sync."""
        event = DashboardEvent(
            event_id=self.generate_event_id(),
            event_type="REST_SNAPSHOT_REQUIRED",
            entity="SYSTEM",
            severity="WARNING",
            data={
                "action": "FETCH_REST_SNAPSHOT",
                "message": "Replay window elapsed or unrecoverable Last-Event-ID. Fetch fresh snapshot from REST endpoints.",
                "endpoints": [
                    "/api/observability/summary",
                    "/api/observability/executions",
                    "/api/observability/dependencies"
                ]
            }
        )
        return event.to_sse_format()


# Global Singleton EventHub instance for the application process
hub = EventHub()
