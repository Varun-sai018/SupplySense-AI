"""
SupplySense AI - Simulation Engine (Phase 2)

Implements a thread-safe simulation lifecycle with configurable Kafka event
generation at 5-10 second intervals.

Lifecycle controls:
  start(scenario_id, mode)  — start a named scenario or continuous rotation.
  pause()                   — suspend the worker without stopping it.
  resume()                  — continue after a pause.
  stop()                    — gracefully shut down the worker and reset state.
  get_status()              — return a JSON-serializable status snapshot.

Safety Invariants:
  - Never invokes deliver_replenishment() or any operational order API.
  - Never changes operational_inventory rows.
  - Never creates replenishment or supplier orders.
  - Never triggers Airflow DAGs.
  - Never runs ML training or XGBoost.
  - Never modifies historical Olist data.
  - Never modifies protected executions #36, #99586, #99653.
  - Kafka publish failures are logged; the event counter is NOT incremented.
  - At most one worker thread is active at any time.
"""

import os
import sys
import json
import uuid
import time
import logging
import datetime
import threading
from enum import Enum
from typing import Optional, Dict, Any, List

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from config import settings
from services.simulator.scenarios import (
    SimScenario,
    SimEvent,
    SCENARIO_REGISTRY,
    SCENARIO_ROTATION_ORDER,
    list_scenarios,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MIN_INTERVAL_SECONDS = 5
MAX_INTERVAL_SECONDS = 10

# Sentinel: used as the mode for continuous rotation
MODE_CONTINUOUS = "CONTINUOUS"

# ---------------------------------------------------------------------------
# Engine State Enum
# ---------------------------------------------------------------------------

class SimEngineState(str, Enum):
    IDLE = "IDLE"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    STOPPING = "STOPPING"
    ERROR = "ERROR"


# ---------------------------------------------------------------------------
# Simulation Engine
# ---------------------------------------------------------------------------

class SimulationEngine:
    """
    Thread-safe simulation engine.

    Only one worker thread may run at a time. All state mutations go through
    a single threading.Lock to eliminate data races between the API thread
    and the background worker.
    """

    def __init__(self, event_interval_seconds: float = 7.0):
        if not (MIN_INTERVAL_SECONDS <= event_interval_seconds <= MAX_INTERVAL_SECONDS):
            raise ValueError(
                f"event_interval_seconds must be between {MIN_INTERVAL_SECONDS} "
                f"and {MAX_INTERVAL_SECONDS}. Got: {event_interval_seconds}"
            )

        self._interval = event_interval_seconds
        self._lock = threading.RLock()   # RLock: allows re-entrant acquisition from the same thread
        self._state = SimEngineState.IDLE
        self._thread: Optional[threading.Thread] = None

        # Pause control — set when paused, cleared to resume
        self._pause_event = threading.Event()
        self._pause_event.set()  # Initially not paused (event is set → unblocked)

        # Stop signal
        self._stop_event = threading.Event()

        # Runtime tracking
        self._simulation_id: Optional[str] = None
        self._mode: Optional[str] = None               # scenario_id or MODE_CONTINUOUS
        self._active_scenario_id: Optional[str] = None
        self._emitted_count: int = 0
        self._last_event_ts: Optional[str] = None
        self._started_at: Optional[str] = None
        self._error_message: Optional[str] = None
        self._max_publish_retries: int = 3
        self._retry_interval: float = 1.0

    # -----------------------------------------------------------------------
    # Public lifecycle controls
    # -----------------------------------------------------------------------

    def start(
        self,
        mode: str = MODE_CONTINUOUS,
        event_interval_seconds: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Starts the simulation worker.

        Args:
            mode: Scenario ID (e.g. 'S1_ALL') or 'CONTINUOUS' for rotation.
            event_interval_seconds: Override interval (5-10s). Uses engine default if None.

        Returns:
            Status dict.

        Raises:
            ValueError: Invalid mode or interval.
            RuntimeError: Engine not IDLE.
        """
        if mode != MODE_CONTINUOUS and mode not in SCENARIO_REGISTRY:
            raise ValueError(
                f"Unknown mode '{mode}'. Valid: 'CONTINUOUS' or one of {sorted(SCENARIO_REGISTRY)}."
            )

        if event_interval_seconds is not None:
            if not (MIN_INTERVAL_SECONDS <= event_interval_seconds <= MAX_INTERVAL_SECONDS):
                raise ValueError(
                    f"event_interval_seconds must be between {MIN_INTERVAL_SECONDS} "
                    f"and {MAX_INTERVAL_SECONDS}."
                )
            self._interval = event_interval_seconds

        with self._lock:
            if self._state != SimEngineState.IDLE:
                raise RuntimeError(
                    f"Cannot start: engine is currently '{self._state.value}'. "
                    "Call stop() first."
                )

            # Initialise run metadata
            self._simulation_id = str(uuid.uuid4())
            self._mode = mode
            self._active_scenario_id = None
            self._emitted_count = 0
            self._last_event_ts = None
            self._started_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
            self._error_message = None

            # Reset control events
            self._stop_event.clear()
            self._pause_event.set()   # Not paused on start

            # Launch worker
            self._thread = threading.Thread(
                target=self._worker,
                name=f"sim-engine-{self._simulation_id[:8]}",
                daemon=True,
            )
            self._state = SimEngineState.RUNNING
            self._thread.start()

        logger.info(
            f"SimulationEngine started: simulation_id={self._simulation_id}, mode={mode}, "
            f"interval={self._interval}s"
        )
        return self.get_status()

    def pause(self) -> Dict[str, Any]:
        """
        Suspends event emission. The worker waits until resume() is called.

        Raises:
            RuntimeError: Engine is not RUNNING.
        """
        with self._lock:
            if self._state != SimEngineState.RUNNING:
                raise RuntimeError(
                    f"Cannot pause: engine is '{self._state.value}' (must be RUNNING)."
                )
            self._state = SimEngineState.PAUSED
            self._pause_event.clear()   # Block worker

        logger.info(f"SimulationEngine paused (simulation_id={self._simulation_id}).")
        return self.get_status()

    def resume(self) -> Dict[str, Any]:
        """
        Resumes event emission after a pause.

        Raises:
            RuntimeError: Engine is not PAUSED.
        """
        with self._lock:
            if self._state != SimEngineState.PAUSED:
                raise RuntimeError(
                    f"Cannot resume: engine is '{self._state.value}' (must be PAUSED)."
                )
            self._state = SimEngineState.RUNNING
            self._pause_event.set()     # Unblock worker

        logger.info(f"SimulationEngine resumed (simulation_id={self._simulation_id}).")
        return self.get_status()

    def stop(self) -> Dict[str, Any]:
        """
        Gracefully shuts down the worker and resets engine to IDLE.

        Returns:
            Final status snapshot (state='IDLE').
        """
        with self._lock:
            if self._state == SimEngineState.IDLE:
                return self.get_status()

            self._state = SimEngineState.STOPPING
            self._stop_event.set()
            self._pause_event.set()     # Unblock a paused worker so it can exit

        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=MAX_INTERVAL_SECONDS + 2)

        with self._lock:
            self._state = SimEngineState.IDLE
            self._thread = None
            self._error_message = None

        logger.info(f"SimulationEngine stopped (simulation_id={self._simulation_id}).")
        return self.get_status()

    def get_status(self) -> Dict[str, Any]:
        """Returns a JSON-serializable snapshot of current engine state."""
        with self._lock:
            return {
                "state": self._state.value,
                "simulation_id": self._simulation_id,
                "mode": self._mode,
                "active_scenario_id": self._active_scenario_id,
                "emitted_event_count": self._emitted_count,
                "last_event_timestamp": self._last_event_ts,
                "started_at": self._started_at,
                "event_interval_seconds": self._interval,
                "error_message": self._error_message,
            }

    # -----------------------------------------------------------------------
    # Worker
    # -----------------------------------------------------------------------

    def _worker(self):
        """
        Background worker thread.

        In CONTINUOUS mode, rotates through SCENARIO_ROTATION_ORDER forever.
        In single-scenario mode, executes one pass of the scenario and stops.
        """
        try:
            if self._mode == MODE_CONTINUOUS:
                self._run_continuous()
            else:
                scenario = SCENARIO_REGISTRY[self._mode]
                completed = self._run_scenario(scenario, loop=False)
                if completed:
                    with self._lock:
                        self._state = SimEngineState.IDLE
                        self._thread = None
        except Exception as exc:
            logger.error(f"SimulationEngine worker crashed: {exc}", exc_info=True)
            with self._lock:
                self._error_message = str(exc)
                self._state = SimEngineState.ERROR
                self._thread = None

    def _run_continuous(self):
        """Rotates through scenarios indefinitely until stopped."""
        rotation = SCENARIO_ROTATION_ORDER
        idx = 0
        while not self._stop_event.is_set():
            scenario = rotation[idx % len(rotation)]
            logger.info(
                f"[Continuous] Starting scenario {scenario.scenario_id} "
                f"({scenario.condition_type})"
            )
            with self._lock:
                self._active_scenario_id = scenario.scenario_id

            completed = self._run_scenario(scenario, loop=False)
            if not completed:
                break   # Stop requested during scenario

            idx += 1
            # Brief inter-scenario pause (still respects stop signal)
            self._stop_event.wait(timeout=2.0)

        # Worker exits cleanly; state may already be STOPPING
        with self._lock:
            if self._state == SimEngineState.STOPPING:
                pass  # stop() will finalize

    def _run_scenario(self, scenario: SimScenario, loop: bool = False) -> bool:
        """
        Executes one pass (or loop) of a scenario's event sequence.

        Returns:
            True if the scenario completed normally.
            False if a stop was requested mid-run or retries were exhausted.
        """
        with self._lock:
            self._active_scenario_id = scenario.scenario_id

        for step_idx, sim_event in enumerate(scenario.event_sequence):
            if self._stop_event.is_set():
                return False

            published = False
            for attempt in range(1, self._max_publish_retries + 1):
                if self._stop_event.is_set():
                    return False

                # Respect pause — block until resumed or stopped
                while not self._pause_event.wait(timeout=1.0):
                    if self._stop_event.is_set():
                        return False

                if self._stop_event.is_set():
                    return False

                # Build and publish the Kafka event
                event_payload = self._build_event_payload(scenario, sim_event, step_idx)
                success = self._publish_to_kafka(event_payload)

                if success:
                    with self._lock:
                        self._emitted_count += 1
                        self._last_event_ts = datetime.datetime.now(
                            datetime.timezone.utc
                        ).isoformat()
                    logger.info(
                        f"[{scenario.scenario_id}] Published step {step_idx + 1}/"
                        f"{len(scenario.event_sequence)}: {sim_event.dataset_name} → "
                        f"expected={sim_event.expected_decision} (attempt {attempt})"
                    )

                    # SSE Step Progress Enhancement (Phase 3 Requirement 7)
                    try:
                        from services.observability.publisher import publish_dashboard_event
                        publish_dashboard_event(
                            event_type="SIMULATION_STATUS_CHANGED",
                            entity="SIMULATOR",
                            severity="INFO",
                            data={
                                "action": "STEP_EMITTED",
                                "simulation_id": self._simulation_id,
                                "scenario_id": scenario.scenario_id,
                                "step_index": step_idx,
                                "total_steps": len(scenario.event_sequence),
                                "dataset_name": sim_event.dataset_name,
                                "expected_decision": sim_event.expected_decision,
                                "emitted_count": self._emitted_count,
                                "timestamp": self._last_event_ts,
                            }
                        )
                    except Exception as sse_err:
                        logger.debug(f"STEP_EMITTED dashboard publication skipped: {sse_err}")

                    published = True
                    break
                else:
                    logger.warning(
                        f"[{scenario.scenario_id}] Kafka publish attempt {attempt}/{self._max_publish_retries} "
                        f"FAILED for step {step_idx + 1} ({sim_event.dataset_name})."
                    )
                    if attempt < self._max_publish_retries:
                        # Wait before retry, listening for stop signal
                        self._stop_event.wait(timeout=self._retry_interval)
                    else:
                        err_msg = (
                            f"Kafka publish exhausted after {self._max_publish_retries} attempts "
                            f"for scenario '{scenario.scenario_id}' step {step_idx + 1} ({sim_event.dataset_name})"
                        )
                        logger.error(err_msg)
                        with self._lock:
                            self._error_message = err_msg
                            self._state = SimEngineState.ERROR
                        return False

            if not published:
                return False

            # Wait for the configured interval before next event
            # Use wait() so stop signals wake us early
            if step_idx < len(scenario.event_sequence) - 1:
                self._stop_event.wait(timeout=self._interval)

        return True

    # -----------------------------------------------------------------------
    # Kafka Publishing
    # -----------------------------------------------------------------------

    def _build_event_payload(
        self,
        scenario: SimScenario,
        sim_event: SimEvent,
        step_idx: int,
    ) -> Dict[str, Any]:
        """
        Constructs a Kafka event payload that conforms to the existing
        dependency-engine process_event() contract.

        Verified contract (from services/dependency-engine/main.py):
          - event_id         → idempotency key (pipeline_executions.triggering_event_id)
          - event_type       → routing discriminator (must NOT match CART_ABANDONMENT,
                               INVENTORY_REORDER, or PAYMENT_SPIKE routes)
          - pipeline_name    → selects pipeline_dependencies row
          - dataset_name     → used for logging
          - expression       → passed to evaluate_condition() for AST
          - simulation_id    → simulation tracking
          - source           → identifies simulator origin
        """
        event_id = str(uuid.uuid4())
        ts = datetime.datetime.now(datetime.timezone.utc).isoformat()

        payload: Dict[str, Any] = {
            # --- Core dependency-engine contract ---
            "event_id": event_id,
            "event_type": "DATASET_UPDATED",          # neutral type; avoids rule routes
            "pipeline_name": scenario.pipeline_name,
            "dataset_name": sim_event.dataset_name,
            "dataset_version": 1,
            "rows_changed": 0,
            "new_status": sim_event.new_status,
            # --- Simulator identity ---
            "source": "SIMULATOR",
            "simulation_id": self._simulation_id,
            "scenario_id": scenario.scenario_id,
            "step_index": step_idx,
            "expected_decision": sim_event.expected_decision,
            "timestamp": ts,
        }

        # Include AST expression for AST-type scenarios
        if scenario.ast_expression:
            payload["expression"] = scenario.ast_expression

        return payload

    def _publish_to_kafka(self, payload: Dict[str, Any]) -> bool:
        """
        Publishes a single event to the Kafka topic used by the dependency engine.

        Returns:
            True on successful publish.
            False on any error (error is logged; no exception propagated).
        """
        try:
            from kafka import KafkaProducer
            producer = KafkaProducer(
                bootstrap_servers=settings.KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=1,
                request_timeout_ms=5000,
            )
            topic = settings.KAFKA_TOPIC_EVENTS
            future = producer.send(topic, value=payload)
            record_metadata = future.get(timeout=10)   # blocks until ack
            producer.flush()
            producer.close()
            logger.debug(
                f"Kafka publish OK: topic={record_metadata.topic}, "
                f"partition={record_metadata.partition}, "
                f"offset={record_metadata.offset}"
            )
            return True
        except Exception as exc:
            logger.error(
                f"Kafka publish FAILED for event_id={payload.get('event_id')}: {exc}"
            )
            return False


# ---------------------------------------------------------------------------
# Module-level singleton (used by FastAPI endpoints)
# ---------------------------------------------------------------------------

_engine: Optional[SimulationEngine] = None
_engine_lock = threading.Lock()


def get_engine() -> SimulationEngine:
    """
    Returns the module-level SimulationEngine singleton, creating it on first call.
    Thread-safe via a module-level lock.
    """
    global _engine
    if _engine is None:
        with _engine_lock:
            if _engine is None:
                _engine = SimulationEngine(event_interval_seconds=7.0)
    return _engine
