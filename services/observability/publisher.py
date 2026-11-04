"""
SupplySense AI - Dashboard Event Publisher Helper (Module 3)

Exposes a safe, uniform publishing interface for background services,
dependency rules, and pipeline runners to emit versioned live events.

Ensures cross-process delivery by:
1. Broadcasting immediately to in-process EventHub subscribers.
2. Persisting into MySQL dashboard_events table for cross-process synchronization.
"""

import logging
from typing import Dict, Any, Optional
from .event_hub import hub, DashboardEvent
from .event_journal import record_event_to_db

logger = logging.getLogger(__name__)


def publish_dashboard_event(
    event_type: str,
    entity: str,
    severity: str = "INFO",
    data: Optional[Dict[str, Any]] = None,
    decision_id: Optional[int] = None,
    execution_id: Optional[int] = None,
    correlation_id: Optional[str] = None,
    event_id: Optional[str] = None,
    connection=None,
    commit: bool = True,
    broadcast: bool = True,
) -> Optional[DashboardEvent]:
    """
    Safely publishes an event to the local EventHub and the MySQL cross-process event journal.
    When `connection` is provided with `commit=False`, the event is persisted within the caller's
    active MySQL transaction without committing immediately.
    When `broadcast=False`, in-process EventHub delivery is deferred until caller's transaction commits.
    Never raises an exception to avoid disrupting primary transaction execution.
    """
    try:
        # 1. Construct and validate event
        resolved_event_id = event_id or hub.generate_event_id()
        event = DashboardEvent(
            event_id=resolved_event_id,
            event_type=event_type,
            entity=entity,
            severity=severity,
            data=data or {},
            decision_id=decision_id,
            execution_id=execution_id,
            correlation_id=correlation_id
        )

        # 2. Local in-process broadcast (if enabled)
        if broadcast:
            try:
                hub.publish(event)
            except Exception as hub_err:
                logger.debug(f"In-process event hub broadcast skipped: {hub_err}")

        # 3. Cross-process persistence to MySQL
        try:
            record_event_to_db(event, connection=connection, commit=commit)
        except Exception as db_err:
            logger.debug(f"Event journal db persistence skipped: {db_err}")

        return event
    except Exception as exc:
        logger.warning(f"Non-critical error publishing live dashboard event ({event_type}): {exc}")
        return None


def broadcast_dashboard_event(event: DashboardEvent) -> None:
    """Safely broadcasts an already-persisted event to local in-process EventHub subscribers."""
    try:
        hub.publish(event)
    except Exception as exc:
        logger.debug(f"Error broadcasting dashboard event ({event.event_type}): {exc}")
