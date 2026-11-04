"""
SupplySense AI - Cross-Process Event Journal (Module 3)

Provides persistent, bounded MySQL storage and replay retrieval for
Server-Sent Events (SSE). Enables events published from separate CLI/runner
processes to be synchronized across processes and dispatched to browser subscribers.

Features:
- Idempotent event journaling (INSERT IGNORE on event_id).
- Safe parameterization and JSON payload serialization.
- Bounded retention pruning to prevent unbounded disk growth.
- Database-backed event replay for reconnecting SSE clients.
"""

import os
import sys
import json
import logging
from typing import Dict, Any, List, Optional, Tuple

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from .event_hub import DashboardEvent, SCHEMA_VERSION

logger = logging.getLogger(__name__)

MAX_JOURNAL_RECORDS = 1000


def record_event_to_db(event: DashboardEvent, connection=None, commit: bool = True) -> Optional[int]:
    """
    Persists a DashboardEvent into the dashboard_events MySQL table.
    Idempotent: ignores duplicates on event_id.
    When connection is supplied and commit=False, caller controls transaction commit/rollback.
    """
    should_close = False
    if connection is None:
        try:
            connection = get_connection()
            should_close = True
        except Exception as conn_err:
            logger.warning(f"Unable to connect to database for event journaling: {conn_err}")
            return None

    try:
        cur = connection.cursor()
        payload_json = json.dumps(event.data if isinstance(event.data, dict) else {})
        cur.execute("""
            INSERT IGNORE INTO dashboard_events (
                event_id, event_type, entity, severity, payload,
                decision_id, execution_id, correlation_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            event.event_id,
            event.event_type,
            event.entity,
            event.severity,
            payload_json,
            event.decision_id,
            event.execution_id,
            event.correlation_id
        ))
        row_id = cur.lastrowid
        if commit:
            connection.commit()
        cur.close()
        return row_id
    except Exception as e:
        logger.warning(f"Error persisting dashboard event #{event.event_id} to MySQL: {e}")
        return None
    finally:
        if should_close and connection:
            connection.close()


def get_latest_event_row_id(connection=None) -> int:
    """Returns the highest auto-increment ID currently in dashboard_events."""
    should_close = False
    if connection is None:
        try:
            connection = get_connection()
            should_close = True
        except Exception as conn_err:
            logger.warning(f"Unable to connect to database to read max event ID: {conn_err}")
            return 0

    try:
        cur = connection.cursor()
        cur.execute("SELECT COALESCE(MAX(id), 0) AS max_id FROM dashboard_events")
        row = cur.fetchone()
        cur.close()
        if row:
            return int(row["max_id"] if isinstance(row, dict) else row[0])
        return 0
    except Exception as e:
        logger.warning(f"Error reading latest event row ID: {e}")
        return 0
    finally:
        if should_close and connection:
            connection.close()


def fetch_events_after_id(after_id: int, limit: int = 50, connection=None) -> List[Tuple[int, DashboardEvent]]:
    """
    Queries events created after after_id.
    Returns list of (row_id, DashboardEvent).
    """
    should_close = False
    if connection is None:
        try:
            connection = get_connection()
            should_close = True
        except Exception as conn_err:
            logger.warning(f"Unable to connect to database to fetch events: {conn_err}")
            return []

    try:
        cur = connection.cursor()
        cur.execute("""
            SELECT id, event_id, event_type, entity, severity, payload,
                   decision_id, execution_id, correlation_id, created_at
            FROM dashboard_events
            WHERE id > %s
            ORDER BY id ASC
            LIMIT %s
        """, (after_id, limit))
        rows = cur.fetchall() or []
        cur.close()

        results = []
        for r in rows:
            row_id = int(r["id"] if isinstance(r, dict) else r[0])
            raw_payload = r["payload"] if isinstance(r, dict) else r[5]
            if isinstance(raw_payload, str):
                try:
                    payload = json.loads(raw_payload)
                except Exception:
                    payload = {}
            elif isinstance(raw_payload, dict):
                payload = raw_payload
            else:
                payload = {}

            created_at = r["created_at"] if isinstance(r, dict) else r[9]
            ts_str = created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at)

            event = DashboardEvent(
                event_id=r["event_id"] if isinstance(r, dict) else r[1],
                event_type=r["event_type"] if isinstance(r, dict) else r[2],
                entity=r["entity"] if isinstance(r, dict) else r[3],
                severity=r["severity"] if isinstance(r, dict) else r[4],
                data=payload,
                schema_version=SCHEMA_VERSION,
                timestamp=ts_str,
                decision_id=r["decision_id"] if isinstance(r, dict) else r[6],
                execution_id=r["execution_id"] if isinstance(r, dict) else r[7],
                correlation_id=r["correlation_id"] if isinstance(r, dict) else r[8]
            )
            results.append((row_id, event))

        return results
    except Exception as e:
        logger.warning(f"Error fetching new dashboard events from MySQL: {e}")
        return []
    finally:
        if should_close and connection:
            connection.close()


def fetch_replay_events_by_event_id(last_event_id: str, limit: int = 50, connection=None) -> Tuple[bool, List[DashboardEvent]]:
    """
    Attempts to look up last_event_id in dashboard_events and return subsequent events.
    Returns (found, events).
    """
    should_close = False
    if connection is None:
        try:
            connection = get_connection()
            should_close = True
        except Exception:
            return False, []

    try:
        cur = connection.cursor()
        cur.execute("SELECT id FROM dashboard_events WHERE event_id = %s LIMIT 1", (last_event_id,))
        row = cur.fetchone()
        if not row:
            cur.close()
            return False, []

        last_id = int(row["id"] if isinstance(row, dict) else row[0])
        cur.close()

        pairs = fetch_events_after_id(after_id=last_id, limit=limit, connection=connection)
        return True, [ev for _, ev in pairs]
    except Exception as e:
        logger.warning(f"Error querying replay events for {last_event_id}: {e}")
        return False, []
    finally:
        if should_close and connection:
            connection.close()


def prune_old_events(keep_records: int = MAX_JOURNAL_RECORDS, connection=None) -> int:
    """
    Prunes dashboard_events to ensure the table does not grow unbounded.
    Keeps the most recent keep_records rows.
    """
    should_close = False
    if connection is None:
        try:
            connection = get_connection()
            should_close = True
        except Exception:
            return 0

    try:
        cur = connection.cursor()
        # Find threshold ID
        cur.execute("""
            SELECT id FROM dashboard_events
            ORDER BY id DESC
            LIMIT 1 OFFSET %s
        """, (keep_records,))
        row = cur.fetchone()
        if not row:
            cur.close()
            return 0

        cutoff_id = int(row["id"] if isinstance(row, dict) else row[0])
        cur.execute("DELETE FROM dashboard_events WHERE id < %s", (cutoff_id,))
        deleted_count = cur.rowcount
        connection.commit()
        cur.close()
        if deleted_count > 0:
            logger.info(f"Pruned {deleted_count} stale events from dashboard_events table.")
        return deleted_count
    except Exception as e:
        logger.warning(f"Error pruning dashboard_events: {e}")
        return 0
    finally:
        if should_close and connection:
            connection.close()
