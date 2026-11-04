"""
SupplySense AI - MySQL Event Consumer (Phase 2)

Provides reliable, local, transactional event consumption from dataset_events:
  - Row-level claiming via `SELECT ... FOR UPDATE SKIP LOCKED` (multi-worker safe, zero collisions).
  - Configurable polling intervals and batch sizes.
  - Safe worker crash recovery for events abandoned in 'PROCESSING' state.
  - FIFO / version-ordered processing (ORDER BY event_id ASC).
  - Explicit lifecycle states: NEW -> PROCESSING -> COMPLETED / FAILED.
  - Clean shutdown handling.
"""

import os
import sys
import time
import logging
import datetime
import threading
from typing import Dict, Any, List, Optional, Tuple, Callable

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection

logger = logging.getLogger(__name__)


def claim_events(
    batch_size: int = 10,
    stale_timeout_seconds: int = 300,
    connection=None
) -> List[Dict[str, Any]]:
    """
    Atomically claims up to `batch_size` pending or stale events from `dataset_events`.

    Multi-Worker Safety:
      Uses `FOR UPDATE SKIP LOCKED` so concurrent workers can poll simultaneously
      without blocking each other or claiming duplicate events.

    Recovery:
      Also claims events left in 'PROCESSING' state for longer than `stale_timeout_seconds`
      (indicating a worker crashed or was killed mid-processing).

    Returns:
        List of event dictionaries ready for dependency evaluation.
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()
        now = datetime.datetime.now()
        stale_threshold = now - datetime.timedelta(seconds=stale_timeout_seconds)

        # 1. Lock available rows
        cur.execute(
            """
            SELECT
                event_id,
                dataset_id,
                dataset_name,
                event_type,
                event_time,
                dataset_version,
                rows_changed,
                source_file,
                source_pos,
                source_change_id,
                batch_id,
                event_status,
                processed_at,
                created_at
            FROM dataset_events
            WHERE event_status = 'NEW'
               OR (event_status = 'PROCESSING' AND processed_at < %s)
            ORDER BY event_id ASC
            LIMIT %s
            FOR UPDATE SKIP LOCKED
            """,
            (stale_threshold, batch_size)
        )
        rows = cur.fetchall()

        if not rows:
            connection.rollback()
            cur.close()
            return []

        # 2. Mark claimed rows as PROCESSING
        event_ids = [r["event_id"] if isinstance(r, dict) else r[0] for r in rows]
        format_strings = ','.join(['%s'] * len(event_ids))
        cur.execute(
            f"""
            UPDATE dataset_events
            SET event_status = 'PROCESSING',
                processed_at = %s
            WHERE event_id IN ({format_strings})
            """,
            (now, *event_ids)
        )
        connection.commit()
        cur.close()

        logger.info(f"CLAIMED: Successfully claimed {len(rows)} event(s) for processing: {event_ids}")

        # Format events
        claimed_events = []
        for r in rows:
            claimed_events.append({
                "event_id": r["event_id"],
                "dataset_id": r["dataset_id"],
                "dataset_name": r["dataset_name"],
                "event_type": r["event_type"],
                "event_time": str(r["event_time"]),
                "dataset_version": r["dataset_version"],
                "rows_changed": r["rows_changed"],
                "source_file": r.get("source_file"),
                "source_pos": r.get("source_pos"),
                "source_change_id": r.get("source_change_id"),
                "batch_id": r.get("batch_id"),
                "event_status": "PROCESSING",
                "source": "MYSQL_CDC" if (r.get("source_change_id") and "binlog" in str(r["source_change_id"])) else "MYSQL_EVENT"
            })

        return claimed_events

    except Exception as exc:
        if connection:
            connection.rollback()
        logger.error(f"Error claiming events from dataset_events: {exc}")
        raise
    finally:
        if should_close and connection:
            connection.close()


def mark_event_completed(event_id: int, connection=None):
    """Marks an event as successfully COMPLETED in dataset_events."""
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()
        cur.execute(
            """
            UPDATE dataset_events
            SET event_status = 'COMPLETED',
                processed_at = NOW()
            WHERE event_id = %s
            """,
            (event_id,)
        )
        connection.commit()
        cur.close()
        logger.info(f"COMPLETED: Event #{event_id} marked COMPLETED in dataset_events.")
    except Exception as exc:
        logger.error(f"Failed to mark event #{event_id} as COMPLETED: {exc}")
        if connection:
            connection.rollback()
    finally:
        if should_close and connection:
            connection.close()


def mark_event_failed(event_id: int, error_message: Optional[str] = None, connection=None):
    """Marks an event as FAILED in dataset_events."""
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()
        cur.execute(
            """
            UPDATE dataset_events
            SET event_status = 'FAILED',
                processed_at = NOW()
            WHERE event_id = %s
            """,
            (event_id,)
        )
        connection.commit()
        cur.close()
        logger.error(f"FAILED: Event #{event_id} marked FAILED in dataset_events. Error: {error_message}")
    except Exception as exc:
        logger.error(f"Failed to mark event #{event_id} as FAILED: {exc}")
        if connection:
            connection.rollback()
    finally:
        if should_close and connection:
            connection.close()


def poll_and_process_batch(
    batch_size: int = 10,
    stale_timeout_seconds: int = 300,
    connection=None,
    process_func: Optional[Callable] = None
) -> Dict[str, Any]:
    """
    Executes a single polling and evaluation pass:
      1. Claims up to `batch_size` pending/stale events.
      2. Dispatches each event to `process_func`.
      3. Updates status to COMPLETED or FAILED.

    Returns:
        Summary dict: {"claimed": int, "processed": int, "failed": int, "results": list}
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        events = claim_events(
            batch_size=batch_size,
            stale_timeout_seconds=stale_timeout_seconds,
            connection=connection
        )

        if not events:
            return {"claimed": 0, "processed": 0, "failed": 0, "results": []}

        if process_func is None:
            from services.dependency_engine.main import process_event
            process_func = process_event

        results = []
        processed_count = 0
        failed_count = 0

        for evt in events:
            evt_id = evt["event_id"]
            logger.info(f"EVALUATING: Processing claimed event #{evt_id} ({evt.get('dataset_name')})...")
            try:
                # Ensure connection is active
                try:
                    connection.ping(reconnect=True)
                except Exception:
                    pass

                decision, reason = process_func(evt, connection)
                mark_event_completed(evt_id, connection=connection)
                processed_count += 1
                results.append({
                    "event_id": evt_id,
                    "dataset_name": evt.get("dataset_name"),
                    "decision": decision,
                    "reason": reason,
                    "status": "COMPLETED"
                })
            except Exception as proc_err:
                failed_count += 1
                logger.error(f"Error evaluating event #{evt_id}: {proc_err}", exc_info=True)
                mark_event_failed(evt_id, error_message=str(proc_err), connection=connection)
                results.append({
                    "event_id": evt_id,
                    "dataset_name": evt.get("dataset_name"),
                    "decision": "ERROR",
                    "reason": str(proc_err),
                    "status": "FAILED"
                })

        return {
            "claimed": len(events),
            "processed": processed_count,
            "failed": failed_count,
            "results": results
        }

    finally:
        if should_close and connection:
            connection.close()


def run_mysql_consumer(
    poll_interval: float = 1.0,
    batch_size: int = 10,
    stale_timeout_seconds: int = 300,
    stop_event: Optional[threading.Event] = None,
    process_func: Optional[Callable] = None
):
    """
    Long-running MySQL event poller with graceful shutdown and connection health check.
    Runs until `stop_event` is set or KeyboardInterrupt is received.
    """
    logger.info("==================================================================")
    logger.info("SupplySense AI - MySQL Event Consumer Initialized")
    logger.info(f"Polling Interval: {poll_interval}s | Batch Size: {batch_size}")
    logger.info(f"Stale Event Timeout: {stale_timeout_seconds}s")
    logger.info("==================================================================")

    connection = None
    try:
        connection = get_connection()
        while stop_event is None or not stop_event.is_set():
            try:
                # Connection ping / auto-reconnect check
                try:
                    connection.ping(reconnect=True)
                except Exception as ping_err:
                    logger.warning(f"MySQL connection lost ({ping_err}). Reconnecting...")
                    try:
                        connection.close()
                    except Exception:
                        pass
                    connection = get_connection()
                    logger.info("MySQL connection restored successfully.")

                summary = poll_and_process_batch(
                    batch_size=batch_size,
                    stale_timeout_seconds=stale_timeout_seconds,
                    connection=connection,
                    process_func=process_func
                )

                if summary["claimed"] == 0:
                    # No pending events: avoid busy loop
                    if stop_event:
                        stop_event.wait(timeout=poll_interval)
                    else:
                        time.sleep(poll_interval)

            except Exception as loop_err:
                logger.error(f"Error in MySQL consumer polling cycle: {loop_err}")
                time.sleep(poll_interval)

    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutdown signal received. Exiting MySQL consumer loop...")
    finally:
        if connection:
            try:
                connection.close()
            except Exception:
                pass
        logger.info("MySQL Event Consumer stopped cleanly.")
