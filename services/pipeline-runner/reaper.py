"""
SupplySense AI - Pipeline Execution Reaper / Reconciliation Service

Detects and reconciles orphaned pipeline executions stuck in RUNNING status
due to process terminations, container restarts, or unhandled crashes.
"""

import os
import sys
import logging
import datetime
from typing import Dict, Any, Optional

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from config.settings import PIPELINE_EXECUTION_STALE_TIMEOUT_SECONDS
from .retry_policy import calculate_backoff_delay

logger = logging.getLogger(__name__)


def reap_stale_executions(
    timeout_seconds: Optional[int] = None,
    connection=None
) -> Dict[str, int]:
    """
    Identifies pipeline executions stuck in 'RUNNING' status longer than the
    configured timeout threshold and safely transitions them to 'FAILED',
    scheduling them for retry if within retry limits.

    Uses a conditional atomic SQL UPDATE (WHERE execution_id = %s AND status = 'RUNNING')
    to prevent race conditions with pipelines completing concurrently.

    Args:
        timeout_seconds: Stale execution threshold in seconds. Defaults to
                         PIPELINE_EXECUTION_STALE_TIMEOUT_SECONDS (1800s).
        connection: Optional open PyMySQL database connection.

    Returns:
        Dict containing reconciliation statistics:
        {
            "scanned": int,       # Total RUNNING executions checked
            "stale_found": int,   # Executions older than timeout threshold
            "reconciled": int,    # Executions successfully transitioned to FAILED
            "skipped": int        # Executions that completed concurrently
        }
    """
    if timeout_seconds is None:
        timeout_seconds = PIPELINE_EXECUTION_STALE_TIMEOUT_SECONDS

    should_close_conn = False
    if connection is None:
        connection = get_connection()
        should_close_conn = True

    stats = {
        "scanned": 0,
        "stale_found": 0,
        "reconciled": 0,
        "skipped": 0,
    }

    cursor = connection.cursor()

    try:
        now = datetime.datetime.now()
        cutoff_time = now - datetime.timedelta(seconds=timeout_seconds)

        # Find all executions currently RUNNING
        cursor.execute("""
            SELECT execution_id, pipeline_name, status, retry_count, max_retries, started_at, created_at
            FROM pipeline_executions
            WHERE status = 'RUNNING'
            ORDER BY execution_id ASC
        """)
        running_executions = cursor.fetchall()
        stats["scanned"] = len(running_executions)

        for exec_row in running_executions:
            exec_id = exec_row["execution_id"]
            p_name = exec_row["pipeline_name"]
            curr_retries = exec_row.get("retry_count") or 0
            max_ret = exec_row.get("max_retries") or 3
            started_time = exec_row.get("started_at") or exec_row.get("created_at")

            # Check if this execution exceeds the stale timeout threshold
            if started_time and started_time <= cutoff_time:
                stats["stale_found"] += 1
                logger.warning(
                    f"Stale execution detected: ID #{exec_id} ('{p_name}'), "
                    f"started at {started_time} (elapsed > {timeout_seconds}s). Reaping..."
                )

                error_msg = (
                    f"Execution timed out and marked FAILED by reaper after exceeding "
                    f"stale timeout of {timeout_seconds}s (started at {started_time})."
                )

                next_retry = None
                if curr_retries < max_ret:
                    delay = calculate_backoff_delay(curr_retries)
                    next_retry = now + datetime.timedelta(seconds=delay)

                # Conditional atomic update: only update if still RUNNING
                cursor.execute("""
                    UPDATE pipeline_executions
                    SET status = 'FAILED',
                        completed_at = %s,
                        error_message = %s,
                        retry_error_type = 'TRANSIENT_REAPER',
                        next_retry_at = %s
                    WHERE execution_id = %s
                      AND status = 'RUNNING'
                """, (now, error_msg, next_retry, exec_id))

                if cursor.rowcount > 0:
                    stats["reconciled"] += 1
                    logger.info(f"Execution #{exec_id} successfully reaped and marked FAILED.")
                else:
                    stats["skipped"] += 1
                    logger.info(
                        f"Execution #{exec_id} was already completed concurrently; skipped reaping."
                    )

        connection.commit()
        logger.info(
            f"Reconciliation sweep completed: {stats['scanned']} scanned, "
            f"{stats['stale_found']} stale found, {stats['reconciled']} reconciled, "
            f"{stats['skipped']} skipped."
        )
        return stats

    except Exception as exc:
        logger.error(f"Error during execution reaper reconciliation: {exc}")
        if connection:
            connection.rollback()
        raise
    finally:
        if should_close_conn and connection:
            connection.close()
