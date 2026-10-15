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
from config.settings import (
    PIPELINE_EXECUTION_STALE_TIMEOUT_SECONDS,
    AIRFLOW_BASE_URL,
    AIRFLOW_API_USER,
    AIRFLOW_API_PASSWORD,
    AIRFLOW_REQUEST_TIMEOUT_SECONDS,
)
from .retry_policy import calculate_backoff_delay
from .airflow_client import (
    AirflowClient,
    AirflowClientError,
    AirflowConnectionError,
    AirflowTimeoutError,
)

logger = logging.getLogger(__name__)


def reap_stale_executions(
    timeout_seconds: Optional[int] = None,
    connection=None,
    airflow_client: Optional[AirflowClient] = None
) -> Dict[str, int]:
    """
    Identifies pipeline executions stuck in 'RUNNING' status longer than the
    configured timeout threshold and safely reconciles them:
      - LOCAL orchestrations: Transitions to 'FAILED' with retry backoff.
      - AIRFLOW orchestrations: Queries Airflow REST API. If the DAG run is
        still 'running' or 'queued', it is NOT reaped. If 'success', transitions
        to 'COMPLETED'. If 'failed' or missing (404), transitions to 'FAILED'.
      - Airflow API connectivity/timeout errors: Kept in 'RUNNING' to prevent
        premature failure or duplicate execution attempts.

    Uses a conditional atomic SQL UPDATE to prevent race conditions with
    pipelines completing concurrently.

    Args:
        timeout_seconds: Stale execution threshold in seconds. Defaults to
                         PIPELINE_EXECUTION_STALE_TIMEOUT_SECONDS (1800s).
        connection: Optional open PyMySQL database connection.
        airflow_client: Optional injected AirflowClient instance for testing.

    Returns:
        Dict containing reconciliation statistics:
        {
            "scanned": int,       # Total RUNNING executions checked
            "stale_found": int,   # Executions older than timeout threshold
            "reconciled": int,    # Executions successfully transitioned to FAILED/COMPLETED
            "skipped": int        # Executions skipped (active Airflow run, concurrent completion, or connectivity error)
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
            SELECT execution_id, pipeline_name, status, orchestrator, airflow_dag_id, airflow_run_id,
                   retry_count, max_retries, started_at, created_at
            FROM pipeline_executions
            WHERE status = 'RUNNING'
            ORDER BY execution_id ASC
        """)
        running_executions = cursor.fetchall()
        stats["scanned"] = len(running_executions)

        for exec_row in running_executions:
            exec_id = exec_row["execution_id"]
            p_name = exec_row["pipeline_name"]
            orchestrator = (exec_row.get("orchestrator") or "LOCAL").upper()
            dag_id = exec_row.get("airflow_dag_id")
            dag_run_id = exec_row.get("airflow_run_id")
            curr_retries = exec_row.get("retry_count") or 0
            max_ret = exec_row.get("max_retries") or 3
            started_time = exec_row.get("started_at") or exec_row.get("created_at")

            # Check if this execution exceeds the stale timeout threshold
            if started_time and started_time <= cutoff_time:
                stats["stale_found"] += 1
                logger.warning(
                    f"Stale execution detected: ID #{exec_id} ('{p_name}', orchestrator={orchestrator}), "
                    f"started at {started_time} (elapsed > {timeout_seconds}s). Evaluating reconciliation..."
                )

                # AIRFLOW Mode Reconciliation
                if orchestrator == "AIRFLOW" and dag_id and dag_run_id:
                    client = airflow_client
                    if client is None:
                        client = AirflowClient(
                            base_url=AIRFLOW_BASE_URL,
                            username=AIRFLOW_API_USER,
                            password=AIRFLOW_API_PASSWORD,
                            timeout=AIRFLOW_REQUEST_TIMEOUT_SECONDS
                        )

                    try:
                        run_info = client.get_dag_run(dag_id, dag_run_id)
                    except (AirflowConnectionError, AirflowTimeoutError, AirflowClientError) as exc:
                        logger.warning(
                            f"Execution #{exec_id}: Indeterminate Airflow status for DAG '{dag_id}' run '{dag_run_id}': {exc}. "
                            f"Preserving RUNNING status to avoid premature failure or duplicate dispatch."
                        )
                        stats["skipped"] += 1
                        continue

                    # If Airflow reports active (running or queued), do NOT fail
                    if run_info and run_info.get("state") in ("running", "queued"):
                        logger.info(
                            f"Execution #{exec_id}: Airflow run '{dag_run_id}' is active in state '{run_info.get('state')}'. "
                            f"Skipping reaping (DAG is still executing)."
                        )
                        stats["skipped"] += 1
                        continue

                    # If Airflow reports success, synchronize state to COMPLETED
                    if run_info and run_info.get("state") == "success":
                        cursor.execute("""
                            UPDATE pipeline_executions
                            SET status = 'COMPLETED',
                                completed_at = %s,
                                error_message = NULL
                            WHERE execution_id = %s
                              AND status = 'RUNNING'
                        """, (now, exec_id))
                        if cursor.rowcount > 0:
                            stats["reconciled"] += 1
                            logger.info(f"Execution #{exec_id} synchronized to COMPLETED from Airflow success state.")
                        else:
                            stats["skipped"] += 1
                        continue

                    # If Airflow reports failed, or 404 (missing run), reap as FAILED
                    if run_info and run_info.get("state") == "failed":
                        error_msg = (
                            f"Airflow DAG run '{dag_run_id}' failed in Airflow (elapsed > {timeout_seconds}s)."
                        )
                    elif run_info is None:
                        error_msg = (
                            f"Airflow DAG run '{dag_run_id}' not found in Airflow (HTTP 404) after {timeout_seconds}s."
                        )
                    else:
                        error_msg = (
                            f"Airflow DAG run '{dag_run_id}' is in non-active terminal state '{run_info.get('state')}' "
                            f"after {timeout_seconds}s."
                        )

                    next_retry = None
                    if curr_retries < max_ret:
                        delay = calculate_backoff_delay(curr_retries)
                        next_retry = now + datetime.timedelta(seconds=delay)

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
                        logger.info(f"Execution #{exec_id} successfully reaped and marked FAILED based on Airflow state.")
                    else:
                        stats["skipped"] += 1
                    continue

                # LOCAL Mode or AIRFLOW without DAG metadata: standard local reaping
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
