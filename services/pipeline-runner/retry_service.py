"""
SupplySense AI - Pipeline Execution Retry Service

Manages automated retries for transient pipeline execution failures,
enforcing bounded retries, exponential backoff, atomic claiming, and idempotency.
"""

import os
import sys
import logging
import datetime
from typing import Dict, Any, List, Optional

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from config.settings import (
    PIPELINE_MAX_RETRIES,
    PIPELINE_ORCHESTRATOR,
    AIRFLOW_BASE_URL,
    AIRFLOW_API_USER,
    AIRFLOW_API_PASSWORD,
    AIRFLOW_DAG_ID,
    AIRFLOW_REQUEST_TIMEOUT_SECONDS
)
from .registry import get_pipeline_handler, list_registered_pipelines
from .retry_policy import is_retryable_error, classify_error_type, calculate_backoff_delay
from .airflow_client import (
    AirflowClient,
    AirflowClientError,
    AirflowConnectionError,
    AirflowTimeoutError,
    build_dag_run_id,
)

logger = logging.getLogger(__name__)


def retry_execution(
    execution_id: int,
    connection=None,
    airflow_client: Optional[AirflowClient] = None
) -> Dict[str, Any]:
    """
    Attempts to atomically claim and re-execute a single FAILED pipeline execution.

    Transitions:
      FAILED -> RUNNING (retry_count incremented) -> COMPLETED / FAILED

    Atomic Claiming:
      Uses conditional UPDATE ... WHERE execution_id = %s AND status = 'FAILED' AND retry_count < max_retries
      to prevent duplicate execution across concurrent retry workers.

    Orchestration Modes:
      - AIRFLOW: Dispatches new attempt to Airflow REST API with a deterministic attempt ID.
                 Leaves in RUNNING state. Never invokes internal ML handler.
      - LOCAL: Invokes registered internal pipeline handler in-process.

    Args:
        execution_id: ID of the pipeline_executions row to retry.
        connection: Optional open PyMySQL database connection.
        airflow_client: Optional injected AirflowClient for testing.

    Returns:
        Dict describing retry outcome (status, retry_count, output_location/error).
    """
    should_close_conn = False
    if connection is None:
        connection = get_connection()
        should_close_conn = True

    cursor = connection.cursor()

    try:
        now = datetime.datetime.now()

        # Step 1: Query current execution details
        cursor.execute("""
            SELECT execution_id, pipeline_name, status, orchestrator, airflow_dag_id, airflow_run_id,
                   retry_count, max_retries, error_message, triggering_event_id
            FROM pipeline_executions
            WHERE execution_id = %s
        """, (execution_id,))
        exec_row = cursor.fetchone()

        if not exec_row:
            return {
                "execution_id": execution_id,
                "status": "NOT_FOUND",
                "error": f"Execution #{execution_id} does not exist."
            }

        pipeline_name = exec_row["pipeline_name"]
        orchestrator = (exec_row.get("orchestrator") or PIPELINE_ORCHESTRATOR or "LOCAL").upper()
        dag_id = exec_row.get("airflow_dag_id")
        dag_run_id = exec_row.get("airflow_run_id")
        curr_retries = exec_row["retry_count"]
        max_retries = exec_row.get("max_retries") or PIPELINE_MAX_RETRIES
        prev_error = exec_row.get("error_message")

        # Step 2: Validate eligibility
        if exec_row["status"] != "FAILED":
            logger.info(f"Execution #{execution_id} is not in FAILED status (current: {exec_row['status']}). Skipping.")
            return {
                "execution_id": execution_id,
                "status": "SKIPPED",
                "reason": f"Status is '{exec_row['status']}', expected 'FAILED'."
            }

        if curr_retries >= max_retries:
            logger.warning(f"Execution #{execution_id} has exhausted retries ({curr_retries}/{max_retries}).")
            return {
                "execution_id": execution_id,
                "status": "EXHAUSTED",
                "retry_count": curr_retries,
                "max_retries": max_retries,
                "reason": "Retry limit reached."
            }

        # Check if error was retryable
        if prev_error and not is_retryable_error(prev_error):
            logger.warning(f"Execution #{execution_id} failure was non-retryable: {prev_error}")
            cursor.execute("""
                UPDATE pipeline_executions
                SET next_retry_at = NULL, retry_error_type = %s
                WHERE execution_id = %s
            """, (classify_error_type(prev_error), execution_id))
            connection.commit()
            return {
                "execution_id": execution_id,
                "status": "NON_RETRYABLE",
                "reason": "Error classified as non-retryable."
            }

        # For AIRFLOW mode: Verify the previous attempt is not still active in Airflow
        if orchestrator == "AIRFLOW" and dag_id and dag_run_id:
            client = airflow_client or AirflowClient(
                base_url=AIRFLOW_BASE_URL,
                username=AIRFLOW_API_USER,
                password=AIRFLOW_API_PASSWORD,
                timeout=AIRFLOW_REQUEST_TIMEOUT_SECONDS
            )
            try:
                prev_run = client.get_dag_run(dag_id, dag_run_id)
                if prev_run and prev_run.get("state") in ("running", "queued"):
                    logger.warning(
                        f"Execution #{execution_id} cannot be retried: previous Airflow run '{dag_run_id}' "
                        f"is still active (state: '{prev_run.get('state')}')."
                    )
                    return {
                        "execution_id": execution_id,
                        "status": "SKIPPED",
                        "reason": f"Previous Airflow run '{dag_run_id}' is still active ({prev_run.get('state')})."
                    }
            except (AirflowConnectionError, AirflowTimeoutError, AirflowClientError) as exc:
                logger.warning(
                    f"Execution #{execution_id} retry skipped: unable to verify previous Airflow run '{dag_run_id}': {exc}"
                )
                return {
                    "execution_id": execution_id,
                    "status": "SKIPPED",
                    "reason": f"Unable to verify previous Airflow run state: {exc}"
                }

        # Step 3: Atomic Claim (Conditional UPDATE)
        # Immediately establish the new attempt's identity (incremented retry_count and new deterministic run ID)
        # in the database so that delayed callbacks from previous attempts cannot match or overwrite this execution.
        new_retry_count = curr_retries + 1
        new_run_id = (
            build_dag_run_id(execution_id, new_retry_count)
            if orchestrator == "AIRFLOW"
            else None
        )

        if orchestrator == "AIRFLOW":
            cursor.execute("""
                UPDATE pipeline_executions
                SET status = 'RUNNING',
                    retry_count = %s,
                    airflow_dag_id = %s,
                    airflow_run_id = %s,
                    started_at = %s,
                    completed_at = NULL,
                    next_retry_at = NULL,
                    error_message = NULL
                WHERE execution_id = %s
                  AND status = 'FAILED'
                  AND retry_count < max_retries
            """, (new_retry_count, AIRFLOW_DAG_ID, new_run_id, now, execution_id))
        else:
            cursor.execute("""
                UPDATE pipeline_executions
                SET status = 'RUNNING',
                    retry_count = %s,
                    started_at = %s,
                    completed_at = NULL,
                    next_retry_at = NULL,
                    error_message = NULL
                WHERE execution_id = %s
                  AND status = 'FAILED'
                  AND retry_count < max_retries
            """, (new_retry_count, now, execution_id))

        if cursor.rowcount == 0:
            connection.commit()
            logger.info(f"Execution #{execution_id} was claimed or modified by another worker concurrently.")
            return {
                "execution_id": execution_id,
                "status": "SKIPPED",
                "reason": "Concurrent claim conflict or state changed."
            }

        connection.commit()
        logger.info(
            f"Atomic claim succeeded for execution #{execution_id} ('{pipeline_name}'). "
            f"Starting retry attempt {new_retry_count}/{max_retries} (Orchestrator: {orchestrator}, Run ID: {new_run_id})..."
        )

        # Step 4: Dispatch based on orchestrator
        if orchestrator == "AIRFLOW":
            client = airflow_client or AirflowClient(
                base_url=AIRFLOW_BASE_URL,
                username=AIRFLOW_API_USER,
                password=AIRFLOW_API_PASSWORD,
                timeout=AIRFLOW_REQUEST_TIMEOUT_SECONDS
            )

            try:
                trigger_res = client.trigger_dag_run(
                    dag_id=AIRFLOW_DAG_ID,
                    execution_id=execution_id,
                    pipeline_name=pipeline_name,
                    retry_count=new_retry_count
                )
                dag_id = trigger_res["dag_id"]
                dag_run_id = trigger_res["dag_run_id"]

                # Ensure error_message is cleared upon successful trigger confirmation
                cursor.execute("""
                    UPDATE pipeline_executions
                    SET orchestrator = 'AIRFLOW',
                        airflow_dag_id = %s,
                        airflow_run_id = %s,
                        error_message = NULL
                    WHERE execution_id = %s
                """, (dag_id, dag_run_id, execution_id))
                connection.commit()

                logger.info(
                    f"Retry attempt {new_retry_count} for execution #{execution_id} "
                    f"successfully dispatched to Airflow (DAG: {dag_id}, Run ID: {dag_run_id})."
                )
                return {
                    "execution_id": execution_id,
                    "pipeline_name": pipeline_name,
                    "status": "RUNNING",
                    "orchestrator": "AIRFLOW",
                    "retry_count": new_retry_count,
                    "max_retries": max_retries,
                    "airflow_dag_id": dag_id,
                    "airflow_run_id": dag_run_id,
                    "is_reused": trigger_res.get("is_reused", False)
                }

            except Exception as exc:
                err_msg = str(exc)
                err_type = classify_error_type(err_msg, exception=exc)
                retryable = is_retryable_error(err_msg, exception=exc)
                retry_exhausted = (new_retry_count >= max_retries)

                next_retry = None
                if retryable and not retry_exhausted:
                    delay = calculate_backoff_delay(new_retry_count)
                    next_retry = datetime.datetime.now() + datetime.timedelta(seconds=delay)
                    logger.info(
                        f"Airflow retry attempt {new_retry_count} FAILED: {err_msg}. "
                        f"Scheduled next retry in {delay}s at {next_retry}."
                    )
                else:
                    logger.warning(
                        f"Airflow retry attempt {new_retry_count} FAILED permanently (exhausted or non-retryable): {err_msg}"
                    )

                cursor.execute("""
                    UPDATE pipeline_executions
                    SET status = 'FAILED',
                        completed_at = %s,
                        error_message = %s,
                        retry_error_type = %s,
                        next_retry_at = %s
                    WHERE execution_id = %s
                """, (datetime.datetime.now(), err_msg, err_type, next_retry, execution_id))
                connection.commit()

                return {
                    "execution_id": execution_id,
                    "pipeline_name": pipeline_name,
                    "status": "FAILED",
                    "orchestrator": "AIRFLOW",
                    "retry_count": new_retry_count,
                    "max_retries": max_retries,
                    "error": err_msg,
                    "retry_error_type": err_type,
                    "next_retry_at": next_retry
                }

        # LOCAL Orchestration (In-process handler invocation)
        handler = get_pipeline_handler(pipeline_name)
        if handler is None:
            available = list_registered_pipelines()
            err_msg = f"Unknown pipeline '{pipeline_name}'. Registered: {available}"
            logger.error(err_msg)
            cursor.execute("""
                UPDATE pipeline_executions
                SET status = 'FAILED',
                    completed_at = %s,
                    error_message = %s,
                    retry_error_type = 'DETERMINISTIC_CONFIG',
                    next_retry_at = NULL
                WHERE execution_id = %s
            """, (datetime.datetime.now(), err_msg, execution_id))
            connection.commit()
            return {
                "execution_id": execution_id,
                "pipeline_name": pipeline_name,
                "status": "FAILED",
                "orchestrator": "LOCAL",
                "retry_count": new_retry_count,
                "error": err_msg
            }

        try:
            handler_result = handler(execution_id, pipeline_name)
            out_loc = handler_result.get("output_location") if isinstance(handler_result, dict) else None

            # Step 5: Successful Retry -> COMPLETED
            cursor.execute("""
                UPDATE pipeline_executions
                SET status = 'COMPLETED',
                    completed_at = %s,
                    output_location = %s,
                    error_message = NULL,
                    next_retry_at = NULL
                WHERE execution_id = %s
            """, (datetime.datetime.now(), out_loc, execution_id))
            connection.commit()

            logger.info(
                f"Retry attempt {new_retry_count} for execution #{execution_id} SUCCEEDED. "
                f"Status: COMPLETED."
            )
            return {
                "execution_id": execution_id,
                "pipeline_name": pipeline_name,
                "status": "COMPLETED",
                "retry_count": new_retry_count,
                "max_retries": max_retries,
                "output_location": out_loc,
                "result": handler_result
            }

        except Exception as exc:
            err_msg = str(exc)
            err_type = classify_error_type(err_msg, exception=exc)
            retryable = is_retryable_error(err_msg, exception=exc)
            retry_exhausted = (new_retry_count >= max_retries)

            next_retry = None
            if retryable and not retry_exhausted:
                delay = calculate_backoff_delay(new_retry_count)
                next_retry = datetime.datetime.now() + datetime.timedelta(seconds=delay)
                logger.info(
                    f"Retry attempt {new_retry_count} FAILED: {err_msg}. "
                    f"Scheduled next retry in {delay}s at {next_retry}."
                )
            else:
                logger.warning(
                    f"Retry attempt {new_retry_count} FAILED permanently "
                    f"(retryable={retryable}, exhausted={retry_exhausted}): {err_msg}"
                )

            cursor.execute("""
                UPDATE pipeline_executions
                SET status = 'FAILED',
                    completed_at = %s,
                    error_message = %s,
                    retry_error_type = %s,
                    next_retry_at = %s
                WHERE execution_id = %s
            """, (datetime.datetime.now(), err_msg, err_type, next_retry, execution_id))
            connection.commit()

            return {
                "execution_id": execution_id,
                "pipeline_name": pipeline_name,
                "status": "FAILED",
                "retry_count": new_retry_count,
                "max_retries": max_retries,
                "retry_exhausted": retry_exhausted,
                "next_retry_at": next_retry,
                "error": err_msg,
                "retry_error_type": err_type
            }

    except Exception as db_exc:
        logger.error(f"Database error during retry execution #{execution_id}: {db_exc}")
        if connection:
            connection.rollback()
        raise
    finally:
        if should_close_conn and connection:
            connection.close()


def process_retry_candidates(
    max_batch: int = 10,
    connection=None,
    airflow_client: Optional[AirflowClient] = None
) -> List[Dict[str, Any]]:
    """
    Scans for eligible FAILED pipeline executions whose next_retry_at timestamp has arrived,
    and executes retries up to max_batch records.

    Args:
        max_batch: Maximum number of retry candidates to process in this sweep.
        connection: Optional open PyMySQL database connection.
        airflow_client: Optional injected AirflowClient for testing.

    Returns:
        List of result summaries for each processed candidate.
    """
    should_close_conn = False
    if connection is None:
        connection = get_connection()
        should_close_conn = True

    cursor = connection.cursor()
    results = []

    try:
        now = datetime.datetime.now()

        # Find candidates ready for retry
        cursor.execute("""
            SELECT execution_id, pipeline_name, retry_count, max_retries, error_message, next_retry_at
            FROM pipeline_executions
            WHERE status = 'FAILED'
              AND retry_count < max_retries
              AND (next_retry_at IS NULL OR next_retry_at <= %s)
            ORDER BY execution_id ASC
            LIMIT %s
        """, (now, max_batch))
        candidates = cursor.fetchall()

        logger.info(f"Found {len(candidates)} candidate execution(s) eligible for retry.")

        for cand in candidates:
            exec_id = cand["execution_id"]
            err_msg = cand.get("error_message")

            # Check if retryable before attempting claim
            if err_msg and not is_retryable_error(err_msg):
                logger.info(f"Candidate execution #{exec_id} is non-retryable; skipping.")
                cursor.execute("""
                    UPDATE pipeline_executions
                    SET next_retry_at = NULL, retry_error_type = %s
                    WHERE execution_id = %s
                """, (classify_error_type(err_msg), exec_id))
                connection.commit()
                continue

            res = retry_execution(exec_id, connection=connection, airflow_client=airflow_client)
            results.append(res)

        return results

    except Exception as exc:
        logger.error(f"Error during retry sweep: {exc}")
        if connection:
            connection.rollback()
        raise
    finally:
        if should_close_conn and connection:
            connection.close()
