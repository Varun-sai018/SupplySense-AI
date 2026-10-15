"""
SupplySense AI - Pipeline Runner

Executes registered downstream pipelines for triggered dependency evaluations,
tracking their lifecycle status (RUNNING -> COMPLETED / FAILED) in MySQL.
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
    PIPELINE_ORCHESTRATOR,
    AIRFLOW_BASE_URL,
    AIRFLOW_API_USER,
    AIRFLOW_API_PASSWORD,
    AIRFLOW_DAG_ID,
    AIRFLOW_REQUEST_TIMEOUT_SECONDS
)
from .registry import get_pipeline_handler, list_registered_pipelines
from .retry_policy import is_retryable_error, classify_error_type, calculate_backoff_delay
from .airflow_client import AirflowClient, AirflowClientError, build_dag_run_id

logger = logging.getLogger(__name__)


def execute_pipeline(
    execution_id: int,
    pipeline_name: str,
    connection=None,
    orchestrator: Optional[str] = None,
    airflow_client: Optional[AirflowClient] = None
) -> Dict[str, Any]:
    """
    Executes a downstream pipeline either locally (in-process ML handler) or by
    dispatching to Apache Airflow REST API, managing lifecycle status in MySQL.

    Lifecycle:
      1. Verify/Set status to RUNNING and populate started_at.
      2. If orchestrator == 'AIRFLOW':
         - Trigger supplysense_demand_forecast DAG via Airflow REST API.
         - Persist orchestrator='AIRFLOW', airflow_dag_id, and airflow_run_id.
         - Leave execution in RUNNING state for Airflow to complete.
         - Do NOT execute internal handler.
      3. If orchestrator == 'LOCAL':
         - Resolve and run internal pipeline handler in-process.
         - On success: mark COMPLETED with output_location.
         - On failure: mark FAILED, classify error, compute next_retry_at if retryable.

    Args:
        execution_id: The ID of the canonical pipeline_executions row.
        pipeline_name: Name of the configured pipeline.
        connection: Optional open PyMySQL database connection.
        orchestrator: Optional orchestrator override ('LOCAL' or 'AIRFLOW').
        airflow_client: Optional injected AirflowClient for testing.

    Returns:
        Dict with execution summary (execution_id, pipeline_name, status, orchestrator, etc.).
    """
    active_orchestrator = (orchestrator or PIPELINE_ORCHESTRATOR or "LOCAL").upper()
    should_close_conn = False
    if connection is None:
        connection = get_connection()
        should_close_conn = True

    cursor = connection.cursor()

    try:
        # Step 1: Ensure execution is marked RUNNING and started_at is populated
        now = datetime.datetime.now()
        cursor.execute("""
            UPDATE pipeline_executions
            SET status = 'RUNNING',
                started_at = COALESCE(started_at, %s),
                orchestrator = %s
            WHERE execution_id = %s
        """, (now, active_orchestrator, execution_id))
        connection.commit()

        # Step 2: Handle AIRFLOW Dispatch Mode
        if active_orchestrator == "AIRFLOW":
            logger.info(
                f"Dispatching pipeline '{pipeline_name}' (Execution ID: {execution_id}) "
                f"to Apache Airflow REST API..."
            )
            client = airflow_client or AirflowClient(
                base_url=AIRFLOW_BASE_URL,
                username=AIRFLOW_API_USER,
                password=AIRFLOW_API_PASSWORD,
                timeout=AIRFLOW_REQUEST_TIMEOUT_SECONDS
            )

            # Retrieve current retry count from database for deterministic run ID
            cursor.execute(
                "SELECT retry_count, max_retries FROM pipeline_executions WHERE execution_id = %s",
                (execution_id,)
            )
            row = cursor.fetchone()
            curr_retries = (row.get("retry_count") if isinstance(row, dict) else row[0]) if row else 0
            max_retries = ((row.get("max_retries") if isinstance(row, dict) else row[1]) if row else None) or 3

            # Establish deterministic Airflow run ID in MySQL before dispatching HTTP request
            expected_run_id = build_dag_run_id(execution_id, curr_retries)
            cursor.execute("""
                UPDATE pipeline_executions
                SET orchestrator = 'AIRFLOW',
                    airflow_dag_id = %s,
                    airflow_run_id = %s,
                    error_message = NULL
                WHERE execution_id = %s
            """, (AIRFLOW_DAG_ID, expected_run_id, execution_id))
            connection.commit()

            try:
                trigger_res = client.trigger_dag_run(
                    dag_id=AIRFLOW_DAG_ID,
                    execution_id=execution_id,
                    pipeline_name=pipeline_name,
                    retry_count=curr_retries
                )
                dag_id = trigger_res["dag_id"]
                dag_run_id = trigger_res["dag_run_id"]

                logger.info(
                    f"Pipeline '{pipeline_name}' execution #{execution_id} successfully "
                    f"dispatched to Airflow (DAG: {dag_id}, Run ID: {dag_run_id})."
                )
                return {
                    "execution_id": execution_id,
                    "pipeline_name": pipeline_name,
                    "status": "RUNNING",
                    "orchestrator": "AIRFLOW",
                    "airflow_dag_id": dag_id,
                    "airflow_run_id": dag_run_id,
                    "is_reused": trigger_res.get("is_reused", False)
                }

            except Exception as exc:
                error_msg = str(exc)
                err_type = classify_error_type(error_msg, exception=exc)
                retryable = is_retryable_error(error_msg, exception=exc)
                logger.error(
                    f"Airflow dispatch FAILED for execution #{execution_id} ({err_type}): {error_msg}"
                )

                next_retry = None
                if retryable and curr_retries < max_retries:
                    delay = calculate_backoff_delay(curr_retries)
                    next_retry = datetime.datetime.now() + datetime.timedelta(seconds=delay)
                    logger.info(f"Scheduled retry for execution #{execution_id} in {delay}s (at {next_retry}).")

                cursor.execute("""
                    UPDATE pipeline_executions
                    SET status = 'FAILED',
                        completed_at = %s,
                        error_message = %s,
                        retry_error_type = %s,
                        next_retry_at = %s
                    WHERE execution_id = %s
                """, (datetime.datetime.now(), error_msg, err_type, next_retry, execution_id))
                connection.commit()

                return {
                    "execution_id": execution_id,
                    "pipeline_name": pipeline_name,
                    "status": "FAILED",
                    "orchestrator": "AIRFLOW",
                    "error": error_msg,
                    "retry_error_type": err_type,
                    "next_retry_at": next_retry
                }

        # Step 3: Handle LOCAL Mode (In-process Handler Execution)
        handler = get_pipeline_handler(pipeline_name)
        if handler is None:
            available = list_registered_pipelines()
            error_msg = f"Unknown pipeline '{pipeline_name}'. Registered pipelines: {available}"
            logger.error(error_msg)
            
            cursor.execute("""
                UPDATE pipeline_executions
                SET status = 'FAILED',
                    completed_at = %s,
                    error_message = %s,
                    retry_error_type = 'DETERMINISTIC_CONFIG',
                    next_retry_at = NULL
                WHERE execution_id = %s
            """, (datetime.datetime.now(), error_msg, execution_id))
            connection.commit()

            return {
                "execution_id": execution_id,
                "pipeline_name": pipeline_name,
                "status": "FAILED",
                "orchestrator": "LOCAL",
                "error": error_msg
            }

        # Step 4: Run internal pipeline handler
        logger.info(f"Executing handler for pipeline '{pipeline_name}' (Execution ID: {execution_id})...")
        try:
            handler_result = handler(execution_id, pipeline_name)
        except Exception as exc:
            error_msg = str(exc)
            err_type = classify_error_type(error_msg, exception=exc)
            retryable = is_retryable_error(error_msg, exception=exc)
            logger.error(f"Pipeline '{pipeline_name}' execution #{execution_id} FAILED ({err_type}): {error_msg}")

            cursor.execute(
                "SELECT retry_count, max_retries FROM pipeline_executions WHERE execution_id = %s",
                (execution_id,)
            )
            row = cursor.fetchone()
            curr_retries = (row.get("retry_count") if isinstance(row, dict) else row[0]) if row else 0
            max_retries = ((row.get("max_retries") if isinstance(row, dict) else row[1]) if row else None) or 3

            next_retry = None
            if retryable and curr_retries < max_retries:
                delay = calculate_backoff_delay(curr_retries)
                next_retry = datetime.datetime.now() + datetime.timedelta(seconds=delay)
                logger.info(f"Scheduled retry for execution #{execution_id} in {delay}s (at {next_retry}).")

            cursor.execute("""
                UPDATE pipeline_executions
                SET status = 'FAILED',
                    completed_at = %s,
                    error_message = %s,
                    retry_error_type = %s,
                    next_retry_at = %s
                WHERE execution_id = %s
            """, (datetime.datetime.now(), error_msg, err_type, next_retry, execution_id))
            connection.commit()

            return {
                "execution_id": execution_id,
                "pipeline_name": pipeline_name,
                "status": "FAILED",
                "orchestrator": "LOCAL",
                "error": error_msg,
                "retry_error_type": err_type,
                "next_retry_at": next_retry
            }

        # Step 5: Record success and artifact location
        output_location = handler_result.get("output_location") if isinstance(handler_result, dict) else None
        cursor.execute("""
            UPDATE pipeline_executions
            SET status = 'COMPLETED',
                completed_at = %s,
                output_location = %s,
                error_message = NULL,
                next_retry_at = NULL
            WHERE execution_id = %s
        """, (datetime.datetime.now(), output_location, execution_id))
        connection.commit()

        logger.info(f"Pipeline '{pipeline_name}' execution #{execution_id} successfully marked COMPLETED.")
        return {
            "execution_id": execution_id,
            "pipeline_name": pipeline_name,
            "status": "COMPLETED",
            "orchestrator": "LOCAL",
            "output_location": output_location,
            "result": handler_result
        }

    except Exception as db_exc:
        logger.error(f"Database error during pipeline execution tracking: {db_exc}")
        if connection:
            connection.rollback()
        return {
            "execution_id": execution_id,
            "pipeline_name": pipeline_name,
            "status": "FAILED",
            "error": f"Database tracking error: {db_exc}"
        }
    finally:
        if should_close_conn and connection:
            connection.close()
