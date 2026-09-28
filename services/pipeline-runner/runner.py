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
from .registry import get_pipeline_handler, list_registered_pipelines

logger = logging.getLogger(__name__)


def execute_pipeline(
    execution_id: int,
    pipeline_name: str,
    connection=None
) -> Dict[str, Any]:
    """
    Executes a downstream pipeline by resolving its handler from the registry,
    managing its status transitions in MySQL, and recording the outcome.

    Lifecycle:
      1. Verify/Set status to RUNNING.
      2. Resolve registered handler for pipeline_name.
         - If unknown: mark FAILED and return.
      3. Execute handler.
         - If successful: mark COMPLETED, record output_location, return success dict.
         - If error raised: mark FAILED, record error_message, return failure dict.

    Args:
        execution_id: The ID of the pipeline_executions row.
        pipeline_name: Name of the configured pipeline.
        connection: Optional open PyMySQL database connection.

    Returns:
        Dict with execution summary (execution_id, pipeline_name, status, output_location/error).
    """
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
            SET status = 'RUNNING', started_at = COALESCE(started_at, %s)
            WHERE execution_id = %s
        """, (now, execution_id))
        connection.commit()

        # Step 2: Resolve pipeline handler
        handler = get_pipeline_handler(pipeline_name)
        if handler is None:
            available = list_registered_pipelines()
            error_msg = f"Unknown pipeline '{pipeline_name}'. Registered pipelines: {available}"
            logger.error(error_msg)
            
            cursor.execute("""
                UPDATE pipeline_executions
                SET status = 'FAILED', completed_at = %s, error_message = %s
                WHERE execution_id = %s
            """, (datetime.datetime.now(), error_msg, execution_id))
            connection.commit()

            return {
                "execution_id": execution_id,
                "pipeline_name": pipeline_name,
                "status": "FAILED",
                "error": error_msg
            }

        # Step 3: Run pipeline handler
        logger.info(f"Executing handler for pipeline '{pipeline_name}' (Execution ID: {execution_id})...")
        try:
            handler_result = handler(execution_id, pipeline_name)
        except Exception as exc:
            error_msg = str(exc)
            logger.error(f"Pipeline '{pipeline_name}' execution #{execution_id} FAILED: {error_msg}")
            
            cursor.execute("""
                UPDATE pipeline_executions
                SET status = 'FAILED', completed_at = %s, error_message = %s
                WHERE execution_id = %s
            """, (datetime.datetime.now(), error_msg, execution_id))
            connection.commit()

            return {
                "execution_id": execution_id,
                "pipeline_name": pipeline_name,
                "status": "FAILED",
                "error": error_msg
            }

        # Step 4: Record success and artifact location
        output_location = handler_result.get("output_location") if isinstance(handler_result, dict) else None
        cursor.execute("""
            UPDATE pipeline_executions
            SET status = 'COMPLETED',
                completed_at = %s,
                output_location = %s,
                error_message = NULL
            WHERE execution_id = %s
        """, (datetime.datetime.now(), output_location, execution_id))
        connection.commit()

        logger.info(f"Pipeline '{pipeline_name}' execution #{execution_id} successfully marked COMPLETED.")
        return {
            "execution_id": execution_id,
            "pipeline_name": pipeline_name,
            "status": "COMPLETED",
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
