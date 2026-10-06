"""
Database Repository for SupplySense AI Observability & Monitoring.

Executes efficient, read-only SQL queries and aggregations over pipeline executions,
dataset states, dependency decisions, and forecast results.
"""

import os
import sys
import logging
from typing import Dict, Any, List, Optional

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection

logger = logging.getLogger(__name__)


def check_db_health(connection=None) -> bool:
    """Verifies MySQL connectivity."""
    should_close = False
    if connection is None:
        try:
            connection = get_connection()
            should_close = True
        except Exception as e:
            logger.warning(f"Database health check connection failed: {e}")
            return False

    try:
        cur = connection.cursor()
        cur.execute("SELECT 1 AS health_check")
        row = cur.fetchone()
        cur.close()
        return row is not None and row.get("health_check") == 1
    except Exception as e:
        logger.warning(f"Database health check query failed: {e}")
        return False
    finally:
        if should_close and connection:
            connection.close()


def get_execution_counts(connection=None) -> Dict[str, Any]:
    """
    Computes aggregate metrics for pipeline executions:
    total, running, completed, failed, retrying, retried_total, avg_duration_seconds.
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()
        cur.execute("""
            SELECT
                COUNT(*) AS total,
                COALESCE(SUM(CASE WHEN status = 'RUNNING' THEN 1 ELSE 0 END), 0) AS running,
                COALESCE(SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END), 0) AS completed,
                COALESCE(SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END), 0) AS failed,
                COALESCE(SUM(CASE WHEN status = 'FAILED' AND next_retry_at IS NOT NULL AND retry_count < max_retries THEN 1 ELSE 0 END), 0) AS retrying,
                COALESCE(SUM(CASE WHEN retry_count > 0 THEN 1 ELSE 0 END), 0) AS retried_total,
                AVG(CASE WHEN status IN ('COMPLETED', 'FAILED') AND completed_at IS NOT NULL AND started_at IS NOT NULL
                         THEN TIMESTAMPDIFF(SECOND, started_at, completed_at) ELSE NULL END) AS avg_duration_seconds
            FROM pipeline_executions
        """)
        row = cur.fetchone()
        cur.close()
        if not row:
            return {
                "total": 0, "running": 0, "completed": 0, "failed": 0,
                "retrying": 0, "retried_total": 0, "avg_duration_seconds": None
            }
        return {
            "total": int(row["total"] or 0),
            "running": int(row["running"] or 0),
            "completed": int(row["completed"] or 0),
            "failed": int(row["failed"] or 0),
            "retrying": int(row["retrying"] or 0),
            "retried_total": int(row["retried_total"] or 0),
            "avg_duration_seconds": float(row["avg_duration_seconds"]) if row["avg_duration_seconds"] is not None else None
        }
    finally:
        if should_close and connection:
            connection.close()


def get_latest_execution(connection=None) -> Optional[Dict[str, Any]]:
    """Fetches the most recent pipeline execution record."""
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()
        cur.execute("""
            SELECT 
                execution_id, pipeline_name, decision_id, triggering_event_id,
                status, retry_count, max_retries, started_at, completed_at,
                next_retry_at, retry_error_type, output_location, error_message, created_at
            FROM pipeline_executions
            ORDER BY execution_id DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        cur.close()
        return row
    finally:
        if should_close and connection:
            connection.close()


def get_dataset_counts(connection=None) -> Dict[str, int]:
    """Fetches dataset count aggregations from dataset_metadata."""
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()
        cur.execute("""
            SELECT
                COUNT(*) AS total,
                COALESCE(SUM(CASE WHEN status = 'READY' THEN 1 ELSE 0 END), 0) AS ready,
                COALESCE(SUM(CASE WHEN status = 'WAITING' THEN 1 ELSE 0 END), 0) AS waiting
            FROM dataset_metadata
        """)
        row = cur.fetchone()
        cur.close()
        if not row:
            return {"total": 0, "ready": 0, "waiting": 0}
        return {
            "total": int(row["total"] or 0),
            "ready": int(row["ready"] or 0),
            "waiting": int(row["waiting"] or 0)
        }
    finally:
        if should_close and connection:
            connection.close()


def get_latest_forecast_summary(connection=None) -> Optional[Dict[str, Any]]:
    """Fetches summary of the latest forecast execution."""
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()
        cur.execute("""
            SELECT
                f.execution_id,
                f.pipeline_name,
                f.model_name,
                f.model_version,
                COUNT(*) AS row_count,
                COUNT(DISTINCT f.product_category) AS category_count,
                MIN(f.forecast_week) AS min_forecast_week,
                MAX(f.forecast_week) AS max_forecast_week,
                AVG(f.predicted_demand) AS avg_predicted_demand,
                MAX(f.created_at) AS created_at,
                p.output_location
            FROM forecast_results f
            LEFT JOIN pipeline_executions p ON f.execution_id = p.execution_id
            WHERE f.execution_id = (
                SELECT execution_id FROM forecast_results ORDER BY forecast_id DESC LIMIT 1
            )
            GROUP BY f.execution_id, f.pipeline_name, f.model_name, f.model_version, p.output_location
        """)
        row = cur.fetchone()
        cur.close()
        return row
    finally:
        if should_close and connection:
            connection.close()


def get_executions(
    status: Optional[str] = None,
    pipeline_name: Optional[str] = None,
    limit: int = 20,
    connection=None
) -> List[Dict[str, Any]]:
    """
    Fetches recent pipeline executions with optional filtering and bounded limit.
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    query = """
        SELECT 
            execution_id, pipeline_name, decision_id, triggering_event_id,
            status, retry_count, max_retries, started_at, completed_at,
            next_retry_at, retry_error_type, output_location, error_message, created_at
        FROM pipeline_executions
        WHERE 1=1
    """
    params = []

    if status:
        query += " AND status = %s"
        params.append(status)

    if pipeline_name:
        query += " AND pipeline_name = %s"
        params.append(pipeline_name)

    query += " ORDER BY execution_id DESC LIMIT %s"
    params.append(limit)

    try:
        cur = connection.cursor()
        cur.execute(query, tuple(params))
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        if should_close and connection:
            connection.close()


def get_execution_by_id(execution_id: int, connection=None) -> Optional[Dict[str, Any]]:
    """Fetches a single pipeline execution record by execution_id."""
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()
        cur.execute("""
            SELECT 
                execution_id, pipeline_name, decision_id, triggering_event_id,
                status, retry_count, max_retries, started_at, completed_at,
                next_retry_at, retry_error_type, output_location, error_message, created_at
            FROM pipeline_executions
            WHERE execution_id = %s
        """, (execution_id,))
        row = cur.fetchone()
        cur.close()
        return row
    finally:
        if should_close and connection:
            connection.close()


def get_decision_by_id(decision_id: int, connection=None) -> Optional[Dict[str, Any]]:
    """Fetches a pipeline decision by decision_id."""
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()
        cur.execute("""
            SELECT 
                decision_id, pipeline_name, condition_type, decision,
                required_count, ready_count, total_required, reason, triggered_at, created_at
            FROM pipeline_decisions
            WHERE decision_id = %s
        """, (decision_id,))
        row = cur.fetchone()
        cur.close()
        return row
    finally:
        if should_close and connection:
            connection.close()


def get_forecast_summary_by_execution_id(execution_id: int, connection=None) -> Optional[Dict[str, Any]]:
    """Fetches aggregated forecast summary for a specific execution_id."""
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()
        cur.execute("""
            SELECT
                execution_id,
                pipeline_name,
                model_name,
                model_version,
                COUNT(*) AS row_count,
                COUNT(DISTINCT product_category) AS category_count,
                MIN(forecast_week) AS min_forecast_week,
                MAX(forecast_week) AS max_forecast_week,
                AVG(predicted_demand) AS avg_predicted_demand,
                MAX(created_at) AS created_at
            FROM forecast_results
            WHERE execution_id = %s
            GROUP BY execution_id, pipeline_name, model_name, model_version
        """, (execution_id,))
        row = cur.fetchone()
        cur.close()
        return row
    finally:
        if should_close and connection:
            connection.close()


def get_datasets_with_latest_events(connection=None) -> List[Dict[str, Any]]:
    """
    Fetches all datasets from dataset_metadata along with their latest event metadata.
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()
        cur.execute("""
            SELECT 
                m.dataset_id,
                m.dataset_name,
                m.source_type,
                m.table_name,
                m.current_version,
                m.status,
                m.row_count,
                m.last_updated_at,
                e.event_id AS latest_event_id,
                e.event_time AS latest_event_time,
                e.event_type AS latest_event_type,
                e.batch_id AS latest_batch_id,
                e.rows_changed AS latest_rows_changed
            FROM dataset_metadata m
            LEFT JOIN (
                SELECT de.*
                FROM dataset_events de
                INNER JOIN (
                    SELECT dataset_name, MAX(event_id) AS max_event_id
                    FROM dataset_events
                    GROUP BY dataset_name
                ) latest ON de.event_id = latest.max_event_id
            ) e ON m.dataset_name = e.dataset_name
            ORDER BY m.dataset_id ASC
        """)
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        if should_close and connection:
            connection.close()


def get_pipeline_dependencies(connection=None) -> List[Dict[str, Any]]:
    """
    Fetches pipeline dependency definitions and their associated datasets.
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()
        cur.execute("""
            SELECT 
                pd.dependency_id,
                pd.pipeline_name,
                pd.condition_type,
                pd.required_count,
                GROUP_CONCAT(dm.dataset_name ORDER BY dm.dataset_name SEPARATOR ', ') AS datasets
            FROM pipeline_dependencies pd
            LEFT JOIN dependency_datasets dd ON pd.dependency_id = dd.dependency_id
            LEFT JOIN dataset_metadata dm ON dd.dataset_id = dm.dataset_id
            GROUP BY pd.dependency_id, pd.pipeline_name, pd.condition_type, pd.required_count
            ORDER BY pd.dependency_id ASC
        """)
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        if should_close and connection:
            connection.close()


def get_latest_decision_for_pipeline(pipeline_name: str, connection=None) -> Optional[Dict[str, Any]]:
    """Fetches the latest evaluated decision for a given pipeline."""
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()
        cur.execute("""
            SELECT 
                decision_id, pipeline_name, condition_type, decision,
                required_count, ready_count, total_required, reason, triggered_at, created_at
            FROM pipeline_decisions
            WHERE pipeline_name = %s
            ORDER BY decision_id DESC
            LIMIT 1
        """, (pipeline_name,))
        row = cur.fetchone()
        cur.close()
        return row
    finally:
        if should_close and connection:
            connection.close()


def get_forecast_summaries(limit: int = 20, connection=None) -> List[Dict[str, Any]]:
    """
    Fetches grouped summaries of recent forecast runs from forecast_results.
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()
        cur.execute("""
            SELECT
                f.execution_id,
                f.pipeline_name,
                f.model_name,
                f.model_version,
                COUNT(*) AS row_count,
                COUNT(DISTINCT f.product_category) AS category_count,
                MIN(f.forecast_week) AS min_forecast_week,
                MAX(f.forecast_week) AS max_forecast_week,
                AVG(f.predicted_demand) AS avg_predicted_demand,
                MAX(f.created_at) AS created_at,
                p.output_location
            FROM forecast_results f
            LEFT JOIN pipeline_executions p ON f.execution_id = p.execution_id
            GROUP BY f.execution_id, f.pipeline_name, f.model_name, f.model_version, p.output_location
            ORDER BY f.execution_id DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        if should_close and connection:
            connection.close()
