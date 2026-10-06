"""
Business Logic & Aggregation Service for SupplySense AI Observability.

Provides business-level aggregation, duration calculations, rate computation,
and structured formatting for observability APIs.
"""

import os
import sys
import datetime
from typing import Dict, Any, List, Optional

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from . import repository


def _format_datetime(dt: Any) -> Optional[str]:
    """Safely formats a datetime object or string into an ISO string."""
    if dt is None:
        return None
    if isinstance(dt, (datetime.datetime, datetime.date)):
        return dt.isoformat()
    return str(dt)


def _calculate_duration(started_at: Any, completed_at: Any, status: str) -> Optional[float]:
    """
    Computes duration in seconds:
      - If completed or failed with completed_at: (completed_at - started_at).total_seconds()
      - If running with started_at: (now - started_at).total_seconds()
      - Otherwise None.
    """
    if not started_at or not isinstance(started_at, datetime.datetime):
        return None

    if status in ("COMPLETED", "FAILED") and completed_at and isinstance(completed_at, datetime.datetime):
        return round((completed_at - started_at).total_seconds(), 2)

    if status == "RUNNING":
        return round((datetime.datetime.now() - started_at).total_seconds(), 2)

    return None


def get_health_status(connection=None) -> Dict[str, str]:
    """Returns the operational health and database connectivity status."""
    is_db_connected = repository.check_db_health(connection=connection)
    return {
        "status": "healthy" if is_db_connected else "unhealthy",
        "database": "connected" if is_db_connected else "disconnected"
    }


def get_observability_summary(connection=None) -> Dict[str, Any]:
    """
    Aggregates top-level operational metrics across executions, datasets, and forecasts.
    """
    exec_counts = repository.get_execution_counts(connection=connection)
    total_execs = exec_counts["total"]
    completed_execs = exec_counts["completed"]
    failed_execs = exec_counts["failed"]

    success_rate = round((completed_execs / total_execs) * 100.0, 2) if total_execs > 0 else 0.0
    failure_rate = round((failed_execs / total_execs) * 100.0, 2) if total_execs > 0 else 0.0

    # Latest execution
    latest_exec_row = repository.get_latest_execution(connection=connection)
    latest_exec = None
    if latest_exec_row:
        latest_exec = {
            "execution_id": latest_exec_row["execution_id"],
            "pipeline_name": latest_exec_row["pipeline_name"],
            "status": latest_exec_row["status"],
            "started_at": _format_datetime(latest_exec_row["started_at"]),
            "completed_at": _format_datetime(latest_exec_row["completed_at"]),
            "duration_seconds": _calculate_duration(
                latest_exec_row["started_at"],
                latest_exec_row["completed_at"],
                latest_exec_row["status"]
            ),
            "output_location": latest_exec_row["output_location"]
        }

    # Dataset counts
    dataset_counts = repository.get_dataset_counts(connection=connection)

    # Latest forecast
    latest_fc_row = repository.get_latest_forecast_summary(connection=connection)
    latest_forecast = None
    if latest_fc_row:
        latest_forecast = {
            "execution_id": latest_fc_row["execution_id"],
            "pipeline_name": latest_fc_row["pipeline_name"],
            "model_name": latest_fc_row["model_name"],
            "model_version": latest_fc_row["model_version"],
            "row_count": int(latest_fc_row["row_count"]),
            "category_count": int(latest_fc_row["category_count"]),
            "min_forecast_week": _format_datetime(latest_fc_row["min_forecast_week"]),
            "max_forecast_week": _format_datetime(latest_fc_row["max_forecast_week"]),
            "avg_predicted_demand": round(float(latest_fc_row["avg_predicted_demand"]), 4) if latest_fc_row["avg_predicted_demand"] is not None else None,
            "created_at": _format_datetime(latest_fc_row["created_at"]),
            "output_location": latest_fc_row.get("output_location")
        }

    return {
        "executions": {
            "total": total_execs,
            "running": exec_counts["running"],
            "completed": completed_execs,
            "failed": failed_execs,
            "retrying": exec_counts["retrying"],
            "retried_total": exec_counts["retried_total"]
        },
        "success_rate": success_rate,
        "failure_rate": failure_rate,
        "average_duration_seconds": round(exec_counts["avg_duration_seconds"], 2) if exec_counts["avg_duration_seconds"] is not None else None,
        "latest_execution": latest_exec,
        "datasets": {
            "total": dataset_counts["total"],
            "ready": dataset_counts["ready"],
            "waiting": dataset_counts["waiting"]
        },
        "latest_forecast": latest_forecast
    }


def get_recent_executions(
    status: Optional[str] = None,
    pipeline_name: Optional[str] = None,
    limit: int = 20,
    connection=None
) -> List[Dict[str, Any]]:
    """
    Returns filtered and sorted recent pipeline executions with duration calculations.
    """
    # Bound limit safely between 1 and 100
    safe_limit = max(1, min(limit, 100))
    rows = repository.get_executions(
        status=status,
        pipeline_name=pipeline_name,
        limit=safe_limit,
        connection=connection
    )

    results = []
    for r in rows:
        duration = _calculate_duration(r["started_at"], r["completed_at"], r["status"])
        results.append({
            "execution_id": r["execution_id"],
            "pipeline_name": r["pipeline_name"],
            "decision_id": r["decision_id"],
            "triggering_event_id": r["triggering_event_id"],
            "status": r["status"],
            "retry_count": r["retry_count"],
            "max_retries": r["max_retries"],
            "started_at": _format_datetime(r["started_at"]),
            "completed_at": _format_datetime(r["completed_at"]),
            "duration_seconds": duration,
            "next_retry_at": _format_datetime(r["next_retry_at"]),
            "retry_error_type": r["retry_error_type"],
            "output_location": r["output_location"],
            "error_message": r["error_message"]
        })
    return results


def get_execution_detail(execution_id: int, connection=None) -> Optional[Dict[str, Any]]:
    """
    Returns full operational details for a specific pipeline execution including:
    metadata, decision context, forecast statistics, and artifact check.
    """
    exec_row = repository.get_execution_by_id(execution_id, connection=connection)
    if not exec_row:
        return None

    # Decision information
    decision_info = None
    if exec_row.get("decision_id"):
        dec_row = repository.get_decision_by_id(exec_row["decision_id"], connection=connection)
        if dec_row:
            decision_info = {
                "decision_id": dec_row["decision_id"],
                "condition_type": dec_row["condition_type"],
                "decision": dec_row["decision"],
                "ready_count": dec_row["ready_count"],
                "total_required": dec_row["total_required"],
                "reason": dec_row["reason"],
                "triggered_at": _format_datetime(dec_row["triggered_at"])
            }

    # Forecast statistics
    forecast_info = None
    fc_row = repository.get_forecast_summary_by_execution_id(execution_id, connection=connection)
    if fc_row:
        forecast_info = {
            "row_count": int(fc_row["row_count"]),
            "category_count": int(fc_row["category_count"]),
            "model_name": fc_row["model_name"],
            "model_version": fc_row["model_version"],
            "min_forecast_week": _format_datetime(fc_row["min_forecast_week"]),
            "max_forecast_week": _format_datetime(fc_row["max_forecast_week"]),
            "avg_predicted_demand": round(float(fc_row["avg_predicted_demand"]), 4) if fc_row["avg_predicted_demand"] is not None else None
        }

    # Scoped artifacts audit
    output_location = exec_row["output_location"]
    artifacts_info = {
        "output_location": output_location,
        "exists": False,
        "scoped_files": []
    }

    exec_dir = os.path.join(REPO_ROOT, 'ml', 'results', 'executions', str(execution_id))
    if os.path.isdir(exec_dir):
        artifacts_info["exists"] = True
        try:
            artifacts_info["scoped_files"] = sorted(os.listdir(exec_dir))
        except Exception:
            artifacts_info["scoped_files"] = []
    elif output_location:
        full_loc = os.path.join(REPO_ROOT, output_location.replace('/', os.sep))
        artifacts_info["exists"] = os.path.isfile(full_loc)

    duration = _calculate_duration(exec_row["started_at"], exec_row["completed_at"], exec_row["status"])

    return {
        "execution_id": exec_row["execution_id"],
        "pipeline_name": exec_row["pipeline_name"],
        "decision_id": exec_row["decision_id"],
        "triggering_event_id": exec_row["triggering_event_id"],
        "status": exec_row["status"],
        "retry_count": exec_row["retry_count"],
        "max_retries": exec_row["max_retries"],
        "started_at": _format_datetime(exec_row["started_at"]),
        "completed_at": _format_datetime(exec_row["completed_at"]),
        "duration_seconds": duration,
        "next_retry_at": _format_datetime(exec_row["next_retry_at"]),
        "retry_error_type": exec_row["retry_error_type"],
        "output_location": output_location,
        "error_message": exec_row["error_message"],
        "decision": decision_info,
        "forecast": forecast_info,
        "artifacts": artifacts_info
    }


def get_dataset_status(connection=None) -> List[Dict[str, Any]]:
    """
    Returns current status and latest event metadata for all tracked datasets.
    """
    rows = repository.get_datasets_with_latest_events(connection=connection)
    results = []
    for r in rows:
        results.append({
            "dataset_id": r["dataset_id"],
            "dataset_name": r["dataset_name"],
            "source_type": r["source_type"],
            "table_name": r["table_name"],
            "current_version": r["current_version"],
            "status": r["status"],
            "row_count": r["row_count"],
            "last_updated_at": _format_datetime(r["last_updated_at"]),
            "latest_event_id": r["latest_event_id"],
            "latest_event_time": _format_datetime(r["latest_event_time"]),
            "latest_event_type": r["latest_event_type"],
            "latest_batch_id": r["latest_batch_id"],
            "latest_rows_changed": r["latest_rows_changed"]
        })
    return results


def get_dependency_status(connection=None) -> List[Dict[str, Any]]:
    """
    Returns dependency configurations and their latest evaluated decision states.
    """
    dep_rows = repository.get_pipeline_dependencies(connection=connection)
    results = []
    for d in dep_rows:
        pipeline_name = d["pipeline_name"]
        latest_dec = repository.get_latest_decision_for_pipeline(pipeline_name, connection=connection)
        
        dec_info = None
        if latest_dec:
            dec_info = {
                "decision_id": latest_dec["decision_id"],
                "decision": latest_dec["decision"],
                "ready_count": latest_dec["ready_count"],
                "total_required": latest_dec["total_required"],
                "reason": latest_dec["reason"],
                "triggered_at": _format_datetime(latest_dec["triggered_at"]),
                "evaluated_at": _format_datetime(latest_dec["created_at"])
            }

        datasets_list = [name.strip() for name in d["datasets"].split(",")] if d["datasets"] else []

        results.append({
            "dependency_id": d["dependency_id"],
            "pipeline_name": pipeline_name,
            "condition_type": d["condition_type"],
            "required_count": d["required_count"],
            "configured_datasets": datasets_list,
            "latest_decision": dec_info
        })
    return results


def get_forecast_status(limit: int = 20, connection=None) -> List[Dict[str, Any]]:
    """
    Returns aggregated forecast execution summaries.
    """
    safe_limit = max(1, min(limit, 100))
    rows = repository.get_forecast_summaries(limit=safe_limit, connection=connection)
    results = []
    for r in rows:
        results.append({
            "execution_id": r["execution_id"],
            "pipeline_name": r["pipeline_name"],
            "model_name": r["model_name"],
            "model_version": r["model_version"],
            "row_count": int(r["row_count"]),
            "category_count": int(r["category_count"]),
            "min_forecast_week": _format_datetime(r["min_forecast_week"]),
            "max_forecast_week": _format_datetime(r["max_forecast_week"]),
            "average_predicted_demand": round(float(r["avg_predicted_demand"]), 4) if r["avg_predicted_demand"] is not None else None,
            "created_at": _format_datetime(r["created_at"]),
            "output_location": r.get("output_location")
        })
    return results
