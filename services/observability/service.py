"""
Business Logic & Aggregation Service for SupplySense AI Observability.

Provides business-level aggregation, duration calculations, rate computation,
and structured formatting for observability APIs.
"""

import os
import sys
import re
import datetime
from typing import Dict, Any, List, Optional, Union

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from . import repository


def sanitize_error_message(msg: Optional[str]) -> Optional[str]:
    """Sanitizes potential passwords, tokens, and database secrets from error messages."""
    if not msg:
        return msg
    # Redact credentials like password=xyz, pwd=xyz, secret=xyz, token=xyz, api_key=xyz
    pattern = r"(?i)(password|pwd|secret|token|api_key|auth|credential)\s*([:=])\s*['\"]?([^\s,'\"&]+)['\"]?"
    sanitized = re.sub(pattern, r"\1\2[REDACTED]", msg)
    # Redact database connection URLs (e.g. mysql://user:pass@host)
    url_pattern = r"(?i)(mysql(?:\+pymysql)?:\/\/)([^:@\s]+):([^@\s]+)@"
    sanitized = re.sub(url_pattern, r"\1\2:[REDACTED]@", sanitized)
    return sanitized


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


def get_observability_summary(connection=None, exclude_simulated: bool = True) -> Dict[str, Any]:
    """
    Aggregates top-level operational metrics across executions, datasets, and forecasts.
    By default (exclude_simulated=True), scopes metrics to operational executions and datasets.
    """
    exec_counts = repository.get_execution_counts(connection=connection, exclude_simulated=exclude_simulated)
    total_execs = exec_counts["total"]
    completed_execs = exec_counts["completed"]
    failed_execs = exec_counts["failed"]

    success_rate = round((completed_execs / total_execs) * 100.0, 2) if total_execs > 0 else 0.0
    failure_rate = round((failed_execs / total_execs) * 100.0, 2) if total_execs > 0 else 0.0

    # Latest execution
    latest_exec_row = repository.get_latest_execution(connection=connection, exclude_simulated=exclude_simulated)
    latest_exec = None
    if latest_exec_row:
        latest_exec = {
            "execution_id": latest_exec_row["execution_id"],
            "pipeline_name": latest_exec_row["pipeline_name"],
            "status": latest_exec_row["status"],
            "orchestrator": latest_exec_row.get("orchestrator", "LOCAL"),
            "airflow_dag_id": latest_exec_row.get("airflow_dag_id"),
            "airflow_run_id": latest_exec_row.get("airflow_run_id"),
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
    dataset_counts = repository.get_dataset_counts(connection=connection, exclude_simulated=exclude_simulated)

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



def _extract_records_processed(output_location: Optional[str]) -> Optional[int]:
    if not output_location:
        return None
    match = re.search(r"count=(\d+)", output_location)
    return int(match.group(1)) if match else None


def _derive_current_stage(pipeline_name: str, status: str) -> str:
    status_upper = (status or "").upper()
    if status_upper == "RUNNING":
        if "Feature" in pipeline_name:
            return "Feature Engineering"
        elif "Forecast" in pipeline_name:
            return "Forward Forecasting"
        return "Running"
    elif status_upper == "COMPLETED":
        return "Completed"
    elif status_upper == "FAILED":
        return "Failed"
    return status or "Unknown"


def _derive_triggering_dataset(pipeline_name: str) -> Optional[str]:
    if "Feature Pipeline" in pipeline_name:
        return "Operational Orders"
    elif "Forecast Pipeline" in pipeline_name:
        return "Operational Demand Features"
    elif "Demand Forecast" in pipeline_name:
        return "Olist Historical Orders"
    return None


def get_recent_executions(
    status: Optional[str] = None,
    pipeline_name: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
    exclude_simulated: bool = False,
    scope: Optional[str] = "OPERATIONAL",
    as_envelope: bool = True,
    connection=None
) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Returns filtered, paginated, and sorted recent pipeline executions with duration calculations and sanitized error messages.
    Supports scope parameter: 'OPERATIONAL' (default), 'SIMULATED', or 'ALL'.
    Returns envelope { items, total_count, limit, offset } when as_envelope is True.
    """
    safe_limit = max(1, min(limit, 100))
    safe_offset = max(0, offset)
    rows = repository.get_executions(
        status=status,
        pipeline_name=pipeline_name,
        limit=safe_limit,
        offset=safe_offset,
        exclude_simulated=exclude_simulated,
        scope=scope,
        connection=connection
    )
    total_count = repository.count_executions(
        status=status,
        pipeline_name=pipeline_name,
        exclude_simulated=exclude_simulated,
        scope=scope,
        connection=connection
    )

    results = []
    for r in rows:
        duration = _calculate_duration(r["started_at"], r["completed_at"], r["status"])
        output_loc = r["output_location"]
        records_proc = _extract_records_processed(output_loc)
        curr_stage = _derive_current_stage(r["pipeline_name"], r["status"])
        trig_dataset = _derive_triggering_dataset(r["pipeline_name"])

        results.append({
            "execution_id": r["execution_id"],
            "pipeline_name": r["pipeline_name"],
            "decision_id": r["decision_id"],
            "action_id": r.get("action_id"),
            "triggering_event_id": r["triggering_event_id"],
            "status": r["status"],
            "current_stage": curr_stage,
            "records_processed": records_proc,
            "triggering_dataset": trig_dataset,
            "orchestrator": r.get("orchestrator", "LOCAL"),
            "airflow_dag_id": r.get("airflow_dag_id"),
            "airflow_run_id": r.get("airflow_run_id"),
            "retry_count": r["retry_count"],
            "max_retries": r["max_retries"],
            "started_at": _format_datetime(r["started_at"]),
            "completed_at": _format_datetime(r["completed_at"]),
            "duration_seconds": duration,
            "next_retry_at": _format_datetime(r["next_retry_at"]),
            "retry_error_type": r["retry_error_type"],
            "output_location": output_loc,
            "error_message": sanitize_error_message(r["error_message"])
        })

    if as_envelope:
        return {
            "items": results,
            "total_count": total_count,
            "limit": safe_limit,
            "offset": safe_offset
        }
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
        "action_id": exec_row.get("action_id"),
        "triggering_event_id": exec_row["triggering_event_id"],
        "status": exec_row["status"],
        "orchestrator": exec_row.get("orchestrator", "LOCAL"),
        "airflow_dag_id": exec_row.get("airflow_dag_id"),
        "airflow_run_id": exec_row.get("airflow_run_id"),
        "retry_count": exec_row["retry_count"],
        "max_retries": exec_row["max_retries"],
        "started_at": _format_datetime(exec_row["started_at"]),
        "completed_at": _format_datetime(exec_row["completed_at"]),
        "duration_seconds": duration,
        "next_retry_at": _format_datetime(exec_row["next_retry_at"]),
        "retry_error_type": exec_row["retry_error_type"],
        "output_location": output_location,
        "error_message": sanitize_error_message(exec_row["error_message"]),
        "decision": decision_info,
        "forecast": forecast_info,
        "artifacts": artifacts_info,
        "model_comparison": repository.get_model_comparison_metrics(execution_id)
    }


def get_category_forecast_trends(
    execution_id: int,
    categories: Optional[List[str]] = None,
    limit: int = 5,
    connection=None
) -> Optional[Dict[str, Any]]:
    """
    Retrieves category-level weekly forecast trends for the specified execution.
    """
    return repository.get_category_forecast_trends(
        execution_id=execution_id,
        categories=categories,
        limit=limit,
        connection=connection
    )


def get_model_comparison_metrics(execution_id: int) -> Optional[Dict[str, Any]]:
    """
    Retrieves model comparison metrics from execution-scoped artifacts.
    """
    return repository.get_model_comparison_metrics(execution_id=execution_id)



def get_dataset_status(
    source_type: Optional[str] = None,
    exclude_simulated: bool = False,
    scope: Optional[str] = None,
    limit: Optional[int] = None,
    offset: int = 0,
    connection=None
) -> List[Dict[str, Any]]:
    """
    Returns current status and latest event metadata for all tracked datasets.
    Supports filtering by source_type (e.g. 'SIMULATED'), scope ('OPERATIONAL', 'SIMULATED', 'ALL'),
    or excluding simulated datasets. Returns flat list of dataset items.
    """
    rows = repository.get_datasets_with_latest_events(
        source_type=source_type,
        exclude_simulated=exclude_simulated,
        scope=scope,
        limit=limit,
        offset=offset,
        connection=connection
    )
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


def get_dataset_status_paginated(
    source_type: Optional[str] = None,
    exclude_simulated: bool = False,
    scope: str = "ALL",
    limit: int = 20,
    offset: int = 0,
    connection=None
) -> Dict[str, Any]:
    """
    Returns paginated dataset metadata with matching SQL count query.
    Returns envelope { items, total_count, limit, offset }.
    """
    safe_limit = max(1, min(limit, 100))
    safe_offset = max(0, offset)
    items = get_dataset_status(
        source_type=source_type,
        exclude_simulated=exclude_simulated,
        scope=scope,
        limit=safe_limit,
        offset=safe_offset,
        connection=connection
    )
    total_count = repository.count_datasets(
        source_type=source_type,
        exclude_simulated=exclude_simulated,
        scope=scope,
        connection=connection
    )
    return {
        "items": items,
        "total_count": total_count,
        "limit": safe_limit,
        "offset": safe_offset
    }


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


def get_dependency_decisions(
    limit: int = 50,
    offset: int = 0,
    scope: str = "OPERATIONAL",
    decision: Optional[str] = None,
    pipeline_name: Optional[str] = None,
    connection=None
) -> Dict[str, Any]:
    """
    Returns recent evaluated decisions from pipeline_decisions audit table
    with reliable provenance classification, pagination, and matching count.
    """
    if hasattr(limit, "default"):
        limit = limit.default
    if hasattr(offset, "default"):
        offset = offset.default
    safe_limit = max(1, min(int(limit), 200))
    safe_offset = max(0, int(offset))
    scope_upper = scope.upper() if scope else "OPERATIONAL"

    rows = repository.get_recent_decisions(
        limit=safe_limit,
        offset=safe_offset,
        scope=scope_upper,
        decision=decision,
        pipeline_name=pipeline_name,
        connection=connection
    )
    total_count = repository.get_recent_decisions_count(
        scope=scope_upper,
        decision=decision,
        pipeline_name=pipeline_name,
        connection=connection
    )

    results = []
    for r in rows:
        results.append({
            "decision_id": r["decision_id"],
            "pipeline_name": r["pipeline_name"],
            "condition_type": r["condition_type"],
            "decision": r["decision"],
            "required_count": r.get("required_count"),
            "ready_count": r.get("ready_count"),
            "total_required": r.get("total_required"),
            "reason": r.get("reason"),
            "triggered_at": _format_datetime(r.get("triggered_at")),
            "evaluated_at": _format_datetime(r.get("created_at")),
            "scope": r.get("classification") or "OPERATIONAL",
            "classification": r.get("classification") or "OPERATIONAL"
        })
    return {
        "items": results,
        "total_count": total_count,
        "limit": safe_limit,
        "offset": safe_offset
    }


def get_dependency_events(
    limit: int = 25,
    offset: int = 0,
    dataset_name: Optional[str] = None,
    event_status: Optional[str] = None,
    connection=None
) -> Dict[str, Any]:
    """
    Returns incoming dataset events arriving at Dependency Engine with metadata,
    realistic context, and total count.
    """
    if hasattr(limit, "default"):
        limit = limit.default
    if hasattr(offset, "default"):
        offset = offset.default
    safe_limit = max(1, min(int(limit), 100))
    safe_offset = max(0, int(offset))

    events = repository.get_incoming_dependency_events(
        limit=safe_limit,
        offset=safe_offset,
        dataset_name=dataset_name,
        event_status=event_status,
        connection=connection
    )
    total = repository.get_incoming_dependency_events_count(
        dataset_name=dataset_name,
        event_status=event_status,
        connection=connection
    )
    return {
        "items": events,
        "total_count": total,
        "limit": safe_limit,
        "offset": safe_offset
    }


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
            "output_location": r.get("output_location"),
            "data_provenance": r.get("data_provenance", "HISTORICAL")
        })
    return results


# ---------------------------------------------------------------------------
# Phase 3A: Operational Inventory, Supplier & Replenishment Service Functions
# ---------------------------------------------------------------------------

def get_operational_inventory(
    warehouse_id: Optional[str] = None,
    low_stock_only: bool = False,
    search: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    connection=None
) -> Dict[str, Any]:
    return repository.get_operational_inventory(
        warehouse_id=warehouse_id,
        low_stock_only=low_stock_only,
        search=search,
        limit=limit,
        offset=offset,
        connection=connection
    )


def get_operational_suppliers(
    supplier_id: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    connection=None
) -> Dict[str, Any]:
    return repository.get_operational_suppliers(
        supplier_id=supplier_id,
        status=status,
        limit=limit,
        offset=offset,
        connection=connection
    )


def get_replenishment_orders(
    status: Optional[str] = None,
    sku: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    connection=None
) -> Dict[str, Any]:
    return repository.get_replenishment_orders(
        status=status,
        sku=sku,
        limit=limit,
        offset=offset,
        connection=connection
    )


def deliver_replenishment(
    replenishment_id: str,
    connection=None
) -> Dict[str, Any]:
    """
    Executes deterministic simulated delivery for an in-flight replenishment order.
    Transitions status to DELIVERED, increments on-hand inventory, and persists/broadcasts
    the DELIVERY_COMPLETED event atomically.
    """
    from services.simulator import delivery_service
    return delivery_service.deliver_replenishment(
        replenishment_id=replenishment_id,
        connection=connection
    )


def get_operational_summary_metrics(connection=None) -> Dict[str, Any]:
    return repository.get_operational_summary_metrics(connection=connection)


def get_recent_operational_events(
    limit: int = 25,
    event_type: Optional[str] = None,
    connection=None
) -> List[Dict[str, Any]]:
    return repository.get_recent_operational_events(
        limit=limit,
        event_type=event_type,
        connection=connection
    )



def get_operational_alerts(
    severity: Optional[str] = None,
    category: Optional[str] = None,
    stuck_threshold_hours: int = 24,
    limit: int = 50,
    exclude_simulated: bool = True,
    connection=None
) -> Dict[str, Any]:
    """
    Computes, categorizes, and prioritizes actionable operational alerts
    across suppliers, catalog availability, replenishment lifecycle, and pipeline executions.
    """
    raw_data = repository.get_operational_alerts_raw(
        stuck_threshold_hours=stuck_threshold_hours,
        exclude_simulated=exclude_simulated,
        connection=connection
    )

    alerts = []

    # 1. Problem suppliers (Offline or Degraded)
    for s in raw_data.get("problem_suppliers", []):
        sup_id = s["supplier_id"]
        sup_status = s["status"]
        sev = "CRITICAL" if sup_status == "Offline" else "WARNING"
        alerts.append({
            "alert_id": f"SUPP_{sup_status.upper()}_{sup_id}",
            "category": "SUPPLIER",
            "severity": sev,
            "title": f"Supplier Connectivity {sup_status}",
            "condition": f"Supplier '{s.get('supplier_name', sup_id)}' is {sup_status} (Reliability score: {s.get('reliability_score', 'N/A')}, Lead time: {s.get('average_lead_time_days', 'N/A')}d)",
            "timestamp": _format_datetime(s.get("last_health_check_at")) or datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "identifiers": {
                "supplier_id": sup_id
            },
            "actionable_link": {
                "tab": "inventory",
                "filter_type": "supplier",
                "value": sup_id
            }
        })

    # 2. Catalog availability constraints
    for c in raw_data.get("catalog_failures", []):
        sku = c["sku"]
        sup_id = c["supplier_id"]
        avail = c["supply_availability"]
        sev = "CRITICAL" if avail == "OUT_OF_STOCK" else "WARNING"
        alerts.append({
            "alert_id": f"CATALOG_{avail}_{sup_id}_{sku[:8]}",
            "category": "CATALOG",
            "severity": sev,
            "title": f"Catalog SKU {avail.replace('_', ' ').title()}",
            "condition": f"SKU '{sku}' at supplier '{c.get('supplier_name', sup_id)}' is marked {avail} (Min reorder qty: {c.get('min_reorder_quantity', 10)})",
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "identifiers": {
                "supplier_id": sup_id,
                "sku": sku
            },
            "actionable_link": {
                "tab": "inventory",
                "filter_type": "sku",
                "value": sku
            }
        })

    # 3. Failed or cancelled replenishments
    for rep in raw_data.get("failed_replenishments", []):
        rep_id = rep["replenishment_id"]
        status = rep["status"]
        alerts.append({
            "alert_id": f"REP_{status}_{rep_id}",
            "category": "REPLENISHMENT",
            "severity": "CRITICAL" if status == "FAILED" else "WARNING",
            "title": f"Replenishment Order {status.title()}",
            "condition": f"Replenishment '{rep_id}' for SKU '{rep['sku']}' ({rep['quantity']} units) terminated with status {status}",
            "timestamp": _format_datetime(rep.get("dispatched_at")) or datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "identifiers": {
                "replenishment_id": rep_id,
                "sku": rep["sku"],
                "supplier_id": rep["supplier_id"],
                "action_id": rep.get("action_id"),
                "decision_id": rep.get("decision_id"),
                "execution_id": rep.get("execution_id")
            },
            "actionable_link": {
                "tab": "inventory",
                "filter_type": "replenishment_status",
                "value": status
            }
        })

    # 4. Overdue replenishments
    for o in raw_data.get("overdue_replenishments", []):
        rep_id = o["replenishment_id"]
        od_hours = o.get("overdue_hours") or 0
        alerts.append({
            "alert_id": f"REP_OVERDUE_{rep_id}",
            "category": "REPLENISHMENT",
            "severity": "WARNING",
            "title": "Replenishment Delivery Overdue",
            "condition": f"Replenishment '{rep_id}' for SKU '{o['sku']}' is overdue by ~{od_hours}h (Est. delivery was {_format_datetime(o.get('estimated_delivery_at'))})",
            "timestamp": _format_datetime(o.get("estimated_delivery_at")) or datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "identifiers": {
                "replenishment_id": rep_id,
                "sku": o["sku"],
                "supplier_id": o["supplier_id"],
                "action_id": o.get("action_id"),
                "decision_id": o.get("decision_id"),
                "execution_id": o.get("execution_id")
            },
            "actionable_link": {
                "tab": "inventory",
                "filter_type": "replenishment",
                "value": rep_id
            }
        })

    # 5. Stuck replenishments
    for s in raw_data.get("stuck_replenishments", []):
        rep_id = s["replenishment_id"]
        stuck_h = s.get("stuck_hours") or 0
        if any(a["alert_id"] == f"REP_OVERDUE_{rep_id}" for a in alerts):
            continue
        alerts.append({
            "alert_id": f"REP_STUCK_{rep_id}",
            "category": "REPLENISHMENT",
            "severity": "WARNING",
            "title": "Replenishment In-Flight Delay",
            "condition": f"Replenishment '{rep_id}' has been in '{s['status']}' state for ~{stuck_h}h without completion",
            "timestamp": _format_datetime(s.get("dispatched_at")) or datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "identifiers": {
                "replenishment_id": rep_id,
                "sku": s["sku"],
                "supplier_id": s["supplier_id"],
                "action_id": s.get("action_id"),
                "decision_id": s.get("decision_id"),
                "execution_id": s.get("execution_id")
            },
            "actionable_link": {
                "tab": "inventory",
                "filter_type": "replenishment",
                "value": rep_id
            }
        })

    # 6. Failed pipeline executions
    for fe in raw_data.get("failed_executions", []):
        exec_id = fe["execution_id"]
        sanitized_err = sanitize_error_message(fe.get("error_message") or "Unknown error")
        alerts.append({
            "alert_id": f"EXEC_FAILED_{exec_id}",
            "category": "PIPELINE",
            "severity": "CRITICAL",
            "title": "Pipeline Execution Failed",
            "condition": f"Pipeline '{fe.get('pipeline_name')}' execution #{exec_id} failed: {sanitized_err[:150]} (Retries: {fe.get('retry_count', 0)}/{fe.get('max_retries', 0)})",
            "timestamp": _format_datetime(fe.get("completed_at") or fe.get("started_at")) or datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "identifiers": {
                "execution_id": exec_id,
                "decision_id": fe.get("decision_id"),
                "action_id": fe.get("action_id")
            },
            "actionable_link": {
                "tab": "executions",
                "filter_type": "execution_id",
                "value": exec_id
            }
        })

    filtered_alerts = alerts
    if severity:
        filtered_alerts = [a for a in filtered_alerts if a["severity"].upper() == severity.upper()]
    if category:
        filtered_alerts = [a for a in filtered_alerts if a["category"].upper() == category.upper()]

    severity_order = {"CRITICAL": 0, "WARNING": 1, "INFO": 2}
    filtered_alerts.sort(key=lambda a: (severity_order.get(a["severity"], 3), a["timestamp"]), reverse=False)

    safe_limit = max(1, min(limit, 100))
    paged_alerts = filtered_alerts[:safe_limit]

    critical_cnt = sum(1 for a in alerts if a["severity"] == "CRITICAL")
    warning_cnt = sum(1 for a in alerts if a["severity"] == "WARNING")
    info_cnt = sum(1 for a in alerts if a["severity"] == "INFO")

    cat_counts = {}
    for a in alerts:
        cat_counts[a["category"]] = cat_counts.get(a["category"], 0) + 1

    return {
        "summary": {
            "total_alerts": len(alerts),
            "critical_count": critical_cnt,
            "warning_count": warning_cnt,
            "info_count": info_cnt,
            "by_category": cat_counts
        },
        "alerts": paged_alerts,
        "count": len(paged_alerts),
        "total_count": len(filtered_alerts),
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }


def get_event_processing_health(connection=None) -> Dict[str, Any]:
    """
    Returns real-time event hub and MySQL event journal diagnostics.
    """
    from .event_hub import hub, SCHEMA_VERSION, HEARTBEAT_INTERVAL_SECONDS

    journal_metrics = repository.get_event_processing_metrics(connection=connection)

    with hub._lock:
        active_subs = len(hub._subscribers)
        buf_size = len(hub._ring_buffer)
        buf_capacity = hub.buffer_size
        oldest_id = hub._ring_buffer[0].event_id if buf_size > 0 else None
        newest_id = hub._ring_buffer[-1].event_id if buf_size > 0 else None

    is_healthy = True
    degradation_reasons = []

    if journal_metrics.get("total_journaled_events", 0) == 0:
        degradation_reasons.append("No journaled events found in database.")

    return {
        "status": "HEALTHY" if is_healthy else "DEGRADED",
        "reasons": degradation_reasons,
        "sse": {
            "active_subscribers": active_subs,
            "ring_buffer_size": buf_size,
            "ring_buffer_capacity": buf_capacity,
            "schema_version": SCHEMA_VERSION,
            "heartbeat_interval_seconds": HEARTBEAT_INTERVAL_SECONDS,
            "oldest_buffered_event_id": oldest_id,
            "newest_buffered_event_id": newest_id,
        },
        "journal": journal_metrics,
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }


# ---------------------------------------------------------------------------
# Phase 3D: Forecast-Driven Replenishment Recommendation Service Functions
# ---------------------------------------------------------------------------

def get_forecast_recommendations(
    execution_id: Optional[int] = None,
    scope_type: Optional[str] = None,
    status: Optional[str] = None,
    category: Optional[str] = None,
    sku: Optional[str] = None,
    warehouse_id: Optional[str] = None,
    planning_horizon_weeks: int = 4,
    limit: int = 50,
    offset: int = 0,
    connection=None
) -> Dict[str, Any]:
    """
    Computes forecast-driven replenishment recommendations by joining
    model demand forecasts with real-time operational inventory snapshots.
    100% read-only and advisory: does NOT mutate inventory or place orders.
    """
    from . import recommendation_engine

    target_exec_id = execution_id
    if target_exec_id is None:
        target_exec_id = repository.get_latest_completed_forecast_execution_id(connection=connection) or 99653

    # Retrieve forecast trends (uses MySQL forecast_results with artifact CSV fallback)
    forecast_data = repository.get_category_forecast_trends(
        execution_id=target_exec_id,
        limit=100,
        connection=connection
    )

    if not forecast_data:
        # Return structured empty recommendation result
        return {
            "summary": {
                "total_recommendations": 0,
                "replenishment_recommended_count": 0,
                "stock_sufficient_count": 0,
                "needs_review_count": 0,
                "insufficient_data_count": 0,
                "by_scope": {"SKU": 0, "CATEGORY": 0},
                "execution_id": target_exec_id,
                "model_name": "XGBoost",
                "model_version": "xgboost-v1",
                "planning_horizon_weeks": planning_horizon_weeks
            },
            "recommendations": [],
            "count": 0,
            "total_count": 0,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        }

    # Retrieve operational inventory snapshot
    inventory_snapshot = repository.get_all_operational_inventory_snapshot(
        warehouse_id=warehouse_id,
        connection=connection
    )

    # Extract SKUs to query authoritative mappings and supplier catalogs
    sku_list = [item["sku"] for item in inventory_snapshot] if inventory_snapshot else []

    sku_to_cat = repository.get_sku_category_mappings(skus=sku_list, connection=connection)
    cat_translations = repository.get_category_translations(connection=connection)
    supplier_cat = repository.get_supplier_catalog_for_skus(skus=sku_list, connection=connection)

    safe_horizon = max(1, min(int(planning_horizon_weeks), 12))

    return recommendation_engine.generate_recommendations(
        forecast_data=forecast_data,
        inventory_snapshot=inventory_snapshot,
        sku_to_category_map=sku_to_cat,
        category_translations=cat_translations,
        supplier_catalog_map=supplier_cat,
        planning_horizon_weeks=safe_horizon,
        scope_filter=scope_type,
        status_filter=status,
        category_filter=category,
        sku_filter=sku,
        warehouse_filter=warehouse_id,
        limit=limit,
        offset=offset
    )


def get_forecast_recommendation_by_id(
    recommendation_id: str,
    connection=None
) -> Optional[Dict[str, Any]]:
    """
    Looks up a specific recommendation by its deterministic identifier
    (e.g. REC-99653-SKU-99a4788cb24856965c36a24e339b6058).
    """
    if not recommendation_id:
        return None

    # Parse execution_id and target scope from recommendation_id if possible
    parts = recommendation_id.split("-")
    exec_id = None
    if len(parts) >= 2 and parts[1].isdigit():
        exec_id = int(parts[1])

    target_sku = None
    target_cat = None
    if "-SKU-" in recommendation_id:
        target_sku = recommendation_id.split("-SKU-", 1)[1]
    elif "-CAT-" in recommendation_id:
        target_cat = recommendation_id.split("-CAT-", 1)[1]

    # Fetch recommendations for this execution with targeted filter to prevent pagination truncation
    result = get_forecast_recommendations(
        execution_id=exec_id,
        sku=target_sku,
        category=target_cat,
        limit=200,
        connection=connection
    )

    for rec in result.get("recommendations", []):
        if rec.get("recommendation_id") == recommendation_id:
            return rec

    return None


# ---------------------------------------------------------------------------
# Phase 3E: Human-in-the-Loop Replenishment Approval Service Delegations
# ---------------------------------------------------------------------------

def submit_recommendation_for_approval(
    recommendation_id: str,
    actor_id: str,
    actor_role: str,
    requested_quantity: Optional[int] = None,
    sku_override: Optional[str] = None,
    warehouse_id_override: Optional[str] = None,
    idempotency_key: Optional[str] = None,
    connection=None
) -> Dict[str, Any]:
    from . import approval_service
    return approval_service.submit_recommendation_for_approval(
        recommendation_id=recommendation_id,
        actor_id=actor_id,
        actor_role=actor_role,
        requested_quantity=requested_quantity,
        sku_override=sku_override,
        warehouse_id_override=warehouse_id_override,
        idempotency_key=idempotency_key,
        connection=connection
    )


def list_replenishment_approvals(
    status: Optional[str] = None,
    sku: Optional[str] = None,
    warehouse_id: Optional[str] = None,
    execution_id: Optional[int] = None,
    limit: int = 50,
    offset: int = 0,
    connection=None
) -> Dict[str, Any]:
    from . import approval_service
    return approval_service.list_replenishment_approvals(
        status=status,
        sku=sku,
        warehouse_id=warehouse_id,
        execution_id=execution_id,
        limit=limit,
        offset=offset,
        connection=connection
    )


def get_replenishment_approval_detail(
    approval_id: str,
    connection=None
) -> Optional[Dict[str, Any]]:
    from . import approval_service
    return approval_service.get_replenishment_approval_detail(
        approval_id=approval_id,
        connection=connection
    )


def approve_replenishment(
    approval_id: str,
    actor_id: str,
    actor_role: str,
    notes: Optional[str] = None,
    connection=None
) -> Dict[str, Any]:
    from . import approval_service
    return approval_service.approve_replenishment(
        approval_id=approval_id,
        actor_id=actor_id,
        actor_role=actor_role,
        notes=notes,
        connection=connection
    )


def reject_replenishment(
    approval_id: str,
    actor_id: str,
    actor_role: str,
    reason: str,
    connection=None
) -> Dict[str, Any]:
    from . import approval_service
    return approval_service.reject_replenishment(
        approval_id=approval_id,
        actor_id=actor_id,
        actor_role=actor_role,
        reason=reason,
        connection=connection
    )


def get_forecast_products(
    execution_id: int,
    category: Optional[str] = None,
    search: Optional[str] = None,
    sort_by: str = "demand",
    order: str = "desc",
    limit: int = 25,
    offset: int = 0,
    connection=None
) -> Dict[str, Any]:
    """
    Service wrapper for retrieving paginated product-level demand and stock metrics.
    """
    return repository.get_forecast_products(
        execution_id=execution_id,
        category=category,
        search=search,
        sort_by=sort_by,
        order=order,
        limit=limit,
        offset=offset,
        connection=connection
    )


def get_product_detail(
    product_id: str,
    execution_id: Optional[int] = None,
    connection=None
) -> Optional[Dict[str, Any]]:
    """
    Service wrapper for retrieving single product details, timeseries, and inventory.
    """
    return repository.get_product_detail(
        product_id=product_id,
        execution_id=execution_id,
        connection=connection
    )


# ==============================================================================
# Operational Order Ingestion & Observability (Phase 1)
# ==============================================================================

def ingest_operational_orders(
    orders: List[Dict[str, Any]],
    idempotency_key: Optional[str] = None,
    source: str = "OPERATIONAL_API",
    warehouse_id: str = "WH-SP-01",
    connection=None
) -> Dict[str, Any]:
    """Service wrapper for transactional operational order ingestion."""
    return repository.ingest_operational_orders(
        orders=orders,
        idempotency_key=idempotency_key,
        source=source,
        warehouse_id=warehouse_id,
        connection=connection
    )


def get_operational_orders(
    limit: int = 50,
    offset: int = 0,
    source: Optional[str] = None,
    order_status: Optional[str] = None,
    connection=None
) -> Dict[str, Any]:
    """Service wrapper for paginated operational orders."""
    return repository.get_operational_orders(
        limit=limit,
        offset=offset,
        source=source,
        order_status=order_status,
        connection=connection
    )


def get_operational_order_by_id(
    order_id: str,
    connection=None
) -> Optional[Dict[str, Any]]:
    """Service wrapper for retrieving single operational order by order_id."""
    return repository.get_operational_order_by_id(
        order_id=order_id,
        connection=connection
    )


def get_operational_forecasts(
    execution_id: Optional[int] = None,
    category: Optional[str] = None,
    horizon_days: Optional[int] = None,
    limit: int = 50,
    offset: int = 0,
    connection=None
) -> Dict[str, Any]:
    """Service wrapper for paginated operational forward forecasts."""
    return repository.get_operational_forecast_results(
        execution_id=execution_id,
        category=category,
        horizon_days=horizon_days,
        limit=limit,
        offset=offset,
        connection=connection
    )
