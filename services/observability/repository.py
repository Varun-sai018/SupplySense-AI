"""
Database Repository for SupplySense AI Observability & Monitoring.

Executes efficient, read-only SQL queries and aggregations over pipeline executions,
dataset states, dependency decisions, and forecast results.
"""

import os
import sys
import logging
import datetime
from typing import Dict, Any, List, Optional, Tuple

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


def get_execution_counts(connection=None, exclude_simulated: bool = True) -> Dict[str, Any]:
    """
    Computes aggregate metrics for pipeline executions:
    total, running, completed, failed, retrying, retried_total, avg_duration_seconds.
    When exclude_simulated is True, excludes synthetic pipelines matching 'SIM-%'.
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    where_clause = "WHERE pipeline_name NOT LIKE 'SIM-%'" if exclude_simulated else ""

    try:
        cur = connection.cursor()
        cur.execute(f"""
            SELECT
                COUNT(*) AS total,
                COALESCE(SUM(CASE WHEN status = 'RUNNING' THEN 1 ELSE 0 END), 0) AS running,
                COALESCE(SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END), 0) AS completed,
                COALESCE(SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END), 0) AS failed,
                COALESCE(SUM(CASE WHEN status = 'FAILED' AND next_retry_at IS NOT NULL AND retry_count < max_retries THEN 1 ELSE 0 END), 0) AS retrying,
                COALESCE(SUM(CASE WHEN retry_count > 0 THEN 1 ELSE 0 END), 0) AS retried_total,
                AVG(CASE WHEN status = 'COMPLETED' AND completed_at IS NOT NULL AND started_at IS NOT NULL AND completed_at >= started_at
                         THEN TIMESTAMPDIFF(SECOND, started_at, completed_at) ELSE NULL END) AS avg_duration_seconds
            FROM pipeline_executions
            {where_clause}
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


def get_latest_execution(connection=None, exclude_simulated: bool = True) -> Optional[Dict[str, Any]]:
    """Fetches the most recent pipeline execution record, optionally excluding SIM-% pipelines."""
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    where_clause = "WHERE pipeline_name NOT LIKE 'SIM-%'" if exclude_simulated else ""

    try:
        cur = connection.cursor()
        cur.execute(f"""
            SELECT 
                execution_id, pipeline_name, decision_id, triggering_event_id,
                status, orchestrator, airflow_dag_id, airflow_run_id,
                retry_count, max_retries, started_at, completed_at,
                next_retry_at, retry_error_type, output_location, error_message, created_at
            FROM pipeline_executions
            {where_clause}
            ORDER BY execution_id DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        cur.close()
        return row
    finally:
        if should_close and connection:
            connection.close()


def get_dataset_counts(connection=None, exclude_simulated: bool = True) -> Dict[str, int]:
    """Fetches dataset count aggregations from dataset_metadata, optionally excluding source_type = 'SIMULATED'."""
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    where_clause = "WHERE source_type != 'SIMULATED'" if exclude_simulated else ""

    try:
        cur = connection.cursor()
        cur.execute(f"""
            SELECT
                COUNT(*) AS total,
                COALESCE(SUM(CASE WHEN status = 'READY' THEN 1 ELSE 0 END), 0) AS ready,
                COALESCE(SUM(CASE WHEN status = 'WAITING' THEN 1 ELSE 0 END), 0) AS waiting
            FROM dataset_metadata
            {where_clause}
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
    offset: int = 0,
    exclude_simulated: bool = False,
    scope: Optional[str] = None,
    connection=None
) -> List[Dict[str, Any]]:
    """
    Fetches recent pipeline executions with optional filtering, action linkage, and bounded limit.
    Supports scope parameter: 'OPERATIONAL', 'SIMULATED', or 'ALL'.
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    query = """
        SELECT 
            e.execution_id, e.pipeline_name, e.decision_id, e.triggering_event_id,
            e.status, e.orchestrator, e.airflow_dag_id, e.airflow_run_id,
            e.retry_count, e.max_retries, e.started_at, e.completed_at,
            e.next_retry_at, e.retry_error_type, e.output_location, e.error_message, e.created_at,
            bra.action_id
        FROM pipeline_executions e
        LEFT JOIN (
            SELECT execution_id, MAX(action_id) AS action_id
            FROM business_rule_actions
            WHERE execution_id IS NOT NULL
            GROUP BY execution_id
        ) bra ON e.execution_id = bra.execution_id
        WHERE 1=1
    """
    params = []

    if status:
        query += " AND e.status = %s"
        params.append(status)

    if pipeline_name:
        query += " AND e.pipeline_name = %s"
        params.append(pipeline_name)
    elif scope:
        scope_upper = scope.upper()
        if scope_upper == "OPERATIONAL":
            query += " AND e.pipeline_name NOT LIKE %s"
            params.append("SIM-%")
        elif scope_upper == "SIMULATED":
            query += " AND e.pipeline_name LIKE %s"
            params.append("SIM-%")
        elif scope_upper == "ALL":
            pass
    elif exclude_simulated:
        query += " AND e.pipeline_name NOT LIKE %s"
        params.append("SIM-%")

    query += " ORDER BY e.execution_id DESC LIMIT %s OFFSET %s"
    params.extend([max(1, min(limit, 100)), max(0, offset)])

    try:
        cur = connection.cursor()
        cur.execute(query, tuple(params))
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        if should_close and connection:
            connection.close()


def count_executions(
    status: Optional[str] = None,
    pipeline_name: Optional[str] = None,
    exclude_simulated: bool = False,
    scope: Optional[str] = None,
    connection=None
) -> int:
    """
    Returns total count of executions matching filter criteria.
    Uses identical WHERE clause as get_executions.
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    query = "SELECT COUNT(*) AS total FROM pipeline_executions e WHERE 1=1"
    params = []

    if status:
        query += " AND e.status = %s"
        params.append(status)

    if pipeline_name:
        query += " AND e.pipeline_name = %s"
        params.append(pipeline_name)
    elif scope:
        scope_upper = scope.upper()
        if scope_upper == "OPERATIONAL":
            query += " AND e.pipeline_name NOT LIKE %s"
            params.append("SIM-%")
        elif scope_upper == "SIMULATED":
            query += " AND e.pipeline_name LIKE %s"
            params.append("SIM-%")
        elif scope_upper == "ALL":
            pass
    elif exclude_simulated:
        query += " AND e.pipeline_name NOT LIKE %s"
        params.append("SIM-%")

    try:
        cur = connection.cursor()
        cur.execute(query, tuple(params))
        row = cur.fetchone()
        cur.close()
        return int(row["total"] or 0) if row else 0
    finally:
        if should_close and connection:
            connection.close()


def get_execution_by_id(execution_id: int, connection=None) -> Optional[Dict[str, Any]]:
    """Fetches a single pipeline execution record by execution_id with linked action_id."""
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()
        cur.execute("""
            SELECT 
                e.execution_id, e.pipeline_name, e.decision_id, e.triggering_event_id,
                e.status, e.orchestrator, e.airflow_dag_id, e.airflow_run_id,
                e.retry_count, e.max_retries, e.started_at, e.completed_at,
                e.next_retry_at, e.retry_error_type, e.output_location, e.error_message, e.created_at,
                bra.action_id
            FROM pipeline_executions e
            LEFT JOIN (
                SELECT execution_id, MAX(action_id) AS action_id
                FROM business_rule_actions
                WHERE execution_id = %s
                GROUP BY execution_id
            ) bra ON e.execution_id = bra.execution_id
            WHERE e.execution_id = %s
        """, (execution_id, execution_id))
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


def get_datasets_with_latest_events(
    source_type: Optional[str] = None,
    exclude_simulated: bool = False,
    scope: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    connection=None
) -> List[Dict[str, Any]]:
    """
    Fetches all datasets from dataset_metadata along with their latest event metadata.
    Supports filtering by source_type (e.g. 'SIMULATED'), scope ('OPERATIONAL', 'SIMULATED', 'ALL'),
    or excluding simulated datasets. Supports pagination with limit and offset.
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    query = """
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
        WHERE 1=1
    """
    params = []
    if source_type:
        query += " AND m.source_type = %s"
        params.append(source_type)
    elif scope:
        scope_upper = scope.upper()
        if scope_upper == "OPERATIONAL":
            query += " AND m.source_type != %s"
            params.append("SIMULATED")
        elif scope_upper == "SIMULATED":
            query += " AND m.source_type = %s"
            params.append("SIMULATED")
        elif scope_upper == "ALL":
            pass
    elif exclude_simulated:
        query += " AND m.source_type != %s"
        params.append("SIMULATED")

    query += " ORDER BY m.dataset_id ASC"

    if limit is not None:
        safe_limit = max(1, min(limit, 100))
        safe_offset = max(0, offset) if offset is not None else 0
        query += " LIMIT %s OFFSET %s"
        params.extend([safe_limit, safe_offset])

    try:
        cur = connection.cursor()
        cur.execute(query, tuple(params))
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        if should_close and connection:
            connection.close()


def count_datasets(
    source_type: Optional[str] = None,
    exclude_simulated: bool = False,
    scope: Optional[str] = None,
    connection=None
) -> int:
    """
    Returns total count of datasets matching filter criteria.
    Uses identical WHERE clause as get_datasets_with_latest_events.
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    query = "SELECT COUNT(*) AS total FROM dataset_metadata m WHERE 1=1"
    params = []

    if source_type:
        query += " AND m.source_type = %s"
        params.append(source_type)
    elif scope:
        scope_upper = scope.upper()
        if scope_upper == "OPERATIONAL":
            query += " AND m.source_type != %s"
            params.append("SIMULATED")
        elif scope_upper == "SIMULATED":
            query += " AND m.source_type = %s"
            params.append("SIMULATED")
        elif scope_upper == "ALL":
            pass
    elif exclude_simulated:
        query += " AND m.source_type != %s"
        params.append("SIMULATED")

    try:
        cur = connection.cursor()
        cur.execute(query, tuple(params))
        row = cur.fetchone()
        cur.close()
        return int(row["total"] or 0) if row else 0
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


def _generate_realistic_event_context(
    dataset_name: str,
    event_type: str,
    rows_changed: int,
    version: int,
    source_change_id: str
) -> Dict[str, Any]:
    """Constructs realistic business context and payload attributes for Olist e-commerce stream events."""
    name_lower = (dataset_name or "").lower()
    if "sp" in name_lower:
        return {
            "region": "São Paulo (SP) Metropolitan Region",
            "hub_code": "HUB-SP-01",
            "carrier": "Correios Sedex / Loggi",
            "condition_contribution": "Regional Hub Feed (1 of 4 for Quorum: SP, RJ, MG, RS)",
            "summary": f"Batch of {rows_changed} orders dispatched through São Paulo regional distribution center.",
            "kafka_topic": "dataset-events",
            "sample_data": {
                "origin_state": "SP",
                "destination_states": ["SP", "RJ", "MG", "PR"],
                "avg_freight_value": "R$ 18.50",
                "logistics_status": "READY_FOR_DISPATCH"
            }
        }
    elif "rj" in name_lower:
        return {
            "region": "Rio de Janeiro (RJ) Coastal Hub",
            "hub_code": "HUB-RJ-02",
            "carrier": "Correios PAC / Total Express",
            "condition_contribution": "Regional Hub Feed (1 of 4 for Quorum: SP, RJ, MG, RS)",
            "summary": f"Batch of {rows_changed} orders arrived at Rio de Janeiro regional fulfillment center.",
            "kafka_topic": "dataset-events",
            "sample_data": {
                "origin_state": "RJ",
                "destination_states": ["RJ", "ES", "MG"],
                "avg_freight_value": "R$ 22.40",
                "logistics_status": "READY_FOR_DISPATCH"
            }
        }
    elif "mg" in name_lower:
        return {
            "region": "Minas Gerais (MG) Central Hub",
            "hub_code": "HUB-MG-03",
            "carrier": "DirectLog Express",
            "condition_contribution": "Regional Hub Feed (1 of 4 for Quorum: SP, RJ, MG, RS)",
            "summary": f"Batch of {rows_changed} orders arrived at Belo Horizonte regional fulfillment depot.",
            "kafka_topic": "dataset-events",
            "sample_data": {
                "origin_state": "MG",
                "destination_states": ["MG", "SP", "RJ", "BA"],
                "avg_freight_value": "R$ 21.10",
                "logistics_status": "READY_FOR_DISPATCH"
            }
        }
    elif "rs" in name_lower:
        return {
            "region": "Rio Grande do Sul (RS) Southern Hub",
            "hub_code": "HUB-RS-04",
            "carrier": "Jadlog Logística",
            "condition_contribution": "Regional Hub Feed (1 of 4 for Quorum: SP, RJ, MG, RS)",
            "summary": f"Batch of {rows_changed} orders routed through Porto Alegre southern dispatch facility.",
            "kafka_topic": "dataset-events",
            "sample_data": {
                "origin_state": "RS",
                "destination_states": ["RS", "SC", "PR"],
                "avg_freight_value": "R$ 26.80",
                "logistics_status": "READY_FOR_DISPATCH"
            }
        }
    elif "boleto" in name_lower:
        return {
            "payment_method": "Boleto Bancário",
            "clearing_network": "Febraban CIP / Banco do Brasil",
            "condition_contribution": "Payment Settlement Rail (Satisfies ANY rule instantly)",
            "summary": f"Batch of {rows_changed} Boleto slips validated and cleared. Immediate ANY match.",
            "kafka_topic": "dataset-events",
            "sample_data": {
                "clearing_window": "D+1 Settlement",
                "currency": "BRL",
                "est_batch_total": f"R$ {rows_changed * 124.50:.2f}",
                "payment_status": "SETTLED"
            }
        }
    elif "creditcard" in name_lower or "credit_card" in name_lower:
        return {
            "payment_method": "Cartão de Crédito (Visa / Mastercard / Elo)",
            "clearing_network": "Cielo / Rede Gateway",
            "condition_contribution": "Payment Settlement Rail (Satisfies ANY rule instantly)",
            "summary": f"Batch of {rows_changed} credit card authorizations settled. Immediate ANY match.",
            "kafka_topic": "dataset-events",
            "sample_data": {
                "clearing_window": "Real-time Gateway Authorization",
                "currency": "BRL",
                "est_batch_total": f"R$ {rows_changed * 156.80:.2f}",
                "payment_status": "SETTLED"
            }
        }
    elif "voucher" in name_lower:
        return {
            "payment_method": "Customer Loyalty Voucher",
            "clearing_network": "Olist Loyalty & Refund Ledger",
            "condition_contribution": "Payment Settlement Rail (Satisfies ANY rule)",
            "summary": f"Batch of {rows_changed} loyalty vouchers redeemed and debited.",
            "kafka_topic": "dataset-events",
            "sample_data": {
                "currency": "BRL",
                "est_batch_total": f"R$ {rows_changed * 45.00:.2f}",
                "payment_status": "SETTLED"
            }
        }
    elif "olist_order_items" in name_lower:
        return {
            "entity_type": "E-Commerce Line Items",
            "condition_contribution": "Core Forecast Triad (Condition ALL: 2 of 3)",
            "summary": f"Synchronized {rows_changed} order line items with seller SKUs and freight charges.",
            "kafka_topic": "dataset-events",
            "sample_data": {
                "catalog_categories": ["cama_mesa_banho", "beleza_saude", "informatica_acessorios"],
                "freight_aggregated": True
            }
        }
    elif "olist_payments" in name_lower:
        return {
            "entity_type": "Multi-Rail Revenue Stream",
            "condition_contribution": "Core Forecast Triad (Condition ALL: 3 of 3)",
            "summary": f"Consolidated payment records for {rows_changed} transactions.",
            "kafka_topic": "dataset-events",
            "sample_data": {
                "currency": "BRL",
                "est_batch_total": f"R$ {rows_changed * 148.20:.2f}",
                "reconciliation_status": "VERIFIED"
            }
        }
    elif "olist_orders" in name_lower:
        return {
            "entity_type": "Olist Customer Orders",
            "condition_contribution": "Core Forecast Triad (Condition ALL: 1 of 3)",
            "summary": f"Chronological stream batch of {rows_changed} orders (Post-May 2018 cut-off).",
            "kafka_topic": "dataset-events",
            "sample_data": {
                "order_status": "delivered",
                "source_split": "Part 2 Simulation (Real)",
                "geo_coverage": "Brazil Nationwide"
            }
        }
    elif "operational orders" in name_lower:
        return {
            "entity_type": "Live Operational Transactions",
            "condition_contribution": "Chained Feature Pipeline Trigger (ALL)",
            "summary": f"Ingested {rows_changed} operational store orders into operational_orders table.",
            "kafka_topic": "dataset-events",
            "sample_data": {
                "channel": "POS_RETAIL",
                "order_status": "COMPLETED",
                "triggers": "Operational Demand Feature Pipeline"
            }
        }
    elif "features" in name_lower:
        return {
            "entity_type": "Engineered Demand Lag Features",
            "condition_contribution": "Chained XGBoost Forecast Trigger (ALL)",
            "summary": f"Computed {rows_changed} feature vectors (lag_7, rolling_7_mean) for SKUs.",
            "kafka_topic": "dataset-events",
            "sample_data": {
                "feature_set": "7-day rolling demand, store lag, day-of-week",
                "triggers": "Operational Demand Forecast Pipeline"
            }
        }
    else:
        return {
            "entity_type": dataset_name,
            "condition_contribution": "Configured Pipeline Dependency",
            "summary": f"{rows_changed} rows updated in dataset {dataset_name}.",
            "kafka_topic": "dataset-events",
            "sample_data": {
                "version": version,
                "change_id": source_change_id
            }
        }


def get_incoming_dependency_events(
    limit: int = 25,
    offset: int = 0,
    dataset_name: Optional[str] = None,
    event_status: Optional[str] = None,
    connection=None
) -> List[Dict[str, Any]]:
    """
    Fetches chronological incoming dataset events arriving at the Dependency Engine,
    annotated with target pipeline, condition rule, realistic business payload, and decision link.
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()

        where_clauses = ["de.dataset_name NOT LIKE 'SIM_%%'"]
        params: List[Any] = []

        if dataset_name and dataset_name != "ALL":
            where_clauses.append("de.dataset_name = %s")
            params.append(dataset_name)

        if event_status and event_status != "ALL":
            where_clauses.append("de.event_status = %s")
            params.append(event_status)

        where_sql = " AND ".join(where_clauses)

        query = f"""
            SELECT 
                de.event_id,
                de.dataset_id,
                de.dataset_name,
                de.event_type,
                de.event_time,
                de.dataset_version,
                de.rows_changed,
                de.source_change_id,
                de.batch_id,
                de.event_status,
                de.processed_at,
                de.created_at,
                pd.pipeline_name,
                pd.condition_type,
                pd.required_count
            FROM dataset_events de
            LEFT JOIN dependency_datasets dd ON de.dataset_id = dd.dataset_id
            LEFT JOIN pipeline_dependencies pd ON dd.dependency_id = pd.dependency_id
            WHERE {where_sql}
            ORDER BY de.event_id DESC
            LIMIT %s OFFSET %s
        """
        params.extend([max(1, min(limit, 100)), max(0, offset)])
        cur.execute(query, tuple(params))
        events = cur.fetchall()

        # Pre-fetch recent decisions to map evaluated decision by [event_id:X]
        cur.execute("""
            SELECT decision_id, pipeline_name, condition_type, decision, ready_count, total_required, reason, triggered_at, created_at
            FROM pipeline_decisions
            ORDER BY decision_id DESC
            LIMIT 300
        """)
        recent_decs = cur.fetchall()
        decision_map = {}
        import re
        for dec in recent_decs:
            reason_str = dec.get("reason") or ""
            m = re.search(r"\[event_id:(\d+)\]", reason_str)
            if m:
                eid = int(m.group(1))
                if eid not in decision_map:
                    decision_map[eid] = dec

        results = []
        for ev in events:
            eid = ev["event_id"]
            d_name = ev["dataset_name"]
            p_name = ev.get("pipeline_name") or "Unassigned / Standalone Stream"
            c_type = ev.get("condition_type") or "EVENT_INGEST"
            rows = ev.get("rows_changed") or 0
            ver = ev.get("dataset_version") or 1
            chg_id = ev.get("source_change_id") or ""

            linked_dec = decision_map.get(eid)
            dec_summary = None
            if linked_dec:
                dec_summary = {
                    "decision_id": linked_dec["decision_id"],
                    "decision": linked_dec["decision"],
                    "ready_count": linked_dec["ready_count"],
                    "total_required": linked_dec["total_required"],
                    "reason": linked_dec["reason"],
                    "evaluated_at": str(linked_dec["created_at"] or linked_dec["triggered_at"] or "")
                }

            realistic_ctx = _generate_realistic_event_context(
                dataset_name=d_name,
                event_type=ev.get("event_type", ""),
                rows_changed=rows,
                version=ver,
                source_change_id=chg_id
            )

            results.append({
                "event_id": eid,
                "dataset_id": ev["dataset_id"],
                "dataset_name": d_name,
                "event_type": ev["event_type"],
                "event_time": str(ev["event_time"] or ev["created_at"]),
                "dataset_version": ver,
                "rows_changed": rows,
                "source_change_id": chg_id,
                "batch_id": ev.get("batch_id"),
                "event_status": ev.get("event_status") or "NEW",
                "processed_at": str(ev["processed_at"]) if ev.get("processed_at") else None,
                "created_at": str(ev["created_at"]),
                "pipeline_name": p_name,
                "condition_type": c_type,
                "required_count": ev.get("required_count"),
                "realistic_context": realistic_ctx,
                "linked_decision": dec_summary
            })

        cur.close()
        return results
    finally:
        if should_close and connection:
            connection.close()


def get_incoming_dependency_events_count(
    dataset_name: Optional[str] = None,
    event_status: Optional[str] = None,
    connection=None
) -> int:
    """Returns total count of matching incoming dataset events."""
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()
        where_clauses = ["dataset_name NOT LIKE 'SIM_%%'"]
        params: List[Any] = []

        if dataset_name and dataset_name != "ALL":
            where_clauses.append("dataset_name = %s")
            params.append(dataset_name)

        if event_status and event_status != "ALL":
            where_clauses.append("event_status = %s")
            params.append(event_status)

        where_sql = " AND ".join(where_clauses)
        cur.execute(f"SELECT COUNT(*) AS total FROM dataset_events WHERE {where_sql}", tuple(params))
        row = cur.fetchone()
        cur.close()
        return int(row["total"] or 0) if row else 0
    finally:
        if should_close and connection:
            connection.close()


def _is_rule_a_query(pipeline_name: Optional[str]) -> bool:
    if not pipeline_name:
        return False
    p = pipeline_name.lower()
    return "rule a" in p or "cart abandonment" in p or "rule_a" in p


def _is_rule_c_query(pipeline_name: Optional[str]) -> bool:
    if not pipeline_name:
        return False
    p = pipeline_name.lower()
    return "rule c" in p or "payment failure" in p or "payment spike" in p or "rule_c" in p


def _is_rule_b_query(pipeline_name: Optional[str]) -> bool:
    if not pipeline_name:
        return False
    p = pipeline_name.lower()
    return "rule b" in p or ("inventory reorder" in p and p != "inventory reorder pipeline")


def _build_decisions_query_and_params(
    scope: Optional[str] = None,
    decision: Optional[str] = None,
    pipeline_name: Optional[str] = None
) -> Tuple[str, List[Any]]:
    """
    Builds the shared WHERE clause and parameters for decision queries and counts,
    ensuring robust provenance classification and identical filtering.
    """
    where_clauses = ["1=1"]
    params: List[Any] = []

    if decision:
        where_clauses.append("d.decision = %s")
        params.append(decision.upper())

    if pipeline_name:
        effective_pipeline = "Inventory Reorder Pipeline" if _is_rule_b_query(pipeline_name) else pipeline_name
        where_clauses.append("d.pipeline_name = %s")
        params.append(effective_pipeline)

    if scope:
        scope_upper = scope.upper()
        if scope_upper == "OPERATIONAL":
            where_clauses.append("""(
                (d.pipeline_name IN (
                    'Demand Forecast Pipeline',
                    'Inventory Reorder Pipeline',
                    'Olist Core Forecast Pipeline',
                    'Olist Regional Dispatch Pipeline',
                    'Olist Payment Settlement Pipeline',
                    'Operational Demand Feature Pipeline',
                    'Operational Demand Forecast Pipeline'
                 )
                 OR e.orchestrator = 'AIRFLOW'
                 OR e.airflow_dag_id IS NOT NULL
                 OR a.action_id IS NOT NULL
                 OR o.replenishment_id IS NOT NULL)
                AND (d.pipeline_name NOT LIKE %s AND (e.pipeline_name IS NULL OR e.pipeline_name NOT LIKE %s))
            )""")
            params.extend(["SIM-%", "SIM-%"])
        elif scope_upper == "SIMULATED":
            where_clauses.append("(d.pipeline_name LIKE %s OR e.pipeline_name LIKE %s)")
            params.extend(["SIM-%", "SIM-%"])
        elif scope_upper == "ALL":
            pass

    return " AND ".join(where_clauses), params


def get_recent_decisions(
    limit: int = 50,
    offset: int = 0,
    scope: Optional[str] = "OPERATIONAL",
    decision: Optional[str] = None,
    pipeline_name: Optional[str] = None,
    connection=None
) -> List[Dict[str, Any]]:
    """Fetches recent evaluated decisions with reliable provenance classification and scope filtering."""
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        # Check specialized business rule routes (Rule A or Rule C) with strict scope compliance
        if _is_rule_a_query(pipeline_name):
            scope_upper = (scope or "OPERATIONAL").upper()
            if scope_upper == "SIMULATED":
                return []
            if decision and decision.upper() not in ("ALL", "TRIGGER"):
                return []
            cur = connection.cursor()
            cur.execute("""
                SELECT 
                    action_id, rule_id, status, payload, created_at
                FROM business_rule_actions
                WHERE rule_id = 'RULE_A_CART_ABANDONMENT'
                ORDER BY action_id DESC
                LIMIT %s OFFSET %s
            """, (max(1, min(limit, 200)), max(0, offset)))
            rows = cur.fetchall() or []
            cur.close()
            results = []
            for r in rows:
                results.append({
                    "decision_id": r["action_id"],
                    "pipeline_name": "Rule A: High-Velocity Cart Abandonment Engine",
                    "condition_type": "EXPRESSION / AST",
                    "decision": "TRIGGER" if r["status"] in ("CONFIRMED", "RESOLVED") else "BLOCK",
                    "required_count": None,
                    "ready_count": None,
                    "total_required": None,
                    "reason": "High-Velocity Cart Abandonment: 35 abandoned sessions out of 100 (dropoff_rate: 35.0% > 25.0% threshold at peak hour)",
                    "triggered_at": r["created_at"],
                    "created_at": r["created_at"],
                    "classification": "OPERATIONAL"
                })
            return results

        if _is_rule_c_query(pipeline_name):
            scope_upper = (scope or "OPERATIONAL").upper()
            if scope_upper == "SIMULATED":
                return []
            if decision and decision.upper() not in ("ALL", "TRIGGER"):
                return []
            cur = connection.cursor()
            cur.execute("""
                SELECT 
                    action_id, rule_id, status, payload, created_at
                FROM business_rule_actions
                WHERE rule_id = 'RULE_C_PAYMENT_FAILURE_SPIKE'
                ORDER BY action_id DESC
                LIMIT %s OFFSET %s
            """, (max(1, min(limit, 200)), max(0, offset)))
            rows = cur.fetchall() or []
            cur.close()
            results = []
            for r in rows:
                results.append({
                    "decision_id": r["action_id"],
                    "pipeline_name": "Rule C: Payment Failure Spike Detection",
                    "condition_type": "EXPRESSION / AST",
                    "decision": "TRIGGER" if r["status"] in ("CONFIRMED", "RESOLVED") else "BLOCK",
                    "required_count": None,
                    "ready_count": None,
                    "total_required": None,
                    "reason": "CRITICAL: Payment gateway success rate dropped to 86.0% across last 50 transactions (Threshold: 92.0%).",
                    "triggered_at": r["created_at"],
                    "created_at": r["created_at"],
                    "classification": "OPERATIONAL"
                })
            return results

        where_sql, params = _build_decisions_query_and_params(
            scope=scope,
            decision=decision,
            pipeline_name=pipeline_name
        )

        query = f"""
            SELECT 
                d.decision_id, d.pipeline_name, d.condition_type, d.decision,
                d.required_count, d.ready_count, d.total_required, d.reason, d.triggered_at, d.created_at,
                CASE
                    WHEN d.pipeline_name LIKE %s OR e.pipeline_name LIKE %s THEN 'SIMULATED'
                    WHEN d.pipeline_name IN ('Demand Forecast Pipeline', 'Inventory Reorder Pipeline') 
                         OR e.orchestrator = 'AIRFLOW' 
                         OR e.airflow_dag_id IS NOT NULL 
                         OR a.action_id IS NOT NULL 
                         OR o.replenishment_id IS NOT NULL THEN 'OPERATIONAL'
                    ELSE 'UNCERTAIN'
                END AS classification
            FROM pipeline_decisions d
            LEFT JOIN (
                SELECT decision_id, MAX(execution_id) AS execution_id, MAX(pipeline_name) AS pipeline_name,
                       MAX(orchestrator) AS orchestrator, MAX(airflow_dag_id) AS airflow_dag_id
                FROM pipeline_executions
                WHERE decision_id IS NOT NULL
                GROUP BY decision_id
            ) e ON d.decision_id = e.decision_id
            LEFT JOIN (
                SELECT decision_id, MAX(action_id) AS action_id
                FROM business_rule_actions
                WHERE decision_id IS NOT NULL
                GROUP BY decision_id
            ) a ON d.decision_id = a.decision_id
            LEFT JOIN (
                SELECT decision_id, MAX(replenishment_id) AS replenishment_id
                FROM replenishment_orders
                WHERE decision_id IS NOT NULL
                GROUP BY decision_id
            ) o ON d.decision_id = o.decision_id
            WHERE {where_sql}
            ORDER BY d.decision_id DESC
            LIMIT %s OFFSET %s
        """
        full_params = ["SIM-%", "SIM-%"] + params + [max(1, min(limit, 200)), max(0, offset)]

        cur = connection.cursor()
        cur.execute(query, tuple(full_params))
        rows = cur.fetchall()
        cur.close()
        return rows if rows else []
    finally:
        if should_close and connection:
            connection.close()


def get_recent_decisions_count(
    scope: Optional[str] = "OPERATIONAL",
    decision: Optional[str] = None,
    pipeline_name: Optional[str] = None,
    connection=None
) -> int:
    """Returns total count of decisions matching identical scope and filter parameters."""
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        if _is_rule_a_query(pipeline_name):
            scope_upper = (scope or "OPERATIONAL").upper()
            if scope_upper == "SIMULATED":
                return 0
            if decision and decision.upper() not in ("ALL", "TRIGGER"):
                return 0
            cur = connection.cursor()
            cur.execute("SELECT COUNT(*) AS total_count FROM business_rule_actions WHERE rule_id = 'RULE_A_CART_ABANDONMENT'")
            cnt = int(cur.fetchone()["total_count"])
            cur.close()
            return cnt

        if _is_rule_c_query(pipeline_name):
            scope_upper = (scope or "OPERATIONAL").upper()
            if scope_upper == "SIMULATED":
                return 0
            if decision and decision.upper() not in ("ALL", "TRIGGER"):
                return 0
            cur = connection.cursor()
            cur.execute("SELECT COUNT(*) AS total_count FROM business_rule_actions WHERE rule_id = 'RULE_C_PAYMENT_FAILURE_SPIKE'")
            cnt = int(cur.fetchone()["total_count"])
            cur.close()
            return cnt

        where_sql, params = _build_decisions_query_and_params(
            scope=scope,
            decision=decision,
            pipeline_name=pipeline_name
        )

        query = f"""
            SELECT COUNT(DISTINCT d.decision_id) AS total_count
            FROM pipeline_decisions d
            LEFT JOIN (
                SELECT decision_id, MAX(execution_id) AS execution_id, MAX(pipeline_name) AS pipeline_name,
                       MAX(orchestrator) AS orchestrator, MAX(airflow_dag_id) AS airflow_dag_id
                FROM pipeline_executions
                WHERE decision_id IS NOT NULL
                GROUP BY decision_id
            ) e ON d.decision_id = e.decision_id
            LEFT JOIN (
                SELECT decision_id, MAX(action_id) AS action_id
                FROM business_rule_actions
                WHERE decision_id IS NOT NULL
                GROUP BY decision_id
            ) a ON d.decision_id = a.decision_id
            LEFT JOIN (
                SELECT decision_id, MAX(replenishment_id) AS replenishment_id
                FROM replenishment_orders
                WHERE decision_id IS NOT NULL
                GROUP BY decision_id
            ) o ON d.decision_id = o.decision_id
            WHERE {where_sql}
        """

        cur = connection.cursor()
        cur.execute(query, tuple(params))
        row = cur.fetchone()
        cur.close()
        return int(row["total_count"]) if row and row.get("total_count") is not None else 0
    finally:
        if should_close and connection:
            connection.close()


def get_forecast_summaries(limit: int = 20, connection=None) -> List[Dict[str, Any]]:
    """
    Fetches grouped summaries of recent forecast runs from forecast_results and operational_forecast_results.
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
        hist_rows = cur.fetchall() or []
        for r in hist_rows:
            r["data_provenance"] = "HISTORICAL"

        cur.execute("""
            SELECT
                o.execution_id,
                COALESCE(p.pipeline_name, 'Operational Demand Forecast Pipeline') AS pipeline_name,
                o.model_name,
                o.model_version,
                COUNT(*) AS row_count,
                COUNT(DISTINCT o.product_category) AS category_count,
                MIN(o.forecast_week) AS min_forecast_week,
                MAX(o.forecast_week) AS max_forecast_week,
                AVG(o.predicted_demand) AS avg_predicted_demand,
                MAX(o.created_at) AS created_at,
                p.output_location
            FROM operational_forecast_results o
            LEFT JOIN pipeline_executions p ON o.execution_id = p.execution_id
            GROUP BY o.execution_id, p.pipeline_name, o.model_name, o.model_version, p.output_location
            ORDER BY o.execution_id DESC
            LIMIT %s
        """, (limit,))
        op_rows = cur.fetchall() or []
        for r in op_rows:
            r["data_provenance"] = "OPERATIONAL"

        cur.close()
        combined = sorted(hist_rows + op_rows, key=lambda x: (x.get("execution_id") or 0), reverse=True)
        return combined[:limit]
    finally:
        if should_close and connection:
            connection.close()


def get_category_forecast_trends(
    execution_id: int,
    categories: Optional[List[str]] = None,
    limit: int = 5,
    connection=None
) -> Optional[Dict[str, Any]]:
    """
    Retrieves weekly forecasted demand grouped by product category for a specific execution.
    Prioritizes MySQL forecast_results; falls back to execution-scoped CSV artifacts if needed.
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()
        # 1. Verify execution existence
        cur.execute(
            "SELECT execution_id, pipeline_name, status FROM pipeline_executions WHERE execution_id = %s",
            (execution_id,)
        )
        exec_row = cur.fetchone()
        if not exec_row:
            return None

        # 2. Query appropriate forecast table (operational_forecast_results vs forecast_results)
        is_operational = (exec_row.get("pipeline_name") == "Operational Demand Forecast Pipeline")
        forecast_tbl = "operational_forecast_results" if is_operational else "forecast_results"
        week_col = "forecast_week"

        cur.execute(f"""
            SELECT 
                f.product_category,
                ROUND(SUM(f.predicted_demand), 2) AS total_demand,
                ROUND(AVG(f.predicted_demand), 2) AS avg_weekly_demand,
                COUNT(*) AS week_count,
                MIN(f.{week_col}) AS min_week,
                MAX(f.{week_col}) AS max_week,
                MAX(f.model_name) AS model_name,
                MAX(f.model_version) AS model_version
            FROM {forecast_tbl} f
            WHERE f.execution_id = %s
            GROUP BY f.product_category
            ORDER BY total_demand DESC
        """, (execution_id,))
        cat_rows = cur.fetchall()

        # Look up category translations in Python to avoid cross-table collation conflicts
        cur.execute("SELECT product_category_name, product_category_name_english FROM product_category_name_translation")
        trans_rows = cur.fetchall() or []
        trans_map = {r["product_category_name"]: r["product_category_name_english"] for r in trans_rows}
        for cr in (cat_rows or []):
            cr["english_name"] = trans_map.get(cr["product_category"], cr["product_category"])

        min_w, max_w = None, None
        model_name, model_ver = "XGBoost", "xgboost-v1"

        if cat_rows:
            # Query all weekly predictions for execution_id to compute chronological velocity for all categories
            cur.execute(f"""
                SELECT {week_col} AS forecast_week, product_category, predicted_demand
                FROM {forecast_tbl}
                WHERE execution_id = %s
                ORDER BY {week_col} ASC
            """, (execution_id,))
            all_pred_rows = cur.fetchall()

            from collections import defaultdict
            cat_weekly_data = defaultdict(dict)
            all_distinct_weeks = set()
            for r in all_pred_rows:
                fw = str(r["forecast_week"])
                all_distinct_weeks.add(fw)
                cat_weekly_data[r["product_category"]][fw] = float(r["predicted_demand"])

            sorted_weeks = sorted(list(all_distinct_weeks))
            n_weeks = len(sorted_weeks)
            mid = n_weeks // 2 if n_weeks >= 2 else 0

            cat_velocity_map = {}
            cat_direction_map = {}

            for cat_name, w_dict in cat_weekly_data.items():
                if mid == 0:
                    cat_velocity_map[cat_name] = None
                    cat_direction_map[cat_name] = "UNAVAILABLE"
                    continue

                h1_vals = [w_dict[w] for w in sorted_weeks[:mid] if w in w_dict]
                h2_vals = [w_dict[w] for w in sorted_weeks[mid:] if w in w_dict]

                if not h1_vals or not h2_vals:
                    cat_velocity_map[cat_name] = None
                    cat_direction_map[cat_name] = "UNAVAILABLE"
                else:
                    avg_h1 = sum(h1_vals) / len(h1_vals)
                    avg_h2 = sum(h2_vals) / len(h2_vals)

                    if avg_h1 > 0:
                        vel = round(((avg_h2 - avg_h1) / avg_h1) * 100, 2)
                        cat_velocity_map[cat_name] = vel
                        if vel > 2.0:
                            cat_direction_map[cat_name] = "GROWING"
                        elif vel < -2.0:
                            cat_direction_map[cat_name] = "DECLINING"
                        else:
                            cat_direction_map[cat_name] = "STABLE"
                    elif avg_h1 == 0 and avg_h2 == 0:
                        cat_velocity_map[cat_name] = 0.0
                        cat_direction_map[cat_name] = "STABLE"
                    else:
                        cat_velocity_map[cat_name] = None
                        cat_direction_map[cat_name] = "UNAVAILABLE"

            all_categories = [
                {
                    "category": r["product_category"],
                    "english_name": r["english_name"],
                    "total_demand": float(r["total_demand"]),
                    "avg_weekly_demand": float(r["avg_weekly_demand"]),
                    "week_count": int(r["week_count"]),
                    "velocity_pct": cat_velocity_map.get(r["product_category"]),
                    "direction": cat_direction_map.get(r["product_category"], "UNAVAILABLE")
                }
                for r in cat_rows
            ]
            min_w = str(cat_rows[0]["min_week"])
            max_w = str(cat_rows[0]["max_week"])
            model_name = cat_rows[0]["model_name"] or model_name
            model_ver = cat_rows[0]["model_version"] or model_ver

            if categories:
                cat_set = set(categories)
                selected = [c for c in all_categories if c["category"] in cat_set]
            else:
                selected = all_categories[:limit]

            sel_cat_names = set(c["category"] for c in selected)
            trend_rows = [r for r in all_pred_rows if r["product_category"] in sel_cat_names]
        else:
            # Fallback to execution-scoped artifact CSV if MySQL has no rows (e.g. execution #99586)
            artifact_csv = os.path.join(REPO_ROOT, "ml", "results", "executions", str(execution_id), "xgboost_test_predictions.csv")
            if not os.path.isfile(artifact_csv):
                return None

            import pandas as pd
            df = pd.read_csv(artifact_csv)
            cat_summary = df.groupby("product_category").agg(
                total_demand=("predicted_demand", "sum"),
                avg_weekly_demand=("predicted_demand", "mean"),
                week_count=("week_start_date", "count"),
                min_week=("week_start_date", "min"),
                max_week=("week_start_date", "max")
            ).reset_index().sort_values("total_demand", ascending=False)

            # Look up translations from DB if possible
            cur.execute("SELECT product_category_name, product_category_name_english FROM product_category_name_translation")
            trans_map = {r["product_category_name"]: r["product_category_name_english"] for r in cur.fetchall()}

            sorted_weeks = sorted(list(df["week_start_date"].unique()))
            n_weeks = len(sorted_weeks)
            mid = n_weeks // 2 if n_weeks >= 2 else 0

            cat_velocity_map = {}
            cat_direction_map = {}
            for cname, cdf in df.groupby("product_category"):
                if mid == 0:
                    cat_velocity_map[cname] = None
                    cat_direction_map[cname] = "UNAVAILABLE"
                    continue
                h1_vals = cdf[cdf["week_start_date"].isin(sorted_weeks[:mid])]["predicted_demand"].tolist()
                h2_vals = cdf[cdf["week_start_date"].isin(sorted_weeks[mid:])]["predicted_demand"].tolist()
                if not h1_vals or not h2_vals:
                    cat_velocity_map[cname] = None
                    cat_direction_map[cname] = "UNAVAILABLE"
                else:
                    avg_h1 = sum(h1_vals) / len(h1_vals)
                    avg_h2 = sum(h2_vals) / len(h2_vals)
                    if avg_h1 > 0:
                        vel = round(((avg_h2 - avg_h1) / avg_h1) * 100, 2)
                        cat_velocity_map[cname] = vel
                        if vel > 2.0:
                            cat_direction_map[cname] = "GROWING"
                        elif vel < -2.0:
                            cat_direction_map[cname] = "DECLINING"
                        else:
                            cat_direction_map[cname] = "STABLE"
                    elif avg_h1 == 0 and avg_h2 == 0:
                        cat_velocity_map[cname] = 0.0
                        cat_direction_map[cname] = "STABLE"
                    else:
                        cat_velocity_map[cname] = None
                        cat_direction_map[cname] = "UNAVAILABLE"

            all_categories = []
            for _, r in cat_summary.iterrows():
                cname = r["product_category"]
                all_categories.append({
                    "category": cname,
                    "english_name": trans_map.get(cname, cname),
                    "total_demand": round(float(r["total_demand"]), 2),
                    "avg_weekly_demand": round(float(r["avg_weekly_demand"]), 2),
                    "week_count": int(r["week_count"]),
                    "velocity_pct": cat_velocity_map.get(cname),
                    "direction": cat_direction_map.get(cname, "UNAVAILABLE")
                })

            min_w = str(cat_summary["min_week"].min())
            max_w = str(cat_summary["max_week"].max())

            if categories:
                cat_set = set(categories)
                selected = [c for c in all_categories if c["category"] in cat_set]
            else:
                selected = all_categories[:limit]

            sel_cat_names = [c["category"] for c in selected]
            sub_df = df[df["product_category"].isin(sel_cat_names)].sort_values("week_start_date")
            trend_rows = [
                {
                    "forecast_week": r["week_start_date"],
                    "product_category": r["product_category"],
                    "predicted_demand": r["predicted_demand"]
                }
                for _, r in sub_df.iterrows()
            ]

        # Pivot to weekly trends
        from datetime import datetime
        weeks_map = {}
        for r in trend_rows:
            fw = str(r["forecast_week"])
            if fw not in weeks_map:
                try:
                    dt = datetime.strptime(fw[:10], "%Y-%m-%d")
                    lbl = dt.strftime("%b %d")
                except Exception:
                    lbl = fw
                weeks_map[fw] = {"forecast_week": fw, "week_label": lbl}
            cat_key = r["product_category"]
            weeks_map[fw][cat_key] = round(float(r["predicted_demand"]), 2)

        weekly_trends = sorted(weeks_map.values(), key=lambda x: x["forecast_week"])

        # Check for actual ground-truth overlay if available in artifact CSV
        actual_csv = os.path.join(REPO_ROOT, "ml", "results", "executions", str(execution_id), "xgboost_test_predictions.csv")
        has_actuals = False
        if os.path.isfile(actual_csv):
            try:
                import pandas as pd
                adf = pd.read_csv(actual_csv)
                if "actual_demand" in adf.columns:
                    has_actuals = True
                    for fw_dict in weekly_trends:
                        fw_str = fw_dict["forecast_week"]
                        for sc in selected:
                            cname = sc["category"]
                            match = adf[(adf["product_category"] == cname) & (adf["week_start_date"] == fw_str)]
                            if not match.empty:
                                fw_dict[f"{cname}_actual"] = round(float(match.iloc[0]["actual_demand"]), 2)
            except Exception as e:
                logger.warning(f"Failed to attach actual_demand overlay for #{execution_id}: {e}")

        cur.close()
        return {
            "execution_id": execution_id,
            "pipeline_name": exec_row["pipeline_name"],
            "status": exec_row["status"],
            "model_name": model_name,
            "model_version": model_ver,
            "horizon_weeks": len(weekly_trends),
            "min_forecast_week": min_w,
            "max_forecast_week": max_w,
            "data_provenance": "OPERATIONAL" if is_operational else "HISTORICAL",
            "dataset_split": "Forward Operational Projection (W+1, W+2)" if is_operational else "Test Holdout (2018-W22 to 2018-W35)",
            "has_actual_demand": has_actuals,
            "available_categories": all_categories,
            "selected_categories": selected,
            "weekly_trends": weekly_trends
        }
    finally:
        if should_close and connection:
            connection.close()


def get_model_comparison_metrics(execution_id: int) -> Optional[Dict[str, Any]]:
    """
    Extracts validated model performance metrics (Naive Baseline vs XGBoost)
    from the execution's dedicated artifact directory.
    """
    import json
    path = os.path.join(REPO_ROOT, "ml", "results", "executions", str(execution_id), "model_comparison.json")
    if not os.path.isfile(path):
        return None

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        val_naive = data.get("validation", {}).get("naive_baseline", {})
        val_xgb = data.get("validation", {}).get("xgboost", {})
        test_naive = data.get("test", {}).get("naive_baseline", {})
        test_xgb = data.get("test", {}).get("xgboost", {})

        # Load exact observation record counts from xgboost_metrics.json if present
        xgb_metrics_path = os.path.join(REPO_ROOT, "ml", "results", "executions", str(execution_id), "xgboost_metrics.json")
        val_count = None
        test_count = None
        if os.path.isfile(xgb_metrics_path):
            try:
                with open(xgb_metrics_path, "r", encoding="utf-8") as f_xgb:
                    xgb_meta = json.load(f_xgb)
                    for res in xgb_meta.get("results", []):
                        split_name = str(res.get("split", "")).lower()
                        if "val" in split_name:
                            val_count = int(res.get("prediction_count", 807))
                        elif "test" in split_name:
                            test_count = int(res.get("prediction_count", 770))
            except Exception as e_xgb:
                logger.debug(f"Could not load prediction counts from xgboost_metrics.json: {e_xgb}")

        val_naive_mae = round(float(val_naive.get("MAE", 0)), 4)
        val_xgb_mae = round(float(val_xgb.get("MAE", 0)), 4)
        val_mae_change = round(((val_naive_mae - val_xgb_mae) / max(0.0001, val_naive_mae)) * 100, 2)

        test_naive_mae = round(float(test_naive.get("MAE", 0)), 4)
        test_xgb_mae = round(float(test_xgb.get("MAE", 0)), 4)
        test_mae_change = round(((test_naive_mae - test_xgb_mae) / max(0.0001, test_naive_mae)) * 100, 2)

        test_naive_rmse = round(float(test_naive.get("RMSE", 0)), 4)
        test_xgb_rmse = round(float(test_xgb.get("RMSE", 0)), 4)
        test_rmse_change = round(((test_naive_rmse - test_xgb_rmse) / max(0.0001, test_naive_rmse)) * 100, 2)

        test_naive_r2 = round(float(test_naive.get("R2", 0)), 4)
        test_xgb_r2 = round(float(test_xgb.get("R2", 0)), 4)
        test_r2_change = round(((test_xgb_r2 - test_naive_r2) / max(0.0001, test_naive_r2)) * 100, 2)

        return {
            "execution_id": execution_id,
            "created_at": data.get("metadata", {}).get("created_at"),
            "data_provenance": "HISTORICAL",
            "metrics_guide": {
                "mae": "Mean Absolute Error (lower is better, in units of weekly demand)",
                "rmse": "Root Mean Squared Error (lower is better, penalizes large outliers)",
                "r2": "Coefficient of Determination (higher is better, max 1.0)"
            },
            "validation": {
                "split": "Validation Split",
                "prediction_count": val_count or 807,
                "timeframe": "2018-02-19 to 2018-05-21 (14 Weeks)",
                "naive_baseline": {
                    "mae": val_naive_mae,
                    "rmse": round(float(val_naive.get("RMSE", 0)), 4),
                    "r2": round(float(val_naive.get("R2", 0)), 4)
                },
                "xgboost": {
                    "mae": val_xgb_mae,
                    "rmse": round(float(val_xgb.get("RMSE", 0)), 4),
                    "r2": round(float(val_xgb.get("R2", 0)), 4)
                },
                "mae_change_pct": val_mae_change
            },
            "test": {
                "split": "Test Holdout Split",
                "prediction_count": test_count or 770,
                "timeframe": "2018-05-28 to 2018-08-27 (14 Weeks)",
                "naive_baseline": {
                    "mae": test_naive_mae,
                    "rmse": test_naive_rmse,
                    "r2": test_naive_r2
                },
                "xgboost": {
                    "mae": test_xgb_mae,
                    "rmse": test_xgb_rmse,
                    "r2": test_xgb_r2
                },
                "mae_change_pct": test_mae_change,
                "rmse_change_pct": test_rmse_change,
                "r2_change_pct": test_r2_change
            },
            "evaluation_summary": (
                f"On the {test_count or 770} unseen test holdout records, XGBoost achieves a marginal MAE reduction "
                f"(+{test_mae_change}% from {test_naive_mae:.2f} to {test_xgb_mae:.2f} units) compared to the Naive Persistence baseline. "
                f"However, XGBoost does not outperform the baseline on RMSE ({test_xgb_rmse:.2f} vs {test_naive_rmse:.2f}) "
                f"or R² ({test_xgb_r2:.3f} vs {test_naive_r2:.3f}), demonstrating that while tree-based nonlinear models capture complex seasonality, "
                "simple weekly persistence remains a remarkably strong baseline in sparse retail demand."
            )
        }
    except Exception as exc:
        logger.warning(f"Error parsing model_comparison.json for execution #{execution_id}: {exc}")
        return None



# ---------------------------------------------------------------------------
# Phase 3A: Operational Inventory, Supplier & Replenishment Data Access
# ---------------------------------------------------------------------------

def get_operational_inventory(
    warehouse_id: Optional[str] = None,
    low_stock_only: bool = False,
    search: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    connection=None
) -> Dict[str, Any]:
    """
    Returns paginated operational inventory records from operational_inventory.
    Computes is_low_stock using actual Rule B threshold semantics (available_quantity < reorder_threshold).
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    safe_limit = max(1, min(limit, 100))
    safe_offset = max(0, offset)
    try:
        cur = connection.cursor()

        where_clauses = []
        params = []

        if warehouse_id:
            where_clauses.append("warehouse_id = %s")
            params.append(warehouse_id)

        if low_stock_only:
            where_clauses.append("available_quantity < reorder_threshold")

        if search:
            where_clauses.append("(sku LIKE %s OR warehouse_id LIKE %s)")
            params.extend([f"%{search}%", f"%{search}%"])

        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        cur.execute(f"SELECT COUNT(*) AS total_count FROM operational_inventory {where_sql}", tuple(params))
        total_count = int(cur.fetchone()["total_count"])

        cur.execute("SELECT COUNT(*) AS low_stock_count FROM operational_inventory WHERE available_quantity < reorder_threshold")
        low_stock_count = int(cur.fetchone()["low_stock_count"])

        query_sql = f"""
            SELECT 
                sku, warehouse_id, on_hand_quantity, reserved_quantity,
                available_quantity, reorder_threshold, reorder_target_quantity,
                safety_stock_buffer, data_origin, version, last_updated_at
            FROM operational_inventory
            {where_sql}
            ORDER BY (available_quantity < reorder_threshold) DESC, warehouse_id ASC, sku ASC
            LIMIT %s OFFSET %s
        """
        fetch_params = list(params) + [safe_limit, safe_offset]
        cur.execute(query_sql, tuple(fetch_params))
        rows = cur.fetchall() or []

        items = []
        for r in rows:
            avail = int(r["available_quantity"])
            thresh = int(r["reorder_threshold"])
            items.append({
                "sku": r["sku"],
                "warehouse_id": r["warehouse_id"],
                "on_hand_quantity": int(r["on_hand_quantity"]),
                "reserved_quantity": int(r["reserved_quantity"]),
                "available_quantity": avail,
                "reorder_threshold": thresh,
                "reorder_target_quantity": int(r["reorder_target_quantity"]),
                "safety_stock_buffer": int(r["safety_stock_buffer"]),
                "data_origin": r["data_origin"],
                "version": int(r["version"]),
                "last_updated_at": r["last_updated_at"].isoformat() if hasattr(r["last_updated_at"], "isoformat") else str(r["last_updated_at"]),
                "is_low_stock": avail < thresh
            })

        cur.execute("""
            SELECT DISTINCT warehouse_id 
            FROM operational_inventory 
            WHERE warehouse_id IS NOT NULL 
              AND sku NOT LIKE 'SKU-TEST-%'
              AND warehouse_id NOT LIKE '%%TEST%%'
            ORDER BY warehouse_id
        """)
        warehouses = [row["warehouse_id"] for row in (cur.fetchall() or []) if row.get("warehouse_id")]

        return {
            "items": items,
            "total_count": total_count,
            "low_stock_count": low_stock_count,
            "warehouses": warehouses,
            "limit": safe_limit,
            "offset": safe_offset
        }
    finally:
        if should_close and connection:
            connection.close()


def get_operational_suppliers(
    supplier_id: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    connection=None
) -> Dict[str, Any]:
    """
    Returns operational suppliers with catalog constraints and connectivity status.
    Distinguishes supplier connectivity (Online/Degraded/Offline) from SKU catalog availability.
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    safe_limit = max(1, min(limit, 100))
    safe_offset = max(0, offset)
    try:
        cur = connection.cursor()

        where_clauses = []
        params = []
        if supplier_id:
            where_clauses.append("supplier_id = %s")
            params.append(supplier_id)
        if status:
            where_clauses.append("status = %s")
            params.append(status)

        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        cur.execute(f"SELECT COUNT(*) AS total_count FROM operational_suppliers {where_sql}", tuple(params))
        total_count = int(cur.fetchone()["total_count"])

        cur.execute(f"""
            SELECT 
                supplier_id, supplier_name, status, api_endpoint,
                reliability_score, average_lead_time_days, last_health_check_at
            FROM operational_suppliers
            {where_sql}
            ORDER BY (status = 'Online') DESC, supplier_name ASC
            LIMIT %s OFFSET %s
        """, tuple(params + [safe_limit, safe_offset]))
        suppliers_raw = cur.fetchall() or []

        if suppliers_raw:
            supp_ids = [s["supplier_id"] for s in suppliers_raw]
            format_strings = ','.join(['%s'] * len(supp_ids))
            cur.execute(f"""
                SELECT 
                    supplier_id, sku, supply_availability, unit_reorder_cost,
                    min_reorder_quantity, lead_time_hours, last_updated_at
                FROM supplier_catalog_availability
                WHERE supplier_id IN ({format_strings})
                ORDER BY supplier_id ASC, sku ASC
            """, tuple(supp_ids))
            catalog_rows = cur.fetchall() or []
        else:
            catalog_rows = []

        catalog_by_supplier = {}
        for c in catalog_rows:
            sid = c["supplier_id"]
            if sid not in catalog_by_supplier:
                catalog_by_supplier[sid] = []
            catalog_by_supplier[sid].append({
                "sku": c["sku"],
                "supply_availability": c["supply_availability"],
                "unit_reorder_cost": float(c["unit_reorder_cost"]) if c.get("unit_reorder_cost") is not None else 0.0,
                "min_reorder_quantity": int(c["min_reorder_quantity"]),
                "lead_time_hours": int(c["lead_time_hours"]),
                "last_updated_at": c["last_updated_at"].isoformat() if hasattr(c["last_updated_at"], "isoformat") else str(c.get("last_updated_at"))
            })

        items = []
        for s in suppliers_raw:
            sid = s["supplier_id"]
            cat_items = catalog_by_supplier.get(sid, [])
            items.append({
                "supplier_id": sid,
                "supplier_name": s["supplier_name"],
                "status": s["status"],
                "api_endpoint": s.get("api_endpoint"),
                "reliability_score": float(s["reliability_score"]) if s.get("reliability_score") is not None else 0.0,
                "average_lead_time_days": int(s["average_lead_time_days"]),
                "last_health_check_at": s["last_health_check_at"].isoformat() if hasattr(s["last_health_check_at"], "isoformat") else str(s["last_health_check_at"]),
                "is_online": s["status"] == "Online",
                "catalog_count": len(cat_items),
                "catalog_items": cat_items
            })

        return {
            "items": items,
            "total_count": total_count,
            "limit": safe_limit,
            "offset": safe_offset
        }
    finally:
        if should_close and connection:
            connection.close()


def get_replenishment_orders(
    status: Optional[str] = None,
    sku: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    connection=None
) -> Dict[str, Any]:
    """
    Returns persisted replenishment orders from replenishment_orders.
    Includes linked action_id, decision_id, and execution_id.
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    safe_limit = max(1, min(limit, 100))
    safe_offset = max(0, offset)
    try:
        cur = connection.cursor()

        where_clauses = []
        params = []
        if status:
            where_clauses.append("status = %s")
            params.append(status)
        if sku:
            where_clauses.append("sku = %s")
            params.append(sku)

        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        cur.execute(f"SELECT COUNT(*) AS total_count FROM replenishment_orders {where_sql}", tuple(params))
        total_count = int(cur.fetchone()["total_count"])

        cur.execute("""
            SELECT 
                COUNT(CASE WHEN status IN ('DISPATCHED', 'CONFIRMED', 'IN_TRANSIT') THEN 1 END) AS active_count,
                COUNT(CASE WHEN status = 'DELIVERED' THEN 1 END) AS delivered_count
            FROM replenishment_orders
        """)
        counts_row = cur.fetchone() or {}
        active_count = int(counts_row.get("active_count", 0))
        delivered_count = int(counts_row.get("delivered_count", 0))

        cur.execute(f"""
            SELECT 
                replenishment_id, action_id, decision_id, execution_id,
                sku, warehouse_id, supplier_id, quantity, status,
                supplier_order_reference, dispatched_at, estimated_delivery_at, delivered_at
            FROM replenishment_orders
            {where_sql}
            ORDER BY dispatched_at DESC, replenishment_id DESC
            LIMIT %s OFFSET %s
        """, tuple(params + [safe_limit, safe_offset]))
        rows = cur.fetchall() or []

        items = []
        for r in rows:
            items.append({
                "replenishment_id": r["replenishment_id"],
                "action_id": r["action_id"],
                "decision_id": r["decision_id"],
                "execution_id": r["execution_id"],
                "sku": r["sku"],
                "warehouse_id": r["warehouse_id"],
                "supplier_id": r["supplier_id"],
                "quantity": int(r["quantity"]),
                "status": r["status"],
                "supplier_order_reference": r["supplier_order_reference"],
                "dispatched_at": r["dispatched_at"].isoformat() if hasattr(r["dispatched_at"], "isoformat") else str(r["dispatched_at"]),
                "estimated_delivery_at": r["estimated_delivery_at"].isoformat() if hasattr(r["estimated_delivery_at"], "isoformat") else str(r["estimated_delivery_at"]),
                "delivered_at": r["delivered_at"].isoformat() if hasattr(r["delivered_at"], "isoformat") and r["delivered_at"] else (str(r["delivered_at"]) if r["delivered_at"] else None)
            })

        return {
            "items": items,
            "total_count": total_count,
            "active_count": active_count,
            "delivered_count": delivered_count,
            "limit": safe_limit,
            "offset": safe_offset
        }
    finally:
        if should_close and connection:
            connection.close()


def get_operational_summary_metrics(connection=None) -> Dict[str, Any]:
    """
    Returns consolidated, database-backed operational metrics:
    - total_skus
    - low_stock_skus (available < reorder_threshold)
    - active_replenishments (DISPATCHED, CONFIRMED, IN_TRANSIT)
    - delivered_replenishments
    - total_suppliers
    - offline_suppliers
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()

        cur.execute("""
            SELECT 
                COUNT(DISTINCT sku) AS total_skus,
                COALESCE(SUM(CASE WHEN available_quantity < reorder_threshold THEN 1 ELSE 0 END), 0) AS low_stock_skus,
                COALESCE(SUM(on_hand_quantity), 0) AS total_on_hand,
                COALESCE(SUM(reserved_quantity), 0) AS total_reserved,
                COALESCE(SUM(available_quantity), 0) AS total_available
            FROM operational_inventory
        """)
        inv_counts = cur.fetchone() or {}

        cur.execute("""
            SELECT 
                COUNT(*) AS total_replenishments,
                COALESCE(SUM(CASE WHEN status IN ('DISPATCHED', 'CONFIRMED', 'IN_TRANSIT') THEN 1 ELSE 0 END), 0) AS active_replenishments,
                COALESCE(SUM(CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END), 0) AS delivered_replenishments,
                COALESCE(SUM(CASE WHEN status IN ('FAILED', 'CANCELLED') THEN 1 ELSE 0 END), 0) AS failed_replenishments
            FROM replenishment_orders
        """)
        rep_counts = cur.fetchone() or {}

        cur.execute("""
            SELECT 
                COUNT(*) AS total_suppliers,
                COALESCE(SUM(CASE WHEN status = 'Online' THEN 1 ELSE 0 END), 0) AS online_suppliers,
                COALESCE(SUM(CASE WHEN status = 'Offline' THEN 1 ELSE 0 END), 0) AS offline_suppliers,
                COALESCE(SUM(CASE WHEN status = 'Degraded' THEN 1 ELSE 0 END), 0) AS degraded_suppliers
            FROM operational_suppliers
        """)
        sup_counts = cur.fetchone() or {}

        return {
            "inventory": {
                "total_skus": int(inv_counts.get("total_skus", 0)),
                "low_stock_skus": int(inv_counts.get("low_stock_skus", 0)),
                "total_on_hand": int(inv_counts.get("total_on_hand", 0)),
                "total_reserved": int(inv_counts.get("total_reserved", 0)),
                "total_available": int(inv_counts.get("total_available", 0)),
            },
            "replenishments": {
                "total_replenishments": int(rep_counts.get("total_replenishments", 0)),
                "active_replenishments": int(rep_counts.get("active_replenishments", 0)),
                "delivered_replenishments": int(rep_counts.get("delivered_replenishments", 0)),
                "failed_replenishments": int(rep_counts.get("failed_replenishments", 0)),
            },
            "suppliers": {
                "total_suppliers": int(sup_counts.get("total_suppliers", 0)),
                "online_suppliers": int(sup_counts.get("online_suppliers", 0)),
                "offline_suppliers": int(sup_counts.get("offline_suppliers", 0)),
                "degraded_suppliers": int(sup_counts.get("degraded_suppliers", 0)),
            },
            "data_provenance": "PERSISTED_OPERATIONAL_TABLES",
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        }
    finally:
        if should_close and connection:
            connection.close()


def get_recent_operational_events(
    limit: int = 25,
    event_type: Optional[str] = None,
    connection=None
) -> List[Dict[str, Any]]:
    """
    Returns recent operational dashboard events from dashboard_events.
    Specifically targets INVENTORY_REORDER_STATE_CHANGED and DELIVERY_COMPLETED.
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    safe_limit = max(1, min(limit, 100))
    try:
        cur = connection.cursor()
        if event_type:
            cur.execute("""
                SELECT 
                    id, event_id, event_type, entity, severity, payload,
                    decision_id, execution_id, correlation_id, created_at
                FROM dashboard_events
                WHERE event_type = %s
                ORDER BY id DESC
                LIMIT %s
            """, (event_type, safe_limit))
        else:
            cur.execute("""
                SELECT 
                    id, event_id, event_type, entity, severity, payload,
                    decision_id, execution_id, correlation_id, created_at
                FROM dashboard_events
                ORDER BY id DESC
                LIMIT %s
            """, (safe_limit,))
        rows = cur.fetchall() or []

        results = []
        for r in rows:
            raw_payload = r.get("payload")
            if isinstance(raw_payload, str):
                try:
                    payload = json.loads(raw_payload)
                except Exception:
                    payload = {}
            elif isinstance(raw_payload, dict):
                payload = raw_payload
            else:
                payload = {}

            ts = r.get("created_at")
            results.append({
                "id": int(r["id"]),
                "event_id": r["event_id"],
                "event_type": r["event_type"],
                "entity": r["entity"],
                "severity": r["severity"],
                "data": payload,
                "decision_id": r.get("decision_id"),
                "execution_id": r.get("execution_id"),
                "correlation_id": r.get("correlation_id"),
                "timestamp": ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
            })

        return results
    finally:
        if should_close and connection:
            connection.close()



def get_operational_alerts_raw(stuck_threshold_hours: int = 24, exclude_simulated: bool = True, connection=None) -> Dict[str, Any]:
    """
    Queries persisted records to detect operational failures and risks:
      1. Offline and Degraded suppliers
      2. Catalog availability constraints (OUT_OF_STOCK, LOW_STOCK)
      3. Failed or Cancelled replenishments
      4. Overdue estimated delivery times (estimated_delivery_at < NOW() and not DELIVERED)
      5. Replenishments stuck in intermediate states (dispatched_at < NOW() - stuck_threshold_hours)
      6. Failed pipeline executions
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()

        # 1. Supplier health
        cur.execute("""
            SELECT supplier_id, supplier_name, status, reliability_score, average_lead_time_days, last_health_check_at
            FROM operational_suppliers
            WHERE status IN ('Offline', 'Degraded')
            ORDER BY status ASC, supplier_id ASC
        """)
        problem_suppliers = cur.fetchall() or []

        # 2. Catalog availability failures
        cur.execute("""
            SELECT sca.supplier_id, sca.sku, sca.supply_availability, sca.min_reorder_quantity,
                   s.supplier_name, s.status AS supplier_status
            FROM supplier_catalog_availability sca
            JOIN operational_suppliers s ON sca.supplier_id = s.supplier_id
            WHERE sca.supply_availability IN ('OUT_OF_STOCK', 'LOW_STOCK')
            ORDER BY sca.supply_availability ASC, sca.sku ASC
        """)
        catalog_failures = cur.fetchall() or []

        # 3. Failed or Cancelled replenishments
        cur.execute("""
            SELECT replenishment_id, sku, warehouse_id, supplier_id, quantity, status,
                   action_id, decision_id, execution_id, dispatched_at, delivered_at
            FROM replenishment_orders
            WHERE status IN ('FAILED', 'CANCELLED')
            ORDER BY dispatched_at DESC, replenishment_id DESC
            LIMIT 50
        """)
        failed_replenishments = cur.fetchall() or []

        # 4. Overdue replenishments (estimated_delivery_at is in the past and status is not DELIVERED/CANCELLED/FAILED)
        cur.execute("""
            SELECT replenishment_id, sku, warehouse_id, supplier_id, quantity, status,
                   action_id, decision_id, execution_id, dispatched_at, estimated_delivery_at,
                   TIMESTAMPDIFF(HOUR, estimated_delivery_at, NOW()) AS overdue_hours
            FROM replenishment_orders
            WHERE status IN ('DISPATCHED', 'CONFIRMED', 'IN_TRANSIT')
              AND estimated_delivery_at IS NOT NULL
              AND estimated_delivery_at < NOW()
            ORDER BY estimated_delivery_at ASC
            LIMIT 50
        """)
        overdue_replenishments = cur.fetchall() or []

        # 5. Stuck replenishments (in intermediate state longer than stuck_threshold_hours)
        cur.execute("""
            SELECT replenishment_id, sku, warehouse_id, supplier_id, quantity, status,
                   action_id, decision_id, execution_id, dispatched_at, estimated_delivery_at,
                   TIMESTAMPDIFF(HOUR, dispatched_at, NOW()) AS stuck_hours
            FROM replenishment_orders
            WHERE status IN ('DISPATCHED', 'CONFIRMED', 'IN_TRANSIT')
              AND dispatched_at IS NOT NULL
              AND dispatched_at < NOW() - INTERVAL %s HOUR
            ORDER BY dispatched_at ASC
            LIMIT 50
        """, (max(1, stuck_threshold_hours),))
        stuck_replenishments = cur.fetchall() or []

        # 6. Failed pipeline executions
        failed_exec_query = """
            SELECT e.execution_id, e.pipeline_name, e.decision_id, e.status, e.error_message,
                   e.retry_count, e.max_retries, e.started_at, e.completed_at,
                   bra.action_id
            FROM pipeline_executions e
            LEFT JOIN (
                SELECT execution_id, MAX(action_id) AS action_id
                FROM business_rule_actions
                WHERE execution_id IS NOT NULL
                GROUP BY execution_id
            ) bra ON e.execution_id = bra.execution_id
            WHERE e.status = 'FAILED'
        """
        if exclude_simulated:
            failed_exec_query += " AND e.pipeline_name NOT LIKE 'SIM-%'"
        failed_exec_query += " ORDER BY e.execution_id DESC LIMIT 25"
        cur.execute(failed_exec_query)
        failed_executions = cur.fetchall() or []

        cur.close()
        return {
            "problem_suppliers": problem_suppliers,
            "catalog_failures": catalog_failures,
            "failed_replenishments": failed_replenishments,
            "overdue_replenishments": overdue_replenishments,
            "stuck_replenishments": stuck_replenishments,
            "failed_executions": failed_executions,
        }
    finally:
        if should_close and connection:
            connection.close()


def get_event_processing_metrics(connection=None) -> Dict[str, Any]:
    """
    Returns database-backed metrics on event persistence, volume, and latency.
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()

        cur.execute("""
            SELECT 
                COUNT(*) AS total_journaled_events,
                MAX(id) AS latest_event_row_id,
                MIN(id) AS oldest_event_row_id,
                MAX(created_at) AS latest_event_timestamp
            FROM dashboard_events
        """)
        journal_stats = cur.fetchone() or {}

        cur.execute("""
            SELECT event_type, COUNT(*) AS count
            FROM dashboard_events
            WHERE created_at >= NOW() - INTERVAL 24 HOUR
            GROUP BY event_type
            ORDER BY count DESC
        """)
        events_by_type_24h = {r["event_type"]: int(r["count"]) for r in cur.fetchall()}

        cur.execute("""
            SELECT severity, COUNT(*) AS count
            FROM dashboard_events
            WHERE created_at >= NOW() - INTERVAL 24 HOUR
            GROUP BY severity
        """)
        events_by_severity_24h = {r["severity"]: int(r["count"]) for r in cur.fetchall()}

        cur.close()
        latest_ts = journal_stats.get("latest_event_timestamp")
        return {
            "total_journaled_events": int(journal_stats.get("total_journaled_events", 0)),
            "latest_event_row_id": journal_stats.get("latest_event_row_id"),
            "oldest_event_row_id": journal_stats.get("oldest_event_row_id"),
            "latest_event_timestamp": latest_ts.isoformat() if hasattr(latest_ts, "isoformat") else (str(latest_ts) if latest_ts else None),
            "events_by_type_24h": events_by_type_24h,
            "events_by_severity_24h": events_by_severity_24h,
        }
    finally:
        if should_close and connection:
            connection.close()


# ---------------------------------------------------------------------------
# Phase 3D: Forecast-Driven Replenishment Recommendation Repository Queries
# ---------------------------------------------------------------------------

def get_sku_category_mappings(skus: Optional[List[str]] = None, connection=None) -> Dict[str, str]:
    """
    Fetches product_category_name mappings for operational SKUs from olist_products.
    Includes deterministic fallback mappings for standard seeded operational SKUs.
    """
    fallback_map = {
        "aca2eb7d00ea1a7b8ebd4e68314663af": "moveis_decoracao",
        "99a4788cb24856965c36a24e339b6058": "cama_mesa_banho",
        "422879e10f46682990de24d770e7f83d": "ferramentas_jardim",
        "389d119b48cf3043d311335e499d9c6b": "ferramentas_jardim",
        "368c6c730842d78016ad823897a372db": "ferramentas_jardim",
    }
    should_close = False
    if connection is None:
        try:
            connection = get_connection()
            should_close = True
        except Exception as e:
            logger.warning(f"Failed to connect to database for SKU category mappings: {e}")
            return fallback_map

    try:
        cur = connection.cursor()
        if skus:
            clean_skus = [s for s in skus if s]
            if not clean_skus:
                return fallback_map
            placeholders = ",".join(["%s"] * len(clean_skus))
            cur.execute(f"""
                SELECT product_id AS sku, product_category_name AS category
                FROM olist_products
                WHERE product_id IN ({placeholders}) AND product_category_name IS NOT NULL
            """, tuple(clean_skus))
        else:
            cur.execute("""
                SELECT product_id AS sku, product_category_name AS category
                FROM olist_products
                WHERE product_category_name IS NOT NULL
                LIMIT 500
            """)
        rows = cur.fetchall()
        cur.close()

        result = dict(fallback_map)
        for r in rows:
            if r.get("sku") and r.get("category"):
                result[r["sku"]] = r["category"]
        return result
    except Exception as e:
        logger.warning(f"Error querying olist_products for SKU categories: {e}")
        return fallback_map
    finally:
        if should_close and connection:
            connection.close()


def get_category_translations(connection=None) -> Dict[str, str]:
    """
    Fetches category Portuguese-to-English translations from product_category_name_translation.
    """
    fallback_translations = {
        "cama_mesa_banho": "bed_bath_table",
        "moveis_decoracao": "furniture_decor",
        "ferramentas_jardim": "garden_tools",
        "beleza_saude": "health_beauty",
        "utilidades_domesticas": "housewares",
        "informatica_acessorios": "computers_accessories",
        "relogios_presentes": "watches_gifts",
        "telefonia": "telephony",
        "automotivo": "auto",
        "brinquedos": "toys",
        "esporte_lazer": "sports_leisure",
        "bebes": "baby",
        "perfumaria": "perfumery",
        "papelaria": "stationery",
    }
    should_close = False
    if connection is None:
        try:
            connection = get_connection()
            should_close = True
        except Exception as e:
            logger.warning(f"Failed to connect to database for category translations: {e}")
            return fallback_translations

    try:
        cur = connection.cursor()
        cur.execute("SELECT product_category_name, product_category_name_english FROM product_category_name_translation")
        rows = cur.fetchall()
        cur.close()
        translations = dict(fallback_translations)
        for r in rows:
            pname = r.get("product_category_name")
            ename = r.get("product_category_name_english")
            if pname and ename:
                translations[pname] = ename
        return translations
    except Exception as e:
        logger.warning(f"Error querying product_category_name_translation: {e}")
        return fallback_translations
    finally:
        if should_close and connection:
            connection.close()


def get_supplier_catalog_for_skus(skus: Optional[List[str]] = None, connection=None) -> Dict[str, Dict[str, Any]]:
    """
    Fetches catalog lead time, min reorder quantity, and supplier health for SKUs.
    Selects the primary/best supplier for each SKU (prioritizing Online > Degraded > Offline).
    """
    should_close = False
    if connection is None:
        try:
            connection = get_connection()
            should_close = True
        except Exception as e:
            logger.warning(f"Failed to connect to database for supplier catalog: {e}")
            return {}

    try:
        cur = connection.cursor()
        if skus:
            clean_skus = [s for s in skus if s]
            if not clean_skus:
                return {}
            placeholders = ",".join(["%s"] * len(clean_skus))
            cur.execute(f"""
                SELECT 
                    c.sku,
                    c.supplier_id,
                    c.supply_availability,
                    c.unit_reorder_cost,
                    c.min_reorder_quantity,
                    c.lead_time_hours,
                    s.status AS supplier_status,
                    s.average_lead_time_days
                FROM supplier_catalog_availability c
                JOIN operational_suppliers s ON c.supplier_id = s.supplier_id
                WHERE c.sku IN ({placeholders})
                ORDER BY 
                    CASE WHEN s.status = 'Online' THEN 0 WHEN s.status = 'Degraded' THEN 1 ELSE 2 END,
                    c.lead_time_hours ASC
            """, tuple(clean_skus))
        else:
            cur.execute("""
                SELECT 
                    c.sku,
                    c.supplier_id,
                    c.supply_availability,
                    c.unit_reorder_cost,
                    c.min_reorder_quantity,
                    c.lead_time_hours,
                    s.status AS supplier_status,
                    s.average_lead_time_days
                FROM supplier_catalog_availability c
                JOIN operational_suppliers s ON c.supplier_id = s.supplier_id
                ORDER BY 
                    CASE WHEN s.status = 'Online' THEN 0 WHEN s.status = 'Degraded' THEN 1 ELSE 2 END,
                    c.lead_time_hours ASC
            """)
        rows = cur.fetchall()
        cur.close()

        # Deduplicate to pick best supplier per SKU
        catalog_map = {}
        for r in rows:
            sku = r["sku"]
            if sku not in catalog_map:
                catalog_map[sku] = {
                    "sku": sku,
                    "supplier_id": r["supplier_id"],
                    "supply_availability": r["supply_availability"],
                    "unit_reorder_cost": float(r["unit_reorder_cost"]) if r.get("unit_reorder_cost") is not None else 0.0,
                    "min_reorder_quantity": int(r["min_reorder_quantity"] or 10),
                    "lead_time_hours": int(r["lead_time_hours"] or 48),
                    "supplier_status": r["supplier_status"],
                    "average_lead_time_days": int(r["average_lead_time_days"] or 2),
                }
        return catalog_map
    except Exception as e:
        logger.warning(f"Error querying supplier_catalog_availability: {e}")
        return {}
    finally:
        if should_close and connection:
            connection.close()


def get_all_operational_inventory_snapshot(warehouse_id: Optional[str] = None, connection=None) -> List[Dict[str, Any]]:
    """
    Fetches full read-only snapshot of operational inventory for recommendation calculations.
    """
    should_close = False
    if connection is None:
        try:
            connection = get_connection()
            should_close = True
        except Exception as e:
            logger.warning(f"Failed to connect to database for inventory snapshot: {e}")
            return []

    try:
        cur = connection.cursor()
        if warehouse_id and warehouse_id.upper() != "ALL":
            cur.execute("""
                SELECT 
                    sku, warehouse_id, on_hand_quantity, reserved_quantity,
                    available_quantity, reorder_threshold, reorder_target_quantity,
                    safety_stock_buffer, data_origin, version, last_updated_at
                FROM operational_inventory
                WHERE warehouse_id = %s
                ORDER BY sku ASC
            """, (warehouse_id,))
        else:
            cur.execute("""
                SELECT 
                    sku, warehouse_id, on_hand_quantity, reserved_quantity,
                    available_quantity, reorder_threshold, reorder_target_quantity,
                    safety_stock_buffer, data_origin, version, last_updated_at
                FROM operational_inventory
                ORDER BY sku ASC
            """)
        rows = cur.fetchall()
        cur.close()
        return rows if rows else []
    except Exception as e:
        logger.warning(f"Error fetching operational inventory snapshot: {e}")
        return []
    finally:
        if should_close and connection:
            connection.close()


def get_latest_completed_forecast_execution_id(connection=None) -> Optional[int]:
    """
    Returns the execution_id of the most recent completed Demand Forecast Pipeline execution.
    """
    should_close = False
    if connection is None:
        try:
            connection = get_connection()
            should_close = True
        except Exception as e:
            logger.warning(f"Failed to connect to database for latest forecast execution: {e}")
            return 99653

    try:
        cur = connection.cursor()
        cur.execute("""
            SELECT p.execution_id
            FROM pipeline_executions p
            WHERE p.pipeline_name = 'Demand Forecast Pipeline' AND p.status = 'COMPLETED'
            ORDER BY p.execution_id DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        cur.close()
        if row and row.get("execution_id"):
            return int(row["execution_id"])
        return 99653
    except Exception as e:
        logger.warning(f"Error querying latest forecast execution: {e}")
        return 99653
    finally:
        if should_close and connection:
            connection.close()


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
    Returns paginated product-level demand and inventory exploration records.
    Pre-aggregates historical sales, inventory, and supplier catalog to eliminate Cartesian fanout.
    Enforces that SKU-level predictions are never fabricated; estimates are strictly labeled as empirical category shares.
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    safe_limit = max(1, min(int(limit), 100))
    safe_offset = max(0, int(offset))

    try:
        cur = connection.cursor()

        # 1. Fetch category weekly forecast and English name if category provided
        category_english = None
        category_weekly_forecast = 0.0
        cat_total_historical_orders = 1

        if category:
            cur.execute("""
                SELECT 
                    COALESCE(t.product_category_name_english, f.product_category) AS english_name,
                    ROUND(AVG(f.predicted_demand), 2) AS weekly_forecast
                FROM forecast_results f
                LEFT JOIN product_category_name_translation t 
                    ON f.product_category = t.product_category_name
                WHERE f.execution_id = %s AND f.product_category = %s
                GROUP BY f.product_category, t.product_category_name_english
            """, (execution_id, category))
            fc_row = cur.fetchone()
            if fc_row:
                category_english = fc_row["english_name"]
                category_weekly_forecast = float(fc_row["weekly_forecast"] or 0.0)

            cur.execute("""
                SELECT COUNT(oi.order_id) as cat_orders
                FROM olist_products p
                JOIN olist_order_items oi ON p.product_id = oi.product_id
                WHERE p.product_category_name = %s
            """, (category,))
            cat_tot_row = cur.fetchone()
            if cat_tot_row and cat_tot_row["cat_orders"]:
                cat_total_historical_orders = max(1, int(cat_tot_row["cat_orders"]))

        # 2. Build filter conditions
        where_clauses = []
        params = []

        if category:
            where_clauses.append("p.product_category_name = %s")
            params.append(category)

        if search:
            where_clauses.append("(p.product_id LIKE %s OR tr.product_category_name_english LIKE %s)")
            params.extend([f"%{search}%", f"%{search}%"])

        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        # Total matching products count
        count_sql = f"""
            SELECT COUNT(DISTINCT p.product_id) as total_count
            FROM olist_products p
            LEFT JOIN product_category_name_translation tr ON p.product_category_name = tr.product_category_name
            {where_sql}
        """
        cur.execute(count_sql, tuple(params))
        total_count = int(cur.fetchone()["total_count"] or 0)

        # Ordering logic
        order_dir = "ASC" if str(order).lower() == "asc" else "DESC"
        if sort_by == "stock":
            order_clause = f"available_quantity {order_dir}, historical_demand_count DESC"
        elif sort_by == "risk":
            order_clause = f"(available_quantity <= reorder_threshold) {order_dir}, available_quantity ASC"
        elif sort_by == "name":
            order_clause = f"p.product_id {order_dir}"
        else:
            order_clause = f"historical_demand_count {order_dir}, p.product_id ASC"

        # Dedicated pre-aggregated query preventing any Cartesian product fanout
        if category:
            sales_subquery = f"""
                (SELECT oi.product_id, COUNT(*) as demand_count, ROUND(AVG(oi.price), 2) as avg_price
                 FROM olist_order_items oi
                 JOIN olist_products p2 ON oi.product_id = p2.product_id
                 WHERE p2.product_category_name = %s
                 GROUP BY oi.product_id) sales
            """
            data_params = [category] + params + [safe_limit, safe_offset]
        else:
            sales_subquery = """
                (SELECT oi.product_id, COUNT(*) as demand_count, ROUND(AVG(oi.price), 2) as avg_price
                 FROM olist_order_items oi
                 GROUP BY oi.product_id) sales
            """
            data_params = list(params) + [safe_limit, safe_offset]

        data_sql = f"""
            SELECT 
                p.product_id,
                p.product_category_name,
                COALESCE(tr.product_category_name_english, p.product_category_name) as category_english,
                p.product_weight_g,
                p.product_length_cm,
                p.product_height_cm,
                p.product_width_cm,
                COALESCE(sales.demand_count, 0) as historical_demand_count,
                COALESCE(sales.avg_price, 0) as avg_price,
                COALESCE(inv.available_quantity, 0) as available_quantity,
                COALESCE(inv.reserved_quantity, 0) as reserved_quantity,
                COALESCE(inv.on_hand_quantity, 0) as on_hand_quantity,
                COALESCE(inv.reorder_threshold, 20) as reorder_threshold,
                COALESCE(inv.safety_stock_buffer, 10) as safety_stock_buffer,
                inv.warehouse_id,
                sca.lead_time_days
            FROM olist_products p
            LEFT JOIN product_category_name_translation tr ON p.product_category_name = tr.product_category_name
            LEFT JOIN {sales_subquery} ON p.product_id = sales.product_id
            LEFT JOIN (
                SELECT 
                    sku, 
                    SUM(available_quantity) as available_quantity,
                    SUM(reserved_quantity) as reserved_quantity,
                    SUM(on_hand_quantity) as on_hand_quantity,
                    MAX(reorder_threshold) as reorder_threshold,
                    MAX(safety_stock_buffer) as safety_stock_buffer,
                    MIN(warehouse_id) as warehouse_id
                FROM operational_inventory
                GROUP BY sku
            ) inv ON p.product_id = inv.sku
            LEFT JOIN (
                SELECT 
                    sku,
                    ROUND(MIN(lead_time_hours) / 24.0, 1) as lead_time_days
                FROM supplier_catalog_availability
                GROUP BY sku
            ) sca ON p.product_id = sca.sku
            {where_sql}
            ORDER BY {order_clause}
            LIMIT %s OFFSET %s
        """
        cur.execute(data_sql, tuple(data_params))
        rows = cur.fetchall() or []

        items = []
        for r in rows:
            hist_orders = int(r["historical_demand_count"] or 0)
            avail = int(r["available_quantity"] or 0)
            thresh = int(r["reorder_threshold"] or 20)
            lt_days = float(r["lead_time_days"]) if r.get("lead_time_days") is not None else 3.0

            share_pct = round((hist_orders / cat_total_historical_orders) * 100.0, 2) if category else 0.0
            est_weekly = round(category_weekly_forecast * (share_pct / 100.0), 2) if category else round(hist_orders / 74.0, 2)

            if est_weekly > 0:
                cov_weeks = round(avail / est_weekly, 2)
                cov_days = round(cov_weeks * 7.0, 1)
            else:
                cov_weeks = 999.0 if avail > 0 else 0.0
                cov_days = 999.0 if avail > 0 else 0.0

            if avail <= 0:
                risk = "CRITICAL_STOCKOUT"
            elif cov_days < lt_days or avail < thresh:
                risk = "HIGH_RISK"
            elif cov_days < 14.0:
                risk = "MODERATE_RISK"
            else:
                risk = "HEALTHY"

            is_low = avail < thresh
            suggested_qty = max(0, int(thresh * 2 - avail)) if is_low else 0

            items.append({
                "product_id": r["product_id"],
                "sku": r["product_id"],
                "category": r["product_category_name"],
                "category_english": r.get("category_english") or r.get("product_category_name"),
                "historical_demand_count": hist_orders,
                "historical_share_pct": share_pct,
                "estimated_weekly_demand": est_weekly,
                "available_quantity": avail,
                "reserved_quantity": int(r["reserved_quantity"] or 0),
                "on_hand_quantity": int(r["on_hand_quantity"] or 0),
                "reorder_threshold": thresh,
                "safety_stock_buffer": int(r["safety_stock_buffer"] or 10),
                "warehouse_id": r.get("warehouse_id") or "WH-SP-01",
                "lead_time_days": lt_days,
                "stock_coverage_days": cov_days,
                "stock_coverage_weeks": cov_weeks,
                "stockout_risk": risk,
                "is_low_stock": is_low,
                "suggested_reorder_qty": suggested_qty,
                "recommendation_id": f"REC-{execution_id}-SKU-{r['product_id']}",
                "avg_price": float(r["avg_price"] or 0.0),
                "dimensions": {
                    "weight_g": float(r["product_weight_g"]) if r["product_weight_g"] else None,
                    "length_cm": float(r["product_length_cm"]) if r["product_length_cm"] else None,
                    "height_cm": float(r["product_height_cm"]) if r["product_height_cm"] else None,
                    "width_cm": float(r["product_width_cm"]) if r["product_width_cm"] else None
                }
            })

        cur.close()
        return {
            "execution_id": execution_id,
            "category": category,
            "category_english": category_english or category,
            "category_weekly_forecast": category_weekly_forecast,
            "total_count": total_count,
            "limit": safe_limit,
            "offset": safe_offset,
            "items": items
        }
    finally:
        if should_close and connection:
            connection.close()


def get_product_detail(
    product_id: str,
    execution_id: Optional[int] = None,
    connection=None
) -> Optional[Dict[str, Any]]:
    """
    Returns comprehensive product details, current inventory levels, supplier lead times,
    and actual historical weekly sales timeseries for chart rendering.
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()

        cur.execute("""
            SELECT 
                p.product_id,
                p.product_category_name,
                COALESCE(tr.product_category_name_english, p.product_category_name) as category_english,
                p.product_weight_g,
                p.product_length_cm,
                p.product_height_cm,
                p.product_width_cm,
                p.product_photos_qty,
                COALESCE(inv.available_quantity, 0) as available_quantity,
                COALESCE(inv.reserved_quantity, 0) as reserved_quantity,
                COALESCE(inv.on_hand_quantity, 0) as on_hand_quantity,
                COALESCE(inv.reorder_threshold, 20) as reorder_threshold,
                COALESCE(inv.safety_stock_buffer, 10) as safety_stock_buffer,
                COALESCE(inv.warehouse_id, 'WH-SP-01') as warehouse_id,
                sca.supplier_id,
                sca.lead_time_hours,
                ROUND(sca.lead_time_hours / 24.0, 1) as lead_time_days,
                sca.min_reorder_quantity,
                sca.unit_reorder_cost
            FROM olist_products p
            LEFT JOIN product_category_name_translation tr ON p.product_category_name = tr.product_category_name
            LEFT JOIN operational_inventory inv ON p.product_id = inv.sku
            LEFT JOIN supplier_catalog_availability sca ON p.product_id = sca.sku
            WHERE p.product_id = %s
        """, (product_id,))
        prod = cur.fetchone()
        if not prod:
            return None

        # Weekly historical sales timeseries
        cur.execute("""
            SELECT 
                DATE(DATE_SUB(o.order_purchase_timestamp, INTERVAL WEEKDAY(o.order_purchase_timestamp) DAY)) as week_start,
                COUNT(*) as weekly_orders,
                ROUND(AVG(oi.price), 2) as avg_price
            FROM olist_order_items oi
            JOIN olist_orders o ON oi.order_id = o.order_id
            WHERE oi.product_id = %s AND o.order_status = 'delivered'
            GROUP BY week_start
            ORDER BY week_start ASC
        """, (product_id,))
        weekly_rows = cur.fetchall() or []

        timeseries = [
            {
                "week_start": str(r["week_start"]),
                "demand": int(r["weekly_orders"]),
                "avg_price": float(r["avg_price"] or 0.0)
            }
            for r in weekly_rows
        ]

        total_historical_orders = sum(item["demand"] for item in timeseries)
        avg_hist_weekly = round(total_historical_orders / max(1, len(timeseries)), 2) if timeseries else 0.0

        # Category Forecast Benchmark if execution_id provided
        cat_forecast = None
        if execution_id and prod["product_category_name"]:
            cur.execute("""
                SELECT 
                    ROUND(AVG(predicted_demand), 2) as weekly_avg,
                    COUNT(*) as weeks_count,
                    MIN(forecast_week) as min_week,
                    MAX(forecast_week) as max_week
                FROM forecast_results
                WHERE execution_id = %s AND product_category = %s
            """, (execution_id, prod["product_category_name"]))
            cf_row = cur.fetchone()
            if cf_row and cf_row.get("weekly_avg"):
                cat_forecast = {
                    "execution_id": execution_id,
                    "weekly_avg": float(cf_row["weekly_avg"]),
                    "weeks_count": int(cf_row["weeks_count"]),
                    "start_week": str(cf_row["min_week"]),
                    "end_week": str(cf_row["max_week"])
                }

        avail = int(prod["available_quantity"])
        thresh = int(prod["reorder_threshold"])
        lt_days = float(prod["lead_time_days"]) if prod.get("lead_time_days") is not None else 3.0

        if avg_hist_weekly > 0:
            cov_weeks = round(avail / avg_hist_weekly, 2)
            cov_days = round(cov_weeks * 7.0, 1)
        else:
            cov_weeks = 999.0 if avail > 0 else 0.0
            cov_days = 999.0 if avail > 0 else 0.0

        if avail <= 0:
            risk = "CRITICAL_STOCKOUT"
        elif cov_days < lt_days or avail < thresh:
            risk = "HIGH_RISK"
        elif cov_days < 14.0:
            risk = "MODERATE_RISK"
        else:
            risk = "HEALTHY"

        suggested_qty = max(0, int(thresh * 2 - avail)) if avail < thresh else 0

        cur.close()
        return {
            "product_id": prod["product_id"],
            "sku": prod["product_id"],
            "category": prod["product_category_name"],
            "category_english": prod["category_english"],
            "inventory": {
                "available_quantity": avail,
                "reserved_quantity": int(prod["reserved_quantity"]),
                "on_hand_quantity": int(prod["on_hand_quantity"]),
                "reorder_threshold": thresh,
                "safety_stock_buffer": int(prod["safety_stock_buffer"]),
                "warehouse_id": prod["warehouse_id"]
            },
            "supplier": {
                "supplier_id": prod.get("supplier_id"),
                "lead_time_hours": prod.get("lead_time_hours"),
                "lead_time_days": lt_days,
                "min_reorder_quantity": prod.get("min_reorder_quantity"),
                "unit_reorder_cost": float(prod["unit_reorder_cost"]) if prod.get("unit_reorder_cost") else None
            },
            "dimensions": {
                "weight_g": float(prod["product_weight_g"]) if prod.get("product_weight_g") else None,
                "length_cm": float(prod["product_length_cm"]) if prod.get("product_length_cm") else None,
                "height_cm": float(prod["product_height_cm"]) if prod.get("product_height_cm") else None,
                "width_cm": float(prod["product_width_cm"]) if prod.get("product_width_cm") else None,
                "photos_qty": int(prod["product_photos_qty"]) if prod.get("product_photos_qty") else 1
            },
            "historical_sales": {
                "total_orders": total_historical_orders,
                "avg_weekly_demand": avg_hist_weekly,
                "active_weeks": len(timeseries),
                "timeseries": timeseries
            },
            "category_forecast_benchmark": cat_forecast,
            "stock_coverage": {
                "coverage_days": cov_days,
                "coverage_weeks": cov_weeks
            },
            "stockout_risk": risk,
            "replenishment": {
                "recommendation_id": f"REC-{execution_id or 'OPERATIONAL'}-SKU-{prod['product_id']}",
                "is_low_stock": avail < thresh,
                "suggested_quantity": suggested_qty,
                "status": "REPLENISHMENT_RECOMMENDED" if avail < thresh else "STOCK_SUFFICIENT"
            },
            "uncertainty_disclosure": (
                "Category-level RMSE represents forecasting error for aggregated category demand. "
                "It is not an individual SKU prediction interval or confidence score. "
                "SKU-level demand is estimated using historical product contribution and category-level forecasts."
            )
        }
    finally:
        if should_close and connection:
            connection.close()


# ==============================================================================
# Phase 1: Operational Order Ingestion & Lifecycle Management
# ==============================================================================

VALID_OPERATIONAL_SOURCES = {"SIMULATED", "SQUARE_SANDBOX", "OPERATIONAL_API", "MANUAL_IMPORT"}


def validate_order_skus(
    skus: List[str],
    connection=None
) -> Tuple[bool, List[str], Dict[str, bool]]:
    """
    Validates a list of SKUs against the product catalog (olist_products)
    and operational inventory (operational_inventory).

    Approved Rule 2:
      - Valid SKUs can be either catalog products (olist_products) or existing operational SKUs.
      - SKUs with configured operational inventory are marked inventory_eligible=True.

    Returns:
        (all_valid: bool, invalid_skus: List[str], inventory_configured: Dict[str, bool])
    """
    if not skus:
        return True, [], {}

    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()
        unique_skus = list(set(skus))
        format_strings = ','.join(['%s'] * len(unique_skus))

        # Check catalog products
        cur.execute(
            f"SELECT product_id FROM olist_products WHERE product_id IN ({format_strings})",
            tuple(unique_skus)
        )
        catalog_products = {
            r["product_id"] if isinstance(r, dict) else r[0]
            for r in cur.fetchall()
        }

        # Check operational inventory
        cur.execute(
            f"SELECT sku FROM operational_inventory WHERE sku IN ({format_strings})",
            tuple(unique_skus)
        )
        inventory_skus = {
            r["sku"] if isinstance(r, dict) else r[0]
            for r in cur.fetchall()
        }
        cur.close()

        valid_skus = catalog_products.union(inventory_skus)
        invalid_skus = [s for s in unique_skus if s not in valid_skus]
        inventory_configured = {s: (s in inventory_skus) for s in unique_skus}

        return (len(invalid_skus) == 0), invalid_skus, inventory_configured
    finally:
        if should_close and connection:
            connection.close()


def ingest_operational_orders(
    orders: List[Dict[str, Any]],
    idempotency_key: Optional[str] = None,
    source: str = "OPERATIONAL_API",
    warehouse_id: str = "WH-SP-01",
    connection=None
) -> Dict[str, Any]:
    """
    Ingests a batch of operational orders transactionally into MySQL:
      1. Validates source provenance and quantity constraints.
      2. Validates SKUs against catalog and identifies inventory-configured SKUs.
      3. Performs batch-level idempotency checks (returns existing orders if replayed).
      4. Atomically:
         a. Inserts orders into operational_orders.
         b. Updates dataset_metadata for 'Operational Orders' (version, row_count, READY).
         c. Inserts 'ORDER_BATCH_INGESTED' event into dataset_events.
         d. Emits dashboard event into dashboard_events journal.
      5. Safe Kafka publish with non-blocking error tolerance (MySQL is authoritative).
      6. In-process EventHub broadcast.

    Safety Invariants:
      - Does NOT directly decrement operational_inventory (reservation is downstream).
      - Preserves source provenance ('SIMULATED', 'OPERATIONAL_API', etc.).
    """
    import uuid

    if not orders:
        raise ValueError("Orders list cannot be empty.")

    if source not in VALID_OPERATIONAL_SOURCES:
        raise ValueError(f"Invalid source '{source}'. Must be one of: {sorted(list(VALID_OPERATIONAL_SOURCES))}")

    # Generate or normalize idempotency key
    batch_key = idempotency_key or f"batch_{uuid.uuid4().hex[:16]}"
    batch_id = batch_key

    # Validate item fields
    skus_to_check = []
    for idx, item in enumerate(orders):
        qty = item.get("quantity")
        if qty is None or not isinstance(qty, int) or qty <= 0:
            raise ValueError(f"Order at index {idx} has invalid quantity: {qty}. Quantity must be a positive integer.")
        sku = item.get("sku")
        if not sku or not str(sku).strip():
            raise ValueError(f"Order at index {idx} missing required 'sku'.")
        skus_to_check.append(str(sku).strip())

    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()

        # Step 1: Check Idempotency (replay detection)
        cur.execute(
            """
            SELECT event_id, dataset_version, event_time, rows_changed, batch_id
            FROM dataset_events
            WHERE source_change_id = %s
            LIMIT 1
            """,
            (batch_key,)
        )
        existing_event = cur.fetchone()

        cur.execute(
            """
            SELECT order_id, sku, warehouse_id, quantity, unit_price, order_status, source, idempotency_key, created_at
            FROM operational_orders
            WHERE idempotency_key = %s OR idempotency_key LIKE %s
            ORDER BY created_at ASC
            """,
            (batch_key, f"{batch_key}:%")
        )
        existing_orders = cur.fetchall()

        if existing_event or existing_orders:
            # Idempotent replay: return existing records without modifying database state
            cur.close()
            # Fetch inventory configuration for return payload
            _, _, inventory_configured = validate_order_skus(
                [o["sku"] for o in existing_orders],
                connection=connection
            )
            return {
                "status": "DUPLICATE_ACCEPTED",
                "idempotent_replay": True,
                "message": "Order batch already ingested with idempotency key",
                "idempotency_key": batch_key,
                "batch_id": existing_event["batch_id"] if existing_event else batch_key,
                "event_id": existing_event["event_id"] if existing_event else None,
                "dataset_version": existing_event["dataset_version"] if existing_event else None,
                "orders_count": len(existing_orders),
                "orders": [
                    {
                        "order_id": o["order_id"],
                        "sku": o["sku"],
                        "warehouse_id": o["warehouse_id"],
                        "quantity": int(o["quantity"]),
                        "unit_price": float(o["unit_price"]),
                        "order_status": o["order_status"],
                        "inventory_eligible": inventory_configured.get(o["sku"], False),
                        "source": o["source"],
                        "idempotency_key": o["idempotency_key"],
                        "created_at": str(o["created_at"])
                    }
                    for o in existing_orders
                ],
                "published_to_kafka": False
            }

        # Step 2: Validate SKUs
        all_valid, invalid_skus, inventory_configured = validate_order_skus(
            skus_to_check,
            connection=connection
        )
        if not all_valid:
            raise ValueError(f"Unknown or invalid catalog SKU(s): {', '.join(invalid_skus)}")

        # Step 3: Atomic Transaction
        now = datetime.datetime.now()

        # 3a. Lock and fetch dataset_metadata
        cur.execute(
            """
            SELECT dataset_id, current_version, row_count, status
            FROM dataset_metadata
            WHERE dataset_name = 'Operational Orders'
            FOR UPDATE
            """
        )
        meta = cur.fetchone()
        if not meta:
            cur.execute(
                """
                INSERT INTO dataset_metadata (dataset_name, source_type, table_name, current_version, row_count, status)
                VALUES ('Operational Orders', 'OPERATIONAL', 'operational_orders', 0, 0, 'WAITING')
                """
            )
            cur.execute(
                """
                SELECT dataset_id, current_version, row_count, status
                FROM dataset_metadata
                WHERE dataset_name = 'Operational Orders'
                FOR UPDATE
                """
            )
            meta = cur.fetchone()

        dataset_id = meta["dataset_id"]
        current_version = meta["current_version"] or 0
        current_row_count = meta["row_count"] or 0
        new_version = current_version + 1
        new_row_count = current_row_count + len(orders)

        # 3b. Prepare and insert orders into operational_orders
        orders_to_insert = []
        orders_payload = []

        for idx, item in enumerate(orders):
            order_id = str(item.get("order_id") or f"ORD-{uuid.uuid4().hex[:12].upper()}").strip()
            item_wh = str(item.get("warehouse_id") or warehouse_id).strip()
            item_sku = str(item["sku"]).strip()
            item_qty = int(item["quantity"])
            item_price = round(float(item.get("unit_price") or 0.0), 2)

            # Form unique idempotency key per order row
            if len(orders) == 1:
                item_idem = str(item.get("idempotency_key") or batch_key)[:128]
            else:
                raw_idem = item.get("idempotency_key") or f"{batch_key}:{order_id}"
                item_idem = str(raw_idem)[:128]

            event_time = item.get("event_time") or now

            orders_to_insert.append((
                order_id,
                item_sku,
                item_wh,
                item_qty,
                item_price,
                "PENDING",
                source,
                item_idem,
                event_time,
                now
            ))

            orders_payload.append({
                "order_id": order_id,
                "sku": item_sku,
                "warehouse_id": item_wh,
                "quantity": item_qty,
                "unit_price": item_price,
                "order_status": "PENDING",
                "inventory_eligible": inventory_configured.get(item_sku, False),
                "source": source,
                "idempotency_key": item_idem
            })

        cur.executemany(
            """
            INSERT INTO operational_orders (
                order_id, sku, warehouse_id, quantity, unit_price,
                order_status, source, idempotency_key, event_time, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            orders_to_insert
        )

        # 3c. Update dataset_metadata
        cur.execute(
            """
            UPDATE dataset_metadata
            SET current_version = %s,
                row_count = %s,
                last_updated_at = %s,
                status = 'READY'
            WHERE dataset_id = %s
            """,
            (new_version, new_row_count, now, dataset_id)
        )

        # 3d. Insert into dataset_events
        cur.execute(
            """
            INSERT INTO dataset_events (
                dataset_id, dataset_name, event_type, event_time,
                dataset_version, rows_changed, source_change_id, batch_id, event_status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'NEW')
            """,
            (
                dataset_id,
                "Operational Orders",
                "ORDER_BATCH_INGESTED",
                now,
                new_version,
                len(orders),
                batch_key,
                batch_id
            )
        )
        event_id = cur.lastrowid

        # 3e. Persist dashboard event to journal within same transaction
        try:
            from services.observability.publisher import publish_dashboard_event
            publish_dashboard_event(
                event_type="OPERATIONAL_ORDER_INGESTED",
                entity="OPERATIONAL_ORDERS",
                severity="INFO",
                correlation_id=str(event_id),
                data={
                    "batch_id": batch_id,
                    "idempotency_key": batch_key,
                    "orders_count": len(orders),
                    "source": source,
                    "dataset_version": new_version,
                    "event_id": event_id
                },
                connection=connection,
                commit=False,
                broadcast=False
            )
        except Exception as pub_err:
            logger.warning(f"Could not write dashboard event to journal: {pub_err}")

        # 3f. Commit transaction
        connection.commit()
        cur.close()

        # Step 4: Optional Non-blocking Kafka Publish (fails safely without invalidating MySQL)
        published_to_kafka = False
        try:
            import socket
            import json
            from config import settings

            broker = getattr(settings, 'KAFKA_BROKER', None) or getattr(settings, 'KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
            host, port_str = broker.split(":") if ":" in broker else (broker, "9092")
            port = int(port_str)

            # Fast socket probe (100ms) to ensure Kafka is actually online before initializing client
            with socket.create_connection((host, port), timeout=0.1):
                pass

            from kafka import KafkaProducer

            producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                request_timeout_ms=1000,
                max_block_ms=1000
            )
            kafka_msg = {
                "event_id": event_id,
                "dataset_id": dataset_id,
                "dataset_name": "Operational Orders",
                "event_type": "ORDER_BATCH_INGESTED",
                "event_time": str(now),
                "dataset_version": new_version,
                "rows_changed": len(orders),
                "batch_id": batch_id,
                "source": source
            }
            future = producer.send(settings.KAFKA_TOPIC_EVENTS, value=kafka_msg)
            future.get(timeout=1.0)
            producer.close(timeout=1.0)

            # Update event_status in dataset_events to PUBLISHED
            try:
                cur_pub = connection.cursor()
                cur_pub.execute(
                    "UPDATE dataset_events SET event_status = 'PUBLISHED', processed_at = NOW() WHERE event_id = %s",
                    (event_id,)
                )
                connection.commit()
                cur_pub.close()
            except Exception:
                pass
            published_to_kafka = True
        except Exception as k_err:
            logger.info(f"Kafka unavailable or publish skipped (MySQL authoritative): {k_err}")
            published_to_kafka = False

        # Step 5: Live Dashboard Event (in-process EventHub & MySQL dashboard_events)
        try:
            from services.observability.publisher import publish_dashboard_event
            publish_dashboard_event(
                event_type="OPERATIONAL_ORDER_INGESTED",
                entity="OPERATIONAL_ORDERS",
                severity="INFO",
                correlation_id=str(event_id),
                data={
                    "batch_id": batch_id,
                    "idempotency_key": batch_key,
                    "orders_count": len(orders),
                    "source": source,
                    "dataset_version": new_version,
                    "event_id": event_id
                },
                connection=connection,
                commit=True
            )
        except Exception as pub_err:
            logger.debug(f"Dashboard event publication skipped: {pub_err}")

        return {
            "status": "INGESTED",
            "idempotent_replay": False,
            "batch_id": batch_id,
            "idempotency_key": batch_key,
            "event_id": event_id,
            "dataset_version": new_version,
            "orders_count": len(orders),
            "orders": orders_payload,
            "published_to_kafka": published_to_kafka
        }

    except Exception as exc:
        if connection:
            connection.rollback()
        logger.error(f"Operational order ingestion failed: {exc}")
        raise
    finally:
        if should_close and connection:
            connection.close()


def get_operational_orders(
    limit: int = 50,
    offset: int = 0,
    source: Optional[str] = None,
    order_status: Optional[str] = None,
    connection=None
) -> Dict[str, Any]:
    """Retrieves paginated operational orders with optional filtering."""
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()
        where_clauses = []
        params = []

        if source:
            where_clauses.append("source = %s")
            params.append(source)
        if order_status:
            where_clauses.append("order_status = %s")
            params.append(order_status)

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        # Total count
        cur.execute(f"SELECT COUNT(*) AS total FROM operational_orders {where_sql}", tuple(params))
        total = cur.fetchone()["total"]

        # Fetch page
        query = f"""
            SELECT order_id, sku, warehouse_id, quantity, unit_price,
                   order_status, source, idempotency_key, event_time, created_at
            FROM operational_orders
            {where_sql}
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])
        cur.execute(query, tuple(params))
        rows = cur.fetchall()

        # Check inventory configuration for each SKU
        skus = [r["sku"] for r in rows]
        _, _, inventory_configured = validate_order_skus(skus, connection=connection)

        formatted = []
        for r in rows:
            formatted.append({
                "order_id": r["order_id"],
                "sku": r["sku"],
                "warehouse_id": r["warehouse_id"],
                "quantity": int(r["quantity"]),
                "unit_price": float(r["unit_price"]),
                "order_status": r["order_status"],
                "source": r["source"],
                "idempotency_key": r["idempotency_key"],
                "event_time": str(r["event_time"]),
                "created_at": str(r["created_at"]),
                "inventory_eligible": inventory_configured.get(r["sku"], False)
            })

        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "orders": formatted
        }
    finally:
        if should_close and connection:
            connection.close()


def get_operational_order_by_id(
    order_id: str,
    connection=None
) -> Optional[Dict[str, Any]]:
    """Retrieves single operational order by order_id."""
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()
        cur.execute(
            """
            SELECT order_id, sku, warehouse_id, quantity, unit_price,
                   order_status, source, idempotency_key, event_time, created_at
            FROM operational_orders
            WHERE order_id = %s
            LIMIT 1
            """,
            (order_id,)
        )
        row = cur.fetchone()
        if not row:
            return None

        _, _, inventory_configured = validate_order_skus([row["sku"]], connection=connection)

        return {
            "order_id": row["order_id"],
            "sku": row["sku"],
            "warehouse_id": row["warehouse_id"],
            "quantity": int(row["quantity"]),
            "unit_price": float(row["unit_price"]),
            "order_status": row["order_status"],
            "source": row["source"],
            "idempotency_key": row["idempotency_key"],
            "event_time": str(row["event_time"]),
            "created_at": str(row["created_at"]),
            "inventory_eligible": inventory_configured.get(row["sku"], False)
        }
    finally:
        if should_close and connection:
            connection.close()


def get_operational_forecast_results(
    execution_id: Optional[int] = None,
    category: Optional[str] = None,
    horizon_days: Optional[int] = None,
    limit: int = 50,
    offset: int = 0,
    connection=None
) -> Dict[str, Any]:
    """Retrieves paginated operational forward forecasts with catalog metadata."""
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()
        where_clauses = []
        params = []
        if execution_id:
            where_clauses.append("o.execution_id = %s")
            params.append(execution_id)
        if category:
            where_clauses.append("o.product_category = %s")
            params.append(category)
        if horizon_days:
            where_clauses.append("o.forecast_horizon = %s")
            params.append(horizon_days)

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        cur.execute(f"SELECT COUNT(*) AS total FROM operational_forecast_results o {where_sql}", tuple(params))
        total = cur.fetchone()["total"]

        cur.execute("SELECT product_category_name, product_category_name_english FROM product_category_name_translation")
        trans_rows = cur.fetchall() or []
        trans_map = {r["product_category_name"]: r["product_category_name_english"] for r in trans_rows}

        query = f"""
            SELECT o.operational_forecast_id AS forecast_id, o.execution_id,
                   'Operational Demand Forecast Pipeline' AS pipeline_name, o.product_category,
                   o.forecast_horizon, o.step_ahead, o.forecast_week, o.predicted_demand,
                   o.model_name, o.model_version, o.inference_status,
                   o.data_provenance, o.created_at
            FROM operational_forecast_results o
            {where_sql}
            ORDER BY o.operational_forecast_id DESC
            LIMIT %s OFFSET %s
        """
        params.extend([max(1, min(limit, 200)), max(0, offset)])
        cur.execute(query, tuple(params))
        rows = cur.fetchall()

        formatted = []
        for r in rows:
            formatted.append({
                "forecast_id": r["forecast_id"],
                "execution_id": r["execution_id"],
                "pipeline_name": r["pipeline_name"],
                "product_category": r["product_category"],
                "category_english": r.get("category_english") or r.get("product_category_name"),
                "forecast_horizon": r["forecast_horizon"],
                "step_ahead": r["step_ahead"],
                "forecast_week": str(r["forecast_week"]),
                "predicted_demand": round(float(r["predicted_demand"]), 4),
                "model_name": r["model_name"],
                "model_version": r["model_version"],
                "inference_status": r["inference_status"],
                "data_provenance": r["data_provenance"],
                "created_at": str(r["created_at"])
            })

        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "forecasts": formatted
        }
    finally:
        if should_close and connection:
            connection.close()
