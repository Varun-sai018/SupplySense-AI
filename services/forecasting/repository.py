"""
SupplySense AI - Operational Forecast Repository

Persists forward forecast predictions into operational_forecast_results,
updates metadata, and records dataset events.
"""

import os
import sys
import logging
from typing import Dict, Any, List, Optional
import pandas as pd

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection

logger = logging.getLogger(__name__)


def save_operational_forecasts(
    forecasts_df: pd.DataFrame,
    execution_id: Optional[int] = None,
    connection=None
) -> int:
    """
    Persists forecast predictions into operational_forecast_results table.
    Uses ON DUPLICATE KEY UPDATE for idempotency.
    """
    if forecasts_df.empty:
        return 0

    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()
        query = """
            INSERT INTO operational_forecast_results (
                execution_id, product_category, forecast_week, year_week,
                predicted_demand, model_name, model_version,
                source_feature_execution_id, data_provenance,
                forecast_horizon, step_ahead, inference_status
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                predicted_demand = VALUES(predicted_demand),
                model_name = VALUES(model_name),
                model_version = VALUES(model_version),
                source_feature_execution_id = VALUES(source_feature_execution_id),
                inference_status = VALUES(inference_status)
        """
        rows_to_insert = []
        for _, row in forecasts_df.iterrows():
            rows_to_insert.append((
                execution_id,
                str(row["product_category"]),
                str(row["forecast_week"]),
                str(row["year_week"]),
                float(row["predicted_demand"]),
                str(row.get("model_name", "XGBoost")),
                str(row.get("model_version", "xgboost-v1")),
                row.get("source_feature_execution_id"),
                str(row.get("data_provenance", "OPERATIONAL")),
                str(row["forecast_horizon"]),
                int(row["step_ahead"]),
                str(row["inference_status"])
            ))

        cur.executemany(query, rows_to_insert)
        saved_count = len(rows_to_insert)
        connection.commit()
        cur.close()
        logger.info(f"Persisted {saved_count} predictions into operational_forecast_results.")
        return saved_count
    finally:
        if should_close and connection:
            connection.close()


def update_forecast_dataset_metadata(
    forecasts_count: int,
    execution_id: Optional[int] = None,
    connection=None
) -> Dict[str, Any]:
    """
    Updates dataset_metadata for 'Operational Forecasts' and inserts event into dataset_events.
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()
        cur.execute(
            "SELECT dataset_id, current_version FROM dataset_metadata WHERE dataset_name = 'Operational Forecasts' LIMIT 1"
        )
        meta = cur.fetchone()
        if not meta:
            logger.warning("'Operational Forecasts' dataset not found in dataset_metadata.")
            return {"dataset_id": None, "new_version": None, "event_id": None}

        dataset_id = meta["dataset_id"]
        new_version = int(meta.get("current_version") or 0) + 1

        cur.execute(
            """
            UPDATE dataset_metadata
            SET current_version = %s,
                row_count = %s,
                status = 'READY',
                last_updated_at = NOW()
            WHERE dataset_id = %s
            """,
            (new_version, forecasts_count, dataset_id)
        )

        cur.execute(
            """
            INSERT INTO dataset_events (
                dataset_id, dataset_name, event_type, dataset_version,
                rows_changed, event_status, created_at
            ) VALUES (
                %s, 'Operational Forecasts', 'FORECASTS_GENERATED',
                %s, %s, 'NEW', NOW()
            )
            """,
            (dataset_id, new_version, forecasts_count)
        )
        event_id = cur.lastrowid
        connection.commit()
        cur.close()

        return {
            "dataset_id": dataset_id,
            "new_version": new_version,
            "event_id": event_id
        }
    finally:
        if should_close and connection:
            connection.close()


def get_operational_forecasts(
    product_category: Optional[str] = None,
    forecast_week: Optional[str] = None,
    forecast_horizon: Optional[str] = None,
    execution_id: Optional[int] = None,
    limit: int = 100,
    connection=None
) -> List[Dict[str, Any]]:
    """Retrieves operational forecast records from operational_forecast_results."""
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()
        clauses = []
        params: List[Any] = []
        if product_category:
            clauses.append("product_category = %s")
            params.append(product_category)
        if forecast_week:
            clauses.append("forecast_week = %s")
            params.append(forecast_week)
        if forecast_horizon:
            clauses.append("forecast_horizon = %s")
            params.append(forecast_horizon)
        if execution_id:
            clauses.append("execution_id = %s")
            params.append(execution_id)

        where_sql = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        query = f"""
            SELECT * FROM operational_forecast_results
            {where_sql}
            ORDER BY forecast_week ASC, product_category ASC
            LIMIT %s
        """
        params.append(limit)
        cur.execute(query, tuple(params))
        results = cur.fetchall()
        cur.close()
        return results
    finally:
        if should_close and connection:
            connection.close()
