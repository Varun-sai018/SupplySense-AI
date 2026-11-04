"""
SupplySense AI - Operational Feature Pipeline Repository

Manages persistence and queries for:
  - operational_demand_weekly
  - operational_feature_store
  - dataset_metadata & dataset_events (for Operational Demand Features)
"""

import os
import sys
import logging
import datetime
from typing import Dict, Any, List, Optional
import pandas as pd
import pymysql

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection

logger = logging.getLogger(__name__)


def save_operational_demand_weekly(
    records: List[Dict[str, Any]],
    execution_id: Optional[int] = None,
    connection=None
) -> int:
    """
    Persists weekly aggregated demand into operational_demand_weekly table.
    Uses ON DUPLICATE KEY UPDATE for idempotency.
    """
    if not records:
        return 0

    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()
        query = """
            INSERT INTO operational_demand_weekly (
                execution_id, product_category, year_week, week_start_date,
                demand_units, order_count, cancelled_orders_excluded,
                source_provenance, min_order_event_time, max_order_event_time
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                execution_id = VALUES(execution_id),
                demand_units = VALUES(demand_units),
                order_count = VALUES(order_count),
                cancelled_orders_excluded = VALUES(cancelled_orders_excluded),
                min_order_event_time = VALUES(min_order_event_time),
                max_order_event_time = VALUES(max_order_event_time)
        """
        rows_to_insert = [
            (
                execution_id,
                r["product_category"],
                r["year_week"],
                r["week_start_date"],
                int(r["demand_units"]),
                int(r.get("order_count", 0)),
                int(r.get("cancelled_orders_excluded", 0)),
                r.get("source_provenance", "ALL_OPERATIONAL"),
                r.get("min_order_event_time"),
                r.get("max_order_event_time")
            )
            for r in records
        ]
        cur.executemany(query, rows_to_insert)
        saved_count = len(records)
        connection.commit()
        cur.close()
        logger.info(f"Persisted {saved_count} records into operational_demand_weekly.")
        return saved_count
    finally:
        if should_close and connection:
            connection.close()


def save_operational_features(
    features_df: pd.DataFrame,
    execution_id: Optional[int] = None,
    source_provenance: str = "ALL_OPERATIONAL",
    connection=None
) -> int:
    """
    Persists the 11 engineered features and tracking metadata into operational_feature_store.
    Uses ON DUPLICATE KEY UPDATE for idempotency.
    """
    if features_df.empty:
        return 0

    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()
        query = """
            INSERT INTO operational_feature_store (
                execution_id, product_category, year_week, week_start_date,
                lag_1_demand, lag_2_demand, lag_4_demand,
                rolling_mean_4w, rolling_std_4w, rolling_mean_8w,
                demand_trend_ratio, product_category_enc,
                month_of_year, week_of_year, quarter,
                history_weeks_count, feature_status, source_provenance
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                execution_id = VALUES(execution_id),
                lag_1_demand = VALUES(lag_1_demand),
                lag_2_demand = VALUES(lag_2_demand),
                lag_4_demand = VALUES(lag_4_demand),
                rolling_mean_4w = VALUES(rolling_mean_4w),
                rolling_std_4w = VALUES(rolling_std_4w),
                rolling_mean_8w = VALUES(rolling_mean_8w),
                demand_trend_ratio = VALUES(demand_trend_ratio),
                product_category_enc = VALUES(product_category_enc),
                month_of_year = VALUES(month_of_year),
                week_of_year = VALUES(week_of_year),
                quarter = VALUES(quarter),
                history_weeks_count = VALUES(history_weeks_count),
                feature_status = VALUES(feature_status)
        """
        rows_to_insert = []
        for _, row in features_df.iterrows():
            rows_to_insert.append((
                execution_id,
                str(row["product_category"]),
                str(row["year_week"]),
                str(row["week_start_date"]),
                float(row["lag_1_demand"]),
                float(row["lag_2_demand"]),
                float(row["lag_4_demand"]),
                float(row["rolling_mean_4w"]),
                float(row["rolling_std_4w"]),
                float(row["rolling_mean_8w"]),
                float(row["demand_trend_ratio"]),
                int(row["product_category_enc"]),
                int(row["month_of_year"]),
                int(row["week_of_year"]),
                int(row["quarter"]),
                int(row["history_weeks_count"]),
                str(row["feature_status"]),
                source_provenance
            ))

        cur.executemany(query, rows_to_insert)
        saved_count = len(rows_to_insert)
        connection.commit()
        cur.close()
        logger.info(f"Persisted {saved_count} feature records into operational_feature_store.")
        return saved_count
    finally:
        if should_close and connection:
            connection.close()


def update_feature_dataset_metadata(
    features_count: int,
    execution_id: Optional[int] = None,
    connection=None
) -> Dict[str, Any]:
    """
    Updates dataset_metadata for 'Operational Demand Features' and
    inserts an event into dataset_events.
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()

        # 1. Fetch current metadata
        cur.execute(
            "SELECT dataset_id, current_version FROM dataset_metadata WHERE dataset_name = 'Operational Demand Features' LIMIT 1"
        )
        meta = cur.fetchone()
        if not meta:
            logger.warning("'Operational Demand Features' not found in dataset_metadata.")
            return {"dataset_id": None, "new_version": None, "event_id": None}

        dataset_id = meta["dataset_id"]
        new_version = int(meta.get("current_version") or 0) + 1

        # 2. Update metadata
        cur.execute(
            """
            UPDATE dataset_metadata
            SET current_version = %s,
                row_count = %s,
                status = 'READY',
                last_updated_at = NOW()
            WHERE dataset_id = %s
            """,
            (new_version, features_count, dataset_id)
        )

        # 3. Insert event into dataset_events
        cur.execute(
            """
            INSERT INTO dataset_events (
                dataset_id, dataset_name, event_type, dataset_version,
                rows_changed, event_status, created_at
            ) VALUES (
                %s, 'Operational Demand Features', 'FEATURES_GENERATED',
                %s, %s, 'NEW', NOW()
            )
            """,
            (dataset_id, new_version, features_count)
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


def get_operational_demand_weekly(
    product_category: Optional[str] = None,
    year_week: Optional[str] = None,
    source_provenance: Optional[str] = None,
    limit: int = 100,
    connection=None
) -> List[Dict[str, Any]]:
    """Retrieves weekly demand records from operational_demand_weekly."""
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
        if year_week:
            clauses.append("year_week = %s")
            params.append(year_week)
        if source_provenance:
            clauses.append("source_provenance = %s")
            params.append(source_provenance)

        where_sql = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        query = f"""
            SELECT * FROM operational_demand_weekly
            {where_sql}
            ORDER BY week_start_date DESC, product_category ASC
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


def get_operational_features(
    product_category: Optional[str] = None,
    year_week: Optional[str] = None,
    feature_status: Optional[str] = None,
    limit: int = 100,
    connection=None
) -> List[Dict[str, Any]]:
    """Retrieves feature records from operational_feature_store."""
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
        if year_week:
            clauses.append("year_week = %s")
            params.append(year_week)
        if feature_status:
            clauses.append("feature_status = %s")
            params.append(feature_status)

        where_sql = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        query = f"""
            SELECT * FROM operational_feature_store
            {where_sql}
            ORDER BY week_start_date DESC, product_category ASC
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
