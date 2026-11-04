"""
SupplySense AI - Operational Order Aggregator

Aggregates operational customer orders into weekly category demand:
  - Filters out CANCELLED orders (counts them separately for auditability).
  - Includes PENDING, ALLOCATED, BACKORDERED, and COMPLETED orders as valid demand units.
  - Maps SKU IDs to product categories via olist_products.
  - Groups demand by product_category and ISO calendar week (Monday week_start_date).
  - Enforces duplicate protection via distinct order_id tracking.
  - Preserves source provenance separation (SIMULATED vs. genuine operational sources).
  - Strictly reads operational_orders; never touches sandbox sim_orders.
"""

import os
import sys
import logging
import datetime
from typing import Dict, Any, List, Optional, Tuple
import pandas as pd

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection

logger = logging.getLogger(__name__)

ELIGIBLE_ORDER_STATUSES = {"PENDING", "ALLOCATED", "BACKORDERED", "COMPLETED"}
EXCLUDED_ORDER_STATUSES = {"CANCELLED"}

GENUINE_OPERATIONAL_SOURCES = {"OPERATIONAL_API", "MANUAL_IMPORT", "SQUARE_SANDBOX"}
ALL_OPERATIONAL_SOURCES = GENUINE_OPERATIONAL_SOURCES | {"SIMULATED"}


def aggregate_operational_orders(
    connection=None,
    source_provenance: str = "ALL_OPERATIONAL",
    since_event_time: Optional[datetime.datetime] = None,
    until_event_time: Optional[datetime.datetime] = None,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Reads eligible records from operational_orders, joins with product catalog,
    and aggregates demand by product category and ISO week.

    Args:
        connection: Optional PyMySQL database connection.
        source_provenance: Provenance filter ('ALL_OPERATIONAL', 'GENUINE_ONLY',
                           'SIMULATED', or specific source name).
        since_event_time: Optional lower bound for event_time.
        until_event_time: Optional upper bound for event_time.

    Returns:
        Tuple of (aggregated_df, aggregation_metadata)
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()

        # Build source filter
        source_filter_sql = ""
        params: List[Any] = []

        if source_provenance == "GENUINE_ONLY":
            placeholders = ",".join(["%s"] * len(GENUINE_OPERATIONAL_SOURCES))
            source_filter_sql = f" AND o.source IN ({placeholders})"
            params.extend(sorted(list(GENUINE_OPERATIONAL_SOURCES)))
        elif source_provenance == "SIMULATED":
            source_filter_sql = " AND o.source = %s"
            params.append("SIMULATED")
        elif source_provenance in ALL_OPERATIONAL_SOURCES:
            source_filter_sql = " AND o.source = %s"
            params.append(source_provenance)
        elif source_provenance == "ALL_OPERATIONAL":
            pass  # Include all sources present in operational_orders
        else:
            raise ValueError(f"Unknown source_provenance filter: {source_provenance}")

        # Build event time filter
        time_filter_sql = ""
        if since_event_time is not None:
            time_filter_sql += " AND o.event_time >= %s"
            params.append(since_event_time)
        if until_event_time is not None:
            time_filter_sql += " AND o.event_time <= %s"
            params.append(until_event_time)

        # 1. Audit check: Count cancelled orders for transparency
        cur.execute(f"""
            SELECT COUNT(*) AS cancelled_count, COALESCE(SUM(quantity), 0) AS cancelled_quantity
            FROM operational_orders o
            WHERE o.order_status = 'CANCELLED'
            {source_filter_sql}
            {time_filter_sql}
        """, tuple(params))
        cancelled_info = cur.fetchone() or {"cancelled_count": 0, "cancelled_quantity": 0}
        cancelled_count = int(cancelled_info["cancelled_count"])
        cancelled_quantity = int(cancelled_info["cancelled_quantity"])

        # 2. Fetch eligible orders with SKU category resolution
        # Uses DISTINCT o.order_id defense-in-depth against duplicate joins
        eligible_placeholders = ",".join(["%s"] * len(ELIGIBLE_ORDER_STATUSES))
        query = f"""
            SELECT
                o.order_id,
                o.sku,
                o.quantity,
                o.unit_price,
                o.order_status,
                o.source,
                o.idempotency_key,
                o.event_time,
                p.product_category_name
            FROM operational_orders o
            LEFT JOIN olist_products p ON o.sku = p.product_id
            WHERE o.order_status IN ({eligible_placeholders})
              AND o.quantity > 0
              {source_filter_sql}
              {time_filter_sql}
            ORDER BY o.event_time ASC
        """
        all_params = list(ELIGIBLE_ORDER_STATUSES) + params
        cur.execute(query, tuple(all_params))
        rows = cur.fetchall()

        if not rows:
            logger.info("No eligible operational orders found for aggregation.")
            empty_df = pd.DataFrame(columns=[
                "product_category", "year_week", "week_start_date",
                "demand_units", "order_count", "cancelled_orders_excluded",
                "source_provenance", "min_order_event_time", "max_order_event_time"
            ])
            return empty_df, {
                "total_orders_inspected": 0,
                "eligible_orders_count": 0,
                "cancelled_orders_excluded": cancelled_count,
                "cancelled_quantity_excluded": cancelled_quantity,
                "unmapped_sku_count": 0,
                "distinct_categories": 0,
                "distinct_weeks": 0,
                "source_provenance": source_provenance,
            }

        # 3. Aggregate in-memory with strict ISO calendar calculation
        from collections import defaultdict
        agg_map = defaultdict(lambda: {
            "demand_units": 0,
            "order_ids": set(),
            "min_event_time": None,
            "max_event_time": None,
        })

        unmapped_skus = set()
        seen_order_ids = set()
        duplicate_orders_dropped = 0

        for row in rows:
            order_id = row["order_id"]
            if order_id in seen_order_ids:
                # Primary key guard
                duplicate_orders_dropped += 1
                continue
            seen_order_ids.add(order_id)

            cat_name = row.get("product_category_name")
            if not cat_name or not str(cat_name).strip():
                unmapped_skus.add(row["sku"])
                continue

            event_time = row["event_time"]
            if isinstance(event_time, str):
                event_time = datetime.datetime.fromisoformat(event_time)

            # ISO calendar week
            iso_year, iso_week, _ = event_time.isocalendar()
            year_week = f"{iso_year}-W{iso_week:02d}"
            # Monday of that ISO week
            week_start_date = datetime.date.fromisocalendar(iso_year, iso_week, 1).isoformat()

            key = (str(cat_name).strip(), year_week, week_start_date)
            entry = agg_map[key]
            entry["demand_units"] += int(row["quantity"])
            entry["order_ids"].add(order_id)

            if entry["min_event_time"] is None or event_time < entry["min_event_time"]:
                entry["min_event_time"] = event_time
            if entry["max_event_time"] is None or event_time > entry["max_event_time"]:
                entry["max_event_time"] = event_time

        # 4. Build output DataFrame
        agg_rows = []
        for (cat, yw, w_start), data in sorted(agg_map.items()):
            agg_rows.append({
                "product_category": cat,
                "year_week": yw,
                "week_start_date": w_start,
                "demand_units": data["demand_units"],
                "order_count": len(data["order_ids"]),
                "cancelled_orders_excluded": cancelled_count,
                "source_provenance": source_provenance,
                "min_order_event_time": data["min_event_time"],
                "max_order_event_time": data["max_event_time"],
            })

        agg_df = pd.DataFrame(agg_rows)
        if unmapped_skus:
            logger.warning(f"Skipped {len(unmapped_skus)} SKU(s) without product category in olist_products.")

        metadata = {
            "total_orders_inspected": len(rows) + cancelled_count,
            "eligible_orders_count": len(seen_order_ids),
            "cancelled_orders_excluded": cancelled_count,
            "cancelled_quantity_excluded": cancelled_quantity,
            "duplicate_orders_dropped": duplicate_orders_dropped,
            "unmapped_sku_count": len(unmapped_skus),
            "distinct_categories": agg_df["product_category"].nunique() if not agg_df.empty else 0,
            "distinct_weeks": agg_df["year_week"].nunique() if not agg_df.empty else 0,
            "source_provenance": source_provenance,
        }

        return agg_df, metadata

    finally:
        if should_close and connection:
            connection.close()
