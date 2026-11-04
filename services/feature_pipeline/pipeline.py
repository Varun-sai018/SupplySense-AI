"""
SupplySense AI - Operational Feature Pipeline Orchestrator

Coordinates the end-to-end execution of operational demand aggregation and
feature engineering:
  1. Aggregates customer demand from operational_orders (excluding CANCELLED).
  2. Persists aggregated demand records to operational_demand_weekly.
  3. Constructs complete continuous weekly grid across all model-supported categories.
  4. Engineers the exact 11 features matching XGBoost training preprocessing contract.
  5. Persists features to operational_feature_store.
  6. Updates dataset_metadata and emits dataset_events.
  7. Emits OPERATIONAL_FEATURES_GENERATED to dashboard_events journal and EventHub.
"""

import os
import sys
import logging
import datetime
from typing import Dict, Any, Optional

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from .aggregator import aggregate_operational_orders
from .grid_generator import build_weekly_demand_grid
from .feature_builder import compute_operational_features
from .repository import (
    save_operational_demand_weekly,
    save_operational_features,
    update_feature_dataset_metadata,
)

logger = logging.getLogger(__name__)


def run_operational_feature_pipeline(
    execution_id: int,
    pipeline_name: str = "Operational Demand Feature Pipeline",
    connection=None,
    source_provenance: str = "ALL_OPERATIONAL",
    since_event_time: Optional[datetime.datetime] = None,
    until_event_time: Optional[datetime.datetime] = None
) -> Dict[str, Any]:
    """
    Executes the complete Operational Feature Pipeline.

    Args:
        execution_id: Execution ID from pipeline_executions table.
        pipeline_name: Name of pipeline being executed.
        connection: Optional open PyMySQL database connection.
        source_provenance: 'ALL_OPERATIONAL', 'GENUINE_ONLY', or 'SIMULATED'.
        since_event_time: Optional start cutoff for order events.
        until_event_time: Optional end cutoff for order events.

    Returns:
        Dict summarizing execution outcome, artifact counts, and status distributions.
    """
    logger.info(f"Starting execution #{execution_id} for '{pipeline_name}' (source={source_provenance})...")

    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        # Step 1: Aggregate operational orders
        logger.info("Step 1/5: Aggregating operational orders from operational_orders...")
        agg_df, agg_meta = aggregate_operational_orders(
            connection=connection,
            source_provenance=source_provenance,
            since_event_time=since_event_time,
            until_event_time=until_event_time
        )
        logger.info(
            f"Aggregation complete: {len(agg_df)} category-week observation(s) from "
            f"{agg_meta['eligible_orders_count']} eligible orders. "
            f"Cancelled excluded: {agg_meta['cancelled_orders_excluded']}."
        )

        # Step 2: Persist weekly demand aggregation
        saved_demand_count = 0
        if not agg_df.empty:
            logger.info("Step 2/5: Persisting aggregated demand to operational_demand_weekly...")
            records = agg_df.to_dict("records")
            saved_demand_count = save_operational_demand_weekly(
                records=records,
                execution_id=execution_id,
                connection=connection
            )

        # Step 3: Expand onto complete continuous weekly grid
        logger.info("Step 3/5: Expanding onto continuous weekly demand grid...")
        grid_df = build_weekly_demand_grid(agg_df)
        logger.info(f"Continuous grid built: {len(grid_df)} rows across {grid_df['product_category'].nunique() if not grid_df.empty else 0} categories.")

        # Step 4: Engineer the 11 XGBoost features
        logger.info("Step 4/5: Computing 11 authoritative features and tracking status...")
        features_df = compute_operational_features(grid_df)

        status_counts = {}
        if not features_df.empty and "feature_status" in features_df.columns:
            status_counts = features_df["feature_status"].value_counts().to_dict()
        logger.info(f"Feature computation complete: {len(features_df)} rows. Status breakdown: {status_counts}")

        # Step 5: Persist features into operational_feature_store
        saved_features_count = 0
        if not features_df.empty:
            logger.info("Step 5/5: Persisting engineered features into operational_feature_store...")
            saved_features_count = save_operational_features(
                features_df=features_df,
                execution_id=execution_id,
                source_provenance=source_provenance,
                connection=connection
            )

        # Step 6: Update dataset metadata and events
        meta_result = update_feature_dataset_metadata(
            features_count=saved_features_count,
            execution_id=execution_id,
            connection=connection
        )

        # Step 7: Emit dashboard event
        try:
            from services.observability.publisher import publish_dashboard_event
            publish_dashboard_event(
                event_type="OPERATIONAL_FEATURES_GENERATED",
                entity="FEATURE_STORE",
                severity="INFO",
                execution_id=execution_id,
                correlation_id=str(execution_id),
                data={
                    "pipeline_name": pipeline_name,
                    "execution_id": execution_id,
                    "source_provenance": source_provenance,
                    "demand_rows_persisted": saved_demand_count,
                    "features_rows_persisted": saved_features_count,
                    "status_breakdown": status_counts,
                    "eligible_orders_count": agg_meta["eligible_orders_count"],
                    "cancelled_orders_excluded": agg_meta["cancelled_orders_excluded"],
                    "dataset_version": meta_result.get("new_version"),
                    "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                },
                connection=connection,
                commit=True
            )
        except Exception as pub_err:
            logger.debug(f"Dashboard event publication skipped: {pub_err}")

        output_location = f"mysql://operational_feature_store?execution_id={execution_id}&count={saved_features_count}"

        return {
            "status": "COMPLETED",
            "execution_id": execution_id,
            "pipeline_name": pipeline_name,
            "output_location": output_location,
            "demand_rows": saved_demand_count,
            "features_rows": saved_features_count,
            "status_breakdown": status_counts,
            "aggregation_metadata": agg_meta,
            "dataset_metadata": meta_result,
        }

    except Exception as exc:
        logger.error(f"Operational Feature Pipeline failed for execution #{execution_id}: {exc}")
        raise
    finally:
        if should_close and connection:
            connection.close()
