"""
SupplySense AI - Operational Forward Demand Forecast Pipeline

Orchestrates the forward forecasting workload:
  1. Retrieves current operational feature vectors from operational_feature_store.
  2. Loads the authoritative trained XGBoost model.
  3. Executes recursive multi-horizon inference (7-day and 14-day horizons).
  4. Persists forward predictions to operational_forecast_results.
  5. Updates dataset_metadata for 'Operational Forecasts' and records dataset_events.
  6. Emits OPERATIONAL_FORECAST_GENERATED to dashboard_events and EventHub.
"""

import os
import sys
import datetime
import logging
from typing import Dict, Any, List, Optional
import pandas as pd

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from .model_loader import load_trained_xgboost_model, DEFAULT_MODEL_VERSION
from .recursive_engine import generate_forward_forecasts
from .repository import save_operational_forecasts, update_forecast_dataset_metadata

logger = logging.getLogger(__name__)


def run_operational_forecast_pipeline(
    execution_id: int,
    pipeline_name: str = "Operational Demand Forecast Pipeline",
    connection=None,
    horizons: Optional[List[str]] = None,
    source_feature_execution_id: Optional[int] = None
) -> Dict[str, Any]:
    """
    Executes the complete Operational Forward Demand Forecast Pipeline.

    Args:
        execution_id: Execution ID from pipeline_executions table.
        pipeline_name: Name of pipeline being executed.
        connection: Optional PyMySQL database connection.
        horizons: List of forecast horizons ('7_DAYS', '14_DAYS').
        source_feature_execution_id: Optional execution ID of source feature generation.

    Returns:
        Dict with execution summary, forecast counts, and horizon distributions.
    """
    if horizons is None:
        horizons = ["7_DAYS", "14_DAYS"]

    logger.info(f"Starting execution #{execution_id} for '{pipeline_name}' (horizons={horizons})...")

    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()

        # Step 1: Fetch current feature store records
        logger.info("Step 1/5: Loading current operational feature store records...")
        cur.execute("""
            SELECT * FROM operational_feature_store
            ORDER BY week_start_date ASC, product_category ASC
        """)
        feature_rows = cur.fetchall()

        if not feature_rows:
            logger.warning("No feature records found in operational_feature_store.")
            empty_df = pd.DataFrame()
            source_exec = source_feature_execution_id
        else:
            empty_df = pd.DataFrame(feature_rows)
            # Infer source feature execution ID if not passed
            source_exec = source_feature_execution_id or feature_rows[-1].get("execution_id")

        # Step 2: Load trained XGBoost model
        logger.info("Step 2/5: Loading authoritative trained XGBoost model...")
        model, model_meta = load_trained_xgboost_model()

        # Step 3: Generate recursive forward forecasts
        logger.info(f"Step 3/5: Running recursive forward forecasting across {len(empty_df['product_category'].unique()) if not empty_df.empty else 0} categories...")
        forecasts_df = generate_forward_forecasts(
            features_df=empty_df,
            model=model,
            horizons=horizons,
            model_version=model_meta["model_version"],
            source_feature_execution_id=source_exec
        )

        status_counts = {}
        horizon_counts = {}
        if not forecasts_df.empty:
            status_counts = forecasts_df["inference_status"].value_counts().to_dict()
            horizon_counts = forecasts_df["forecast_horizon"].value_counts().to_dict()

        logger.info(f"Forecasting complete: {len(forecasts_df)} predictions generated. Status breakdown: {status_counts}")

        # Step 4: Persist predictions to operational_forecast_results
        saved_forecasts_count = 0
        if not forecasts_df.empty:
            logger.info("Step 4/5: Persisting predictions to operational_forecast_results...")
            saved_forecasts_count = save_operational_forecasts(
                forecasts_df=forecasts_df,
                execution_id=execution_id,
                connection=connection
            )

        # Step 5: Update dataset metadata and events
        meta_result = update_forecast_dataset_metadata(
            forecasts_count=saved_forecasts_count,
            execution_id=execution_id,
            connection=connection
        )

        # Step 6: Publish dashboard event
        try:
            from services.observability.publisher import publish_dashboard_event
            publish_dashboard_event(
                event_type="OPERATIONAL_FORECAST_GENERATED",
                entity="FORECASTING",
                severity="INFO",
                execution_id=execution_id,
                correlation_id=str(execution_id),
                data={
                    "pipeline_name": pipeline_name,
                    "execution_id": execution_id,
                    "model_name": model_meta["model_name"],
                    "model_version": model_meta["model_version"],
                    "predictions_count": saved_forecasts_count,
                    "horizons": horizons,
                    "horizon_breakdown": horizon_counts,
                    "status_breakdown": status_counts,
                    "dataset_version": meta_result.get("new_version"),
                    "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                },
                connection=connection,
                commit=True
            )
        except Exception as pub_err:
            logger.debug(f"Dashboard event publication skipped: {pub_err}")

        output_location = f"mysql://operational_forecast_results?execution_id={execution_id}&count={saved_forecasts_count}"

        return {
            "status": "COMPLETED",
            "execution_id": execution_id,
            "pipeline_name": pipeline_name,
            "output_location": output_location,
            "model_version": model_meta["model_version"],
            "predictions_count": saved_forecasts_count,
            "horizon_breakdown": horizon_counts,
            "status_breakdown": status_counts,
            "dataset_metadata": meta_result,
        }

    except Exception as exc:
        logger.error(f"Operational Demand Forecast Pipeline failed for execution #{execution_id}: {exc}")
        raise
    finally:
        if should_close and connection:
            connection.close()
