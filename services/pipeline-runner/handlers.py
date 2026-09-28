"""
Pipeline Execution Handlers for SupplySense AI.

Contains execution logic for registered pipelines (e.g., Demand Forecast Pipeline).
"""

import os
import sys
import logging
from typing import Dict, Any

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from ml.baseline.naive_baseline import run_baseline
from ml.features.engineer_features import build_features
from ml.models.train_xgboost import train_and_evaluate_xgboost
from .forecast_repository import save_forecast_results

logger = logging.getLogger(__name__)

PROCESSED_DIR = os.path.join(REPO_ROOT, 'data', 'processed')
RESULTS_DIR = os.path.join(REPO_ROOT, 'ml', 'results')
MODELS_DIR = os.path.join(REPO_ROOT, 'ml', 'models')


def demand_forecast_pipeline_handler(execution_id: int, pipeline_name: str) -> Dict[str, Any]:
    """
    Executes the Demand Forecast Pipeline using the ML forecasting workload:
      1. Validates that required prepared ML datasets exist.
      2. Invokes the naive baseline forecasting benchmark.
      3. Constructs leak-free lag, rolling, trend, and calendar features.
      4. Trains and evaluates the XGBoost Regressor with early stopping on validation.
      5. Generates model comparison metrics (Naive vs. XGBoost).
      6. Verifies that output prediction and metric artifacts exist.
      7. Persists the out-of-sample test forecast predictions into MySQL (forecast_results).
      8. Returns structured results and artifact locations.

    Args:
        execution_id: ID of the pipeline execution record in MySQL.
        pipeline_name: Name of the pipeline being executed.

    Returns:
        Dict containing output artifact location, metrics, artifact paths, and saved forecast count.

    Raises:
        FileNotFoundError: If required prepared datasets are missing.
        RuntimeError: If execution fails or expected output artifacts are missing.
    """
    logger.info(f"Starting execution #{execution_id} for pipeline '{pipeline_name}'...")

    # 1. Validate required prepared input datasets
    required_inputs = [
        os.path.join(PROCESSED_DIR, 'train.csv'),
        os.path.join(PROCESSED_DIR, 'validation.csv'),
        os.path.join(PROCESSED_DIR, 'test.csv'),
    ]
    missing_inputs = [f for f in required_inputs if not os.path.isfile(f)]
    if missing_inputs:
        error_msg = f"Missing required prepared dataset input(s): {', '.join(missing_inputs)}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    # 2. Invoke the naive baseline benchmark
    logger.info("Step 1/4: Running Naive Baseline Benchmark...")
    try:
        baseline_result = run_baseline()
    except Exception as e:
        error_msg = f"Baseline execution failed with exception: {e}"
        logger.error(error_msg)
        raise RuntimeError(error_msg) from e

    # 3. Feature engineering
    logger.info("Step 2/4: Engineering leak-free features...")
    try:
        features_path = os.path.join(PROCESSED_DIR, 'forecasting_features.csv')
        features_df, _ = build_features(output_path=features_path)
    except Exception as e:
        error_msg = f"Feature engineering failed with exception: {e}"
        logger.error(error_msg)
        raise RuntimeError(error_msg) from e

    # 4. Train and evaluate XGBoost
    logger.info("Step 3/4: Training and evaluating XGBoost Regressor...")
    try:
        xgb_result = train_and_evaluate_xgboost(features_df=features_df)
    except Exception as e:
        error_msg = f"XGBoost pipeline failed with exception: {e}"
        logger.error(error_msg)
        raise RuntimeError(error_msg) from e

    # 5. Verify all expected output artifacts
    output_comparison_json = os.path.join(RESULTS_DIR, 'model_comparison.json')
    output_xgb_metrics_json = os.path.join(RESULTS_DIR, 'xgboost_metrics.json')
    output_base_metrics_json = os.path.join(RESULTS_DIR, 'baseline_metrics.json')
    model_json = os.path.join(MODELS_DIR, 'xgboost_demand_model.json')
    xgb_val_preds_csv = os.path.join(PROCESSED_DIR, 'xgboost_validation_predictions.csv')
    xgb_test_preds_csv = os.path.join(PROCESSED_DIR, 'xgboost_test_predictions.csv')

    expected_outputs = [
        output_comparison_json,
        output_xgb_metrics_json,
        output_base_metrics_json,
        model_json,
        xgb_val_preds_csv,
        xgb_test_preds_csv,
    ]
    missing_outputs = [f for f in expected_outputs if not os.path.isfile(f)]
    if missing_outputs:
        error_msg = f"Expected output artifacts not found after execution: {', '.join(missing_outputs)}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    # 6. Persist forecast predictions into MySQL forecast_results table
    logger.info(f"Step 4/4: Persisting forecast results to MySQL for execution #{execution_id}...")
    try:
        saved_count = save_forecast_results(
            execution_id=execution_id,
            pipeline_name=pipeline_name,
            predictions=xgb_test_preds_csv,
            model_name="XGBoost",
            model_version="xgboost-v1"
        )
        logger.info(f"Successfully persisted {saved_count} forecast predictions into MySQL forecast_results.")
    except Exception as e:
        error_msg = f"Failed to persist forecast predictions to MySQL: {e}"
        logger.error(error_msg)
        raise RuntimeError(error_msg) from e

    # Relative path for portable database storage (model comparison is primary)
    rel_output_location = os.path.relpath(output_comparison_json, REPO_ROOT).replace('\\', '/')

    logger.info(f"Pipeline '{pipeline_name}' execution #{execution_id} completed successfully.")
    return {
        "output_location": rel_output_location,
        "forecasts_saved": saved_count,
        "comparison": xgb_result.get("comparison"),
        "metrics": {
            "naive_baseline": baseline_result,
            "xgboost": {
                "validation": xgb_result.get("validation_metrics"),
                "test": xgb_result.get("test_metrics"),
            }
        },
        "artifacts": [
            os.path.relpath(f, REPO_ROOT).replace('\\', '/') for f in expected_outputs
        ]
    }
