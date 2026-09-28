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

logger = logging.getLogger(__name__)

PROCESSED_DIR = os.path.join(REPO_ROOT, 'data', 'processed')
RESULTS_DIR = os.path.join(REPO_ROOT, 'ml', 'results')


def demand_forecast_pipeline_handler(execution_id: int, pipeline_name: str) -> Dict[str, Any]:
    """
    Executes the Demand Forecast Pipeline using the ML baseline forecasting workload.

    Steps:
      1. Validates that required prepared ML datasets exist (train, validation, test).
      2. Invokes the naive baseline forecasting engine.
      3. Verifies that output prediction and metric artifacts exist.
      4. Returns structured results and artifact locations.

    Args:
        execution_id: ID of the pipeline execution record in MySQL.
        pipeline_name: Name of the pipeline being executed.

    Returns:
        Dict containing output artifact location and summary metrics.

    Raises:
        FileNotFoundError: If required prepared datasets are missing.
        RuntimeError: If baseline execution fails or expected output artifacts are missing.
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

    # 2. Invoke the naive baseline
    try:
        baseline_result = run_baseline()
    except Exception as e:
        error_msg = f"Baseline execution failed with exception: {e}"
        logger.error(error_msg)
        raise RuntimeError(error_msg) from e

    # 3. Verify output artifacts
    output_metrics_json = os.path.join(RESULTS_DIR, 'baseline_metrics.json')
    output_metrics_csv = os.path.join(RESULTS_DIR, 'baseline_metrics.csv')
    val_preds_csv = os.path.join(PROCESSED_DIR, 'baseline_validation_predictions.csv')
    test_preds_csv = os.path.join(PROCESSED_DIR, 'baseline_test_predictions.csv')

    expected_outputs = [output_metrics_json, output_metrics_csv, val_preds_csv, test_preds_csv]
    missing_outputs = [f for f in expected_outputs if not os.path.isfile(f)]
    if missing_outputs:
        error_msg = f"Expected output artifacts not found after execution: {', '.join(missing_outputs)}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    # Relative path for portable database storage
    rel_output_location = os.path.relpath(output_metrics_json, REPO_ROOT).replace('\\', '/')

    logger.info(f"Pipeline '{pipeline_name}' execution #{execution_id} completed successfully.")
    return {
        "output_location": rel_output_location,
        "metrics": baseline_result,
        "artifacts": [
            os.path.relpath(f, REPO_ROOT).replace('\\', '/') for f in expected_outputs
        ]
    }
