"""
SupplySense AI - Model Loader for Operational Forecasting

Loads the serialized XGBoost demand forecasting model and validates its
internal feature specification against the authoritative 11-feature contract.
"""

import os
import sys
import json
import logging
from typing import Tuple, Dict, Any, Optional
import xgboost as xgb

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from ml.features.feature_engineering import FEATURE_COLUMNS

logger = logging.getLogger(__name__)

DEFAULT_MODEL_PATH = os.path.join(REPO_ROOT, 'ml', 'models', 'xgboost_demand_model.json')
DEFAULT_MODEL_VERSION = 'xgboost-v1'


def load_trained_xgboost_model(
    model_path: Optional[str] = None
) -> Tuple[xgb.XGBRegressor, Dict[str, Any]]:
    """
    Loads and validates the trained XGBoost model.

    Args:
        model_path: Optional path to JSON model file. If None, uses default.

    Returns:
        Tuple of (loaded_model, metadata_dict)

    Raises:
        FileNotFoundError: If model file does not exist.
        ValueError: If model features do not match FEATURE_COLUMNS.
    """
    path = model_path or DEFAULT_MODEL_PATH

    if not os.path.isfile(path):
        raise FileNotFoundError(f"Trained XGBoost model file not found at: {path}")

    # Read raw JSON to inspect feature contract
    with open(path, 'r', encoding='utf-8') as f:
        raw_meta = json.load(f)

    learner = raw_meta.get('learner', {})
    expected_names = learner.get('feature_names', [])

    # Validate feature names against FEATURE_COLUMNS
    if expected_names and list(expected_names) != FEATURE_COLUMNS:
        raise ValueError(
            f"Model feature names mismatch!\n"
            f"Expected: {FEATURE_COLUMNS}\n"
            f"Model has: {expected_names}"
        )

    # Initialize XGBRegressor and load model
    model = xgb.XGBRegressor()
    model.load_model(path)

    metadata = {
        "model_path": os.path.relpath(path, REPO_ROOT).replace('\\', '/'),
        "model_name": "XGBoost",
        "model_version": DEFAULT_MODEL_VERSION,
        "feature_names": FEATURE_COLUMNS,
        "objective": learner.get('learner_model_param', {}).get('objective', 'reg:squarederror'),
        "num_trees": int(learner.get('gradient_booster', {}).get('model', {}).get('gbtree_model_param', {}).get('num_trees', 0))
    }

    logger.info(f"Loaded XGBoost model from {metadata['model_path']} (Trees: {metadata['num_trees']}).")
    return model, metadata
