"""
SupplySense AI - Feature Engineering Compatibility Module

Re-exports core feature engineering functions from ml.features.engineer_features
and provides helper loaders for category mappings and feature specifications.
"""

import os
import sys
import json
from typing import Dict, Any, List

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from ml.features.engineer_features import (
    create_continuous_grid,
    engineer_lag_and_rolling_features,
    encode_categories,
    build_features,
    DEFAULT_INPUT_PATH,
    DEFAULT_OUTPUT_PATH,
)

FEATURE_COLUMNS: List[str] = [
    'lag_1_demand',
    'lag_2_demand',
    'lag_4_demand',
    'rolling_mean_4w',
    'rolling_std_4w',
    'rolling_mean_8w',
    'demand_trend_ratio',
    'product_category_enc',
    'month_of_year',
    'week_of_year',
    'quarter',
]

CATEGORY_MAPPING_PATH = os.path.join(os.path.dirname(__file__), 'category_mapping.json')


def load_category_mapping() -> Dict[str, int]:
    """Loads authoritative category-to-integer mapping used by trained XGBoost model."""
    if os.path.isfile(CATEGORY_MAPPING_PATH):
        with open(CATEGORY_MAPPING_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    raise FileNotFoundError(f"Authoritative category mapping not found at {CATEGORY_MAPPING_PATH}")


__all__ = [
    "create_continuous_grid",
    "engineer_lag_and_rolling_features",
    "encode_categories",
    "build_features",
    "load_category_mapping",
    "FEATURE_COLUMNS",
    "DEFAULT_INPUT_PATH",
    "DEFAULT_OUTPUT_PATH",
]
