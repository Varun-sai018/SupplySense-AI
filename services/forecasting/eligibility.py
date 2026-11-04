"""
SupplySense AI - Feature Eligibility & Cold-Start Policy

Enforces explicit validation rules before feature vectors are passed to XGBoost:
  1. Exact 11-feature names and ordering.
  2. Strict numeric type validation and rejection of NaN/null/inf.
  3. Category encoding bounds check (0..72; -1 rejected as UNSEEN_CATEGORY).
  4. Insufficient history detection (< 8 continuous weeks).
  5. Cold-start fallback calculation for ramp-up categories.
"""

import os
import sys
import math
import logging
from typing import Dict, Any, Tuple, Optional, Union
import pandas as pd
import numpy as np

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from ml.features.feature_engineering import FEATURE_COLUMNS

logger = logging.getLogger(__name__)

SUFFICIENT_HISTORY_MIN_WEEKS = 8

FLOAT_FEATURES = [
    "lag_1_demand",
    "lag_2_demand",
    "lag_4_demand",
    "rolling_mean_4w",
    "rolling_std_4w",
    "rolling_mean_8w",
    "demand_trend_ratio",
]

INT_FEATURES = [
    "product_category_enc",
    "month_of_year",
    "week_of_year",
    "quarter",
]


def check_feature_eligibility(
    feature_row: Union[Dict[str, Any], pd.Series]
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Validates a single category feature row against the inference eligibility policy.

    Args:
        feature_row: Dict or pandas Series containing feature columns and metadata.

    Returns:
        Tuple of (is_eligible, eligibility_status, sanitized_feature_dict)
        where eligibility_status is one of:
          - 'ELIGIBLE'
          - 'INSUFFICIENT_HISTORY'
          - 'UNSEEN_CATEGORY'
          - 'INVALID_FEATURES'
    """
    row = dict(feature_row)

    # 1. Check presence of all 11 features
    missing_cols = [f for f in FEATURE_COLUMNS if f not in row]
    if missing_cols:
        return False, "INVALID_FEATURES", {"reason": f"Missing feature columns: {missing_cols}"}

    # 2. Check for NaN, None, or Inf values
    sanitized = {}
    for col in FEATURE_COLUMNS:
        val = row[col]
        if val is None:
            return False, "INVALID_FEATURES", {"reason": f"Feature '{col}' is None"}
        try:
            f_val = float(val)
            if math.isnan(f_val) or math.isinf(f_val):
                return False, "INVALID_FEATURES", {"reason": f"Feature '{col}' contains NaN/Inf: {val}"}
        except (ValueError, TypeError):
            return False, "INVALID_FEATURES", {"reason": f"Feature '{col}' is non-numeric: {val}"}

        if col in INT_FEATURES:
            sanitized[col] = int(round(float(val)))
        else:
            sanitized[col] = float(val)

    # 3. Check Category Encoding (0..72 valid for existing model)
    cat_enc = sanitized["product_category_enc"]
    if cat_enc < 0 or cat_enc > 72:
        return False, "UNSEEN_CATEGORY", sanitized

    # 4. Check Historical History Sufficiency
    history_weeks = int(row.get("history_weeks_count", 0))
    feature_status = str(row.get("feature_status", ""))

    if history_weeks < SUFFICIENT_HISTORY_MIN_WEEKS or feature_status == "INSUFFICIENT_HISTORY":
        return False, "INSUFFICIENT_HISTORY", sanitized

    return True, "ELIGIBLE", sanitized


def validate_feature_vector(
    feature_row: Union[Dict[str, Any], pd.Series]
) -> Tuple[bool, str, Dict[str, Any]]:
    """Alias for check_feature_eligibility."""
    return check_feature_eligibility(feature_row)


def compute_cold_start_fallback(
    feature_row: Union[Dict[str, Any], pd.Series]
) -> Tuple[float, str]:
    """
    Computes a defensible fallback forecast for categories with insufficient history.

    Policy:
      - If history_weeks_count == 0: Return 0.0 with status 'INSUFFICIENT_HISTORY'.
      - If 1 <= history_weeks_count < 8:
          Use available operational rolling_mean_4w (or lag_1_demand) as moving-average baseline,
          clipped non-negative, tagged as 'COLD_START_FALLBACK'.
      - If UNSEEN_CATEGORY: Return 0.0 with status 'UNSEEN_CATEGORY'.

    Returns:
        Tuple of (predicted_demand, inference_status)
    """
    row = dict(feature_row)
    cat_enc = row.get("product_category_enc", -1)
    if cat_enc is None or int(cat_enc) < 0:
        return 0.0, "UNSEEN_CATEGORY"

    history_weeks = int(row.get("history_weeks_count", 0))
    if history_weeks == 0:
        return 0.0, "INSUFFICIENT_HISTORY"

    # For 1..7 weeks, use available observed rolling mean as defensible fallback
    fallback_val = float(row.get("rolling_mean_4w", row.get("lag_1_demand", 0.0)))
    return max(0.0, round(fallback_val, 4)), "COLD_START_FALLBACK"
