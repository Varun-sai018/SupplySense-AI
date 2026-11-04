"""
SupplySense AI - Operational Feature Builder

Constructs the exact 11-feature input dataset expected by the trained XGBoost model:
  1. lag_1_demand (float)
  2. lag_2_demand (float)
  3. lag_4_demand (float)
  4. rolling_mean_4w (float)
  5. rolling_std_4w (float)
  6. rolling_mean_8w (float)
  7. demand_trend_ratio (float)
  8. product_category_enc (int)
  9. month_of_year (int)
  10. week_of_year (int)
  11. quarter (int)

Ensures:
  - Exact match with training-time preprocessing (ml.features.engineer_features).
  - Strict causality: shift(1) prevents lookahead leakage of current week T.
  - Authoritative 73-category mapping loaded from category_mapping.json.
  - Unseen categories mapped to -1 and flagged as UNSEEN_CATEGORY.
  - Categories with fewer than 8 prior observed weeks flagged as INSUFFICIENT_HISTORY.
  - Zero operational lag values derived strictly from operational_orders (no 2018 contamination).
"""

import os
import sys
import logging
from typing import Dict, Any, Optional, List
import pandas as pd
import numpy as np

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from ml.features.feature_engineering import (
    load_category_mapping,
    FEATURE_COLUMNS,
    engineer_lag_and_rolling_features,
    encode_categories,
)

logger = logging.getLogger(__name__)

SUFFICIENT_HISTORY_MIN_WEEKS = 8


def compute_operational_features(
    grid_df: pd.DataFrame,
    category_mapping: Optional[Dict[str, int]] = None
) -> pd.DataFrame:
    """
    Computes the 11 authoritative features for each category-week row on a continuous grid.

    Args:
        grid_df: Continuous grid DataFrame containing:
                 ['product_category', 'week_start_date', 'year_week', 'demand']
                 (from build_weekly_demand_grid).
        category_mapping: Optional category mapping dict. If None, loads from
                          authoritative category_mapping.json.

    Returns:
        DataFrame containing metadata, 11 feature columns, and status flags:
          - product_category
          - year_week
          - week_start_date
          - 11 feature columns in FEATURE_COLUMNS order
          - history_weeks_count (int)
          - feature_status ('SUFFICIENT_HISTORY', 'INSUFFICIENT_HISTORY', 'UNSEEN_CATEGORY')
    """
    if grid_df.empty:
        logger.info("Empty grid received; returning empty feature store DataFrame.")
        cols = [
            "product_category", "year_week", "week_start_date"
        ] + FEATURE_COLUMNS + ["history_weeks_count", "feature_status"]
        return pd.DataFrame(columns=cols)

    df = grid_df.copy()
    df = df.sort_values(["product_category", "week_start_date"]).reset_index(drop=True)

    # 1. Compute lag, rolling, trend ratio, and calendar features using existing implementation
    features_df = engineer_lag_and_rolling_features(df)

    # 2. Encode categories with pre-fitted authoritative mapping
    if category_mapping is None:
        category_mapping = load_category_mapping()

    features_df, _ = encode_categories(features_df, category_mapping=category_mapping)

    # 3. Ensure history_weeks_count is computed strictly causal
    if "history_weeks_count" not in features_df.columns:
        features_df["history_weeks_count"] = features_df.groupby("product_category").cumcount()
    else:
        features_df["history_weeks_count"] = features_df["history_weeks_count"].astype(int)

    # 4. Classify feature_status
    def _determine_status(row) -> str:
        if row["product_category_enc"] == -1:
            return "UNSEEN_CATEGORY"
        if row["history_weeks_count"] < SUFFICIENT_HISTORY_MIN_WEEKS:
            return "INSUFFICIENT_HISTORY"
        return "SUFFICIENT_HISTORY"

    features_df["feature_status"] = features_df.apply(_determine_status, axis=1)

    # 5. Enforce strict dtypes
    float_cols = [
        "lag_1_demand", "lag_2_demand", "lag_4_demand",
        "rolling_mean_4w", "rolling_std_4w", "rolling_mean_8w",
        "demand_trend_ratio"
    ]
    int_cols = [
        "product_category_enc", "month_of_year", "week_of_year", "quarter"
    ]

    for fc in float_cols:
        features_df[fc] = features_df[fc].astype(float)
    for ic in int_cols:
        features_df[ic] = features_df[ic].astype(int)

    # 6. Verify column ordering: metadata first, then 11 features, then tracking columns
    output_columns = [
        "product_category",
        "year_week",
        "week_start_date",
        *FEATURE_COLUMNS,
        "history_weeks_count",
        "feature_status"
    ]
    if "demand" in features_df.columns:
        output_columns.append("demand")
    if "order_count" in features_df.columns:
        output_columns.append("order_count")
    if "is_observed" in features_df.columns:
        output_columns.append("is_observed")

    return features_df[output_columns]
