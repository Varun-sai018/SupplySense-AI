"""
SupplySense AI - Recursive Forward Forecasting Engine

Executes recursive multi-horizon forward inference using the trained XGBoost model:
  - 7-Day Business Horizon (Step 1 Ahead: W+1)
  - 14-Day Business Horizon (Step 2 Ahead: W+2)

Key Features:
  - Preserves weekly granularity (one prediction per category per forecast week).
  - Feeds W+1 predicted demand into W+2 lag_1 and updates rolling metrics.
  - Strict non-negative clipping (np.maximum(0.0, pred)).
  - Respects inference eligibility and cold-start fallback policy.
  - Generates full traceability metadata (execution_id, horizons, provenance).
"""

import os
import sys
import datetime
import logging
from typing import Dict, Any, List, Optional, Tuple
import pandas as pd
import numpy as np
import xgboost as xgb

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from ml.features.feature_engineering import FEATURE_COLUMNS, load_category_mapping
from .eligibility import check_feature_eligibility, compute_cold_start_fallback

logger = logging.getLogger(__name__)


def generate_forward_forecasts(
    features_df: pd.DataFrame,
    model: xgb.XGBRegressor,
    horizons: Optional[List[str]] = None,
    category_mapping: Optional[Dict[str, int]] = None,
    model_version: str = "xgboost-v1",
    source_feature_execution_id: Optional[int] = None
) -> pd.DataFrame:
    """
    Generates recursive forward forecasts for 7-day and 14-day horizons.

    Args:
        features_df: Feature store DataFrame for active categories.
        model: Loaded XGBRegressor.
        horizons: List of horizons to forecast. Defaults to ['7_DAYS', '14_DAYS'].
        category_mapping: Pre-fitted category mapping dict.
        model_version: Model identifier.
        source_feature_execution_id: Traceability link to feature generation execution.

    Returns:
        DataFrame containing forecast records:
          - product_category
          - forecast_week (Monday date string)
          - year_week (ISO year-week)
          - predicted_demand (float >= 0.0)
          - model_name ('XGBoost')
          - model_version
          - source_feature_execution_id
          - data_provenance ('OPERATIONAL')
          - forecast_horizon ('7_DAYS' or '14_DAYS')
          - step_ahead (1 or 2)
          - inference_status ('SUCCESS', 'COLD_START_FALLBACK', 'INSUFFICIENT_HISTORY', 'UNSEEN_CATEGORY')
    """
    if horizons is None:
        horizons = ["7_DAYS", "14_DAYS"]

    if features_df.empty:
        logger.info("Empty feature DataFrame; returning empty forecasts DataFrame.")
        cols = [
            "product_category", "forecast_week", "year_week", "predicted_demand",
            "model_name", "model_version", "source_feature_execution_id",
            "data_provenance", "forecast_horizon", "step_ahead", "inference_status"
        ]
        return pd.DataFrame(columns=cols)

    if category_mapping is None:
        category_mapping = load_category_mapping()

    forecast_records: List[Dict[str, Any]] = []

    # Process each category independently
    grouped = features_df.groupby("product_category")

    for cat_name, cat_df in grouped:
        cat_sorted = cat_df.sort_values("week_start_date").reset_index(drop=True)
        latest_row = cat_sorted.iloc[-1].to_dict()

        latest_date_str = latest_row["week_start_date"]
        if isinstance(latest_date_str, datetime.date):
            latest_monday = latest_date_str
        else:
            latest_monday = datetime.date.fromisoformat(str(latest_date_str)[:10])
        cat_enc = int(latest_row.get("product_category_enc", category_mapping.get(cat_name, -1)))

        # Extract recent demand history for rolling window updates
        # If 'demand' column exists, use it; otherwise approximate from lags
        if "demand" in cat_sorted.columns:
            recent_demands = cat_sorted["demand"].tolist()[-8:]
        else:
            # Reconstruct available lags up to 4 weeks
            recent_demands = [
                float(latest_row.get("lag_4_demand", 0.0)),
                float(latest_row.get("lag_2_demand", 0.0)),
                float(latest_row.get("lag_1_demand", 0.0)),
            ]

        # ---------------------------------------------------------------------
        # Step 1: 7-Day Business Horizon (Week T+1)
        # ---------------------------------------------------------------------
        step1_monday = latest_monday + datetime.timedelta(days=7)
        iso_y1, iso_w1, _ = step1_monday.isocalendar()
        step1_yw = f"{iso_y1}-W{iso_w1:02d}"

        # Calendar features for W+1
        step1_features = {
            "lag_1_demand": float(recent_demands[-1]) if recent_demands else float(latest_row.get("lag_1_demand", 0.0)),
            "lag_2_demand": float(recent_demands[-2]) if len(recent_demands) >= 2 else float(latest_row.get("lag_2_demand", 0.0)),
            "lag_4_demand": float(recent_demands[-4]) if len(recent_demands) >= 4 else float(latest_row.get("lag_4_demand", 0.0)),
            "rolling_mean_4w": float(np.mean(recent_demands[-4:])) if recent_demands else float(latest_row.get("rolling_mean_4w", 0.0)),
            "rolling_std_4w": float(np.std(recent_demands[-4:], ddof=1)) if len(recent_demands) >= 2 else 0.0,
            "rolling_mean_8w": float(np.mean(recent_demands[-8:])) if recent_demands else float(latest_row.get("rolling_mean_8w", 0.0)),
            "product_category_enc": cat_enc,
            "month_of_year": step1_monday.month,
            "week_of_year": iso_w1,
            "quarter": (step1_monday.month - 1) // 3 + 1,
            "history_weeks_count": int(latest_row.get("history_weeks_count", 0)),
            "feature_status": str(latest_row.get("feature_status", ""))
        }
        step1_features["demand_trend_ratio"] = float(
            (step1_features["lag_1_demand"] + 1.0) / (step1_features["rolling_mean_4w"] + 1.0)
        )

        is_eligible_1, status_1, sanitized_1 = check_feature_eligibility(step1_features)

        if is_eligible_1:
            # Predict using XGBoost
            x_input = pd.DataFrame([[sanitized_1[col] for col in FEATURE_COLUMNS]], columns=FEATURE_COLUMNS)
            raw_pred_1 = float(model.predict(x_input)[0])
            pred_1 = max(0.0, round(raw_pred_1, 4))
            infer_status_1 = "SUCCESS"
        else:
            pred_1, infer_status_1 = compute_cold_start_fallback(step1_features)

        forecast_records.append({
            "product_category": cat_name,
            "forecast_week": step1_monday.isoformat(),
            "year_week": step1_yw,
            "predicted_demand": pred_1,
            "model_name": "XGBoost",
            "model_version": model_version,
            "source_feature_execution_id": source_feature_execution_id,
            "data_provenance": "OPERATIONAL",
            "forecast_horizon": "7_DAYS",
            "step_ahead": 1,
            "inference_status": infer_status_1
        })

        # ---------------------------------------------------------------------
        # Step 2: 14-Day Business Horizon (Week T+2 - Recursive Autoregression)
        # ---------------------------------------------------------------------
        if "14_DAYS" in horizons:
            step2_monday = latest_monday + datetime.timedelta(days=14)
            iso_y2, iso_w2, _ = step2_monday.isocalendar()
            step2_yw = f"{iso_y2}-W{iso_w2:02d}"

            # Append Step 1 prediction recursively into the demand sequence
            recursive_demands = recent_demands + [pred_1]

            step2_features = {
                "lag_1_demand": float(pred_1),  # Fed recursively from Step 1
                "lag_2_demand": float(recursive_demands[-2]),
                "lag_4_demand": float(recursive_demands[-4]) if len(recursive_demands) >= 4 else 0.0,
                "rolling_mean_4w": float(np.mean(recursive_demands[-4:])),
                "rolling_std_4w": float(np.std(recursive_demands[-4:], ddof=1)) if len(recursive_demands) >= 2 else 0.0,
                "rolling_mean_8w": float(np.mean(recursive_demands[-8:])),
                "product_category_enc": cat_enc,
                "month_of_year": step2_monday.month,
                "week_of_year": iso_w2,
                "quarter": (step2_monday.month - 1) // 3 + 1,
                "history_weeks_count": int(latest_row.get("history_weeks_count", 0)) + 1,
                "feature_status": str(latest_row.get("feature_status", ""))
            }
            step2_features["demand_trend_ratio"] = float(
                (step2_features["lag_1_demand"] + 1.0) / (step2_features["rolling_mean_4w"] + 1.0)
            )

            is_eligible_2, status_2, sanitized_2 = check_feature_eligibility(step2_features)

            if is_eligible_2:
                x_input_2 = pd.DataFrame([[sanitized_2[col] for col in FEATURE_COLUMNS]], columns=FEATURE_COLUMNS)
                raw_pred_2 = float(model.predict(x_input_2)[0])
                pred_2 = max(0.0, round(raw_pred_2, 4))
                infer_status_2 = "SUCCESS"
            else:
                pred_2, infer_status_2 = compute_cold_start_fallback(step2_features)

            forecast_records.append({
                "product_category": cat_name,
                "forecast_week": step2_monday.isoformat(),
                "year_week": step2_yw,
                "predicted_demand": pred_2,
                "model_name": "XGBoost",
                "model_version": model_version,
                "source_feature_execution_id": source_feature_execution_id,
                "data_provenance": "OPERATIONAL",
                "forecast_horizon": "14_DAYS",
                "step_ahead": 2,
                "inference_status": infer_status_2
            })

    return pd.DataFrame(forecast_records)
