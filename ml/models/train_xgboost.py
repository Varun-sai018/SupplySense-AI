"""
SupplySense AI - XGBoost Demand Forecasting Pipeline

Trains an XGBoost Regressor on leak-free lag, rolling window, trend ratio,
and calendar features, strictly partitioned chronologically into:
  - Train:      2016-09-12 -> 2018-02-12
  - Validation: 2018-02-19 -> 2018-05-21
  - Test:       2018-05-28 -> 2018-08-27

Outputs:
  - ml/models/xgboost_demand_model.json
  - data/processed/xgboost_validation_predictions.csv
  - data/processed/xgboost_test_predictions.csv
  - ml/results/xgboost_metrics.json
  - ml/results/model_comparison.json
"""

import os
import sys
import json
import math
import datetime
import logging
import pandas as pd
import numpy as np
import xgboost as xgb
from typing import Dict, Any, Tuple, Optional

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from ml.features.engineer_features import build_features
from ml.baseline.naive_baseline import calculate_metrics

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

PROCESSED_DIR = os.path.join(REPO_ROOT, 'data', 'processed')
RESULTS_DIR = os.path.join(REPO_ROOT, 'ml', 'results')
MODELS_DIR = os.path.join(REPO_ROOT, 'ml', 'models')

FEATURE_COLUMNS = [
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

TARGET_COLUMN = 'demand'

# Strict Chronological Split Boundaries
TRAIN_END_DATE = '2018-02-12'
VAL_START_DATE = '2018-02-19'
VAL_END_DATE = '2018-05-21'
TEST_START_DATE = '2018-05-28'
TEST_END_DATE = '2018-08-27'


def train_and_evaluate_xgboost(
    features_df: Optional[pd.DataFrame] = None,
    n_estimators: int = 500,
    learning_rate: float = 0.05,
    max_depth: int = 6,
    subsample: float = 0.8,
    colsample_bytree: float = 0.8,
    random_state: int = 42,
    early_stopping_rounds: int = 30
) -> Dict[str, Any]:
    """
    Executes the complete XGBoost training, evaluation, comparison, and serialization pipeline.
    """
    logger.info("=" * 60)
    logger.info("  SUPPLYSENSE AI - XGBOOST DEMAND FORECASTING PIPELINE")
    logger.info("=" * 60)

    # 1. Load or Generate Features
    if features_df is None:
        features_csv = os.path.join(PROCESSED_DIR, 'forecasting_features.csv')
        if not os.path.isfile(features_csv):
            logger.info("Feature dataset not found on disk. Building features...")
            features_df, _ = build_features(output_path=features_csv, train_end_date=TRAIN_END_DATE)
        else:
            logger.info(f"Loading features from {features_csv}...")
            features_df = pd.read_csv(features_csv)

    # 2. Chronological Splits
    val_csv_path = os.path.join(PROCESSED_DIR, 'validation.csv')
    test_csv_path = os.path.join(PROCESSED_DIR, 'test.csv')

    unique_dates = sorted(features_df['week_start_date'].unique())
    if unique_dates[0] <= TRAIN_END_DATE and unique_dates[-1] >= TEST_START_DATE:
        train_df = features_df[features_df['week_start_date'] <= TRAIN_END_DATE].copy()
        if os.path.isfile(val_csv_path) and os.path.isfile(test_csv_path):
            val_targets = pd.read_csv(val_csv_path)[['product_category', 'week_start_date']]
            test_targets = pd.read_csv(test_csv_path)[['product_category', 'week_start_date']]
            val_df = pd.merge(val_targets, features_df, on=['product_category', 'week_start_date'], how='inner')
            test_df = pd.merge(test_targets, features_df, on=['product_category', 'week_start_date'], how='inner')
        else:
            val_df = features_df[
                (features_df['week_start_date'] >= VAL_START_DATE) &
                (features_df['week_start_date'] <= VAL_END_DATE)
            ].copy()
            test_df = features_df[
                (features_df['week_start_date'] >= TEST_START_DATE) &
                (features_df['week_start_date'] <= TEST_END_DATE)
            ].copy()
    else:
        # Relative chronological split (70% / 15% / 15%) for custom / synthetic inputs
        n_dates = len(unique_dates)
        t_idx = max(1, int(n_dates * 0.70))
        v_idx = max(t_idx + 1, int(n_dates * 0.85))
        train_dates = set(unique_dates[:t_idx])
        val_dates = set(unique_dates[t_idx:v_idx])
        test_dates = set(unique_dates[v_idx:])
        train_df = features_df[features_df['week_start_date'].isin(train_dates)].copy()
        val_df = features_df[features_df['week_start_date'].isin(val_dates)].copy()
        test_df = features_df[features_df['week_start_date'].isin(test_dates)].copy()

    logger.info(f"Partition Sizes - Train: {len(train_df)}, Validation: {len(val_df)}, Test: {len(test_df)}")

    X_train = train_df[FEATURE_COLUMNS]
    y_train = train_df[TARGET_COLUMN]

    X_val = val_df[FEATURE_COLUMNS]
    y_val = val_df[TARGET_COLUMN]

    X_test = test_df[FEATURE_COLUMNS]
    y_test = test_df[TARGET_COLUMN]

    # 3. Model Training with Early Stopping on Validation
    model = xgb.XGBRegressor(
        n_estimators=n_estimators,
        learning_rate=learning_rate,
        max_depth=max_depth,
        subsample=subsample,
        colsample_bytree=colsample_bytree,
        random_state=random_state,
        objective='reg:squarederror',
        early_stopping_rounds=early_stopping_rounds,
        eval_metric='rmse'
    )

    logger.info("Training XGBRegressor...")
    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        verbose=False
    )
    best_iteration = getattr(model, 'best_iteration', n_estimators)
    logger.info(f"Training completed. Best iteration: {best_iteration}")

    # 4. Predictions
    val_preds = np.maximum(0.0, model.predict(X_val))
    test_preds = np.maximum(0.0, model.predict(X_test))

    # 5. Metrics Calculation
    val_metrics = calculate_metrics(y_val.tolist(), val_preds.tolist())
    test_metrics = calculate_metrics(y_test.tolist(), test_preds.tolist())

    logger.info("\n--- XGBoost Validation Results ---")
    logger.info(f"  Count: {len(val_preds)}")
    logger.info(f"  MAE:   {val_metrics['mae']:.4f}")
    logger.info(f"  RMSE:  {val_metrics['rmse']:.4f}")
    logger.info(f"  R²:    {val_metrics['r2']:.4f}")

    logger.info("\n--- XGBoost Test Results ---")
    logger.info(f"  Count: {len(test_preds)}")
    logger.info(f"  MAE:   {test_metrics['mae']:.4f}")
    logger.info(f"  RMSE:  {test_metrics['rmse']:.4f}")
    logger.info(f"  R²:    {test_metrics['r2']:.4f}")

    # 6. Save Predictions to CSV
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    val_pred_df = val_df[['product_category', 'week_start_date']].copy()
    val_pred_df['actual_demand'] = y_val.values
    val_pred_df['predicted_demand'] = val_preds
    val_pred_path = os.path.join(PROCESSED_DIR, 'xgboost_validation_predictions.csv')
    val_pred_df.to_csv(val_pred_path, index=False)
    logger.info(f"Saved validation predictions to {val_pred_path}")

    test_pred_df = test_df[['product_category', 'week_start_date']].copy()
    test_pred_df['actual_demand'] = y_test.values
    test_pred_df['predicted_demand'] = test_preds
    test_pred_path = os.path.join(PROCESSED_DIR, 'xgboost_test_predictions.csv')
    test_pred_df.to_csv(test_pred_path, index=False)
    logger.info(f"Saved test predictions to {test_pred_path}")

    # 7. Save Serialized Model
    os.makedirs(MODELS_DIR, exist_ok=True)
    model_path = os.path.join(MODELS_DIR, 'xgboost_demand_model.json')
    model.save_model(model_path)
    logger.info(f"Saved trained XGBoost model to {model_path}")

    # 8. Save Metrics & Benchmark Comparison
    os.makedirs(RESULTS_DIR, exist_ok=True)

    # Load Baseline Metrics
    baseline_json_path = os.path.join(RESULTS_DIR, 'baseline_metrics.json')
    naive_val = {"MAE": 7.9802, "RMSE": 15.3196, "R2": 0.8992}
    naive_test = {"MAE": 9.4636, "RMSE": 17.9912, "R2": 0.8351}

    if os.path.isfile(baseline_json_path):
        try:
            with open(baseline_json_path, 'r') as f:
                base_data = json.load(f)
                for r in base_data.get('results', []):
                    if r.get('split') == 'Validation':
                        naive_val = {"MAE": r['mae'], "RMSE": r['rmse'], "R2": r['r2']}
                    elif r.get('split') == 'Test':
                        naive_test = {"MAE": r['mae'], "RMSE": r['rmse'], "R2": r['r2']}
        except Exception as e:
            logger.warning(f"Could not load baseline JSON: {e}")

    comparison_report = {
        "validation": {
            "naive_baseline": naive_val,
            "xgboost": {
                "MAE": round(val_metrics['mae'], 4),
                "RMSE": round(val_metrics['rmse'], 4),
                "R2": round(val_metrics['r2'], 4)
            }
        },
        "test": {
            "naive_baseline": naive_test,
            "xgboost": {
                "MAE": round(test_metrics['mae'], 4),
                "RMSE": round(test_metrics['rmse'], 4),
                "R2": round(test_metrics['r2'], 4)
            }
        }
    }

    comparison_path = os.path.join(RESULTS_DIR, 'model_comparison.json')
    with open(comparison_path, 'w') as f:
        json.dump(comparison_report, f, indent=2)
    logger.info(f"Saved model comparison report to {comparison_path}")

    # Save XGBoost metrics
    xgb_metrics_report = {
        "metadata": {
            "model": "XGBoost Regressor",
            "target": "demand",
            "features": FEATURE_COLUMNS,
            "hyperparameters": {
                "n_estimators": n_estimators,
                "learning_rate": learning_rate,
                "max_depth": max_depth,
                "subsample": subsample,
                "colsample_bytree": colsample_bytree,
                "random_state": random_state,
                "best_iteration": int(best_iteration)
            },
            "created_at": datetime.datetime.now().isoformat()
        },
        "results": [
            {
                "model": "XGBoost",
                "split": "Validation",
                "prediction_count": len(val_preds),
                "mae": val_metrics['mae'],
                "rmse": val_metrics['rmse'],
                "r2": val_metrics['r2']
            },
            {
                "model": "XGBoost",
                "split": "Test",
                "prediction_count": len(test_preds),
                "mae": test_metrics['mae'],
                "rmse": test_metrics['rmse'],
                "r2": test_metrics['r2']
            }
        ]
    }

    xgb_metrics_path = os.path.join(RESULTS_DIR, 'xgboost_metrics.json')
    with open(xgb_metrics_path, 'w') as f:
        json.dump(xgb_metrics_report, f, indent=2)
    logger.info(f"Saved XGBoost metrics to {xgb_metrics_path}")

    return {
        "model": model,
        "validation_metrics": val_metrics,
        "test_metrics": test_metrics,
        "comparison": comparison_report,
        "artifacts": {
            "model_path": os.path.relpath(model_path, REPO_ROOT).replace('\\', '/'),
            "val_pred_path": os.path.relpath(val_pred_path, REPO_ROOT).replace('\\', '/'),
            "test_pred_path": os.path.relpath(test_pred_path, REPO_ROOT).replace('\\', '/'),
            "metrics_path": os.path.relpath(xgb_metrics_path, REPO_ROOT).replace('\\', '/'),
            "comparison_path": os.path.relpath(comparison_path, REPO_ROOT).replace('\\', '/')
        }
    }


if __name__ == '__main__':
    train_and_evaluate_xgboost()
