"""
SupplySense AI - Naive Forecasting Baseline

Implements a previous-period one-step-ahead rolling naive forecast baseline:
    predicted_demand(C, T) = observed_demand(C, T - 1 week)

Where T - 1 week is the immediately preceding calendar week (T - 7 days).
If category C had no orders in the preceding calendar week, observed demand is 0.

Evaluates predictions using standard regression metrics:
    - MAE (Mean Absolute Error)
    - RMSE (Root Mean Squared Error)
    - R² (Coefficient of Determination)

Outputs:
    - data/processed/baseline_validation_predictions.csv
    - data/processed/baseline_test_predictions.csv
    - ml/results/baseline_metrics.csv
    - ml/results/baseline_metrics.json
"""

import os
import sys
import csv
import json
import math
import datetime
import logging
from typing import List, Dict, Any, Tuple, Optional

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Base paths relative to repository root
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
PROCESSED_DIR = os.path.join(REPO_ROOT, 'data/processed')
RESULTS_DIR = os.path.join(REPO_ROOT, 'ml/results')


def load_dataset(filepath: str) -> List[Dict[str, Any]]:
    """Load forecasting CSV dataset into a list of row dictionaries."""
    rows = []
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            row['demand'] = int(row['demand'])
            rows.append(row)
    return rows


def get_previous_calendar_week(week_start_date_str: str) -> str:
    """Return the ISO date string of the Monday 7 days prior."""
    dt = datetime.date.fromisoformat(week_start_date_str)
    return (dt - datetime.timedelta(days=7)).isoformat()


def calculate_metrics(actuals: List[float], predictions: List[float]) -> Dict[str, float]:
    """Calculate MAE, RMSE, and R² for numeric sequences.

    Args:
        actuals: List of ground-truth target values.
        predictions: List of model predictions.

    Returns:
        Dict with keys 'mae', 'rmse', 'r2'.
    """
    if len(actuals) == 0:
        raise ValueError("Cannot calculate metrics on empty lists.")
    if len(actuals) != len(predictions):
        raise ValueError(f"Length mismatch: {len(actuals)} actuals vs {len(predictions)} predictions.")

    n = len(actuals)
    abs_errors = [abs(a - p) for a, p in zip(actuals, predictions)]
    squared_errors = [(a - p) ** 2 for a, p in zip(actuals, predictions)]

    mae = sum(abs_errors) / n
    rmse = math.sqrt(sum(squared_errors) / n)

    # R² calculation: 1 - (SS_res / SS_tot)
    mean_actual = sum(actuals) / n
    ss_tot = sum((a - mean_actual) ** 2 for a in actuals)
    ss_res = sum(squared_errors)

    if ss_tot == 0:
        # Constant actuals: R² is not defined in standard form; return 0.0
        r2 = 0.0
    else:
        r2 = 1.0 - (ss_res / ss_tot)

    return {
        'mae': mae,
        'rmse': rmse,
        'r2': r2
    }


def generate_naive_predictions(
    history_rows: List[Dict[str, Any]],
    target_rows: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Generate one-step-ahead naive predictions for target_rows.

    For each row in target_rows (processed in chronological order):
        1. Identify the immediately preceding calendar week (T - 7 days).
        2. Look up historical demand for (category, previous_week).
        3. If no demand was recorded in that week, demand is 0.
        4. Record prediction.
        5. Update rolling history with the actual demand of this row for subsequent weeks.

    Args:
        history_rows: Initial historical observations (e.g., Train set).
        target_rows: Evaluation observations (e.g., Validation or Test set).

    Returns:
        List of prediction dicts containing:
            product_category, year_week, week_start_date, actual_demand, predicted_demand
    """
    # Build historical lookup: (product_category, week_start_date) -> demand
    rolling_history: Dict[Tuple[str, str], int] = {}
    for r in history_rows:
        rolling_history[(r['product_category'], r['week_start_date'])] = int(r['demand'])

    # Sort target rows chronologically to preserve one-step-ahead temporal ordering
    sorted_targets = sorted(target_rows, key=lambda r: (r['week_start_date'], r['product_category']))

    predictions: List[Dict[str, Any]] = []

    for row in sorted_targets:
        category = row['product_category']
        current_week = row['week_start_date']
        actual_demand = int(row['demand'])

        prev_week = get_previous_calendar_week(current_week)
        # Naive forecast: demand in previous calendar week, or 0 if no orders were placed
        pred_demand = rolling_history.get((category, prev_week), 0)

        predictions.append({
            'product_category': category,
            'year_week': row.get('year_week', ''),
            'week_start_date': current_week,
            'actual_demand': actual_demand,
            'predicted_demand': pred_demand
        })

        # Causal update: after predicting current_week, its actual value becomes known
        # and is available for predicting future weeks (T + 1)
        rolling_history[(category, current_week)] = actual_demand

    return predictions


def save_predictions_to_csv(predictions: List[Dict[str, Any]], filepath: str) -> None:
    """Save prediction records to CSV."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    fieldnames = ['product_category', 'year_week', 'week_start_date', 'actual_demand', 'predicted_demand']
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(predictions)
    logger.info(f"Saved {len(predictions)} predictions to {filepath}")


def save_metrics_to_files(
    results: List[Dict[str, Any]],
    metadata: Dict[str, Any],
    csv_path: str,
    json_path: str
) -> None:
    """Save metrics to both CSV and JSON formats."""
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    os.makedirs(os.path.dirname(json_path), exist_ok=True)

    # Save CSV
    csv_fieldnames = ['model', 'split', 'prediction_count', 'mae', 'rmse', 'r2']
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=csv_fieldnames)
        writer.writeheader()
        for res in results:
            writer.writerow({
                'model': res['model'],
                'split': res['split'],
                'prediction_count': res['prediction_count'],
                'mae': round(res['mae'], 4),
                'rmse': round(res['rmse'], 4),
                'r2': round(res['r2'], 4)
            })
    logger.info(f"Saved metrics CSV to {csv_path}")

    # Save JSON
    full_report = {
        'metadata': metadata,
        'results': results
    }
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(full_report, f, indent=2)
    logger.info(f"Saved metrics JSON to {json_path}")


def run_baseline() -> Dict[str, Any]:
    """Execute end-to-end baseline evaluation on Train, Validation, and Test."""
    logger.info("=" * 60)
    logger.info("  SUPPLYSENSE AI - NAIVE BASELINE EVALUATION")
    logger.info("=" * 60)

    train_path = os.path.join(PROCESSED_DIR, 'train.csv')
    val_path = os.path.join(PROCESSED_DIR, 'validation.csv')
    test_path = os.path.join(PROCESSED_DIR, 'test.csv')

    train_rows = load_dataset(train_path)
    val_rows = load_dataset(val_path)
    test_rows = load_dataset(test_path)

    logger.info(f"Loaded Train: {len(train_rows)} rows")
    logger.info(f"Loaded Validation: {len(val_rows)} rows")
    logger.info(f"Loaded Test: {len(test_rows)} rows")

    # 1. Validation Predictions (History: Train rows)
    val_predictions = generate_naive_predictions(
        history_rows=train_rows,
        target_rows=val_rows
    )
    val_actuals = [float(p['actual_demand']) for p in val_predictions]
    val_preds = [float(p['predicted_demand']) for p in val_predictions]
    val_metrics = calculate_metrics(val_actuals, val_preds)

    logger.info("\n--- Validation Results ---")
    logger.info(f"  Count: {len(val_predictions)}")
    logger.info(f"  MAE:   {val_metrics['mae']:.4f}")
    logger.info(f"  RMSE:  {val_metrics['rmse']:.4f}")
    logger.info(f"  R²:    {val_metrics['r2']:.4f}")

    val_pred_path = os.path.join(PROCESSED_DIR, 'baseline_validation_predictions.csv')
    save_predictions_to_csv(val_predictions, val_pred_path)

    # 2. Test Predictions (History: Train + Validation rows)
    # The test period evaluation strictly uses all prior history (Train + Validation)
    test_predictions = generate_naive_predictions(
        history_rows=train_rows + val_rows,
        target_rows=test_rows
    )
    test_actuals = [float(p['actual_demand']) for p in test_predictions]
    test_preds = [float(p['predicted_demand']) for p in test_predictions]
    test_metrics = calculate_metrics(test_actuals, test_preds)

    logger.info("\n--- Test Results ---")
    logger.info(f"  Count: {len(test_predictions)}")
    logger.info(f"  MAE:   {test_metrics['mae']:.4f}")
    logger.info(f"  RMSE:  {test_metrics['rmse']:.4f}")
    logger.info(f"  R²:    {test_metrics['r2']:.4f}")

    test_pred_path = os.path.join(PROCESSED_DIR, 'baseline_test_predictions.csv')
    save_predictions_to_csv(test_predictions, test_pred_path)

    # 3. Save Summary Metrics
    results = [
        {
            'model': 'Naive Baseline',
            'split': 'Validation',
            'prediction_count': len(val_predictions),
            'mae': val_metrics['mae'],
            'rmse': val_metrics['rmse'],
            'r2': val_metrics['r2']
        },
        {
            'model': 'Naive Baseline',
            'split': 'Test',
            'prediction_count': len(test_predictions),
            'mae': test_metrics['mae'],
            'rmse': test_metrics['rmse'],
            'r2': test_metrics['r2']
        }
    ]

    metadata = {
        'model': 'Naive Baseline',
        'target': 'demand',
        'entity': 'product_category',
        'time_granularity': 'Weekly (ISO calendar week)',
        'baseline_definition': 'predicted_demand(C, T) = observed_demand(C, T - 1 week)',
        'evaluation_method': 'one-step-ahead rolling naive forecast',
        'missing_handling': 'zero demand for unobserved preceding calendar weeks',
        'created_at': datetime.datetime.now().isoformat()
    }

    metrics_csv = os.path.join(RESULTS_DIR, 'baseline_metrics.csv')
    metrics_json = os.path.join(RESULTS_DIR, 'baseline_metrics.json')
    save_metrics_to_files(results, metadata, metrics_csv, metrics_json)

    logger.info("\nBaseline execution completed successfully.")
    return {
        'validation': val_metrics,
        'test': test_metrics,
        'results': results
    }


if __name__ == '__main__':
    run_baseline()
