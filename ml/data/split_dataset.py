"""
SupplySense AI - Chronological Dataset Split

Splits the forecasting dataset into Train / Validation / Test
sets using a strict chronological split based on week_start_date.

Split ratios (by unique time periods):
    Train:       70%
    Validation:  15%
    Test:        15%

Usage:
    python ml/data/split_dataset.py
"""

import os
import sys
import csv
import logging

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

PROCESSED_DIR = os.path.join(os.path.dirname(__file__), '../../data/processed')


def load_dataset(filepath):
    """Load the forecasting dataset CSV into a list of dicts."""
    rows = []
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            row['demand'] = int(row['demand'])
            rows.append(row)
    return rows


def chronological_split(rows, train_ratio=0.70, val_ratio=0.15):
    """Split rows chronologically based on unique week_start_date values.

    Args:
        rows: list of dicts, must contain 'week_start_date'
        train_ratio: proportion of time periods for training
        val_ratio: proportion of time periods for validation

    Returns:
        (train_rows, val_rows, test_rows)
    """
    # Get sorted unique time periods
    unique_weeks = sorted(set(row['week_start_date'] for row in rows))
    n_weeks = len(unique_weeks)

    if n_weeks < 3:
        raise ValueError(f"Need at least 3 distinct weeks for a 3-way split, got {n_weeks}")

    train_end_idx = max(1, int(n_weeks * train_ratio))
    val_end_idx = max(train_end_idx + 1, int(n_weeks * (train_ratio + val_ratio)))

    train_weeks = set(unique_weeks[:train_end_idx])
    val_weeks = set(unique_weeks[train_end_idx:val_end_idx])
    test_weeks = set(unique_weeks[val_end_idx:])

    if len(test_weeks) == 0:
        # Ensure at least 1 week in test
        last_val = unique_weeks[val_end_idx - 1]
        val_weeks.discard(last_val)
        test_weeks.add(last_val)

    train_rows = [r for r in rows if r['week_start_date'] in train_weeks]
    val_rows = [r for r in rows if r['week_start_date'] in val_weeks]
    test_rows = [r for r in rows if r['week_start_date'] in test_weeks]

    return train_rows, val_rows, test_rows


def validate_split(train_rows, val_rows, test_rows):
    """Validate the chronological split for correctness."""
    errors = []

    # 1. All three sets must have data
    if len(train_rows) == 0:
        errors.append("Train set is empty")
    if len(val_rows) == 0:
        errors.append("Validation set is empty")
    if len(test_rows) == 0:
        errors.append("Test set is empty")

    if errors:
        raise ValueError("Split validation failed: " + "; ".join(errors))

    # 2. Required columns
    required_cols = {'product_category', 'year_week', 'week_start_date', 'demand'}
    for name, dataset in [('train', train_rows), ('val', val_rows), ('test', test_rows)]:
        actual_cols = set(dataset[0].keys())
        missing = required_cols - actual_cols
        if missing:
            errors.append(f"{name} missing columns: {missing}")

    # 3. Demand values are valid
    for name, dataset in [('train', train_rows), ('val', val_rows), ('test', test_rows)]:
        for row in dataset:
            if not isinstance(row['demand'], (int, float)) or row['demand'] < 0:
                errors.append(f"{name}: invalid demand value {row['demand']}")
                break

    # 4. Temporal ordering: train < validation < test
    train_dates = sorted(set(r['week_start_date'] for r in train_rows))
    val_dates = sorted(set(r['week_start_date'] for r in val_rows))
    test_dates = sorted(set(r['week_start_date'] for r in test_rows))

    if train_dates[-1] >= val_dates[0]:
        errors.append(f"Temporal overlap: train ends {train_dates[-1]}, val starts {val_dates[0]}")
    if val_dates[-1] >= test_dates[0]:
        errors.append(f"Temporal overlap: val ends {val_dates[-1]}, test starts {test_dates[0]}")

    # 5. No overlap in date sets
    train_set = set(train_dates)
    val_set = set(val_dates)
    test_set = set(test_dates)
    if train_set & val_set:
        errors.append(f"Train/val share dates: {train_set & val_set}")
    if val_set & test_set:
        errors.append(f"Val/test share dates: {val_set & test_set}")
    if train_set & test_set:
        errors.append(f"Train/test share dates: {train_set & test_set}")

    if errors:
        raise ValueError("Split validation FAILED:\n  " + "\n  ".join(errors))

    logger.info("  Split validation PASSED")


def save_split(train_rows, val_rows, test_rows, output_dir=None):
    """Save the three datasets to CSV files."""
    if output_dir is None:
        output_dir = PROCESSED_DIR

    os.makedirs(output_dir, exist_ok=True)
    fieldnames = ['product_category', 'year_week', 'week_start_date', 'demand']

    for name, dataset in [('train', train_rows), ('validation', val_rows), ('test', test_rows)]:
        filepath = os.path.join(output_dir, f'{name}.csv')
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(dataset)
        logger.info(f"  Saved {name}.csv ({len(dataset)} rows)")


def split(input_path=None, output_dir=None):
    """Full split pipeline: load -> split -> validate -> save."""
    logger.info("=" * 60)
    logger.info("  SUPPLYSENSE AI - CHRONOLOGICAL DATASET SPLIT")
    logger.info("=" * 60)

    if input_path is None:
        input_path = os.path.join(PROCESSED_DIR, 'forecasting_dataset.csv')
    if output_dir is None:
        output_dir = PROCESSED_DIR

    logger.info(f"\nLoading dataset from: {input_path}")
    rows = load_dataset(input_path)
    logger.info(f"  Loaded {len(rows)} rows")

    logger.info("\nSplitting chronologically (70/15/15)...")
    train_rows, val_rows, test_rows = chronological_split(rows)

    # Report
    for name, dataset in [('Train', train_rows), ('Validation', val_rows), ('Test', test_rows)]:
        dates = sorted(set(r['week_start_date'] for r in dataset))
        logger.info(f"\n  {name}:")
        logger.info(f"    Rows:       {len(dataset)}")
        logger.info(f"    Weeks:      {len(dates)}")
        logger.info(f"    Start date: {dates[0]}")
        logger.info(f"    End date:   {dates[-1]}")

    logger.info("\nValidating split...")
    validate_split(train_rows, val_rows, test_rows)

    logger.info("\nSaving split datasets...")
    save_split(train_rows, val_rows, test_rows, output_dir)

    logger.info("\nDataset split complete.")
    return train_rows, val_rows, test_rows


if __name__ == '__main__':
    split()
