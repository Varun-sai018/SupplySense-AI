"""
SupplySense AI - Feature Engineering Module

Constructs leak-free lag, rolling window, trend ratio, calendar, and category features
for weekly product category demand forecasting on a continuous calendar grid.
"""

import os
import sys
import datetime
import logging
import pandas as pd
import numpy as np
from typing import Tuple, Dict, Any, Optional

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

DEFAULT_INPUT_PATH = os.path.join(REPO_ROOT, 'data', 'processed', 'forecasting_dataset.csv')
DEFAULT_OUTPUT_PATH = os.path.join(REPO_ROOT, 'data', 'processed', 'forecasting_features.csv')


def create_continuous_grid(df: pd.DataFrame) -> pd.DataFrame:
    """
    Expands the input dataset onto a complete (product_category x week_start_date) grid.
    Fills unobserved category-week demand with 0.

    Args:
        df: DataFrame with ['product_category', 'week_start_date', 'demand']

    Returns:
        Sorted DataFrame containing full continuous weekly observations for every category.
    """
    df = df.copy()
    df['week_start_date'] = pd.to_datetime(df['week_start_date']).dt.strftime('%Y-%m-%d')
    
    categories = sorted(df['product_category'].unique())
    min_date = pd.to_datetime(df['week_start_date'].min())
    max_date = pd.to_datetime(df['week_start_date'].max())
    
    # Generate weekly calendar of Mondays
    all_mondays = pd.date_range(start=min_date, end=max_date, freq='W-MON').strftime('%Y-%m-%d')
    
    # Full Cartesian product
    full_index = pd.MultiIndex.from_product(
        [categories, all_mondays],
        names=['product_category', 'week_start_date']
    ).to_frame().reset_index(drop=True)
    
    # Merge existing observations
    merged = pd.merge(full_index, df, on=['product_category', 'week_start_date'], how='left')
    
    # Fill missing demand with 0
    merged['demand'] = merged['demand'].fillna(0).astype(int)
    
    # Recompute year_week if missing
    def _compute_year_week(date_str):
        dt = datetime.date.fromisoformat(date_str)
        iso = dt.isocalendar()
        return f"{iso[0]}-W{iso[1]:02d}"
    
    if 'year_week' not in merged.columns or merged['year_week'].isnull().any():
        merged['year_week'] = merged['week_start_date'].apply(_compute_year_week)
        
    merged = merged.sort_values(['product_category', 'week_start_date']).reset_index(drop=True)
    return merged


def engineer_lag_and_rolling_features(grid_df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes lag, rolling window, trend ratio, and calendar features strictly using past observations.

    Features created:
      1. lag_1_demand: demand at T-1
      2. lag_2_demand: demand at T-2
      3. lag_4_demand: demand at T-4
      4. rolling_mean_4w: mean demand over [T-4, T-1]
      5. rolling_std_4w: standard deviation over [T-4, T-1]
      6. rolling_mean_8w: mean demand over [T-8, T-1]
      7. demand_trend_ratio: (lag_1_demand + 1) / (rolling_mean_4w + 1)
      8. month_of_year: Month 1-12
      9. week_of_year: ISO week 1-53
      10. quarter: Quarter 1-4

    Args:
        grid_df: DataFrame on a continuous weekly grid.

    Returns:
        DataFrame with newly engineered feature columns.
    """
    df = grid_df.copy()
    df = df.sort_values(['product_category', 'week_start_date']).reset_index(drop=True)

    # Grouped lag and rolling calculations
    grouped = df.groupby('product_category')['demand']

    # 1. Lags (strictly shifted)
    df['lag_1_demand'] = grouped.shift(1).fillna(0.0).astype(float)
    df['lag_2_demand'] = grouped.shift(2).fillna(0.0).astype(float)
    df['lag_4_demand'] = grouped.shift(4).fillna(0.0).astype(float)

    # 2. Rolling features (shift(1) ensures no lookahead of current week T)
    # rolling(4) applied to shift(1) computes statistics over T-4 through T-1
    df['rolling_mean_4w'] = grouped.apply(
        lambda s: s.shift(1).rolling(window=4, min_periods=1).mean().fillna(0.0)
    ).reset_index(level=0, drop=True).astype(float)

    df['rolling_std_4w'] = grouped.apply(
        lambda s: s.shift(1).rolling(window=4, min_periods=1).std().fillna(0.0)
    ).reset_index(level=0, drop=True).astype(float)

    df['rolling_mean_8w'] = grouped.apply(
        lambda s: s.shift(1).rolling(window=8, min_periods=1).mean().fillna(0.0)
    ).reset_index(level=0, drop=True).astype(float)

    # 3. Trend Ratio
    df['demand_trend_ratio'] = (
        (df['lag_1_demand'] + 1.0) / (df['rolling_mean_4w'] + 1.0)
    ).astype(float)

    # 4. Calendar Features
    dt_series = pd.to_datetime(df['week_start_date'])
    df['month_of_year'] = dt_series.dt.month.astype(int)
    df['week_of_year'] = dt_series.dt.isocalendar().week.astype(int)
    df['quarter'] = dt_series.dt.quarter.astype(int)

    return df


def encode_categories(
    df: pd.DataFrame,
    category_mapping: Optional[Dict[str, int]] = None,
    train_end_date: Optional[str] = None
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Applies deterministic label encoding to product categories.
    If category_mapping is None, fits on categories observed on or before train_end_date.

    Args:
        df: DataFrame with 'product_category' column.
        category_mapping: Pre-fitted mapping {category_name: int_code}.
        train_end_date: ISO date string for training boundary to fit categories on.

    Returns:
        Tuple of (DataFrame with 'product_category_enc', category_mapping dictionary).
    """
    df = df.copy()

    if category_mapping is None:
        if train_end_date is not None:
            train_cats = sorted(df[df['week_start_date'] <= train_end_date]['product_category'].unique())
        else:
            train_cats = sorted(df['product_category'].unique())
        category_mapping = {cat: idx for idx, cat in enumerate(train_cats)}

    # Map categories; unseen categories mapped to -1
    df['product_category_enc'] = df['product_category'].map(category_mapping).fillna(-1).astype(int)
    return df, category_mapping


def build_features(
    input_path: Optional[str] = None,
    output_path: Optional[str] = None,
    train_end_date: str = '2018-02-12'
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Full feature engineering pipeline:
      1. Loads raw aggregated forecasting CSV.
      2. Expands onto continuous weekly grid (0-imputed demand).
      3. Computes causal lag, rolling, trend ratio, and calendar features.
      4. Fits category encoding on training partition.
      5. Optionally saves output to forecasting_features.csv.

    Args:
        input_path: Path to input forecasting_dataset.csv.
        output_path: Path to save engineered features CSV.
        train_end_date: End date of training split for category encoding fit.

    Returns:
        Tuple of (features DataFrame, category mapping).
    """
    if input_path is None:
        input_path = DEFAULT_INPUT_PATH
    if output_path is None:
        output_path = DEFAULT_OUTPUT_PATH

    logger.info(f"Loading forecasting dataset from {input_path}...")
    df = pd.read_csv(input_path)
    logger.info(f"Loaded {len(df)} rows across {df['product_category'].nunique()} categories.")

    logger.info("Constructing continuous weekly grid...")
    grid_df = create_continuous_grid(df)
    logger.info(f"Continuous grid size: {len(grid_df)} rows ({grid_df['week_start_date'].nunique()} weeks).")

    logger.info("Engineering lag, rolling, trend, and calendar features...")
    features_df = engineer_lag_and_rolling_features(grid_df)

    logger.info(f"Fitting category encoding on training partition (<= {train_end_date})...")
    features_df, cat_mapping = encode_categories(features_df, train_end_date=train_end_date)
    logger.info(f"Encoded {len(cat_mapping)} training categories.")

    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        features_df.to_csv(output_path, index=False)
        logger.info(f"Saved {len(features_df)} feature rows to {output_path}")

    return features_df, cat_mapping


if __name__ == '__main__':
    build_features()
