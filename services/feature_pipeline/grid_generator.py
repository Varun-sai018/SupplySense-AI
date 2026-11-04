"""
SupplySense AI - Weekly Demand Grid Generator

Expands aggregated weekly category observations onto a complete, continuous
weekly calendar grid across all 73 model-supported categories (and any new categories).
  - Handles missing weeks consistently with explicit 0-demand imputation.
  - Distinguishes zero observed demand from unobserved weeks.
  - Preserves strict chronological ordering.
  - Computes historical observation count (history_weeks_count) strictly causal (no future leak).
"""

import os
import sys
import datetime
import logging
from typing import List, Optional, Set
import pandas as pd

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from ml.features.feature_engineering import load_category_mapping

logger = logging.getLogger(__name__)


def build_weekly_demand_grid(
    aggregated_df: pd.DataFrame,
    categories: Optional[List[str]] = None,
    min_date: Optional[str] = None,
    max_date: Optional[str] = None
) -> pd.DataFrame:
    """
    Constructs a complete continuous weekly grid of (category x week_start_date).

    Args:
        aggregated_df: DataFrame with ['product_category', 'week_start_date', 'demand_units']
                       (from aggregate_operational_orders).
        categories: Optional explicit list of categories. If None, uses all 73
                    model categories plus any observed in aggregated_df.
        min_date: Optional start Monday (ISO YYYY-MM-DD). If None, inferred from data.
        max_date: Optional end Monday (ISO YYYY-MM-DD). If None, inferred from data.

    Returns:
        DataFrame sorted by ['product_category', 'week_start_date'] containing:
          - product_category
          - week_start_date (Monday)
          - year_week (YYYY-Www)
          - demand (int, 0-filled for unobserved category-weeks)
          - order_count (int, 0-filled)
          - is_observed (bool, True if actual orders existed)
          - history_weeks_count (int, number of strictly prior weeks in grid)
    """
    # 1. Determine categories
    if categories is None:
        try:
            model_cat_map = load_category_mapping()
            cat_set = set(model_cat_map.keys())
        except Exception as e:
            logger.warning(f"Could not load authoritative category mapping: {e}. Using observed categories.")
            cat_set = set()

        if not aggregated_df.empty and "product_category" in aggregated_df.columns:
            observed_cats = set(aggregated_df["product_category"].dropna().unique())
            cat_set = cat_set.union(observed_cats)

        all_categories = sorted(list(cat_set))
    else:
        all_categories = sorted(list(set(categories)))

    if not all_categories:
        logger.warning("No categories specified or found in data. Returning empty grid.")
        return pd.DataFrame(columns=[
            "product_category", "week_start_date", "year_week",
            "demand", "order_count", "is_observed", "history_weeks_count"
        ])

    # 2. Determine calendar range
    if min_date is None or max_date is None:
        if aggregated_df.empty or "week_start_date" not in aggregated_df.columns:
            logger.info("Aggregated data is empty and no date range provided. Returning empty grid.")
            return pd.DataFrame(columns=[
                "product_category", "week_start_date", "year_week",
                "demand", "order_count", "is_observed", "history_weeks_count"
            ])
        inferred_min = pd.to_datetime(aggregated_df["week_start_date"]).min()
        inferred_max = pd.to_datetime(aggregated_df["week_start_date"]).max()
        if min_date is None:
            min_date = inferred_min.strftime('%Y-%m-%d')
        if max_date is None:
            max_date = inferred_max.strftime('%Y-%m-%d')

    # Normalize to Mondays
    dt_min = pd.to_datetime(min_date)
    dt_max = pd.to_datetime(max_date)

    if dt_min > dt_max:
        raise ValueError(f"min_date ({min_date}) cannot be greater than max_date ({max_date}).")

    all_mondays = pd.date_range(start=dt_min, end=dt_max, freq='W-MON').strftime('%Y-%m-%d').tolist()
    if not all_mondays:
        # If range was within a single week not hitting Monday freq, use the Monday of dt_min
        iso_y, iso_w, _ = dt_min.isocalendar()
        monday = datetime.date.fromisocalendar(iso_y, iso_w, 1).isoformat()
        all_mondays = [monday]

    # 3. Create full Cartesian product
    full_index = pd.MultiIndex.from_product(
        [all_categories, all_mondays],
        names=["product_category", "week_start_date"]
    ).to_frame().reset_index(drop=True)

    # 4. Prepare aggregated subset for merging
    if not aggregated_df.empty:
        obs_df = aggregated_df.copy()
        obs_df["week_start_date"] = pd.to_datetime(obs_df["week_start_date"]).dt.strftime('%Y-%m-%d')
        if "demand_units" in obs_df.columns:
            obs_df["demand"] = obs_df["demand_units"]
        elif "demand" not in obs_df.columns:
            obs_df["demand"] = 0

        if "order_count" not in obs_df.columns:
            obs_df["order_count"] = 1

        obs_subset = obs_df[["product_category", "week_start_date", "demand", "order_count"]].drop_duplicates(
            subset=["product_category", "week_start_date"]
        )
        obs_subset["is_observed"] = True

        merged = pd.merge(full_index, obs_subset, on=["product_category", "week_start_date"], how="left")
    else:
        merged = full_index.copy()
        merged["demand"] = 0
        merged["order_count"] = 0
        merged["is_observed"] = False

    merged["is_observed"] = merged["is_observed"].fillna(False).astype(bool)
    merged["demand"] = merged["demand"].fillna(0).astype(int)
    merged["order_count"] = merged["order_count"].fillna(0).astype(int)

    # 5. Compute ISO year_week
    def _compute_year_week(date_str: str) -> str:
        dt = datetime.date.fromisoformat(date_str)
        iso = dt.isocalendar()
        return f"{iso[0]}-W{iso[1]:02d}"

    merged["year_week"] = merged["week_start_date"].apply(_compute_year_week)

    # 6. Sort strictly chronologically per category
    merged = merged.sort_values(["product_category", "week_start_date"]).reset_index(drop=True)

    # 7. Compute strictly prior history weeks count (cumulative row index per category)
    merged["history_weeks_count"] = merged.groupby("product_category").cumcount()

    return merged
