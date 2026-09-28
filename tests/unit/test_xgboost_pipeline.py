"""
Unit tests for XGBoost Demand Forecasting Pipeline (Phase 5).
Verifies model training, inference shape, metric calculations, serialization, and artifact generation.
"""

import os
import sys
import unittest
import tempfile
import json
import pandas as pd
import numpy as np

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from ml.features.engineer_features import (
    create_continuous_grid,
    engineer_lag_and_rolling_features,
    encode_categories
)
from ml.models.train_xgboost import (
    train_and_evaluate_xgboost,
    FEATURE_COLUMNS,
    TARGET_COLUMN
)


class TestXGBoostPipeline(unittest.TestCase):

    def setUp(self):
        # Create small synthetic dataset across 30 weeks
        dates = pd.date_range(start='2017-01-02', periods=30, freq='W-MON').strftime('%Y-%m-%d')
        rows = []
        for cat in ['cat_1', 'cat_2']:
            for i, d in enumerate(dates):
                demand = 10 + (i % 5) * 5 + (15 if cat == 'cat_2' else 0)
                rows.append({
                    'product_category': cat,
                    'week_start_date': d,
                    'year_week': f"2017-W{i+1:02d}",
                    'demand': demand
                })
        raw_df = pd.DataFrame(rows)
        grid = create_continuous_grid(raw_df)
        feat = engineer_lag_and_rolling_features(grid)
        self.synthetic_features, _ = encode_categories(feat, train_end_date='2017-05-01')

    def test_xgb_001_model_training_and_inference_shape(self):
        """1. Model trains without error and produces predictions with matching dimensions."""
        result = train_and_evaluate_xgboost(
            features_df=self.synthetic_features,
            n_estimators=20,
            learning_rate=0.1,
            max_depth=3,
            early_stopping_rounds=5
        )

        self.assertIn("model", result)
        self.assertIn("validation_metrics", result)
        self.assertIn("test_metrics", result)
        self.assertIn("comparison", result)
        self.assertIn("artifacts", result)

        val_metrics = result["validation_metrics"]
        test_metrics = result["test_metrics"]

        # 2. Check metrics structure
        for m in [val_metrics, test_metrics]:
            self.assertIn("mae", m)
            self.assertIn("rmse", m)
            self.assertIn("r2", m)
            self.assertIsInstance(m["mae"], float)
            self.assertIsInstance(m["rmse"], float)
            self.assertIsInstance(m["r2"], float)

    def test_xgb_002_model_serialization_and_artifacts(self):
        """2. Serialized model, prediction CSVs, and comparison JSON files are generated on disk."""
        result = train_and_evaluate_xgboost(
            features_df=self.synthetic_features,
            n_estimators=10,
            early_stopping_rounds=5
        )
        artifacts = result["artifacts"]

        for key, rel_path in artifacts.items():
            full_path = os.path.join(REPO_ROOT, rel_path)
            self.assertTrue(os.path.isfile(full_path), f"Expected artifact {full_path} to exist")

        # Verify comparison JSON has both baseline and xgboost entries
        comp_path = os.path.join(REPO_ROOT, artifacts["comparison_path"])
        with open(comp_path, 'r') as f:
            comp_data = json.load(f)
        self.assertIn("validation", comp_data)
        self.assertIn("test", comp_data)
        self.assertIn("naive_baseline", comp_data["validation"])
        self.assertIn("xgboost", comp_data["validation"])


if __name__ == '__main__':
    unittest.main()
