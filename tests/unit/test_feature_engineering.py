"""
Unit tests for Feature Engineering Module (Phase 5).
Verifies lag 1, 2, 4, rolling 4w/8w, trend ratio, calendar extraction,
continuous grid imputation, and strict absence of lookahead leakage.
"""

import unittest
import os
import sys
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


class TestFeatureEngineering(unittest.TestCase):

    def setUp(self):
        # 8-week synthetic sequence for category A
        self.sample_df = pd.DataFrame([
            {'product_category': 'cat_a', 'week_start_date': '2024-01-01', 'demand': 10},
            {'product_category': 'cat_a', 'week_start_date': '2024-01-08', 'demand': 20},
            {'product_category': 'cat_a', 'week_start_date': '2024-01-15', 'demand': 30},
            {'product_category': 'cat_a', 'week_start_date': '2024-01-22', 'demand': 40},
            {'product_category': 'cat_a', 'week_start_date': '2024-01-29', 'demand': 50},
            {'product_category': 'cat_a', 'week_start_date': '2024-02-05', 'demand': 60},
            {'product_category': 'cat_a', 'week_start_date': '2024-02-12', 'demand': 70},
            {'product_category': 'cat_a', 'week_start_date': '2024-02-19', 'demand': 80},
        ])
        self.grid = create_continuous_grid(self.sample_df)
        self.features = engineer_lag_and_rolling_features(self.grid)

    def test_feat_001_lag_1_correctness(self):
        """1. lag_1_demand is exactly the demand of previous calendar week."""
        # Row index 1 is 2024-01-08; lag_1 should be demand of 2024-01-01 (10)
        self.assertEqual(self.features.loc[1, 'lag_1_demand'], 10.0)
        # Row index 4 is 2024-01-29; lag_1 should be 40
        self.assertEqual(self.features.loc[4, 'lag_1_demand'], 40.0)

    def test_feat_002_lag_2_correctness(self):
        """2. lag_2_demand is exactly the demand of 2 weeks prior."""
        # Row index 2 is 2024-01-15; lag_2 should be demand of 2024-01-01 (10)
        self.assertEqual(self.features.loc[2, 'lag_2_demand'], 10.0)
        # Row index 4 is 2024-01-29; lag_2 should be 30
        self.assertEqual(self.features.loc[4, 'lag_2_demand'], 30.0)

    def test_feat_003_lag_4_correctness(self):
        """3. lag_4_demand is exactly the demand of 4 weeks prior."""
        # Row index 4 is 2024-01-29; lag_4 should be demand of 2024-01-01 (10)
        self.assertEqual(self.features.loc[4, 'lag_4_demand'], 10.0)
        # Row index 0 has no 4-week prior history => 0.0
        self.assertEqual(self.features.loc[0, 'lag_4_demand'], 0.0)

    def test_feat_004_rolling_mean_4w_correctness(self):
        """4. rolling_mean_4w is average over T-4 through T-1 (no current week)."""
        # Row index 4 (2024-01-29): prior 4 weeks are 10, 20, 30, 40 => mean is 25.0
        self.assertAlmostEqual(self.features.loc[4, 'rolling_mean_4w'], 25.0, places=5)
        # Row index 5 (2024-02-05): prior 4 weeks are 20, 30, 40, 50 => mean is 35.0
        self.assertAlmostEqual(self.features.loc[5, 'rolling_mean_4w'], 35.0, places=5)

    def test_feat_005_rolling_std_4w_correctness(self):
        """5. rolling_std_4w is sample standard deviation over T-4 through T-1."""
        # Row index 4: values are 10, 20, 30, 40. Sample std is sqrt(((10-25)^2+(20-25)^2+(30-25)^2+(40-25)^2)/3)
        expected_std = np.std([10, 20, 30, 40], ddof=1)
        self.assertAlmostEqual(self.features.loc[4, 'rolling_std_4w'], expected_std, places=5)

    def test_feat_006_rolling_mean_8w_correctness(self):
        """6. rolling_mean_8w is average over up to T-8 through T-1."""
        # Row index 7 (2024-02-19): prior 7 weeks available: 10, 20, 30, 40, 50, 60, 70 => mean is 40.0
        self.assertAlmostEqual(self.features.loc[7, 'rolling_mean_8w'], 40.0, places=5)

    def test_feat_007_demand_trend_ratio(self):
        """7. demand_trend_ratio is (lag_1 + 1) / (rolling_mean_4w + 1)."""
        # Row index 4: lag_1 = 40.0, rolling_mean_4w = 25.0 => (40 + 1) / (25 + 1) = 41 / 26 = 1.576923
        expected_ratio = 41.0 / 26.0
        self.assertAlmostEqual(self.features.loc[4, 'demand_trend_ratio'], expected_ratio, places=5)

    def test_feat_008_calendar_features(self):
        """8. Calendar features (month_of_year, week_of_year, quarter) are correctly extracted."""
        # Row 0: 2024-01-01 -> month 1, week 1, quarter 1
        self.assertEqual(self.features.loc[0, 'month_of_year'], 1)
        self.assertEqual(self.features.loc[0, 'week_of_year'], 1)
        self.assertEqual(self.features.loc[0, 'quarter'], 1)

    def test_feat_009_missing_week_handling(self):
        """9. Unobserved category-weeks in raw data are expanded to demand=0 on continuous grid."""
        sparse_df = pd.DataFrame([
            {'product_category': 'cat_b', 'week_start_date': '2024-01-01', 'demand': 50},
            # Missing 2024-01-08
            {'product_category': 'cat_b', 'week_start_date': '2024-01-15', 'demand': 100},
        ])
        grid = create_continuous_grid(sparse_df)
        self.assertEqual(len(grid), 3)
        # Check middle week
        mid_row = grid[grid['week_start_date'] == '2024-01-08'].iloc[0]
        self.assertEqual(mid_row['demand'], 0)

        # In features, lag_1 for 2024-01-15 must be 0 (from 2024-01-08), NOT 50
        feat = engineer_lag_and_rolling_features(grid)
        third_row = feat[feat['week_start_date'] == '2024-01-15'].iloc[0]
        self.assertEqual(third_row['lag_1_demand'], 0.0)
        self.assertEqual(third_row['lag_2_demand'], 50.0)

    def test_feat_010_no_future_target_leakage(self):
        """10. Shuffling future demand or altering future values does not affect features for week T."""
        df1 = self.sample_df.copy()
        df2 = self.sample_df.copy()
        # Change future week demand (row 7) in df2
        df2.loc[7, 'demand'] = 99999

        feat1 = engineer_lag_and_rolling_features(create_continuous_grid(df1))
        feat2 = engineer_lag_and_rolling_features(create_continuous_grid(df2))

        # Rows 0 through 6 must be 100% identical in all feature values
        for col in ['lag_1_demand', 'lag_2_demand', 'lag_4_demand', 'rolling_mean_4w', 'rolling_std_4w', 'rolling_mean_8w']:
            np.testing.assert_array_equal(feat1.loc[:6, col].values, feat2.loc[:6, col].values)

    def test_feat_011_category_encoding_unseen_fallback(self):
        """11. Categories unseen during training are mapped to -1 fallback."""
        train_df = pd.DataFrame([
            {'product_category': 'cat_known', 'week_start_date': '2024-01-01', 'demand': 10}
        ])
        eval_df = pd.DataFrame([
            {'product_category': 'cat_known', 'week_start_date': '2024-02-01', 'demand': 20},
            {'product_category': 'cat_unknown', 'week_start_date': '2024-02-01', 'demand': 5},
        ])
        _, mapping = encode_categories(train_df)
        encoded_eval, _ = encode_categories(eval_df, category_mapping=mapping)
        self.assertEqual(encoded_eval.loc[0, 'product_category_enc'], mapping['cat_known'])
        self.assertEqual(encoded_eval.loc[1, 'product_category_enc'], -1)


if __name__ == '__main__':
    unittest.main()
