"""
Unit tests for Naive Forecasting Baseline (BASE-001 through BASE-010).

Uses small deterministic synthetic datasets and does NOT require MySQL.
"""

import unittest
import os
import sys
import math

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from ml.baseline.naive_baseline import (
    calculate_metrics,
    generate_naive_predictions,
    get_previous_calendar_week
)


class TestNaiveBaseline(unittest.TestCase):

    def test_base_001_previous_period_demand_used_correctly(self):
        """BASE-001: Prediction for week T is exactly observed demand from week T-1."""
        history = [
            {'product_category': 'cat_a', 'week_start_date': '2024-01-01', 'demand': 10}
        ]
        target = [
            {'product_category': 'cat_a', 'week_start_date': '2024-01-08', 'demand': 15}
        ]
        preds = generate_naive_predictions(history, target)
        self.assertEqual(len(preds), 1)
        self.assertEqual(preds[0]['predicted_demand'], 10)
        self.assertEqual(preds[0]['actual_demand'], 15)

    def test_base_002_no_future_demand_used(self):
        """BASE-002: Predictions only use strictly past observations, never future or concurrent."""
        history = [
            {'product_category': 'cat_a', 'week_start_date': '2024-01-01', 'demand': 20}
        ]
        # Multi-week sequence: W1(target)=30, W2(target)=40
        target = [
            {'product_category': 'cat_a', 'week_start_date': '2024-01-08', 'demand': 30},
            {'product_category': 'cat_a', 'week_start_date': '2024-01-15', 'demand': 40}
        ]
        preds = generate_naive_predictions(history, target)
        # Week 2024-01-08 uses history from 2024-01-01 (20), NOT 30 or 40
        self.assertEqual(preds[0]['predicted_demand'], 20)
        # Week 2024-01-15 uses rolled observation from 2024-01-08 (30), NOT 40
        self.assertEqual(preds[1]['predicted_demand'], 30)

    def test_base_003_first_validation_boundary_uses_training_end(self):
        """BASE-003: First validation week correctly draws from the final training week."""
        training_history = [
            {'product_category': 'electronics', 'week_start_date': '2024-01-15', 'demand': 50}
        ]
        validation_targets = [
            {'product_category': 'electronics', 'week_start_date': '2024-01-22', 'demand': 55}
        ]
        preds = generate_naive_predictions(training_history, validation_targets)
        self.assertEqual(preds[0]['predicted_demand'], 50)

    def test_base_004_first_test_boundary_follows_methodology(self):
        """BASE-004: First test week correctly draws from the final validation week."""
        history = [
            # Final validation observation
            {'product_category': 'furniture', 'week_start_date': '2024-05-21', 'demand': 80}
        ]
        test_targets = [
            # First test week
            {'product_category': 'furniture', 'week_start_date': '2024-05-28', 'demand': 90}
        ]
        preds = generate_naive_predictions(history, test_targets)
        self.assertEqual(preds[0]['predicted_demand'], 80)

    def test_base_005_category_separation_preserved(self):
        """BASE-005: Different categories never cross-contaminate predictions."""
        history = [
            {'product_category': 'cat_a', 'week_start_date': '2024-01-01', 'demand': 100},
            {'product_category': 'cat_b', 'week_start_date': '2024-01-01', 'demand': 200}
        ]
        target = [
            {'product_category': 'cat_a', 'week_start_date': '2024-01-08', 'demand': 110},
            {'product_category': 'cat_b', 'week_start_date': '2024-01-08', 'demand': 220}
        ]
        preds = generate_naive_predictions(history, target)
        pred_map = {p['product_category']: p['predicted_demand'] for p in preds}
        self.assertEqual(pred_map['cat_a'], 100)
        self.assertEqual(pred_map['cat_b'], 200)

    def test_base_006_missing_category_week_handled_correctly(self):
        """BASE-006: Missing previous-week observation produces 0 demand (unobserved = 0)."""
        # cat_a was observed 2 weeks ago, but NOT in the immediately preceding week (2024-01-08)
        history = [
            {'product_category': 'cat_a', 'week_start_date': '2024-01-01', 'demand': 100}
        ]
        target = [
            # Week is 2024-01-15; previous week is 2024-01-08 which had no orders for cat_a
            {'product_category': 'cat_a', 'week_start_date': '2024-01-15', 'demand': 50}
        ]
        preds = generate_naive_predictions(history, target)
        self.assertEqual(preds[0]['predicted_demand'], 0)

    def test_base_007_mae_calculation(self):
        """BASE-007: Mean Absolute Error calculation is mathematically verified."""
        actuals = [10.0, 20.0, 30.0]
        predictions = [12.0, 18.0, 35.0]
        # |10-12|=2, |20-18|=2, |30-35|=5 => (2 + 2 + 5) / 3 = 3.0
        metrics = calculate_metrics(actuals, predictions)
        self.assertAlmostEqual(metrics['mae'], 3.0, places=6)

    def test_base_008_rmse_calculation(self):
        """BASE-008: Root Mean Squared Error calculation is mathematically verified."""
        actuals = [10.0, 20.0, 30.0]
        predictions = [12.0, 18.0, 35.0]
        # (2^2 + 2^2 + 5^2)/3 = (4 + 4 + 25)/3 = 33/3 = 11.0 => sqrt(11) = 3.31662479
        metrics = calculate_metrics(actuals, predictions)
        self.assertAlmostEqual(metrics['rmse'], math.sqrt(11.0), places=6)

    def test_base_009_r2_calculation(self):
        """BASE-009: Coefficient of Determination (R²) calculation is mathematically verified."""
        actuals = [10.0, 20.0, 30.0]
        predictions = [10.0, 20.0, 30.0]
        # Perfect predictions => R² = 1.0
        metrics = calculate_metrics(actuals, predictions)
        self.assertAlmostEqual(metrics['r2'], 1.0, places=6)

        # Non-perfect example:
        actuals = [10.0, 20.0, 30.0]
        predictions = [12.0, 18.0, 35.0]
        # mean_actual = 20.0
        # SS_tot = (10-20)^2 + (20-20)^2 + (30-20)^2 = 100 + 0 + 100 = 200
        # SS_res = (10-12)^2 + (20-18)^2 + (30-35)^2 = 4 + 4 + 25 = 33
        # R² = 1 - (33 / 200) = 1 - 0.165 = 0.835
        metrics_imperfect = calculate_metrics(actuals, predictions)
        self.assertAlmostEqual(metrics_imperfect['r2'], 0.835, places=6)

    def test_base_010_invalid_empty_input_handling(self):
        """BASE-010: Empty or mismatched lists raise appropriate ValueError."""
        with self.assertRaises(ValueError):
            calculate_metrics([], [])

        with self.assertRaises(ValueError):
            calculate_metrics([10.0], [10.0, 20.0])


if __name__ == '__main__':
    unittest.main()
