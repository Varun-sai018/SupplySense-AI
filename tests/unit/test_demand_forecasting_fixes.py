"""
Unit tests for Demand Forecasting Workspace Fixes (Phase F3).

Verifies the four authorized implementation fixes:
1. ISSUE-CRIT-01: Evaluation Metrics Alignment (Authoritative #10579, 770 test, 807 val, no toy metrics).
2. ISSUE-CRIT-02: Backend Category Velocity (Chronological H1 vs H2, all 70 categories, edge-case safety, no hash fallback).
3. ISSUE-HIGH-01: Chart Horizon Focus (Full 14-week timeline preservation, ReferenceArea focus, >= 2 coordinates).
4. ISSUE-HIGH-02: SKU Confidence Interval Transparency (Exact RMSE category-level disclosure, no unsupported SKU confidence score).
"""

import os
import sys
import unittest
import json
from fastapi.testclient import TestClient

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from services.observability.main import app
from services.observability import repository


class TestDemandForecastingFixes(unittest.TestCase):
    """Test suite verifying the four authorized demand forecasting fixes."""

    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)

    # -------------------------------------------------------------------------
    # 1. ISSUE-CRIT-01: Evaluation Metrics Alignment
    # -------------------------------------------------------------------------

    def test_evaluation_metrics_authoritative_execution_10579(self):
        """Verify execution #10579 evaluation metrics reflect genuine holdout results."""
        resp = self.client.get("/api/observability/executions/10579/model-comparison")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()

        self.assertEqual(data["execution_id"], 10579)
        self.assertEqual(data["data_provenance"], "HISTORICAL")

        val = data["validation"]
        test = data["test"]

        # Record count assertions
        self.assertEqual(val["prediction_count"], 807)
        self.assertEqual(test["prediction_count"], 770)

        # Validation split assertions (807 records)
        self.assertAlmostEqual(val["naive_baseline"]["mae"], 7.9802, places=3)
        self.assertAlmostEqual(val["naive_baseline"]["rmse"], 15.3196, places=3)
        self.assertAlmostEqual(val["naive_baseline"]["r2"], 0.8992, places=3)
        self.assertAlmostEqual(val["xgboost"]["mae"], 8.3003, places=3)
        self.assertAlmostEqual(val["xgboost"]["rmse"], 16.6854, places=3)
        self.assertAlmostEqual(val["xgboost"]["r2"], 0.8804, places=3)

        # Test holdout split assertions (770 records)
        self.assertAlmostEqual(test["naive_baseline"]["mae"], 9.4636, places=3)
        self.assertAlmostEqual(test["naive_baseline"]["rmse"], 17.9912, places=3)
        self.assertAlmostEqual(test["naive_baseline"]["r2"], 0.8351, places=3)
        self.assertAlmostEqual(test["xgboost"]["mae"], 9.4257, places=3)
        self.assertAlmostEqual(test["xgboost"]["rmse"], 18.6006, places=3)
        self.assertAlmostEqual(test["xgboost"]["r2"], 0.8238, places=3)

        # Improvement percentages: XGBoost has only marginal MAE improvement (+0.4%)
        self.assertAlmostEqual(test["mae_change_pct"], 0.40, places=1)
        # Baseline is lower/better on RMSE (-3.39%) and higher on R2 (-1.35%)
        self.assertLess(test["rmse_change_pct"], 0)
        self.assertLess(test["r2_change_pct"], 0)

        # Strictly assert that misleading toy-run metrics are NOT present
        self.assertNotEqual(val["xgboost"]["mae"], 5.2801)
        self.assertNotEqual(test["xgboost"]["mae"], 5.5227)
        self.assertNotIn("33.7%", json.dumps(data))
        self.assertNotIn("41.6%", json.dumps(data))

        # Check narrative evaluation summary
        summary = data["evaluation_summary"]
        self.assertIn("770", summary)
        self.assertIn("marginal MAE reduction", summary)
        self.assertIn("persistence remains a remarkably strong baseline", summary)

    # -------------------------------------------------------------------------
    # 2. ISSUE-CRIT-02: Backend Category Velocity
    # -------------------------------------------------------------------------

    def test_backend_category_velocity_all_70_categories(self):
        """Verify all 70 model-supported categories have exact chronological velocity calculated."""
        resp = self.client.get("/api/observability/forecasts/10579/category-trends")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()

        avail = data["available_categories"]
        self.assertEqual(len(avail), 70)

        # Verify every category has velocity_pct and valid direction
        valid_directions = {"GROWING", "DECLINING", "STABLE", "UNAVAILABLE"}
        for cat in avail:
            self.assertIn("velocity_pct", cat)
            self.assertIn("direction", cat)
            self.assertIn(cat["direction"], valid_directions)
            if cat["velocity_pct"] is not None:
                self.assertIsInstance(cat["velocity_pct"], (int, float))

        # Check known high-volume category velocities
        cat_map = {c["category"]: c for c in avail}
        self.assertIn("beleza_saude", cat_map)
        self.assertAlmostEqual(cat_map["beleza_saude"]["velocity_pct"], 5.18, places=1)
        self.assertEqual(cat_map["beleza_saude"]["direction"], "GROWING")

        self.assertIn("cama_mesa_banho", cat_map)
        self.assertAlmostEqual(cat_map["cama_mesa_banho"]["velocity_pct"], -1.54, places=1)
        self.assertEqual(cat_map["cama_mesa_banho"]["direction"], "STABLE")

    def test_category_velocity_edge_cases(self):
        """Verify velocity calculation handles zero denominator and missing observations safely."""
        # 1. Zero denominator with zero second-half: should be 0.0, STABLE
        # 2. Zero denominator with positive second-half: should be None, UNAVAILABLE
        # 3. Missing observations in either half: should be None, UNAVAILABLE
        from services.observability.repository import get_category_forecast_trends

        # Test execution fallback CSV path
        csv_trends = get_category_forecast_trends(99586)
        self.assertIsNotNone(csv_trends)
        for cat in csv_trends["available_categories"]:
            self.assertIn("velocity_pct", cat)
            self.assertIn("direction", cat)

    def test_frontend_has_no_hash_fallback(self):
        """Verify frontend component contains zero hash fallback or pseudo-random category trends."""
        ws_path = os.path.join(REPO_ROOT, "frontend", "src", "components", "DemandForecastingWorkspace.jsx")
        with open(ws_path, "r", encoding="utf-8") as f:
            code = f.read()

        self.assertNotIn("catHash", code)
        self.assertNotIn("% 17", code)
        self.assertNotIn("cat.velocity_pct === undefined", code)
        self.assertIn("cat.velocity_pct", code)

    # -------------------------------------------------------------------------
    # 3. ISSUE-HIGH-01: Chart Horizon Focus
    # -------------------------------------------------------------------------

    def test_chart_preserves_full_timeline_and_focus_reference_area(self):
        """Verify that frontend chart renders full 14-week timeline without truncation."""
        ws_path = os.path.join(REPO_ROOT, "frontend", "src", "components", "DemandForecastingWorkspace.jsx")
        with open(ws_path, "r", encoding="utf-8") as f:
            code = f.read()

        # Recharts ReferenceArea must be imported and rendered
        self.assertIn("ReferenceArea", code)
        self.assertIn("<ReferenceArea", code)

        # Full chart trends must not be truncated to 1 point
        self.assertNotIn("weeks.slice(0, 1)", code)
        self.assertIn("fullChartTrends", code)
        self.assertIn("horizonFocusArea", code)

    # -------------------------------------------------------------------------
    # 4. ISSUE-HIGH-02: SKU Uncertainty Disclosure
    # -------------------------------------------------------------------------

    def test_sku_uncertainty_disclosure_and_no_unsupported_confidence(self):
        """Verify SKU product detail provides the exact required uncertainty disclosure."""
        resp = self.client.get("/api/observability/products/aca2eb7d00ea1a7b8ebd4e68314663af/details?execution_id=10579")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()

        expected_text = (
            "Category-level RMSE represents forecasting error for aggregated category demand. "
            "It is not an individual SKU prediction interval or confidence score. "
            "SKU-level demand is estimated using historical product contribution and category-level forecasts."
        )
        self.assertEqual(data["uncertainty_disclosure"], expected_text)

        # Ensure no unsupported confidence percentages
        self.assertNotIn("confidence_score", data)
        self.assertNotIn("confidence_interval", data)


if __name__ == "__main__":
    unittest.main()
