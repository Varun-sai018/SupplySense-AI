"""
Unit Tests for Phase 1 - Forecast Analytics Implementation.

Verifies:
  1. Category forecast trends retrieval (14-week horizon, chronological order, data provenance).
  2. Category filtering via query parameters.
  3. Execution isolation between distinct execution IDs.
  4. Missing execution 404 handling.
  5. Fallback to artifact CSV when MySQL forecast rows are unavailable.
  6. Model comparison metrics retrieval from execution-scoped artifacts.
  7. Integration of model comparison into execution details payload.
  8. Graceful handling of missing or malformed artifacts.
"""

import os
import sys
import unittest
from fastapi.testclient import TestClient

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from services.observability.main import app
from services.observability import repository, service


class TestForecastAnalytics(unittest.TestCase):
    """Unit test suite for weekly forecast category trends and model comparisons."""

    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)

    # -------------------------------------------------------------------------
    # Category Forecast Trends Tests
    # -------------------------------------------------------------------------

    def test_category_trends_benchmark_36(self):
        """Verify 14-week forecast category trends for historical benchmark execution #36."""
        resp = self.client.get("/api/observability/forecasts/36/category-trends")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()

        self.assertEqual(data["execution_id"], 36)
        self.assertEqual(data["data_provenance"], "HISTORICAL")
        self.assertEqual(data["horizon_weeks"], 14)
        self.assertEqual(data["min_forecast_week"], "2018-05-28")
        self.assertEqual(data["max_forecast_week"], "2018-08-27")

        # Verify category selection & availability
        self.assertGreater(len(data["available_categories"]), 0)
        self.assertLessEqual(len(data["selected_categories"]), 5)

        # Verify weekly trends structure and chronological ordering
        trends = data["weekly_trends"]
        self.assertEqual(len(trends), 14)
        for i in range(len(trends) - 1):
            self.assertLess(trends[i]["forecast_week"], trends[i+1]["forecast_week"])

        # Top category should have positive predicted demand
        top_cat = data["selected_categories"][0]["category"]
        self.assertIn(top_cat, trends[0])
        self.assertGreater(trends[0][top_cat], 0)

    def test_category_trends_filtering(self):
        """Verify category filtering returns only the requested categories."""
        resp = self.client.get(
            "/api/observability/forecasts/36/category-trends?categories=beleza_saude,esporte_lazer"
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.json()

        selected_names = [c["category"] for c in data["selected_categories"]]
        self.assertEqual(set(selected_names), {"beleza_saude", "esporte_lazer"})

        first_week = data["weekly_trends"][0]
        self.assertIn("beleza_saude", first_week)
        self.assertIn("esporte_lazer", first_week)

    def test_category_trends_execution_isolation(self):
        """Verify that distinct execution IDs return strictly isolated data."""
        resp36 = self.client.get("/api/observability/forecasts/36/category-trends")
        resp99653 = self.client.get("/api/observability/forecasts/99653/category-trends")

        self.assertEqual(resp36.status_code, 200)
        self.assertEqual(resp99653.status_code, 200)

        data36 = resp36.json()
        data99653 = resp99653.json()

        self.assertEqual(data36["execution_id"], 36)
        self.assertEqual(data99653["execution_id"], 99653)

    def test_category_trends_artifact_fallback_99586(self):
        """Verify that execution #99586 (without DB rows) falls back cleanly to artifact CSV."""
        resp = self.client.get("/api/observability/forecasts/99586/category-trends")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()

        self.assertEqual(data["execution_id"], 99586)
        self.assertEqual(data["horizon_weeks"], 14)
        self.assertGreater(len(data["available_categories"]), 0)

    def test_category_trends_missing_execution(self):
        """Verify that unknown execution IDs return 404."""
        resp = self.client.get("/api/observability/forecasts/999999/category-trends")
        self.assertEqual(resp.status_code, 404)
        self.assertIn("not found or unavailable", resp.json()["detail"].lower())

    def test_category_trends_failed_execution_no_data(self):
        """Verify that failed execution without forecast results returns 404."""
        resp = self.client.get("/api/observability/forecasts/99652/category-trends")
        self.assertEqual(resp.status_code, 404)

    # -------------------------------------------------------------------------
    # Model Comparison Metrics Tests
    # -------------------------------------------------------------------------

    def test_model_comparison_benchmark_36(self):
        """Verify validated model comparison metrics for execution #36."""
        resp = self.client.get("/api/observability/executions/36/model-comparison")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()

        self.assertEqual(data["execution_id"], 36)
        self.assertEqual(data["data_provenance"], "HISTORICAL")

        # Validation split assertions
        val = data["validation"]
        self.assertIn("naive_baseline", val)
        self.assertIn("xgboost", val)
        self.assertAlmostEqual(val["naive_baseline"]["mae"], 7.9802, places=3)
        self.assertAlmostEqual(val["xgboost"]["mae"], 8.0522, places=3)

        # Test holdout split assertions
        test = data["test"]
        self.assertIn("naive_baseline", test)
        self.assertIn("xgboost", test)
        self.assertAlmostEqual(test["naive_baseline"]["mae"], 9.4636, places=3)
        self.assertAlmostEqual(test["xgboost"]["mae"], 9.2784, places=3)

    def test_model_comparison_execution_detail_integration(self):
        """Verify that GET /api/observability/executions/{id} embeds model_comparison."""
        resp = self.client.get("/api/observability/executions/36")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()

        self.assertIn("model_comparison", data)
        mc = data["model_comparison"]
        self.assertIsNotNone(mc)
        self.assertIn("validation", mc)
        self.assertIn("test", mc)
        self.assertIn("metrics_guide", mc)

    def test_model_comparison_missing_execution(self):
        """Verify that non-existent execution returns 404 for model comparison."""
        resp = self.client.get("/api/observability/executions/999999/model-comparison")
        self.assertEqual(resp.status_code, 404)

    def test_model_comparison_failed_execution_no_artifact(self):
        """Verify that execution without model_comparison.json artifact returns 404."""
        resp = self.client.get("/api/observability/executions/99652/model-comparison")
        self.assertEqual(resp.status_code, 404)


if __name__ == "__main__":
    unittest.main()
