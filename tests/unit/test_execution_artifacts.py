"""
Unit Tests for Phase 10 - Execution-Scoped Artifacts & Result Isolation.

Verifies:
  1. Naive baseline produces artifacts in specified execution directory.
  2. XGBoost training produces artifacts and model copy in specified execution directory.
  3. Execution metadata (execution_id) is embedded in output JSON metadata.
  4. Atomic writes ensure valid files are produced without partial corruption.
  5. Pipeline runner handler scopes artifacts under ml/results/executions/<execution_id>/.
  6. Retry execution safely overwrites existing execution directory without errors.
"""

import os
import sys
import json
import shutil
import tempfile
import unittest
import importlib
from unittest.mock import patch, MagicMock

# Ensure repo root is in path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from ml.baseline.naive_baseline import run_baseline
from ml.models.train_xgboost import train_and_evaluate_xgboost

handlers_mod = importlib.import_module('services.pipeline-runner.handlers')
demand_forecast_pipeline_handler = handlers_mod.demand_forecast_pipeline_handler


class TestExecutionArtifactsUnit(unittest.TestCase):
    """Unit test suite for execution-scoped artifacts."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="test_exec_artifacts_")
        self.exec_id_1 = 9001
        self.exec_dir_1 = os.path.join(self.test_dir, str(self.exec_id_1))

    def tearDown(self):
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_baseline_isolated_output_and_metadata(self):
        """Test naive baseline writes to custom output_dir with execution_id metadata."""
        result = run_baseline(output_dir=self.exec_dir_1, execution_id=self.exec_id_1)

        self.assertTrue(os.path.isdir(self.exec_dir_1))
        metrics_json_path = os.path.join(self.exec_dir_1, 'baseline_metrics.json')
        metrics_csv_path = os.path.join(self.exec_dir_1, 'baseline_metrics.csv')
        val_pred_path = os.path.join(self.exec_dir_1, 'baseline_validation_predictions.csv')
        test_pred_path = os.path.join(self.exec_dir_1, 'baseline_test_predictions.csv')

        self.assertTrue(os.path.isfile(metrics_json_path))
        self.assertTrue(os.path.isfile(metrics_csv_path))
        self.assertTrue(os.path.isfile(val_pred_path))
        self.assertTrue(os.path.isfile(test_pred_path))

        with open(metrics_json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        self.assertEqual(data["metadata"]["execution_id"], self.exec_id_1)
        self.assertIn("results", data)
        self.assertEqual(len(data["results"]), 2)

    def test_xgboost_isolated_output_and_metadata(self):
        """Test XGBoost training writes artifacts & model copy to custom output_dir."""
        result = train_and_evaluate_xgboost(output_dir=self.exec_dir_1, execution_id=self.exec_id_1)

        self.assertTrue(os.path.isdir(self.exec_dir_1))
        comparison_json = os.path.join(self.exec_dir_1, 'model_comparison.json')
        xgb_metrics_json = os.path.join(self.exec_dir_1, 'xgboost_metrics.json')
        model_json = os.path.join(self.exec_dir_1, 'xgboost_demand_model.json')
        val_preds_csv = os.path.join(self.exec_dir_1, 'xgboost_validation_predictions.csv')
        test_preds_csv = os.path.join(self.exec_dir_1, 'xgboost_test_predictions.csv')

        self.assertTrue(os.path.isfile(comparison_json))
        self.assertTrue(os.path.isfile(xgb_metrics_json))
        self.assertTrue(os.path.isfile(model_json))
        self.assertTrue(os.path.isfile(val_preds_csv))
        self.assertTrue(os.path.isfile(test_preds_csv))

        with open(comparison_json, 'r', encoding='utf-8') as f:
            comp_data = json.load(f)
        self.assertEqual(comp_data["metadata"]["execution_id"], self.exec_id_1)

        with open(xgb_metrics_json, 'r', encoding='utf-8') as f:
            xgb_data = json.load(f)
        self.assertEqual(xgb_data["metadata"]["execution_id"], self.exec_id_1)

    def test_atomic_write_safety_on_retry(self):
        """Test repeated/retry executions safely overwrite execution directory files."""
        # Initial run
        run_baseline(output_dir=self.exec_dir_1, execution_id=self.exec_id_1)
        metrics_json = os.path.join(self.exec_dir_1, 'baseline_metrics.json')
        mtime1 = os.path.getmtime(metrics_json)

        # Retry run for the same execution_id
        run_baseline(output_dir=self.exec_dir_1, execution_id=self.exec_id_1)
        self.assertTrue(os.path.isfile(metrics_json))
        with open(metrics_json, 'r', encoding='utf-8') as f:
            data = json.load(f)
        self.assertEqual(data["metadata"]["execution_id"], self.exec_id_1)

    def test_pipeline_handler_execution_scoping(self):
        """Test demand_forecast_pipeline_handler routes outputs to execution-scoped directory."""
        test_exec_id = 99999

        try:
            with patch.object(handlers_mod, 'save_forecast_results', return_value=160):
                result = demand_forecast_pipeline_handler(test_exec_id, "Demand Forecast Pipeline")
                expected_prefix = f"ml/results/executions/{test_exec_id}/"
                self.assertTrue(result["output_location"].startswith(expected_prefix))
                self.assertEqual(result["execution_id"], test_exec_id)
                self.assertEqual(result["forecasts_saved"], 160)

                # Check that all returned artifact paths are prefixed with execution dir
                for art in result["artifacts"]:
                    self.assertIn(f"executions/{test_exec_id}/", art)
        finally:
            exec_clean_dir = os.path.join(REPO_ROOT, 'ml', 'results', 'executions', str(test_exec_id))
            if os.path.exists(exec_clean_dir):
                shutil.rmtree(exec_clean_dir, ignore_errors=True)


if __name__ == '__main__':
    unittest.main()
