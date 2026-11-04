"""
SupplySense AI - Unit Tests for Phase 4: Operational XGBoost Forward Demand Forecasting

Verifies:
  1. Model loading
  2. Feature schema validation
  3. Feature order
  4. Data type validation
  5. Sufficient-history inference
  6. Insufficient-history handling
  7. Unseen category handling
  8. Seven-day horizon
  9. Fourteen-day horizon
  10. Recursive inference
  11. Non-negative predictions
  12. Forecast persistence
  13. Forecast provenance
  14. Model version tracking
  15. Pipeline Runner integration
  16. Duplicate execution protection
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch
import datetime
import pandas as pd
import numpy as np

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from services.forecasting.model_loader import load_trained_xgboost_model, DEFAULT_MODEL_VERSION
from services.forecasting.eligibility import (
    check_feature_eligibility,
    compute_cold_start_fallback,
    SUFFICIENT_HISTORY_MIN_WEEKS,
)
from services.forecasting.recursive_engine import generate_forward_forecasts
from services.forecasting.repository import save_operational_forecasts
from services.forecasting.pipeline import run_operational_forecast_pipeline
from ml.features.feature_engineering import FEATURE_COLUMNS


class TestOperationalForwardForecasting(unittest.TestCase):

    def setUp(self):
        self.mock_cat_mapping = {
            "alimentos": 1,
            "beleza_saude": 11,
            "cama_mesa_banho": 13,
            "informatica_acessorios": 44,
        }

    # -------------------------------------------------------------------------
    # 1. Model loading
    # -------------------------------------------------------------------------
    def test_01_model_loading(self):
        model, meta = load_trained_xgboost_model()
        self.assertIsNotNone(model)
        self.assertEqual(meta["model_name"], "XGBoost")
        self.assertEqual(meta["model_version"], DEFAULT_MODEL_VERSION)
        self.assertEqual(meta["feature_names"], FEATURE_COLUMNS)

    # -------------------------------------------------------------------------
    # 2. Feature schema validation
    # -------------------------------------------------------------------------
    def test_02_feature_schema_validation(self):
        # Missing 'rolling_std_4w'
        incomplete_features = {
            "lag_1_demand": 10.0,
            "lag_2_demand": 10.0,
            "lag_4_demand": 10.0,
            "rolling_mean_4w": 10.0,
            "rolling_mean_8w": 10.0,
            "demand_trend_ratio": 1.0,
            "product_category_enc": 1,
            "month_of_year": 11,
            "week_of_year": 45,
            "quarter": 4,
            "history_weeks_count": 8
        }
        eligible, status, _ = check_feature_eligibility(incomplete_features)
        self.assertFalse(eligible)
        self.assertEqual(status, "INVALID_FEATURES")

    # -------------------------------------------------------------------------
    # 3. Feature order
    # -------------------------------------------------------------------------
    def test_03_feature_order(self):
        _, meta = load_trained_xgboost_model()
        self.assertEqual(meta["feature_names"], [
            'lag_1_demand',
            'lag_2_demand',
            'lag_4_demand',
            'rolling_mean_4w',
            'rolling_std_4w',
            'rolling_mean_8w',
            'demand_trend_ratio',
            'product_category_enc',
            'month_of_year',
            'week_of_year',
            'quarter',
        ])

    # -------------------------------------------------------------------------
    # 4. Data type validation
    # -------------------------------------------------------------------------
    def test_04_data_type_validation(self):
        # Pass NaN or string for a numeric feature
        features_with_nan = {
            col: 10.0 for col in FEATURE_COLUMNS
        }
        features_with_nan["rolling_mean_4w"] = float("nan")
        features_with_nan["product_category_enc"] = 1
        features_with_nan["month_of_year"] = 11
        features_with_nan["week_of_year"] = 45
        features_with_nan["quarter"] = 4
        features_with_nan["history_weeks_count"] = 8

        eligible, status, _ = check_feature_eligibility(features_with_nan)
        self.assertFalse(eligible)
        self.assertEqual(status, "INVALID_FEATURES")

    # -------------------------------------------------------------------------
    # 5. Sufficient-history inference
    # -------------------------------------------------------------------------
    def test_05_sufficient_history_inference(self):
        model, _ = load_trained_xgboost_model()

        # Build 10 weeks of continuous history
        dates = pd.date_range("2026-09-07", periods=10, freq="W-MON").strftime("%Y-%m-%d")
        rows = []
        for i, d in enumerate(dates):
            rows.append({
                "product_category": "alimentos",
                "week_start_date": d,
                "year_week": f"2026-W{37+i}",
                "demand": 25 + i * 2,
                "lag_1_demand": 25.0,
                "lag_2_demand": 23.0,
                "lag_4_demand": 21.0,
                "rolling_mean_4w": 25.0,
                "rolling_std_4w": 2.0,
                "rolling_mean_8w": 24.0,
                "demand_trend_ratio": 1.0,
                "product_category_enc": 1,
                "month_of_year": 11,
                "week_of_year": 45,
                "quarter": 4,
                "history_weeks_count": i,
                "feature_status": "SUFFICIENT_HISTORY" if i >= 8 else "INSUFFICIENT_HISTORY"
            })
        features_df = pd.DataFrame(rows)

        forecasts = generate_forward_forecasts(
            features_df=features_df,
            model=model,
            category_mapping=self.mock_cat_mapping
        )
        self.assertEqual(len(forecasts), 2)  # 7_DAYS and 14_DAYS
        step1 = forecasts[forecasts["forecast_horizon"] == "7_DAYS"].iloc[0]
        self.assertEqual(step1["inference_status"], "SUCCESS")
        self.assertGreaterEqual(step1["predicted_demand"], 0.0)

    # -------------------------------------------------------------------------
    # 6. Insufficient-history handling
    # -------------------------------------------------------------------------
    def test_06_insufficient_history_handling(self):
        model, _ = load_trained_xgboost_model()

        # Only 2 weeks of history (< 8)
        rows = [
            {
                "product_category": "beleza_saude",
                "week_start_date": "2026-11-02",
                "year_week": "2026-W45",
                "demand": 15,
                "lag_1_demand": 15.0,
                "lag_2_demand": 0.0,
                "lag_4_demand": 0.0,
                "rolling_mean_4w": 15.0,
                "rolling_std_4w": 0.0,
                "rolling_mean_8w": 15.0,
                "demand_trend_ratio": 1.0,
                "product_category_enc": 11,
                "month_of_year": 11,
                "week_of_year": 45,
                "quarter": 4,
                "history_weeks_count": 1,
                "feature_status": "INSUFFICIENT_HISTORY"
            }
        ]
        features_df = pd.DataFrame(rows)

        forecasts = generate_forward_forecasts(
            features_df=features_df,
            model=model,
            category_mapping=self.mock_cat_mapping
        )
        self.assertEqual(len(forecasts), 2)
        step1 = forecasts[forecasts["forecast_horizon"] == "7_DAYS"].iloc[0]
        # Must be handled cleanly via COLD_START_FALLBACK without crashing
        self.assertEqual(step1["inference_status"], "COLD_START_FALLBACK")
        self.assertEqual(step1["predicted_demand"], 15.0)

    # -------------------------------------------------------------------------
    # 7. Unseen category handling
    # -------------------------------------------------------------------------
    def test_07_unseen_category_handling(self):
        model, _ = load_trained_xgboost_model()

        rows = [
            {
                "product_category": "unseen_cat_xyz",
                "week_start_date": "2026-11-02",
                "year_week": "2026-W45",
                "demand": 10,
                "lag_1_demand": 10.0,
                "lag_2_demand": 0.0,
                "lag_4_demand": 0.0,
                "rolling_mean_4w": 10.0,
                "rolling_std_4w": 0.0,
                "rolling_mean_8w": 10.0,
                "demand_trend_ratio": 1.0,
                "product_category_enc": -1,
                "month_of_year": 11,
                "week_of_year": 45,
                "quarter": 4,
                "history_weeks_count": 8,
                "feature_status": "UNSEEN_CATEGORY"
            }
        ]
        features_df = pd.DataFrame(rows)

        forecasts = generate_forward_forecasts(
            features_df=features_df,
            model=model,
            category_mapping=self.mock_cat_mapping
        )
        step1 = forecasts[forecasts["forecast_horizon"] == "7_DAYS"].iloc[0]
        self.assertEqual(step1["inference_status"], "UNSEEN_CATEGORY")
        self.assertEqual(step1["predicted_demand"], 0.0)

    # -------------------------------------------------------------------------
    # 8. Seven-day horizon
    # -------------------------------------------------------------------------
    def test_08_seven_day_horizon(self):
        model, _ = load_trained_xgboost_model()
        row = {
            "product_category": "alimentos",
            "week_start_date": "2026-11-02",
            "year_week": "2026-W45",
            "demand": 20,
            "lag_1_demand": 20.0,
            "lag_2_demand": 20.0,
            "lag_4_demand": 20.0,
            "rolling_mean_4w": 20.0,
            "rolling_std_4w": 1.0,
            "rolling_mean_8w": 20.0,
            "demand_trend_ratio": 1.0,
            "product_category_enc": 1,
            "month_of_year": 11,
            "week_of_year": 45,
            "quarter": 4,
            "history_weeks_count": 8,
            "feature_status": "SUFFICIENT_HISTORY"
        }
        df = pd.DataFrame([row])
        forecasts = generate_forward_forecasts(df, model, horizons=["7_DAYS"], category_mapping=self.mock_cat_mapping)

        self.assertEqual(len(forecasts), 1)
        r = forecasts.iloc[0]
        self.assertEqual(r["forecast_horizon"], "7_DAYS")
        self.assertEqual(r["step_ahead"], 1)
        self.assertEqual(r["forecast_week"], "2026-11-09")  # 7 days after 2026-11-02
        self.assertEqual(r["year_week"], "2026-W46")

    # -------------------------------------------------------------------------
    # 9. Fourteen-day horizon
    # -------------------------------------------------------------------------
    def test_09_fourteen_day_horizon(self):
        model, _ = load_trained_xgboost_model()
        row = {
            "product_category": "alimentos",
            "week_start_date": "2026-11-02",
            "year_week": "2026-W45",
            "demand": 20,
            "lag_1_demand": 20.0,
            "lag_2_demand": 20.0,
            "lag_4_demand": 20.0,
            "rolling_mean_4w": 20.0,
            "rolling_std_4w": 1.0,
            "rolling_mean_8w": 20.0,
            "demand_trend_ratio": 1.0,
            "product_category_enc": 1,
            "month_of_year": 11,
            "week_of_year": 45,
            "quarter": 4,
            "history_weeks_count": 8,
            "feature_status": "SUFFICIENT_HISTORY"
        }
        df = pd.DataFrame([row])
        forecasts = generate_forward_forecasts(df, model, horizons=["7_DAYS", "14_DAYS"], category_mapping=self.mock_cat_mapping)

        self.assertEqual(len(forecasts), 2)
        step2 = forecasts[forecasts["forecast_horizon"] == "14_DAYS"].iloc[0]
        self.assertEqual(step2["step_ahead"], 2)
        self.assertEqual(step2["forecast_week"], "2026-11-16")  # 14 days after 2026-11-02
        self.assertEqual(step2["year_week"], "2026-W47")

    # -------------------------------------------------------------------------
    # 10. Recursive inference
    # -------------------------------------------------------------------------
    def test_10_recursive_inference(self):
        # Verify that step 1 prediction changes the step 2 input
        mock_model = MagicMock()
        # First call returns 50.0, second call returns 60.0
        mock_model.predict.side_effect = [
            np.array([50.0]),
            np.array([60.0])
        ]

        row = {
            "product_category": "alimentos",
            "week_start_date": "2026-11-02",
            "year_week": "2026-W45",
            "demand": 20,
            "lag_1_demand": 20.0,
            "lag_2_demand": 20.0,
            "lag_4_demand": 20.0,
            "rolling_mean_4w": 20.0,
            "rolling_std_4w": 1.0,
            "rolling_mean_8w": 20.0,
            "demand_trend_ratio": 1.0,
            "product_category_enc": 1,
            "month_of_year": 11,
            "week_of_year": 45,
            "quarter": 4,
            "history_weeks_count": 8,
            "feature_status": "SUFFICIENT_HISTORY"
        }
        df = pd.DataFrame([row])
        forecasts = generate_forward_forecasts(df, mock_model, horizons=["7_DAYS", "14_DAYS"], category_mapping=self.mock_cat_mapping)

        # Inspect the input passed to mock_model.predict in call 2 (step 2)
        call_args_list = mock_model.predict.call_args_list
        self.assertEqual(len(call_args_list), 2)
        step2_x = call_args_list[1][0][0]
        # In step 2, lag_1_demand MUST equal step 1 prediction (50.0)
        self.assertEqual(step2_x.iloc[0]["lag_1_demand"], 50.0)

    # -------------------------------------------------------------------------
    # 11. Non-negative predictions
    # -------------------------------------------------------------------------
    def test_11_non_negative_predictions(self):
        mock_model = MagicMock()
        mock_model.predict.return_value = np.array([-42.5])  # Negative prediction from model

        row = {
            "product_category": "alimentos",
            "week_start_date": "2026-11-02",
            "year_week": "2026-W45",
            "demand": 20,
            "lag_1_demand": 20.0,
            "lag_2_demand": 20.0,
            "lag_4_demand": 20.0,
            "rolling_mean_4w": 20.0,
            "rolling_std_4w": 1.0,
            "rolling_mean_8w": 20.0,
            "demand_trend_ratio": 1.0,
            "product_category_enc": 1,
            "month_of_year": 11,
            "week_of_year": 45,
            "quarter": 4,
            "history_weeks_count": 8,
            "feature_status": "SUFFICIENT_HISTORY"
        }
        forecasts = generate_forward_forecasts(pd.DataFrame([row]), mock_model, horizons=["7_DAYS"], category_mapping=self.mock_cat_mapping)
        self.assertEqual(forecasts.iloc[0]["predicted_demand"], 0.0)

    # -------------------------------------------------------------------------
    # 12. Forecast persistence
    # -------------------------------------------------------------------------
    def test_12_forecast_persistence(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        forecasts_df = pd.DataFrame([{
            "product_category": "alimentos",
            "forecast_week": "2026-11-09",
            "year_week": "2026-W46",
            "predicted_demand": 25.5,
            "model_name": "XGBoost",
            "model_version": "xgboost-v1",
            "source_feature_execution_id": 100,
            "data_provenance": "OPERATIONAL",
            "forecast_horizon": "7_DAYS",
            "step_ahead": 1,
            "inference_status": "SUCCESS"
        }])

        saved = save_operational_forecasts(forecasts_df, execution_id=200, connection=mock_conn)
        self.assertEqual(saved, 1)
        self.assertTrue(mock_cur.executemany.called)
        executed_sql = mock_cur.executemany.call_args[0][0]
        self.assertIn("operational_forecast_results", executed_sql)
        self.assertIn("ON DUPLICATE KEY UPDATE", executed_sql)

    # -------------------------------------------------------------------------
    # 13. Forecast provenance
    # -------------------------------------------------------------------------
    def test_13_forecast_provenance(self):
        model, _ = load_trained_xgboost_model()
        row = {
            "product_category": "alimentos",
            "week_start_date": "2026-11-02",
            "year_week": "2026-W45",
            "demand": 20,
            "lag_1_demand": 20.0,
            "lag_2_demand": 20.0,
            "lag_4_demand": 20.0,
            "rolling_mean_4w": 20.0,
            "rolling_std_4w": 1.0,
            "rolling_mean_8w": 20.0,
            "demand_trend_ratio": 1.0,
            "product_category_enc": 1,
            "month_of_year": 11,
            "week_of_year": 45,
            "quarter": 4,
            "history_weeks_count": 8,
            "feature_status": "SUFFICIENT_HISTORY"
        }
        forecasts = generate_forward_forecasts(pd.DataFrame([row]), model, category_mapping=self.mock_cat_mapping)
        self.assertTrue((forecasts["data_provenance"] == "OPERATIONAL").all())

    # -------------------------------------------------------------------------
    # 14. Model version tracking
    # -------------------------------------------------------------------------
    def test_14_model_version_tracking(self):
        model, _ = load_trained_xgboost_model()
        row = {
            "product_category": "alimentos",
            "week_start_date": "2026-11-02",
            "year_week": "2026-W45",
            "demand": 20,
            "lag_1_demand": 20.0,
            "lag_2_demand": 20.0,
            "lag_4_demand": 20.0,
            "rolling_mean_4w": 20.0,
            "rolling_std_4w": 1.0,
            "rolling_mean_8w": 20.0,
            "demand_trend_ratio": 1.0,
            "product_category_enc": 1,
            "month_of_year": 11,
            "week_of_year": 45,
            "quarter": 4,
            "history_weeks_count": 8,
            "feature_status": "SUFFICIENT_HISTORY"
        }
        forecasts = generate_forward_forecasts(pd.DataFrame([row]), model, model_version="custom-xgb-v2", category_mapping=self.mock_cat_mapping)
        self.assertEqual(forecasts.iloc[0]["model_version"], "custom-xgb-v2")

    # -------------------------------------------------------------------------
    # 15. Pipeline Runner integration
    # -------------------------------------------------------------------------
    def test_15_pipeline_runner_integration(self):
        from services.pipeline_runner.registry import get_pipeline_handler
        handler = get_pipeline_handler("Operational Demand Forecast Pipeline")
        self.assertIsNotNone(handler, "Handler not registered in Pipeline Runner registry")

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_cur.fetchone.side_effect = [
            {"dataset_id": 280, "current_version": 1},  # update_forecast_dataset_metadata
        ]
        mock_cur.fetchall.return_value = [
            {
                "product_category": "alimentos",
                "week_start_date": "2026-11-02",
                "year_week": "2026-W45",
                "demand": 20,
                "lag_1_demand": 20.0,
                "lag_2_demand": 20.0,
                "lag_4_demand": 20.0,
                "rolling_mean_4w": 20.0,
                "rolling_std_4w": 1.0,
                "rolling_mean_8w": 20.0,
                "demand_trend_ratio": 1.0,
                "product_category_enc": 1,
                "month_of_year": 11,
                "week_of_year": 45,
                "quarter": 4,
                "history_weeks_count": 8,
                "feature_status": "SUFFICIENT_HISTORY",
                "execution_id": 1050
            }
        ]

        result = run_operational_forecast_pipeline(
            execution_id=99992,
            pipeline_name="Operational Demand Forecast Pipeline",
            connection=mock_conn
        )
        self.assertEqual(result["status"], "COMPLETED")
        self.assertEqual(result["execution_id"], 99992)
        self.assertIn("mysql://operational_forecast_results", result["output_location"])

    # -------------------------------------------------------------------------
    # 16. Duplicate execution protection
    # -------------------------------------------------------------------------
    def test_16_duplicate_execution_protection(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        forecasts_df = pd.DataFrame([{
            "product_category": "alimentos",
            "forecast_week": "2026-11-09",
            "year_week": "2026-W46",
            "predicted_demand": 25.5,
            "model_name": "XGBoost",
            "model_version": "xgboost-v1",
            "source_feature_execution_id": 100,
            "data_provenance": "OPERATIONAL",
            "forecast_horizon": "7_DAYS",
            "step_ahead": 1,
            "inference_status": "SUCCESS"
        }])

        # Save twice
        saved1 = save_operational_forecasts(forecasts_df, execution_id=200, connection=mock_conn)
        saved2 = save_operational_forecasts(forecasts_df, execution_id=200, connection=mock_conn)
        self.assertEqual(saved1, 1)
        self.assertEqual(saved2, 1)
        # Verify ON DUPLICATE KEY UPDATE in SQL
        self.assertIn("ON DUPLICATE KEY UPDATE", mock_cur.executemany.call_args[0][0])


if __name__ == '__main__':
    unittest.main()
