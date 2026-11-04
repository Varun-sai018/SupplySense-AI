"""
SupplySense AI - Unit Tests for Phase 3: Operational Demand Aggregation & Feature Engineering

Verifies:
  1. SKU-to-category mapping
  2. Weekly demand aggregation
  3. Quantity aggregation
  4. Order status filtering (CANCELLED excluded, PENDING/ALLOCATED/BACKORDERED/COMPLETED included)
  5. Source provenance separation
  6. Missing weekly observations
  7. Zero-demand weeks
  8. Category encoding
  9. All 11 feature calculations
  10. Feature order and data types
  11. Insufficient history handling
  12. No future data leakage
  13. Duplicate order protection
  14. Pipeline Runner integration
  15. Reproducible feature output
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

from services.feature_pipeline.aggregator import (
    aggregate_operational_orders,
    ELIGIBLE_ORDER_STATUSES,
    EXCLUDED_ORDER_STATUSES,
)
from services.feature_pipeline.grid_generator import build_weekly_demand_grid
from services.feature_pipeline.feature_builder import compute_operational_features
from services.feature_pipeline.pipeline import run_operational_feature_pipeline
from ml.features.feature_engineering import FEATURE_COLUMNS, load_category_mapping


class TestOperationalFeatureEngineering(unittest.TestCase):

    def setUp(self):
        self.mock_cat_mapping = {
            "agro_industria_e_comercio": 0,
            "alimentos": 1,
            "beleza_saude": 11,
            "cama_mesa_banho": 13,
            "esporte_lazer": 32,
            "informatica_acessorios": 44,
            "utilidades_domesticas": 72,
        }

    # -------------------------------------------------------------------------
    # 1. SKU-to-category mapping
    # -------------------------------------------------------------------------
    def test_01_sku_to_category_mapping(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        # Audit query returns 0 cancelled
        mock_cur.fetchone.return_value = {"cancelled_count": 0, "cancelled_quantity": 0}
        # Order rows: one mapped SKU, one unmapped SKU
        mock_cur.fetchall.return_value = [
            {
                "order_id": "ord_001",
                "sku": "sku_mapped",
                "quantity": 3,
                "unit_price": 45.0,
                "order_status": "COMPLETED",
                "source": "OPERATIONAL_API",
                "idempotency_key": "k1",
                "event_time": datetime.datetime(2026, 11, 2, 10, 0, 0),
                "product_category_name": "beleza_saude",
            },
            {
                "order_id": "ord_002",
                "sku": "sku_unmapped",
                "quantity": 2,
                "unit_price": 20.0,
                "order_status": "PENDING",
                "source": "OPERATIONAL_API",
                "idempotency_key": "k2",
                "event_time": datetime.datetime(2026, 11, 2, 11, 0, 0),
                "product_category_name": None,  # SKU has no category in catalog
            }
        ]

        agg_df, meta = aggregate_operational_orders(connection=mock_conn)
        self.assertEqual(len(agg_df), 1)
        self.assertEqual(agg_df.iloc[0]["product_category"], "beleza_saude")
        self.assertEqual(agg_df.iloc[0]["demand_units"], 3)
        self.assertEqual(meta["unmapped_sku_count"], 1)

    # -------------------------------------------------------------------------
    # 2. Weekly demand aggregation
    # -------------------------------------------------------------------------
    def test_02_weekly_demand_aggregation(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        mock_cur.fetchone.return_value = {"cancelled_count": 0, "cancelled_quantity": 0}
        # Orders across Wednesday (Nov 4) and Friday (Nov 6) of 2026-W45
        mock_cur.fetchall.return_value = [
            {
                "order_id": "ord_w1",
                "sku": "sku_1",
                "quantity": 4,
                "unit_price": 25.0,
                "order_status": "COMPLETED",
                "source": "OPERATIONAL_API",
                "idempotency_key": "kw1",
                "event_time": datetime.datetime(2026, 11, 4, 14, 0, 0),
                "product_category_name": "cama_mesa_banho",
            },
            {
                "order_id": "ord_w2",
                "sku": "sku_1",
                "quantity": 6,
                "unit_price": 25.0,
                "order_status": "ALLOCATED",
                "source": "OPERATIONAL_API",
                "idempotency_key": "kw2",
                "event_time": datetime.datetime(2026, 11, 6, 18, 0, 0),
                "product_category_name": "cama_mesa_banho",
            }
        ]

        agg_df, meta = aggregate_operational_orders(connection=mock_conn)
        self.assertEqual(len(agg_df), 1)
        row = agg_df.iloc[0]
        self.assertEqual(row["year_week"], "2026-W45")
        self.assertEqual(row["week_start_date"], "2026-11-02")  # Monday of week 45
        self.assertEqual(row["demand_units"], 10)
        self.assertEqual(row["order_count"], 2)

    # -------------------------------------------------------------------------
    # 3. Quantity aggregation
    # -------------------------------------------------------------------------
    def test_03_quantity_aggregation(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        mock_cur.fetchone.return_value = {"cancelled_count": 0, "cancelled_quantity": 0}
        mock_cur.fetchall.return_value = [
            {
                "order_id": f"ord_q_{i}",
                "sku": "sku_qty",
                "quantity": q,
                "unit_price": 10.0,
                "order_status": "COMPLETED",
                "source": "OPERATIONAL_API",
                "idempotency_key": f"kq_{i}",
                "event_time": datetime.datetime(2026, 11, 2, 10, i, 0),
                "product_category_name": "alimentos",
            }
            for i, q in enumerate([3, 7, 12])
        ]

        agg_df, meta = aggregate_operational_orders(connection=mock_conn)
        self.assertEqual(agg_df.iloc[0]["demand_units"], 22)
        self.assertEqual(agg_df.iloc[0]["order_count"], 3)

    # -------------------------------------------------------------------------
    # 4. Order status filtering
    # -------------------------------------------------------------------------
    def test_04_order_status_filtering(self):
        # Verify status configuration invariants
        self.assertIn("CANCELLED", EXCLUDED_ORDER_STATUSES)
        self.assertIn("PENDING", ELIGIBLE_ORDER_STATUSES)
        self.assertIn("ALLOCATED", ELIGIBLE_ORDER_STATUSES)
        self.assertIn("BACKORDERED", ELIGIBLE_ORDER_STATUSES)
        self.assertIn("COMPLETED", ELIGIBLE_ORDER_STATUSES)

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        # Audit query reports 2 cancelled orders
        mock_cur.fetchone.return_value = {"cancelled_count": 2, "cancelled_quantity": 5}
        # Only eligible orders returned by database query
        mock_cur.fetchall.return_value = [
            {
                "order_id": "ord_p",
                "sku": "sku_1",
                "quantity": 2,
                "unit_price": 10.0,
                "order_status": "PENDING",
                "source": "OPERATIONAL_API",
                "idempotency_key": "kp",
                "event_time": datetime.datetime(2026, 11, 2, 10, 0, 0),
                "product_category_name": "alimentos",
            },
            {
                "order_id": "ord_b",
                "sku": "sku_1",
                "quantity": 4,
                "unit_price": 10.0,
                "order_status": "BACKORDERED",
                "source": "OPERATIONAL_API",
                "idempotency_key": "kb",
                "event_time": datetime.datetime(2026, 11, 2, 11, 0, 0),
                "product_category_name": "alimentos",
            }
        ]

        agg_df, meta = aggregate_operational_orders(connection=mock_conn)
        self.assertEqual(agg_df.iloc[0]["demand_units"], 6)
        self.assertEqual(meta["cancelled_orders_excluded"], 2)
        self.assertEqual(meta["cancelled_quantity_excluded"], 5)

    # -------------------------------------------------------------------------
    # 5. Source provenance separation
    # -------------------------------------------------------------------------
    def test_05_source_provenance_separation(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        mock_cur.fetchone.return_value = {"cancelled_count": 0, "cancelled_quantity": 0}
        mock_cur.fetchall.return_value = []

        # Filter by GENUINE_ONLY
        agg_df, meta = aggregate_operational_orders(connection=mock_conn, source_provenance="GENUINE_ONLY")
        self.assertEqual(meta["source_provenance"], "GENUINE_ONLY")
        # Ensure query executed against operational_orders table
        executed_sql = mock_cur.execute.call_args[0][0]
        self.assertIn("operational_orders", executed_sql)
        self.assertNotIn("sim_orders", executed_sql)

    # -------------------------------------------------------------------------
    # 6. Missing weekly observations
    # -------------------------------------------------------------------------
    def test_06_missing_weekly_observations(self):
        # Given observation in week 1 (Nov 2) and week 3 (Nov 16), week 2 (Nov 9) is missing
        input_df = pd.DataFrame([
            {"product_category": "alimentos", "week_start_date": "2026-11-02", "demand_units": 10, "order_count": 1},
            {"product_category": "alimentos", "week_start_date": "2026-11-16", "demand_units": 15, "order_count": 1},
        ])

        grid_df = build_weekly_demand_grid(input_df, categories=["alimentos"])
        weeks = grid_df["week_start_date"].tolist()
        self.assertEqual(weeks, ["2026-11-02", "2026-11-09", "2026-11-16"])

        w2 = grid_df[grid_df["week_start_date"] == "2026-11-09"].iloc[0]
        self.assertEqual(w2["demand"], 0)
        self.assertFalse(w2["is_observed"])

    # -------------------------------------------------------------------------
    # 7. Zero-demand weeks
    # -------------------------------------------------------------------------
    def test_07_zero_demand_weeks(self):
        input_df = pd.DataFrame([
            {"product_category": "beleza_saude", "week_start_date": "2026-11-02", "demand_units": 8, "order_count": 1},
        ])
        grid_df = build_weekly_demand_grid(input_df, categories=["beleza_saude", "alimentos"])

        # alimentos has 0 demand for that week
        alim_row = grid_df[grid_df["product_category"] == "alimentos"].iloc[0]
        self.assertEqual(alim_row["demand"], 0)
        self.assertFalse(alim_row["is_observed"])

        # beleza_saude has 8 demand
        bel_row = grid_df[grid_df["product_category"] == "beleza_saude"].iloc[0]
        self.assertEqual(bel_row["demand"], 8)
        self.assertTrue(bel_row["is_observed"])

    # -------------------------------------------------------------------------
    # 8. Category encoding
    # -------------------------------------------------------------------------
    def test_08_category_encoding(self):
        input_df = pd.DataFrame([
            {"product_category": "beleza_saude", "week_start_date": "2026-11-02", "year_week": "2026-W45", "demand": 10, "history_weeks_count": 0},
            {"product_category": "unknown_future_cat", "week_start_date": "2026-11-02", "year_week": "2026-W45", "demand": 5, "history_weeks_count": 0},
        ])

        feat_df = compute_operational_features(input_df, category_mapping=self.mock_cat_mapping)
        bel_enc = feat_df[feat_df["product_category"] == "beleza_saude"].iloc[0]["product_category_enc"]
        unseen_enc = feat_df[feat_df["product_category"] == "unknown_future_cat"].iloc[0]["product_category_enc"]

        self.assertEqual(bel_enc, 11)
        self.assertEqual(unseen_enc, -1)
        self.assertEqual(feat_df[feat_df["product_category"] == "unknown_future_cat"].iloc[0]["feature_status"], "UNSEEN_CATEGORY")

    # -------------------------------------------------------------------------
    # 9. All 11 feature calculations
    # -------------------------------------------------------------------------
    def test_09_all_11_feature_calculations(self):
        # Build 10 weeks of sequential data for a single category
        dates = pd.date_range("2026-09-07", periods=10, freq="W-MON").strftime("%Y-%m-%d")
        demands = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]

        rows = []
        for d, qty in zip(dates, demands):
            iso = datetime.date.fromisoformat(d).isocalendar()
            rows.append({
                "product_category": "alimentos",
                "week_start_date": d,
                "year_week": f"{iso[0]}-W{iso[1]:02d}",
                "demand": qty,
                "order_count": 1,
                "is_observed": True,
            })
        grid_df = pd.DataFrame(rows)
        grid_df["history_weeks_count"] = grid_df.groupby("product_category").cumcount()

        feat_df = compute_operational_features(grid_df, category_mapping=self.mock_cat_mapping)

        # Inspect row at index 8 (week 9, dates[8]='2026-11-02', demand=90)
        # Prior demands: [10, 20, 30, 40, 50, 60, 70, 80]
        r = feat_df.iloc[8]

        # 1. lag_1_demand = 80.0
        self.assertEqual(r["lag_1_demand"], 80.0)
        # 2. lag_2_demand = 70.0
        self.assertEqual(r["lag_2_demand"], 70.0)
        # 3. lag_4_demand = 50.0
        self.assertEqual(r["lag_4_demand"], 50.0)
        # 4. rolling_mean_4w: mean([50, 60, 70, 80]) = 65.0
        self.assertAlmostEqual(r["rolling_mean_4w"], 65.0, places=4)
        # 5. rolling_std_4w: std([50, 60, 70, 80]) = 12.9099
        self.assertAlmostEqual(r["rolling_std_4w"], float(np.std([50, 60, 70, 80], ddof=1)), places=4)
        # 6. rolling_mean_8w: mean([10, 20, 30, 40, 50, 60, 70, 80]) = 45.0
        self.assertAlmostEqual(r["rolling_mean_8w"], 45.0, places=4)
        # 7. demand_trend_ratio: (80 + 1) / (65 + 1) = 81 / 66
        self.assertAlmostEqual(r["demand_trend_ratio"], 81.0 / 66.0, places=4)
        # 8. product_category_enc = 1
        self.assertEqual(r["product_category_enc"], 1)
        # 9. month_of_year
        self.assertEqual(r["month_of_year"], 11)
        # 10. week_of_year
        self.assertEqual(r["week_of_year"], 45)
        # 11. quarter
        self.assertEqual(r["quarter"], 4)

    # -------------------------------------------------------------------------
    # 10. Feature order and data types
    # -------------------------------------------------------------------------
    def test_10_feature_order_and_data_types(self):
        grid_df = pd.DataFrame([{
            "product_category": "alimentos",
            "week_start_date": "2026-11-02",
            "year_week": "2026-W45",
            "demand": 10,
            "history_weeks_count": 0
        }])
        feat_df = compute_operational_features(grid_df, category_mapping=self.mock_cat_mapping)

        # Check all 11 features are present in exact order
        feature_sublist = [col for col in feat_df.columns if col in FEATURE_COLUMNS]
        self.assertEqual(feature_sublist, FEATURE_COLUMNS)

        # Check float types
        for fc in ["lag_1_demand", "lag_2_demand", "lag_4_demand", "rolling_mean_4w", "rolling_std_4w", "rolling_mean_8w", "demand_trend_ratio"]:
            self.assertTrue(np.issubdtype(feat_df[fc].dtype, np.floating), f"{fc} not float")

        # Check int types
        for ic in ["product_category_enc", "month_of_year", "week_of_year", "quarter"]:
            self.assertTrue(np.issubdtype(feat_df[ic].dtype, np.integer), f"{ic} not int")

    # -------------------------------------------------------------------------
    # 11. Insufficient history
    # -------------------------------------------------------------------------
    def test_11_insufficient_history(self):
        # 3 weeks of history (< 8)
        dates = pd.date_range("2026-11-02", periods=3, freq="W-MON").strftime("%Y-%m-%d")
        rows = [{
            "product_category": "alimentos",
            "week_start_date": d,
            "year_week": f"2026-W{45+i}",
            "demand": 10 * (i + 1),
            "history_weeks_count": i
        } for i, d in enumerate(dates)]

        feat_df = compute_operational_features(pd.DataFrame(rows), category_mapping=self.mock_cat_mapping)
        statuses = feat_df["feature_status"].tolist()
        self.assertTrue(all(s == "INSUFFICIENT_HISTORY" for s in statuses))

    # -------------------------------------------------------------------------
    # 12. No future data leakage
    # -------------------------------------------------------------------------
    def test_12_no_future_data_leakage(self):
        dates = ["2026-11-02", "2026-11-09", "2026-11-16"]
        rows_base = [
            {"product_category": "alimentos", "week_start_date": dates[0], "year_week": "2026-W45", "demand": 10, "history_weeks_count": 0},
            {"product_category": "alimentos", "week_start_date": dates[1], "year_week": "2026-W46", "demand": 20, "history_weeks_count": 1},
            {"product_category": "alimentos", "week_start_date": dates[2], "year_week": "2026-W47", "demand": 30, "history_weeks_count": 2},
        ]
        # Mutate week 3 demand from 30 to 999999
        rows_mutated = [
            {"product_category": "alimentos", "week_start_date": dates[0], "year_week": "2026-W45", "demand": 10, "history_weeks_count": 0},
            {"product_category": "alimentos", "week_start_date": dates[1], "year_week": "2026-W46", "demand": 20, "history_weeks_count": 1},
            {"product_category": "alimentos", "week_start_date": dates[2], "year_week": "2026-W47", "demand": 999999, "history_weeks_count": 2},
        ]

        f_base = compute_operational_features(pd.DataFrame(rows_base), category_mapping=self.mock_cat_mapping)
        f_mut = compute_operational_features(pd.DataFrame(rows_mutated), category_mapping=self.mock_cat_mapping)

        # Weeks 1 and 2 features must be IDENTICAL despite massive change in week 3 demand
        for feature in FEATURE_COLUMNS:
            self.assertEqual(f_base.iloc[0][feature], f_mut.iloc[0][feature])
            self.assertEqual(f_base.iloc[1][feature], f_mut.iloc[1][feature])

    # -------------------------------------------------------------------------
    # 13. Duplicate order protection
    # -------------------------------------------------------------------------
    def test_13_duplicate_order_protection(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        mock_cur.fetchone.return_value = {"cancelled_count": 0, "cancelled_quantity": 0}
        # Duplicate orders returned due to multiple item joins
        mock_cur.fetchall.return_value = [
            {
                "order_id": "ord_dup_1",
                "sku": "sku_1",
                "quantity": 5,
                "unit_price": 20.0,
                "order_status": "COMPLETED",
                "source": "OPERATIONAL_API",
                "idempotency_key": "kdup1",
                "event_time": datetime.datetime(2026, 11, 2, 10, 0, 0),
                "product_category_name": "alimentos",
            },
            {
                "order_id": "ord_dup_1",  # duplicate order_id
                "sku": "sku_1",
                "quantity": 5,
                "unit_price": 20.0,
                "order_status": "COMPLETED",
                "source": "OPERATIONAL_API",
                "idempotency_key": "kdup1",
                "event_time": datetime.datetime(2026, 11, 2, 10, 0, 0),
                "product_category_name": "alimentos",
            }
        ]

        agg_df, meta = aggregate_operational_orders(connection=mock_conn)
        self.assertEqual(agg_df.iloc[0]["demand_units"], 5)
        self.assertEqual(agg_df.iloc[0]["order_count"], 1)
        self.assertEqual(meta["duplicate_orders_dropped"], 1)

    # -------------------------------------------------------------------------
    # 14. Pipeline Runner integration
    # -------------------------------------------------------------------------
    def test_14_pipeline_runner_integration(self):
        from services.pipeline_runner.registry import get_pipeline_handler
        handler = get_pipeline_handler("Operational Demand Feature Pipeline")
        self.assertIsNotNone(handler, "Handler not registered in Pipeline Runner registry")

        # Test execution handler with mock connection
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_cur.fetchone.side_effect = [
            {"cancelled_count": 0, "cancelled_quantity": 0},  # audit check
            {"dataset_id": 279, "current_version": 1},  # metadata fetch
        ]
        mock_cur.fetchall.return_value = [
            {
                "order_id": "ord_runner",
                "sku": "sku_runner",
                "quantity": 10,
                "unit_price": 30.0,
                "order_status": "COMPLETED",
                "source": "OPERATIONAL_API",
                "idempotency_key": "krunner",
                "event_time": datetime.datetime(2026, 11, 2, 12, 0, 0),
                "product_category_name": "alimentos",
            }
        ]

        result = run_operational_feature_pipeline(
            execution_id=99991,
            pipeline_name="Operational Demand Feature Pipeline",
            connection=mock_conn
        )
        self.assertEqual(result["status"], "COMPLETED")
        self.assertEqual(result["execution_id"], 99991)
        self.assertIn("mysql://operational_feature_store", result["output_location"])

    # -------------------------------------------------------------------------
    # 15. Reproducible feature output
    # -------------------------------------------------------------------------
    def test_15_reproducible_feature_output(self):
        dates = pd.date_range("2026-09-07", periods=5, freq="W-MON").strftime("%Y-%m-%d")
        rows = [{
            "product_category": "alimentos",
            "week_start_date": d,
            "year_week": f"2026-W{36+i}",
            "demand": (i + 1) * 7,
            "history_weeks_count": i
        } for i, d in enumerate(dates)]
        grid_df = pd.DataFrame(rows)

        f1 = compute_operational_features(grid_df, category_mapping=self.mock_cat_mapping)
        f2 = compute_operational_features(grid_df, category_mapping=self.mock_cat_mapping)

        pd.testing.assert_frame_equal(f1, f2)


if __name__ == '__main__':
    unittest.main()
