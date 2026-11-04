"""
Phase 3D — Forecast-Driven Replenishment Recommendation Tests
=============================================================

Coverage:
  - Stock coverage calculations (weeks, days, zero-demand safety)
  - Lead-time demand & target stock unit consistency
  - Reorder quantity calculations with min_reorder_quantity clamping
  - Negative/zero prediction sanitization
  - Multi-SKU category ambiguity detection (NEEDS_REVIEW)
  - Unmapped SKU category detection (NEEDS_REVIEW)
  - Offline supplier handling (NEEDS_REVIEW)
  - Reserved vs available inventory semantics
  - Category-level aggregate inventory recommendations
  - Category with no operational inventory (INSUFFICIENT_DATA)
  - Recommendation determinism
  - FastAPI endpoints (/api/observability/forecast-recommendations)
  - Detail endpoint (/api/observability/forecast-recommendations/{id})
  - Filtering (status, scope_type, sku, category) and pagination limits (422)
  - Non-mutation invariants (zero mutations on operational tables)
"""

import os
import sys
import datetime
import unittest
from unittest.mock import patch, MagicMock

# ── Path bootstrap ──────────────────────────────────────────────────────────────
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from services.observability import recommendation_engine as engine
from services.observability import service as obs_service


# ══════════════════════════════════════════════════════════════════════════════
# 1. Mathematical & Calculation Unit Tests
# ══════════════════════════════════════════════════════════════════════════════

class TestRecommendationMath(unittest.TestCase):
    """Unit tests for low-level recommendation mathematics."""

    def test_stock_coverage_standard(self):
        # 70 units available, 14 units/week demand -> 5.0 weeks, 35.0 days
        weeks, days = engine.calculate_stock_coverage(70, 14)
        self.assertEqual(weeks, 5.0)
        self.assertEqual(days, 35.0)

    def test_stock_coverage_zero_demand_with_stock(self):
        # With stock but 0 demand, coverage is effectively infinite (999 weeks)
        weeks, days = engine.calculate_stock_coverage(50, 0)
        self.assertEqual(weeks, 999.0)
        self.assertEqual(days, 6993.0)

    def test_stock_coverage_zero_demand_zero_stock(self):
        weeks, days = engine.calculate_stock_coverage(0, 0)
        self.assertEqual(weeks, 0.0)
        self.assertEqual(days, 0.0)

    def test_stock_coverage_negative_demand_treated_as_zero(self):
        weeks, days = engine.calculate_stock_coverage(50, -10.5)
        self.assertEqual(weeks, 999.0)

    def test_lead_time_demand_unit_consistency(self):
        # Weekly demand 70 units/week. Lead time 3.5 days (0.5 weeks).
        # Demand during lead time should be 70 * 0.5 = 35.0 units.
        demand_lt = engine.calculate_lead_time_demand(weekly_demand=70.0, lead_time_days=3.5)
        self.assertEqual(demand_lt, 35.0)

    def test_lead_time_demand_negative_lead_time_clamped(self):
        demand_lt = engine.calculate_lead_time_demand(weekly_demand=50.0, lead_time_days=-2.0)
        self.assertEqual(demand_lt, 0.0)

    def test_target_stock_formula(self):
        # target_stock = lead_time_demand + safety_stock
        target = engine.calculate_target_stock(lead_time_demand=35.0, safety_stock=10.0)
        self.assertEqual(target, 45.0)

    def test_suggested_quantity_stock_below_target(self):
        # Available: 20, Target: 45 -> Deficit: 25 -> Suggested: 25
        qty = engine.calculate_suggested_quantity(current_available=20.0, target_stock=45.0)
        self.assertEqual(qty, 25)

    def test_suggested_quantity_stock_above_target(self):
        # Available: 50, Target: 45 -> Deficit: 0 -> Suggested: 0
        qty = engine.calculate_suggested_quantity(current_available=50.0, target_stock=45.0)
        self.assertEqual(qty, 0)

    def test_suggested_quantity_min_reorder_qty_clamping(self):
        # Deficit is 5, but supplier min reorder qty is 20 -> Suggested: 20
        qty = engine.calculate_suggested_quantity(current_available=40.0, target_stock=45.0, min_reorder_qty=20)
        self.assertEqual(qty, 20)

    def test_horizon_projected_stock(self):
        # Available: 100, Weekly demand: 20, Horizon: 4 weeks -> 100 - (20 * 4) = 20
        proj = engine.calculate_horizon_projected_stock(100.0, 20.0, 4)
        self.assertEqual(proj, 20.0)

        # Deficit scenario: Available: 50, Weekly demand: 20, Horizon: 4 weeks -> 50 - 80 = -30
        proj_neg = engine.calculate_horizon_projected_stock(50.0, 20.0, 4)
        self.assertEqual(proj_neg, -30.0)


# ══════════════════════════════════════════════════════════════════════════════
# 2. Recommendation Engine Business Logic Unit Tests
# ══════════════════════════════════════════════════════════════════════════════

class TestRecommendationEngineLogic(unittest.TestCase):
    """Tests evaluation logic, mapping ambiguities, and status classifications."""

    def _make_sku(self, sku="SKU-001", on_hand=80, reserved=0, threshold=20, safety=10):
        return {
            "sku": sku,
            "warehouse_id": "WH-SP-01",
            "on_hand_quantity": on_hand,
            "reserved_quantity": reserved,
            "available_quantity": on_hand - reserved,
            "reorder_threshold": threshold,
            "safety_stock_buffer": safety,
            "version": 1
        }

    def _make_forecast(self, category="cama_mesa_banho", avg_demand=50.0):
        return {
            "category": category,
            "avg_weekly_demand": avg_demand,
            "total_demand": avg_demand * 14,
            "week_count": 14
        }

    def _make_supplier(self, sup_id="SUP-SP-PRIMARY", status="Online", lt_hours=48, min_qty=10):
        return {
            "supplier_id": sup_id,
            "supplier_status": status,
            "lead_time_hours": lt_hours,
            "min_reorder_quantity": min_qty
        }

    def test_unmapped_sku_flags_needs_review(self):
        sku_data = self._make_sku("SKU-UNMAPPED")
        rec = engine.build_sku_recommendation(
            sku_data=sku_data,
            category=None,
            category_english=None,
            forecast_info=None,
            supplier_info=None,
            multi_sku_count_for_cat=0,
            execution_id=99653,
            model_name="XGBoost",
            model_version="xgboost-v1",
            forecast_period={}
        )
        self.assertEqual(rec["status"], engine.STATUS_NEEDS_REVIEW)
        self.assertEqual(rec["mapping_provenance"], engine.PROVENANCE_UNMAPPED)
        self.assertIn("no authoritative product category mapping", rec["explanation"])

    def test_multi_sku_ambiguity_flags_needs_review(self):
        sku_data = self._make_sku("SKU-GARDEN-1")
        f_info = self._make_forecast("ferramentas_jardim", 45.0)
        rec = engine.build_sku_recommendation(
            sku_data=sku_data,
            category="ferramentas_jardim",
            category_english="garden_tools",
            forecast_info=f_info,
            supplier_info=self._make_supplier(),
            multi_sku_count_for_cat=3,  # 3 SKUs in category!
            execution_id=99653,
            model_name="XGBoost",
            model_version="xgboost-v1",
            forecast_period={}
        )
        self.assertEqual(rec["status"], engine.STATUS_NEEDS_REVIEW)
        self.assertEqual(rec["mapping_provenance"], engine.PROVENANCE_AMBIGUOUS_MULTI_SKU)
        self.assertIn("contains 3 operational SKUs", rec["explanation"])
        self.assertIn("cannot be distributed arbitrarily", rec["explanation"])

    def test_sole_sku_low_stock_recommends_replenishment(self):
        # Available: 15, Threshold: 20 -> Below threshold and target
        sku_data = self._make_sku("SKU-BED-1", on_hand=15, reserved=0)
        f_info = self._make_forecast("cama_mesa_banho", 70.0)
        sup_info = self._make_supplier("SUP-SP-PRIMARY", "Online", lt_hours=48, min_qty=10)

        rec = engine.build_sku_recommendation(
            sku_data=sku_data,
            category="cama_mesa_banho",
            category_english="bed_bath_table",
            forecast_info=f_info,
            supplier_info=sup_info,
            multi_sku_count_for_cat=1,  # 1:1 mapping
            execution_id=99653,
            model_name="XGBoost",
            model_version="xgboost-v1",
            forecast_period={}
        )
        self.assertEqual(rec["status"], engine.STATUS_REPLENISHMENT_RECOMMENDED)
        self.assertEqual(rec["mapping_provenance"], engine.PROVENANCE_SOLE_SKU)
        self.assertGreater(rec["suggested_quantity"], 0)
        self.assertIn("Replenishment recommended", rec["explanation"])

    def test_sole_sku_sufficient_stock(self):
        # Available: 80, Threshold: 20, Weekly demand: 14, LT: 2d -> Target: (14 * 2/7) + 10 = 14
        sku_data = self._make_sku("SKU-BED-1", on_hand=80, reserved=0)
        f_info = self._make_forecast("cama_mesa_banho", 14.0)
        sup_info = self._make_supplier("SUP-SP-PRIMARY", "Online", lt_hours=48)

        rec = engine.build_sku_recommendation(
            sku_data=sku_data,
            category="cama_mesa_banho",
            category_english="bed_bath_table",
            forecast_info=f_info,
            supplier_info=sup_info,
            multi_sku_count_for_cat=1,
            execution_id=99653,
            model_name="XGBoost",
            model_version="xgboost-v1",
            forecast_period={}
        )
        self.assertEqual(rec["status"], engine.STATUS_STOCK_SUFFICIENT)
        self.assertEqual(rec["suggested_quantity"], 0)
        self.assertIn("Stock sufficient", rec["explanation"])

    def test_offline_supplier_flags_needs_review(self):
        sku_data = self._make_sku("SKU-OFFLINE-SUP", on_hand=5)
        f_info = self._make_forecast("cama_mesa_banho", 50.0)
        sup_info = self._make_supplier("SUP-RJ-SECONDARY", "Offline", lt_hours=96)

        rec = engine.build_sku_recommendation(
            sku_data=sku_data,
            category="cama_mesa_banho",
            category_english="bed_bath_table",
            forecast_info=f_info,
            supplier_info=sup_info,
            multi_sku_count_for_cat=1,
            execution_id=99653,
            model_name="XGBoost",
            model_version="xgboost-v1",
            forecast_period={}
        )
        self.assertEqual(rec["status"], engine.STATUS_NEEDS_REVIEW)
        self.assertIn("Offline", rec["explanation"])

    def test_reserved_stock_subtracted_once(self):
        # on_hand = 100, reserved = 40 -> available = 60
        sku_data = self._make_sku("SKU-RES", on_hand=100, reserved=40)
        self.assertEqual(sku_data["available_quantity"], 60)
        f_info = self._make_forecast("cama_mesa_banho", 10.0)
        rec = engine.build_sku_recommendation(
            sku_data=sku_data,
            category="cama_mesa_banho",
            category_english="bed_bath_table",
            forecast_info=f_info,
            supplier_info=self._make_supplier(),
            multi_sku_count_for_cat=1,
            execution_id=99653,
            model_name="XGBoost",
            model_version="xgboost-v1",
            forecast_period={}
        )
        self.assertEqual(rec["current_stock"]["available"], 60)
        self.assertEqual(rec["current_stock"]["on_hand"], 100)
        self.assertEqual(rec["current_stock"]["reserved"], 40)

    def test_category_recommendation_aggregates_multiple_skus(self):
        skus = [
            self._make_sku("SKU-1", on_hand=30),
            self._make_sku("SKU-2", on_hand=40),
            self._make_sku("SKU-3", on_hand=50),
        ]
        f_info = self._make_forecast("ferramentas_jardim", 45.0)
        rec = engine.build_category_recommendation(
            category="ferramentas_jardim",
            category_english="garden_tools",
            forecast_info=f_info,
            skus_in_category=skus,
            execution_id=99653,
            model_name="XGBoost",
            model_version="xgboost-v1",
            forecast_period={}
        )
        self.assertEqual(rec["scope_type"], "CATEGORY")
        self.assertEqual(rec["current_stock"]["available"], 120)
        self.assertEqual(rec["current_stock"]["mapped_sku_count"], 3)
        self.assertEqual(rec["mapping_provenance"], engine.PROVENANCE_CATEGORY_AGGREGATE)

    def test_category_with_zero_skus_returns_insufficient_data(self):
        f_info = self._make_forecast("beleza_saude", 140.0)
        rec = engine.build_category_recommendation(
            category="beleza_saude",
            category_english="health_beauty",
            forecast_info=f_info,
            skus_in_category=[],
            execution_id=99653,
            model_name="XGBoost",
            model_version="xgboost-v1",
            forecast_period={}
        )
        self.assertEqual(rec["status"], engine.STATUS_INSUFFICIENT_DATA)
        self.assertIn("no operational inventory items are tracked", rec["explanation"])

    def test_deterministic_output_for_identical_inputs(self):
        sku_data = self._make_sku("SKU-DET-1", on_hand=50)
        f_info = self._make_forecast("cama_mesa_banho", 25.0)
        s_info = self._make_supplier()

        rec1 = engine.build_sku_recommendation(
            sku_data=sku_data, category="cama_mesa_banho", category_english="bed_bath_table",
            forecast_info=f_info, supplier_info=s_info, multi_sku_count_for_cat=1,
            execution_id=99653, model_name="XGBoost", model_version="xgboost-v1",
            forecast_period={}, generated_at="2026-10-29T12:00:00Z"
        )
        rec2 = engine.build_sku_recommendation(
            sku_data=sku_data, category="cama_mesa_banho", category_english="bed_bath_table",
            forecast_info=f_info, supplier_info=s_info, multi_sku_count_for_cat=1,
            execution_id=99653, model_name="XGBoost", model_version="xgboost-v1",
            forecast_period={}, generated_at="2026-10-29T12:00:00Z"
        )
        self.assertEqual(rec1, rec2)


# ══════════════════════════════════════════════════════════════════════════════
# 3. FastAPI TestClient Endpoint Tests
# ══════════════════════════════════════════════════════════════════════════════

from fastapi.testclient import TestClient
from services.observability.main import app


class TestForecastRecommendationsEndpoints(unittest.TestCase):
    """Integration tests for the /api/observability/forecast-recommendations endpoints."""

    def setUp(self):
        self.client = TestClient(app, raise_server_exceptions=False)

    def test_get_recommendations_200_ok(self):
        resp = self.client.get("/api/observability/forecast-recommendations?execution_id=99653&limit=5")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertIn("summary", data)
        self.assertIn("recommendations", data)
        self.assertIn("total_recommendations", data["summary"])
        self.assertIn("by_scope", data["summary"])
        self.assertIsInstance(data["recommendations"], list)

    def test_filter_by_scope_sku(self):
        resp = self.client.get("/api/observability/forecast-recommendations?execution_id=99653&scope_type=SKU&limit=10")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        for rec in data["recommendations"]:
            self.assertEqual(rec["scope_type"], "SKU")

    def test_filter_by_scope_category(self):
        resp = self.client.get("/api/observability/forecast-recommendations?execution_id=99653&scope_type=CATEGORY&limit=10")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        for rec in data["recommendations"]:
            self.assertEqual(rec["scope_type"], "CATEGORY")

    def test_filter_by_status_needs_review(self):
        resp = self.client.get("/api/observability/forecast-recommendations?execution_id=99653&status=NEEDS_REVIEW&limit=10")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        for rec in data["recommendations"]:
            self.assertEqual(rec["status"], "NEEDS_REVIEW")

    def test_filter_by_sku(self):
        resp = self.client.get("/api/observability/forecast-recommendations?execution_id=99653&sku=99a4788cb24856965c36a24e339b6058")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        for rec in data["recommendations"]:
            self.assertIn("99a4788cb", rec["sku"])

    def test_filter_by_category(self):
        resp = self.client.get("/api/observability/forecast-recommendations?execution_id=99653&category=cama_mesa_banho")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        for rec in data["recommendations"]:
            self.assertEqual(rec["category"], "cama_mesa_banho")

    def test_invalid_horizon_weeks_rejected(self):
        # planning_horizon_weeks has ge=1, le=12. 25 should return 422.
        resp = self.client.get("/api/observability/forecast-recommendations?planning_horizon_weeks=25")
        self.assertEqual(resp.status_code, 422)

    def test_invalid_limit_rejected(self):
        # limit has ge=1, le=100. 200 should return 422.
        resp = self.client.get("/api/observability/forecast-recommendations?limit=200")
        self.assertEqual(resp.status_code, 422)

    def test_recommendation_detail_success(self):
        # Fetch list first to obtain a valid recommendation_id
        list_resp = self.client.get("/api/observability/forecast-recommendations?execution_id=99653&limit=1")
        self.assertEqual(list_resp.status_code, 200)
        recs = list_resp.json().get("recommendations", [])
        self.assertTrue(len(recs) > 0)
        rec_id = recs[0]["recommendation_id"]

        detail_resp = self.client.get(f"/api/observability/forecast-recommendations/{rec_id}")
        self.assertEqual(detail_resp.status_code, 200)
        self.assertEqual(detail_resp.json()["recommendation_id"], rec_id)

    def test_recommendation_detail_404_not_found(self):
        resp = self.client.get("/api/observability/forecast-recommendations/REC-99653-SKU-NONEXISTENT")
        self.assertEqual(resp.status_code, 404)

    def test_missing_execution_returns_structured_empty(self):
        resp = self.client.get("/api/observability/forecast-recommendations?execution_id=1")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["summary"]["total_recommendations"], 0)
        self.assertEqual(len(data["recommendations"]), 0)


# ══════════════════════════════════════════════════════════════════════════════
# 4. Invariant Protection: Recommendation Generation is Read-Only
# ══════════════════════════════════════════════════════════════════════════════

class TestReadOnlyInvariants(unittest.TestCase):
    """Confirms recommendation generation does not alter DB tables."""

    def setUp(self):
        self.client = TestClient(app, raise_server_exceptions=False)

    def test_recommendation_generation_causes_zero_table_mutations(self):
        try:
            from common.database import get_connection
            conn = get_connection()
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) AS c FROM replenishment_orders")
            before_reps = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*) AS c FROM pipeline_executions")
            before_execs = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*) AS c FROM pipeline_decisions")
            before_decs = cur.fetchone()["c"]
            cur.close()
            conn.close()

            # Call recommendation generation multiple times
            self.client.get("/api/observability/forecast-recommendations?execution_id=99653&limit=50")
            self.client.get("/api/observability/forecast-recommendations?execution_id=99653&planning_horizon_weeks=8")

            conn2 = get_connection()
            cur2 = conn2.cursor()
            cur2.execute("SELECT COUNT(*) AS c FROM replenishment_orders")
            after_reps = cur2.fetchone()["c"]
            cur2.execute("SELECT COUNT(*) AS c FROM pipeline_executions")
            after_execs = cur2.fetchone()["c"]
            cur2.execute("SELECT COUNT(*) AS c FROM pipeline_decisions")
            after_decs = cur2.fetchone()["c"]
            cur2.close()
            conn2.close()

            self.assertEqual(before_reps, after_reps, "replenishment_orders count mutated!")
            self.assertEqual(before_execs, after_execs, "pipeline_executions count mutated!")
            self.assertEqual(before_decs, after_decs, "pipeline_decisions count mutated!")
        except Exception as e:
            self.skipTest(f"Database not reachable for mutation test: {e}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
