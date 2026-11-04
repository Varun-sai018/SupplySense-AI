"""
SupplySense AI — Forecast-Driven Replenishment Recommendation Engine (Phase 3D)

Purely read-only advisory computation module.
Correlates ML demand forecasts with real-time operational inventory snapshots,
catalog lead times, and safety stock buffers to recommend replenishment quantities.

Safety Invariants:
  - Generates recommendations strictly as read-only advice.
  - NEVER creates replenishment orders, triggers Rule B, or mutates inventory.
  - Never assumes category-level forecast maps 1-to-1 to SKUs without validation.
  - Explicitly flags ambiguous multi-SKU and unmapped SKUs as NEEDS_REVIEW.
"""

import math
import datetime
from typing import Dict, Any, List, Optional, Tuple


# Valid Recommendation Statuses
STATUS_REPLENISHMENT_RECOMMENDED = "REPLENISHMENT_RECOMMENDED"
STATUS_STOCK_SUFFICIENT = "STOCK_SUFFICIENT"
STATUS_NEEDS_REVIEW = "NEEDS_REVIEW"
STATUS_INSUFFICIENT_DATA = "INSUFFICIENT_DATA"

VALID_STATUSES = {
    STATUS_REPLENISHMENT_RECOMMENDED,
    STATUS_STOCK_SUFFICIENT,
    STATUS_NEEDS_REVIEW,
    STATUS_INSUFFICIENT_DATA,
}

# Mapping Provenance Enums
PROVENANCE_SOLE_SKU = "SOLE_OPERATIONAL_SKU_FOR_CATEGORY"
PROVENANCE_AMBIGUOUS_MULTI_SKU = "AMBIGUOUS_MULTI_SKU"
PROVENANCE_CATEGORY_AGGREGATE = "CATEGORY_AGGREGATE"
PROVENANCE_UNMAPPED = "UNMAPPED"


def calculate_stock_coverage(available_stock: float, weekly_demand: float) -> Tuple[float, float]:
    """
    Computes stock coverage in weeks and days.
    Handles zero/negative demand safely without division by zero.
    Returns (coverage_weeks, coverage_days).
    """
    clean_stock = max(0.0, float(available_stock))
    clean_demand = max(0.0, float(weekly_demand))

    if clean_demand <= 0.0:
        if clean_stock > 0:
            return 999.0, 6993.0
        return 0.0, 0.0

    coverage_weeks = round(clean_stock / clean_demand, 2)
    coverage_days = round(coverage_weeks * 7.0, 1)
    return coverage_weeks, coverage_days


def calculate_lead_time_demand(weekly_demand: float, lead_time_days: float) -> float:
    """
    Computes expected demand during supplier lead time.
    Ensures unit consistency: lead_time_weeks = lead_time_days / 7.0.
    """
    clean_demand = max(0.0, float(weekly_demand))
    clean_lt_days = max(0.0, float(lead_time_days))
    lead_time_weeks = clean_lt_days / 7.0
    return round(clean_demand * lead_time_weeks, 2)


def calculate_target_stock(lead_time_demand: float, safety_stock: float) -> float:
    """
    target_stock = expected_demand_during_lead_time + safety_stock
    """
    return round(max(0.0, float(lead_time_demand)) + max(0.0, float(safety_stock)), 2)


def calculate_suggested_quantity(
    current_available: float,
    target_stock: float,
    min_reorder_qty: int = 0
) -> int:
    """
    suggested_quantity = max(0, ceil(target_stock - current_available_stock))
    If replenishment is required and min_reorder_qty is specified, clamps up to min_reorder_qty.
    """
    deficit = max(0.0, float(target_stock) - float(current_available))
    if deficit <= 0.0:
        return 0

    qty = int(math.ceil(deficit))
    if min_reorder_qty > 0 and qty < min_reorder_qty:
        qty = min_reorder_qty
    return qty


def calculate_horizon_projected_stock(
    current_available: float,
    weekly_demand: float,
    horizon_weeks: int
) -> float:
    """
    projected_stock = current_available_stock - expected_demand_during_horizon
    """
    clean_demand = max(0.0, float(weekly_demand))
    expected_demand = clean_demand * max(1, int(horizon_weeks))
    return round(float(current_available) - expected_demand, 2)


def build_sku_recommendation(
    sku_data: Dict[str, Any],
    category: Optional[str],
    category_english: Optional[str],
    forecast_info: Optional[Dict[str, Any]],
    supplier_info: Optional[Dict[str, Any]],
    multi_sku_count_for_cat: int,
    execution_id: int,
    model_name: str,
    model_version: str,
    forecast_period: Dict[str, Any],
    planning_horizon_weeks: int = 4,
    default_safety_stock: int = 10,
    generated_at: Optional[str] = None
) -> Dict[str, Any]:
    """
    Builds a single SKU-scoped replenishment recommendation.
    Enforces strict mapping validation:
      - If unmapped: NEEDS_REVIEW
      - If category has multiple operational SKUs: NEEDS_REVIEW (AMBIGUOUS_MULTI_SKU)
      - If forecast missing: INSUFFICIENT_DATA
      - If stock < target: REPLENISHMENT_RECOMMENDED
      - If stock >= target: STOCK_SUFFICIENT
    """
    if not generated_at:
        generated_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

    sku = sku_data["sku"]
    wh_id = sku_data.get("warehouse_id", "WH-SP-01")
    on_hand = int(sku_data.get("on_hand_quantity", 0))
    reserved = int(sku_data.get("reserved_quantity", 0))
    # Authoritative available stock: on_hand - reserved
    available = int(sku_data.get("available_quantity", on_hand - reserved))
    reorder_thresh = int(sku_data.get("reorder_threshold", 20))
    safety_stock = int(sku_data.get("safety_stock_buffer", default_safety_stock))
    version = sku_data.get("version", 0)

    rec_id = f"REC-{execution_id}-SKU-{sku}"

    # Base recommendation structure
    base = {
        "recommendation_id": rec_id,
        "scope_type": "SKU",
        "scope_id": sku,
        "sku": sku,
        "category": category,
        "category_english": category_english or category,
        "warehouse_id": wh_id,
        "execution_id": execution_id,
        "model_name": model_name,
        "model_version": model_version,
        "forecast_period": forecast_period,
        "current_stock": {
            "on_hand": on_hand,
            "reserved": reserved,
            "available": available,
            "reorder_threshold": reorder_thresh,
            "safety_stock_buffer": safety_stock,
            "version": version
        },
        "forecast_demand": {
            "weekly_avg": None,
            "planning_horizon_weeks": planning_horizon_weeks,
            "horizon_total": None,
            "unit": "units/week"
        },
        "lead_time": {
            "days": None,
            "hours": None,
            "source_supplier_id": None,
            "supplier_status": None,
            "demand_during_lead_time": None
        },
        "planning_assumptions": {
            "planning_horizon_weeks": planning_horizon_weeks,
            "safety_stock_units": safety_stock,
            "min_reorder_quantity": 0,
            "lead_time_compatibility": "INCOMPLETE"
        },
        "stock_coverage": {
            "coverage_weeks": None,
            "coverage_days": None
        },
        "target_stock": None,
        "projected_stock_end_horizon": None,
        "suggested_quantity": 0,
        "status": STATUS_NEEDS_REVIEW,
        "explanation": "",
        "mapping_provenance": PROVENANCE_UNMAPPED,
        "data_freshness": {
            "forecast_execution_id": execution_id,
            "is_stale_forecast": False
        },
        "generated_at": generated_at
    }

    # 1. Check mapping existence
    if not category:
        base["status"] = STATUS_NEEDS_REVIEW
        base["mapping_provenance"] = PROVENANCE_UNMAPPED
        base["explanation"] = (
            f"SKU '{sku}' has no authoritative product category mapping in the product catalog. "
            "Cannot associate with model forecast without manual category attribution."
        )
        return base

    # 2. Check forecast existence
    if not forecast_info:
        base["status"] = STATUS_INSUFFICIENT_DATA
        base["mapping_provenance"] = PROVENANCE_SOLE_SKU if multi_sku_count_for_cat <= 1 else PROVENANCE_AMBIGUOUS_MULTI_SKU
        base["explanation"] = (
            f"Mapped category '{category}' has no demand forecast records in execution #{execution_id}."
        )
        return base

    weekly_demand = max(0.0, float(forecast_info.get("avg_weekly_demand", 0.0)))
    horizon_total = round(weekly_demand * planning_horizon_weeks, 2)
    base["forecast_demand"]["weekly_avg"] = weekly_demand
    base["forecast_demand"]["horizon_total"] = horizon_total

    coverage_w, coverage_d = calculate_stock_coverage(available, weekly_demand)
    base["stock_coverage"]["coverage_weeks"] = coverage_w
    base["stock_coverage"]["coverage_days"] = coverage_d
    base["projected_stock_end_horizon"] = calculate_horizon_projected_stock(available, weekly_demand, planning_horizon_weeks)

    # 3. Check multi-SKU ambiguity
    if multi_sku_count_for_cat > 1:
        base["status"] = STATUS_NEEDS_REVIEW
        base["mapping_provenance"] = PROVENANCE_AMBIGUOUS_MULTI_SKU
        base["explanation"] = (
            f"Ambiguous multi-SKU mapping: Category '{category}' contains {multi_sku_count_for_cat} operational SKUs. "
            f"Category forecast demand ({weekly_demand} units/wk) cannot be distributed arbitrarily to SKU '{sku}' "
            "without an SKU-level allocation model. Manual review required."
        )
        return base

    # 4. Single operational SKU for category — assess lead time & suppliers
    base["mapping_provenance"] = PROVENANCE_SOLE_SKU

    lt_days = 2.0  # default fallback if supplier missing
    lt_hours = 48
    sup_id = None
    sup_status = "UNKNOWN"
    min_reorder_qty = 0

    if supplier_info:
        sup_id = supplier_info.get("supplier_id")
        sup_status = supplier_info.get("supplier_status", "Online")
        lt_hours = supplier_info.get("lead_time_hours") or 48
        lt_days = round(lt_hours / 24.0, 1)
        min_reorder_qty = int(supplier_info.get("min_reorder_quantity") or 0)

    lt_demand = calculate_lead_time_demand(weekly_demand, lt_days)
    target_stock = calculate_target_stock(lt_demand, safety_stock)
    suggested_qty = calculate_suggested_quantity(available, target_stock, min_reorder_qty)

    base["lead_time"]["days"] = lt_days
    base["lead_time"]["hours"] = lt_hours
    base["lead_time"]["source_supplier_id"] = sup_id
    base["lead_time"]["supplier_status"] = sup_status
    base["lead_time"]["demand_during_lead_time"] = lt_demand

    base["planning_assumptions"]["min_reorder_quantity"] = min_reorder_qty
    base["planning_assumptions"]["lead_time_compatibility"] = "VERIFIED_HOURS_TO_DAYS"
    base["target_stock"] = target_stock
    base["suggested_quantity"] = suggested_qty

    # Status decision
    if sup_status == "Offline":
        base["status"] = STATUS_NEEDS_REVIEW
        base["explanation"] = (
            f"Primary supplier '{sup_id}' for SKU '{sku}' is Offline. "
            f"Available stock is {available} units (target: {target_stock}). Replenishment cannot be safely sourced."
        )
    elif available < target_stock or available < reorder_thresh:
        base["status"] = STATUS_REPLENISHMENT_RECOMMENDED
        base["explanation"] = (
            f"Replenishment recommended: Available stock ({available} units) is below target stock ({target_stock} units: "
            f"lead-time demand {lt_demand} + safety buffer {safety_stock}). Suggested reorder: {suggested_qty} units. "
            f"Stock coverage is ~{coverage_d} days under {weekly_demand} units/wk forecast."
        )
    else:
        base["status"] = STATUS_STOCK_SUFFICIENT
        base["explanation"] = (
            f"Stock sufficient: Available stock ({available} units) meets target stock ({target_stock} units: "
            f"lead-time demand {lt_demand} + safety buffer {safety_stock}). Stock coverage is ~{coverage_d} days."
        )

    return base


def build_category_recommendation(
    category: str,
    category_english: Optional[str],
    forecast_info: Dict[str, Any],
    skus_in_category: List[Dict[str, Any]],
    execution_id: int,
    model_name: str,
    model_version: str,
    forecast_period: Dict[str, Any],
    planning_horizon_weeks: int = 4,
    default_safety_stock: int = 15,
    generated_at: Optional[str] = None
) -> Dict[str, Any]:
    """
    Builds a Category-scoped replenishment recommendation aggregating all operational inventory
    under this category.
    """
    if not generated_at:
        generated_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

    rec_id = f"REC-{execution_id}-CAT-{category}"

    # Aggregate inventory
    total_on_hand = sum(int(s.get("on_hand_quantity", 0)) for s in skus_in_category)
    total_reserved = sum(int(s.get("reserved_quantity", 0)) for s in skus_in_category)
    total_available = sum(int(s.get("available_quantity", s.get("on_hand_quantity", 0) - s.get("reserved_quantity", 0))) for s in skus_in_category)
    total_threshold = sum(int(s.get("reorder_threshold", 20)) for s in skus_in_category)
    total_safety_stock = sum(int(s.get("safety_stock_buffer", default_safety_stock)) for s in skus_in_category) or default_safety_stock
    sku_ids = [s["sku"] for s in skus_in_category]

    weekly_demand = max(0.0, float(forecast_info.get("avg_weekly_demand", 0.0)))
    horizon_total = round(weekly_demand * planning_horizon_weeks, 2)
    coverage_w, coverage_d = calculate_stock_coverage(total_available, weekly_demand)

    # Lead time assumption at category level (default ~3 days / 72 hours)
    lt_days = 3.0
    lt_hours = 72
    lt_demand = calculate_lead_time_demand(weekly_demand, lt_days)
    target_stock = calculate_target_stock(lt_demand, total_safety_stock)
    suggested_qty = calculate_suggested_quantity(total_available, target_stock)

    base = {
        "recommendation_id": rec_id,
        "scope_type": "CATEGORY",
        "scope_id": category,
        "sku": None,
        "category": category,
        "category_english": category_english or category,
        "warehouse_id": "WH-SP-01",
        "execution_id": execution_id,
        "model_name": model_name,
        "model_version": model_version,
        "forecast_period": forecast_period,
        "current_stock": {
            "on_hand": total_on_hand,
            "reserved": total_reserved,
            "available": total_available,
            "reorder_threshold": total_threshold,
            "safety_stock_buffer": total_safety_stock,
            "version": None,
            "mapped_sku_count": len(skus_in_category),
            "mapped_skus": sku_ids
        },
        "forecast_demand": {
            "weekly_avg": weekly_demand,
            "planning_horizon_weeks": planning_horizon_weeks,
            "horizon_total": horizon_total,
            "unit": "units/week"
        },
        "lead_time": {
            "days": lt_days,
            "hours": lt_hours,
            "source_supplier_id": "AGGREGATE_SUPPLIER_POOL",
            "supplier_status": "Online",
            "demand_during_lead_time": lt_demand
        },
        "planning_assumptions": {
            "planning_horizon_weeks": planning_horizon_weeks,
            "safety_stock_units": total_safety_stock,
            "min_reorder_quantity": 0,
            "lead_time_compatibility": "CATEGORY_ESTIMATED_3D"
        },
        "stock_coverage": {
            "coverage_weeks": coverage_w,
            "coverage_days": coverage_d
        },
        "target_stock": target_stock,
        "projected_stock_end_horizon": calculate_horizon_projected_stock(total_available, weekly_demand, planning_horizon_weeks),
        "suggested_quantity": suggested_qty,
        "status": STATUS_STOCK_SUFFICIENT,
        "explanation": "",
        "mapping_provenance": PROVENANCE_CATEGORY_AGGREGATE if len(skus_in_category) > 0 else PROVENANCE_UNMAPPED,
        "data_freshness": {
            "forecast_execution_id": execution_id,
            "is_stale_forecast": False
        },
        "generated_at": generated_at
    }

    if len(skus_in_category) == 0:
        base["status"] = STATUS_INSUFFICIENT_DATA
        base["explanation"] = (
            f"Category '{category}' has forecasted weekly demand ({weekly_demand} units/wk), "
            "but no operational inventory items are tracked for this category in the warehouse."
        )
    elif total_available < target_stock or total_available < total_threshold:
        base["status"] = STATUS_REPLENISHMENT_RECOMMENDED
        base["explanation"] = (
            f"Category replenishment recommended: Combined available stock ({total_available} units across "
            f"{len(skus_in_category)} SKUs) is below target stock ({target_stock} units). "
            f"Suggested aggregate reorder: {suggested_qty} units. Coverage: ~{coverage_d} days."
        )
    else:
        base["status"] = STATUS_STOCK_SUFFICIENT
        base["explanation"] = (
            f"Category stock sufficient: Combined available stock ({total_available} units across "
            f"{len(skus_in_category)} SKUs) meets target stock ({target_stock} units). Coverage: ~{coverage_d} days."
        )

    return base


def generate_recommendations(
    forecast_data: Dict[str, Any],
    inventory_snapshot: List[Dict[str, Any]],
    sku_to_category_map: Dict[str, str],
    category_translations: Dict[str, str],
    supplier_catalog_map: Dict[str, Dict[str, Any]],
    planning_horizon_weeks: int = 4,
    scope_filter: Optional[str] = None,
    status_filter: Optional[str] = None,
    category_filter: Optional[str] = None,
    sku_filter: Optional[str] = None,
    warehouse_filter: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
) -> Dict[str, Any]:
    """
    Coordinates evaluation of SKU and Category recommendations across forecast and inventory snapshots.
    Produces deterministic, prioritized, and filtered recommendation results with summary metrics.
    """
    execution_id = int(forecast_data.get("execution_id") or 0)
    model_name = str(forecast_data.get("model_name") or "XGBoost")
    model_version = str(forecast_data.get("model_version") or "xgboost-v1")
    generated_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

    forecast_period = {
        "start_week": forecast_data.get("min_forecast_week"),
        "end_week": forecast_data.get("max_forecast_week"),
        "total_weeks": 14
    }

    # Build forecast lookup by category
    avail_cats = forecast_data.get("available_categories", [])
    forecast_by_cat = {c["category"]: c for c in avail_cats}

    # Map inventory SKUs to categories and count how many operational SKUs share each category
    category_to_skus = {}
    for inv in inventory_snapshot:
        sku = inv["sku"]
        cat = sku_to_category_map.get(sku)
        if cat:
            category_to_skus.setdefault(cat, []).append(inv)

    recommendations = []

    # 1. Generate SKU-scoped recommendations
    if scope_filter is None or scope_filter.upper() == "SKU":
        for inv in inventory_snapshot:
            sku = inv["sku"]
            cat = sku_to_category_map.get(sku)
            cat_eng = category_translations.get(cat, cat) if cat else None
            f_info = forecast_by_cat.get(cat) if cat else None
            s_info = supplier_catalog_map.get(sku)
            multi_cnt = len(category_to_skus.get(cat, [])) if cat else 0

            rec = build_sku_recommendation(
                sku_data=inv,
                category=cat,
                category_english=cat_eng,
                forecast_info=f_info,
                supplier_info=s_info,
                multi_sku_count_for_cat=multi_cnt,
                execution_id=execution_id,
                model_name=model_name,
                model_version=model_version,
                forecast_period=forecast_period,
                planning_horizon_weeks=planning_horizon_weeks,
                generated_at=generated_at
            )
            recommendations.append(rec)

    # 2. Generate Category-scoped recommendations
    if scope_filter is None or scope_filter.upper() == "CATEGORY":
        # First include categories that actually have operational inventory
        processed_cats = set()
        for cat, skus in category_to_skus.items():
            f_info = forecast_by_cat.get(cat)
            if not f_info:
                continue
            processed_cats.add(cat)
            cat_eng = category_translations.get(cat, cat)
            rec = build_category_recommendation(
                category=cat,
                category_english=cat_eng,
                forecast_info=f_info,
                skus_in_category=skus,
                execution_id=execution_id,
                model_name=model_name,
                model_version=model_version,
                forecast_period=forecast_period,
                planning_horizon_weeks=planning_horizon_weeks,
                generated_at=generated_at
            )
            recommendations.append(rec)

        # Also include top forecast categories that have NO operational inventory (to show INSUFFICIENT_DATA)
        for cat_info in avail_cats[:15]:
            cat = cat_info["category"]
            if cat not in processed_cats:
                cat_eng = category_translations.get(cat, cat)
                rec = build_category_recommendation(
                    category=cat,
                    category_english=cat_eng,
                    forecast_info=cat_info,
                    skus_in_category=[],
                    execution_id=execution_id,
                    model_name=model_name,
                    model_version=model_version,
                    forecast_period=forecast_period,
                    planning_horizon_weeks=planning_horizon_weeks,
                    generated_at=generated_at
                )
                recommendations.append(rec)

    # 3. Filtering
    filtered = recommendations

    if status_filter:
        stat_upper = status_filter.upper()
        filtered = [r for r in filtered if r["status"].upper() == stat_upper]

    if category_filter:
        cat_lower = category_filter.lower()
        filtered = [
            r for r in filtered
            if (r.get("category") and cat_lower in r["category"].lower())
            or (r.get("category_english") and cat_lower in r["category_english"].lower())
        ]

    if sku_filter:
        sku_lower = sku_filter.lower()
        filtered = [r for r in filtered if r.get("sku") and sku_lower in r["sku"].lower()]

    if warehouse_filter and warehouse_filter.upper() != "ALL":
        filtered = [r for r in filtered if r.get("warehouse_id") == warehouse_filter]

    # Priority sorting: REPLENISHMENT_RECOMMENDED first, then NEEDS_REVIEW, STOCK_SUFFICIENT, INSUFFICIENT_DATA
    priority_order = {
        STATUS_REPLENISHMENT_RECOMMENDED: 0,
        STATUS_NEEDS_REVIEW: 1,
        STATUS_STOCK_SUFFICIENT: 2,
        STATUS_INSUFFICIENT_DATA: 3,
    }
    filtered.sort(key=lambda r: (priority_order.get(r["status"], 99), r["recommendation_id"]))

    # Summary metrics (computed over all generated recommendations for this execution)
    total_recs = len(recommendations)
    rec_count = sum(1 for r in recommendations if r["status"] == STATUS_REPLENISHMENT_RECOMMENDED)
    suff_count = sum(1 for r in recommendations if r["status"] == STATUS_STOCK_SUFFICIENT)
    review_count = sum(1 for r in recommendations if r["status"] == STATUS_NEEDS_REVIEW)
    insuf_count = sum(1 for r in recommendations if r["status"] == STATUS_INSUFFICIENT_DATA)

    sku_recs = sum(1 for r in recommendations if r["scope_type"] == "SKU")
    cat_recs = sum(1 for r in recommendations if r["scope_type"] == "CATEGORY")

    # Pagination
    safe_limit = max(1, min(limit, 100))
    paged = filtered[offset : offset + safe_limit]

    return {
        "summary": {
            "total_recommendations": total_recs,
            "replenishment_recommended_count": rec_count,
            "stock_sufficient_count": suff_count,
            "needs_review_count": review_count,
            "insufficient_data_count": insuf_count,
            "by_scope": {
                "SKU": sku_recs,
                "CATEGORY": cat_recs
            },
            "execution_id": execution_id,
            "model_name": model_name,
            "model_version": model_version,
            "planning_horizon_weeks": planning_horizon_weeks
        },
        "recommendations": paged,
        "count": len(paged),
        "total_count": len(filtered),
        "timestamp": generated_at
    }
