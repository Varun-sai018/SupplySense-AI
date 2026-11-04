"""
SupplySense AI - Simulation Observability Repository

Provides data access and orchestration for end-to-end simulation runs:
  - Generates synthetic data and executes simulated pipelines.
  - Retrieves simulated runs, forecasts, inventory snapshots, and replenishment orders.
  - Formats simulated category forecast trends identically to operational trends
    so frontend visualization components render both seamlessly.
"""

import os
import sys
import uuid
import logging
import datetime
from typing import Dict, Any, List, Optional

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from services.simulator.data_generator import generate_simulation_run
from services.dependency_engine.main import process_event
from services.observability.publisher import publish_dashboard_event

logger = logging.getLogger(__name__)


def execute_e2e_simulation(batch_size: int = 200) -> Dict[str, Any]:
    """
    Orchestrates a complete end-to-end simulation cycle:
      1. Generates synthetic orders in sim_orders and inventory in sim_inventory_snapshots.
      2. Dispatches SIM_Orders_Feed event -> triggers SIM-FORECAST-Pipeline (XGBoost).
      3. Dispatches SIM_Inventory_Snapshot event -> triggers SIM-REORDER-Pipeline.
      4. Marks sim_runs as COMPLETED.
      5. Prunes older simulation runs beyond the latest 10 runs.
      6. Emits dashboard events for real-time frontend notification.
    """
    logger.info(f"Starting E2E simulation with batch_size={batch_size}...")

    # Step 1: Generate data
    gen_result = generate_simulation_run(batch_size=batch_size)
    run_id = gen_result["simulation_run_id"]

    conn = get_connection()
    cur = conn.cursor()

    try:
        publish_dashboard_event(
            event_type="SIMULATION_STATUS_CHANGED",
            entity="SIMULATOR",
            severity="INFO",
            data={
                "action": "DATA_GENERATED",
                "simulation_run_id": run_id,
                "orders_count": gen_result["orders_generated"],
                "sku_count": gen_result["sku_count"],
                "low_stock_count": gen_result["low_stock_count"],
            }
        )

        # Step 2: Trigger SIM-FORECAST-Pipeline
        ev_forecast = {
            "event_id": str(uuid.uuid4()),
            "event_type": "DATASET_UPDATED",
            "pipeline_name": "SIM-FORECAST-Pipeline",
            "dataset_name": "SIM_Orders_Feed",
            "new_status": "READY",
            "source": "SIMULATOR",
            "simulation_id": run_id,
        }
        dec_fc, reason_fc = process_event(ev_forecast, conn)

        # Step 3: Trigger SIM-REORDER-Pipeline
        ev_reorder = {
            "event_id": str(uuid.uuid4()),
            "event_type": "DATASET_UPDATED",
            "pipeline_name": "SIM-REORDER-Pipeline",
            "dataset_name": "SIM_Inventory_Snapshot",
            "new_status": "READY",
            "source": "SIMULATOR",
            "simulation_id": run_id,
        }
        dec_ro, reason_ro = process_event(ev_reorder, conn)

        # Step 4: Gather results and update run status
        cur.execute("SELECT COUNT(*) as cnt FROM sim_forecast_results WHERE simulation_run_id = %s", (run_id,))
        fc_count = cur.fetchone()["cnt"]

        cur.execute("SELECT COUNT(*) as cnt FROM sim_replenishment_orders WHERE simulation_run_id = %s", (run_id,))
        ro_count = cur.fetchone()["cnt"]

        # Fetch execution IDs
        cur.execute("""
            SELECT execution_id, pipeline_name FROM pipeline_executions 
            WHERE pipeline_name IN ('SIM-FORECAST-Pipeline', 'SIM-REORDER-Pipeline')
            ORDER BY execution_id DESC LIMIT 2
        """)
        exec_rows = cur.fetchall()
        exec_map = {r["pipeline_name"]: r["execution_id"] for r in exec_rows}

        now = datetime.datetime.now()
        cur.execute("""
            UPDATE sim_runs
            SET status = 'COMPLETED',
                forecast_count = %s,
                replenishment_count = %s,
                completed_at = %s
            WHERE simulation_run_id = %s
        """, (fc_count, ro_count, now, run_id))

        # Step 5: Prune runs older than the latest 10 runs
        cur.execute("SELECT simulation_run_id FROM sim_runs ORDER BY created_at DESC LIMIT 10")
        recent_ids = [r["simulation_run_id"] for r in cur.fetchall()]

        if recent_ids:
            placeholders = ",".join(["%s"] * len(recent_ids))
            cur.execute(f"DELETE FROM sim_orders WHERE simulation_run_id NOT IN ({placeholders})", recent_ids)
            cur.execute(f"DELETE FROM sim_inventory_snapshots WHERE simulation_run_id NOT IN ({placeholders})", recent_ids)
            cur.execute(f"DELETE FROM sim_forecast_results WHERE simulation_run_id NOT IN ({placeholders})", recent_ids)
            cur.execute(f"DELETE FROM sim_replenishment_orders WHERE simulation_run_id NOT IN ({placeholders})", recent_ids)
            cur.execute(f"DELETE FROM sim_runs WHERE simulation_run_id NOT IN ({placeholders})", recent_ids)

        conn.commit()

        publish_dashboard_event(
            event_type="SIMULATION_STATUS_CHANGED",
            entity="SIMULATOR",
            severity="INFO",
            data={
                "action": "COMPLETED",
                "simulation_run_id": run_id,
                "orders_generated": gen_result["orders_generated"],
                "forecast_count": fc_count,
                "replenishment_count": ro_count,
                "forecast_execution_id": exec_map.get("SIM-FORECAST-Pipeline"),
                "reorder_execution_id": exec_map.get("SIM-REORDER-Pipeline"),
            }
        )

        logger.info(
            f"E2E Simulation run {run_id} COMPLETED: "
            f"{gen_result['orders_generated']} orders -> {fc_count} forecast predictions, "
            f"{ro_count} replenishment orders."
        )

        return {
            "simulation_run_id": run_id,
            "status": "COMPLETED",
            "orders_generated": gen_result["orders_generated"],
            "sku_count": gen_result["sku_count"],
            "low_stock_count": gen_result["low_stock_count"],
            "forecast_count": fc_count,
            "replenishment_count": ro_count,
            "forecast_execution_id": exec_map.get("SIM-FORECAST-Pipeline"),
            "reorder_execution_id": exec_map.get("SIM-REORDER-Pipeline"),
            "forecast_decision": dec_fc,
            "reorder_decision": dec_ro,
        }

    except Exception as exc:
        conn.rollback()
        logger.error(f"E2E simulation execution failed for run {run_id}: {exc}", exc_info=True)
        try:
            cur.execute("""
                UPDATE sim_runs
                SET status = 'FAILED', error_message = %s
                WHERE simulation_run_id = %s
            """, (str(exc), run_id))
            conn.commit()
        except Exception:
            pass
        raise
    finally:
        cur.close()
        conn.close()


def get_sim_runs(limit: int = 20) -> List[Dict[str, Any]]:
    """Returns the list of recent simulation runs."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    simulation_run_id, scenario_id, status, orders_generated,
                    sku_count, low_stock_count, forecast_count, replenishment_count,
                    started_at, completed_at, error_message, created_at
                FROM sim_runs
                ORDER BY created_at DESC
                LIMIT %s
            """, (limit,))
            return cur.fetchall()
    finally:
        conn.close()


def get_sim_run_detail(simulation_run_id: str) -> Optional[Dict[str, Any]]:
    """Returns detail of a specific simulation run."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM sim_runs WHERE simulation_run_id = %s LIMIT 1
            """, (simulation_run_id,))
            return cur.fetchone()
    finally:
        conn.close()


def get_sim_forecasts(simulation_run_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    """Returns simulated forecast predictions."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if simulation_run_id:
                cur.execute("""
                    SELECT sim_forecast_id, simulation_run_id, execution_id,
                           product_category, forecast_week, predicted_demand,
                           model_name, model_version, created_at
                    FROM sim_forecast_results
                    WHERE simulation_run_id = %s
                    ORDER BY forecast_week ASC, product_category ASC
                    LIMIT %s
                """, (simulation_run_id, limit))
            else:
                cur.execute("""
                    SELECT sim_forecast_id, simulation_run_id, execution_id,
                           product_category, forecast_week, predicted_demand,
                           model_name, model_version, created_at
                    FROM sim_forecast_results
                    ORDER BY created_at DESC, forecast_week ASC
                    LIMIT %s
                """, (limit,))
            return cur.fetchall()
    finally:
        conn.close()


def get_sim_forecast_category_trends(
    execution_id: Optional[int] = None,
    simulation_run_id: Optional[str] = None,
    categories: Optional[List[str]] = None,
    limit: int = 6
) -> Optional[Dict[str, Any]]:
    """
    Returns simulated category forecast trends formatted for the Recharts frontend chart.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # 1. Resolve execution_id or simulation_run_id
            target_exec = execution_id
            target_run = simulation_run_id

            if not target_exec and not target_run:
                cur.execute("""
                    SELECT execution_id, simulation_run_id 
                    FROM sim_forecast_results 
                    ORDER BY sim_forecast_id DESC LIMIT 1
                """)
                row = cur.fetchone()
                if not row:
                    return None
                target_exec = row["execution_id"]
                target_run = row["simulation_run_id"]

            # 2. Query summary across categories
            where_clause = "WHERE execution_id = %s" if target_exec else "WHERE simulation_run_id = %s"
            param = target_exec if target_exec else target_run

            cur.execute(f"""
                SELECT 
                    f.product_category,
                    COALESCE(t.product_category_name_english, f.product_category) AS english_name,
                    SUM(f.predicted_demand) AS total_predicted_demand,
                    AVG(f.predicted_demand) AS avg_weekly_demand,
                    MIN(f.forecast_week) AS min_week,
                    MAX(f.forecast_week) AS max_week,
                    f.model_name,
                    f.model_version
                FROM sim_forecast_results f
                LEFT JOIN product_category_name_translation t
                    ON f.product_category = t.product_category_name
                {where_clause}
                GROUP BY f.product_category, t.product_category_name_english, f.model_name, f.model_version
                ORDER BY total_predicted_demand DESC
            """, (param,))
            cat_rows = cur.fetchall()

            if not cat_rows:
                return None

            all_categories = [
                {
                    "category": r["product_category"],
                    "english_name": r["english_name"] or r["product_category"],
                    "total_predicted_demand": round(float(r["total_predicted_demand"]), 2),
                    "avg_weekly_demand": round(float(r["avg_weekly_demand"]), 2),
                }
                for r in cat_rows
            ]

            min_w = str(cat_rows[0]["min_week"])
            max_w = str(cat_rows[0]["max_week"])
            model_name = cat_rows[0]["model_name"] or "XGBoost"
            model_ver = cat_rows[0]["model_version"] or "xgboost-v1"

            if categories:
                cat_set = set(categories)
                selected = [c for c in all_categories if c["category"] in cat_set]
            else:
                selected = all_categories[:limit]

            sel_cat_names = [c["category"] for c in selected]
            if sel_cat_names:
                placeholders = ",".join(["%s"] * len(sel_cat_names))
                cur.execute(f"""
                    SELECT forecast_week, product_category, predicted_demand
                    FROM sim_forecast_results
                    {where_clause} AND product_category IN ({placeholders})
                    ORDER BY forecast_week ASC
                """, (param, *sel_cat_names))
                trend_rows = cur.fetchall()
            else:
                trend_rows = []

            # Pivot trend_rows: { week_date: { category: demand } }
            weeks_map = {}
            for r in trend_rows:
                w_str = str(r["forecast_week"])
                if w_str not in weeks_map:
                    weeks_map[w_str] = {"forecast_week": w_str}
                weeks_map[w_str][r["product_category"]] = round(float(r["predicted_demand"]), 2)

            trends_list = sorted(weeks_map.values(), key=lambda x: x["forecast_week"])

            return {
                "execution_id": target_exec,
                "simulation_run_id": target_run,
                "model_name": model_name,
                "model_version": model_ver,
                "horizon_start": min_w,
                "horizon_end": max_w,
                "categories": all_categories,
                "selected_categories": selected,
                "trends": trends_list,
                "is_simulated": True,
            }

    finally:
        conn.close()


def get_sim_inventory_snapshots(simulation_run_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
    """Returns simulated inventory snapshots."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if simulation_run_id:
                cur.execute("""
                    SELECT snapshot_id, simulation_run_id, sku, warehouse_id,
                           on_hand_quantity, reserved_quantity, reorder_threshold,
                           reorder_target_qty, is_below_threshold, created_at
                    FROM sim_inventory_snapshots
                    WHERE simulation_run_id = %s
                    ORDER BY is_below_threshold DESC, on_hand_quantity ASC
                    LIMIT %s
                """, (simulation_run_id, limit))
            else:
                cur.execute("""
                    SELECT snapshot_id, simulation_run_id, sku, warehouse_id,
                           on_hand_quantity, reserved_quantity, reorder_threshold,
                           reorder_target_qty, is_below_threshold, created_at
                    FROM sim_inventory_snapshots
                    ORDER BY created_at DESC, is_below_threshold DESC
                    LIMIT %s
                """, (limit,))
            return cur.fetchall()
    finally:
        conn.close()


def get_sim_replenishment_orders(simulation_run_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
    """Returns simulated replenishment orders."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if simulation_run_id:
                cur.execute("""
                    SELECT sim_replenishment_id, simulation_run_id, execution_id,
                           sku, warehouse_id, supplier_id, quantity, status,
                           dispatched_at, estimated_delivery_at, created_at
                    FROM sim_replenishment_orders
                    WHERE simulation_run_id = %s
                    ORDER BY created_at DESC
                    LIMIT %s
                """, (simulation_run_id, limit))
            else:
                cur.execute("""
                    SELECT sim_replenishment_id, simulation_run_id, execution_id,
                           sku, warehouse_id, supplier_id, quantity, status,
                           dispatched_at, estimated_delivery_at, created_at
                    FROM sim_replenishment_orders
                    ORDER BY created_at DESC
                    LIMIT %s
                """, (limit,))
            return cur.fetchall()
    finally:
        conn.close()


# Compatibility aliases
get_sim_inventory = get_sim_inventory_snapshots
get_sim_replenishments = get_sim_replenishment_orders
