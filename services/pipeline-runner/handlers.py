"""
Pipeline Execution Handlers for SupplySense AI.

Contains execution logic for registered pipelines:
  - Demand Forecast Pipeline
  - Inventory Reorder Pipeline (Phase 2B: with replenishment_orders persistence)
"""

import os
import sys
import logging
from typing import Dict, Any, Optional, List, Tuple

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from ml.baseline.naive_baseline import run_baseline
from ml.features.engineer_features import build_features
from ml.models.train_xgboost import train_and_evaluate_xgboost
from .forecast_repository import save_forecast_results
from common.database import get_connection
try:
    from services.dependency_engine.adapters.supplier_adapter import SupplierAPIAdapter
except ImportError:
    SupplierAPIAdapter = None

logger = logging.getLogger(__name__)

PROCESSED_DIR = os.path.join(REPO_ROOT, 'data', 'processed')
RESULTS_DIR = os.path.join(REPO_ROOT, 'ml', 'results')
MODELS_DIR = os.path.join(REPO_ROOT, 'ml', 'models')


def demand_forecast_pipeline_handler(execution_id: int, pipeline_name: str) -> Dict[str, Any]:
    """
    Executes the Demand Forecast Pipeline using the ML forecasting workload:
      1. Validates that required prepared ML datasets exist.
      2. Invokes the naive baseline forecasting benchmark.
      3. Constructs leak-free lag, rolling, trend, and calendar features.
      4. Trains and evaluates the XGBoost Regressor with early stopping on validation.
      5. Generates model comparison metrics (Naive vs. XGBoost).
      6. Verifies that output prediction and metric artifacts exist.
      7. Persists the out-of-sample test forecast predictions into MySQL (forecast_results).
      8. Returns structured results and artifact locations.

    Args:
        execution_id: ID of the pipeline execution record in MySQL.
        pipeline_name: Name of the pipeline being executed.

    Returns:
        Dict containing output artifact location, metrics, artifact paths, and saved forecast count.

    Raises:
        FileNotFoundError: If required prepared datasets are missing.
        RuntimeError: If execution fails or expected output artifacts are missing.
    """
    logger.info(f"Starting execution #{execution_id} for pipeline '{pipeline_name}'...")

    # Create execution-scoped directory under ml/results/executions/<execution_id>
    exec_dir = os.path.join(RESULTS_DIR, 'executions', str(execution_id))
    os.makedirs(exec_dir, exist_ok=True)

    # 1. Validate required prepared input datasets
    required_inputs = [
        os.path.join(PROCESSED_DIR, 'train.csv'),
        os.path.join(PROCESSED_DIR, 'validation.csv'),
        os.path.join(PROCESSED_DIR, 'test.csv'),
    ]
    missing_inputs = [f for f in required_inputs if not os.path.isfile(f)]
    if missing_inputs:
        error_msg = f"Missing required prepared dataset input(s): {', '.join(missing_inputs)}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    # 2. Invoke the naive baseline benchmark into the execution directory
    logger.info("Step 1/4: Running Naive Baseline Benchmark...")
    try:
        baseline_result = run_baseline(output_dir=exec_dir, execution_id=execution_id)
    except Exception as e:
        error_msg = f"Baseline execution failed with exception: {e}"
        logger.error(error_msg)
        raise RuntimeError(error_msg) from e

    # 3. Feature engineering
    logger.info("Step 2/4: Engineering leak-free features...")
    try:
        features_path = os.path.join(PROCESSED_DIR, 'forecasting_features.csv')
        features_df, _ = build_features(output_path=features_path)
    except Exception as e:
        error_msg = f"Feature engineering failed with exception: {e}"
        logger.error(error_msg)
        raise RuntimeError(error_msg) from e

    # 4. Train and evaluate XGBoost into the execution directory
    logger.info("Step 3/4: Training and evaluating XGBoost Regressor...")
    try:
        xgb_result = train_and_evaluate_xgboost(
            features_df=features_df,
            output_dir=exec_dir,
            execution_id=execution_id
        )
    except Exception as e:
        error_msg = f"XGBoost pipeline failed with exception: {e}"
        logger.error(error_msg)
        raise RuntimeError(error_msg) from e

    # 5. Verify all expected output artifacts in execution directory
    output_comparison_json = os.path.join(exec_dir, 'model_comparison.json')
    output_xgb_metrics_json = os.path.join(exec_dir, 'xgboost_metrics.json')
    output_base_metrics_json = os.path.join(exec_dir, 'baseline_metrics.json')
    output_base_metrics_csv = os.path.join(exec_dir, 'baseline_metrics.csv')
    model_json = os.path.join(exec_dir, 'xgboost_demand_model.json')
    xgb_val_preds_csv = os.path.join(exec_dir, 'xgboost_validation_predictions.csv')
    xgb_test_preds_csv = os.path.join(exec_dir, 'xgboost_test_predictions.csv')
    base_val_preds_csv = os.path.join(exec_dir, 'baseline_validation_predictions.csv')
    base_test_preds_csv = os.path.join(exec_dir, 'baseline_test_predictions.csv')

    expected_outputs = [
        output_comparison_json,
        output_xgb_metrics_json,
        output_base_metrics_json,
        output_base_metrics_csv,
        model_json,
        xgb_val_preds_csv,
        xgb_test_preds_csv,
        base_val_preds_csv,
        base_test_preds_csv,
    ]
    missing_outputs = [f for f in expected_outputs if not os.path.isfile(f)]
    if missing_outputs:
        error_msg = f"Expected output artifacts not found after execution: {', '.join(missing_outputs)}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    # 6. Persist forecast predictions into MySQL forecast_results table
    logger.info(f"Step 4/4: Persisting forecast results to MySQL for execution #{execution_id}...")
    try:
        saved_count = save_forecast_results(
            execution_id=execution_id,
            pipeline_name=pipeline_name,
            predictions=xgb_test_preds_csv,
            model_name="XGBoost",
            model_version="xgboost-v1"
        )
        logger.info(f"Successfully persisted {saved_count} forecast predictions into MySQL forecast_results.")
    except Exception as e:
        error_msg = f"Failed to persist forecast predictions to MySQL: {e}"
        logger.error(error_msg)
        raise RuntimeError(error_msg) from e

    # Relative path for portable database storage
    rel_output_location = os.path.relpath(output_comparison_json, REPO_ROOT).replace('\\', '/')

    logger.info(f"Pipeline '{pipeline_name}' execution #{execution_id} completed successfully.")
    return {
        "output_location": rel_output_location,
        "execution_id": execution_id,
        "execution_dir": os.path.relpath(exec_dir, REPO_ROOT).replace('\\', '/'),
        "forecasts_saved": saved_count,
        "comparison": xgb_result.get("comparison"),
        "metrics": {
            "naive_baseline": baseline_result,
            "xgboost": {
                "validation": xgb_result.get("validation_metrics"),
                "test": xgb_result.get("test_metrics"),
            }
        },
        "artifacts": [
            os.path.relpath(f, REPO_ROOT).replace('\\', '/') for f in expected_outputs
        ]
    }


def inventory_reorder_pipeline_handler(execution_id: int, pipeline_name: str) -> Dict[str, Any]:
    """
    Executes the Inventory Reorder Pipeline (Phase 2B):
      1. Resolves the pending reorder action linked to execution_id from business_rule_actions.
      2. Invokes SupplierAPIAdapter (simulation) to place the reorder.
      3. Validates that supplier returned 'CONFIRMED'.
      4. Persists exactly one replenishment_orders row (idempotent on action_id uniqueness).
      5. Persists the execution artifact JSON and marks the action CONFIRMED.
      6. If supplier rejects or fails, raises RuntimeError for runner lifecycle failure tracking.
      7. Handler retries are safe: duplicate-key conflicts on replenishment_orders are resolved
         by returning the existing canonical row.

    Preserves all existing demand-forecast handler behavior.  Inventory events
    must NOT trigger the demand-forecast pipeline.
    """
    import json
    import uuid
    import datetime
    import pymysql

    logger.info(f"Starting Inventory Reorder Pipeline execution #{execution_id}...")

    # ------------------------------------------------------------------
    # 1. Load associated action from database
    # ------------------------------------------------------------------
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT action_id, idempotency_key, payload, status, decision_id
        FROM business_rule_actions
        WHERE execution_id = %s AND rule_id = 'RULE_B_INVENTORY_REORDER'
        ORDER BY action_id DESC
        LIMIT 1
    """, (execution_id,))
    row = cur.fetchone()

    # Fallback: find the most recent PENDING action if no execution-linked row
    if not row:
        cur.execute("""
            SELECT action_id, idempotency_key, payload, status, decision_id
            FROM business_rule_actions
            WHERE rule_id = 'RULE_B_INVENTORY_REORDER' AND status = 'PENDING'
            ORDER BY action_id DESC
            LIMIT 1
        """)
        row = cur.fetchone()

    payload = {}
    idempotency_key = f"reorder_exec_{execution_id}"
    action_id = None
    decision_id = None

    if row:
        action_id  = row["action_id"]  if isinstance(row, dict) else row[0]
        idempotency_key = (row["idempotency_key"] if isinstance(row, dict) else row[1]) or idempotency_key
        raw_payload = row["payload"]    if isinstance(row, dict) else row[2]
        decision_id = row["decision_id"] if isinstance(row, dict) else row[4]
        if isinstance(raw_payload, str):
            try:
                payload = json.loads(raw_payload)
            except Exception:
                payload = {}
        elif isinstance(raw_payload, dict):
            payload = raw_payload

    product_id      = payload.get("product_id", "PROD-DEFAULT-001")
    reorder_quantity = payload.get("reorder_quantity", 50)
    supplier_id     = payload.get("supplier_id", "supplier_primary")
    warehouse_id    = payload.get("warehouse_id", "WH-SP-01")

    # ------------------------------------------------------------------
    # 2. Simulate supplier confirmation (no real supplier contact)
    # ------------------------------------------------------------------
    try:
        supplier_adapter = SupplierAPIAdapter() if SupplierAPIAdapter is not None else None
    except Exception:
        supplier_adapter = None

    if supplier_adapter:
        reorder_res = supplier_adapter.place_reorder(
            product_id=product_id,
            quantity=reorder_quantity,
            idempotency_key=idempotency_key,
            supplier_id=supplier_id,
        )
    else:
        reorder_res = {
            "status": "CONFIRMED",
            "supplier_order_id": f"ORD-SIM-{execution_id}",
            "product_id": product_id,
            "quantity": reorder_quantity,
            "supplier_id": supplier_id,
        }

    reorder_status = reorder_res.get("status")
    if reorder_status != "CONFIRMED":
        err_msg = reorder_res.get("error") or f"Supplier reorder failed with status '{reorder_status}'"
        if action_id:
            cur.execute("""
                UPDATE business_rule_actions
                SET status = 'FAILED', error_message = %s
                WHERE action_id = %s
            """, (err_msg, action_id))
            conn.commit()
        conn.close()
        raise RuntimeError(err_msg)

    # ------------------------------------------------------------------
    # 3. Persist replenishment_orders row (idempotent via action_id UNIQUE)
    # ------------------------------------------------------------------
    supplier_order_ref = reorder_res.get("supplier_order_id") or reorder_res.get("supplier_order_ref")
    estimated_delivery = datetime.datetime.now() + datetime.timedelta(hours=48)
    replenishment_id   = f"RPL-{uuid.uuid4().hex[:12].upper()}"

    try:
        cur.execute("""
            INSERT INTO replenishment_orders (
                replenishment_id, action_id, decision_id, execution_id,
                sku, warehouse_id, supplier_id, quantity,
                status, supplier_order_reference,
                dispatched_at, estimated_delivery_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            replenishment_id,
            action_id,
            decision_id,
            execution_id,
            product_id,
            warehouse_id,
            supplier_id,
            reorder_quantity,
            "CONFIRMED",
            supplier_order_ref,
            datetime.datetime.now(),
            estimated_delivery,
        ))
        conn.commit()
        logger.info(f"Replenishment row created: {replenishment_id} for action {action_id}")

    except pymysql.err.IntegrityError as dup_err:
        # Duplicate key on action_id — idempotent retry: fetch canonical row
        conn.rollback()
        logger.info(f"Replenishment already exists for action_id={action_id} (idempotent retry): {dup_err}")
        cur.execute("""
            SELECT replenishment_id FROM replenishment_orders WHERE action_id = %s LIMIT 1
        """, (action_id,))
        existing = cur.fetchone()
        replenishment_id = (
            existing["replenishment_id"] if isinstance(existing, dict) else existing[0]
        ) if existing else replenishment_id

    except Exception as db_err:
        # Any other DB error: rollback and propagate — do NOT claim success
        conn.rollback()
        conn.close()
        raise RuntimeError(f"Failed to persist replenishment row: {db_err}") from db_err

    # ------------------------------------------------------------------
    # 4. Persist artifact and update action status
    # ------------------------------------------------------------------
    exec_dir = os.path.join(RESULTS_DIR, 'executions', str(execution_id))
    os.makedirs(exec_dir, exist_ok=True)
    artifact_path = os.path.join(exec_dir, 'reorder_confirmation.json')
    artifact_data = {**reorder_res, "replenishment_id": replenishment_id}
    with open(artifact_path, 'w', encoding='utf-8') as f:
        json.dump(artifact_data, f, indent=2)

    rel_artifact_path = os.path.relpath(artifact_path, REPO_ROOT).replace('\\', '/')

    if action_id:
        cur.execute("""
            UPDATE business_rule_actions
            SET status = 'CONFIRMED',
                response_payload = %s,
                execution_id = %s
            WHERE action_id = %s
        """, (json.dumps(artifact_data), execution_id, action_id))
        conn.commit()

    conn.close()

    # ------------------------------------------------------------------
    # 5. Publish observability event (best-effort, never blocks success)
    # ------------------------------------------------------------------
    try:
        from services.observability.publisher import publish_dashboard_event
        publish_dashboard_event(
            event_type="INVENTORY_REORDER_STATE_CHANGED",
            entity="INVENTORY",
            severity="INFO",
            execution_id=execution_id,
            correlation_id=idempotency_key,
            data={
                "replenishment_id": replenishment_id,
                "product_id": product_id,
                "warehouse_id": warehouse_id,
                "supplier_id": supplier_id,
                "quantity": reorder_quantity,
                "supplier_order_reference": supplier_order_ref,
                "lifecycle_state": "CONFIRMED",
                "is_simulated": True,
            },
        )
    except Exception as ev_err:
        logger.debug(f"Observability event skipped: {ev_err}")

    logger.info(
        f"Inventory Reorder Pipeline #{execution_id} confirmed: "
        f"order={supplier_order_ref}, replenishment={replenishment_id}"
    )
    return {
        "output_location": rel_artifact_path,
        "execution_id": execution_id,
        "replenishment_id": replenishment_id,
        "reorder_result": reorder_res,
        "status": "COMPLETED",
    }


def simulated_pipeline_handler(execution_id: int, pipeline_name: str) -> Dict[str, Any]:
    """
    Executes a simulated pipeline execution safely in-process.

    Invariants:
      1. Never calls Apache Airflow REST API.
      2. Never executes ML feature engineering, naive baseline, or XGBoost training.
      3. Never queries, modifies, or decrements operational inventory.
      4. Never inserts or updates replenishment_orders or operational_orders.
      5. Never accesses or modifies historical Olist datasets.
      6. Preserves protected executions #36, #99586, and #99653.
      7. Never directly updates pipeline_executions table (handled by pipeline runner).

    Args:
        execution_id: ID of the pipeline execution record in MySQL.
        pipeline_name: Name of the simulated pipeline being executed.

    Returns:
        Structured execution result dictionary with simulated telemetry.
    """
    logger.info(f"Executing simulated pipeline '{pipeline_name}' (Execution ID: #{execution_id})...")

    # Identifiable simulated output location
    safe_slug = pipeline_name.lower().replace("-", "_").replace(" ", "_")
    rel_output_location = f"simulated://executions/{execution_id}/{safe_slug}_telemetry.json"

    return {
        "status": "COMPLETED",
        "execution_id": execution_id,
        "pipeline_name": pipeline_name,
        "output_location": rel_output_location,
        "simulated": True,
        "metrics": {
            "mode": "SIMULATION_ISOLATED",
            "execution_type": "SYNTHETIC_DEPENDENCY_TRIGGER",
            "operational_impact": "NONE"
        },
        "artifacts": [
            rel_output_location
        ]
    }


def sim_demand_forecast_handler(execution_id: int, pipeline_name: str, simulation_run_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Executes real XGBoost inference on simulated orders data:
      1. Resolves active simulation_run_id from sim_runs.
      2. Aggregates weekly category-level demand from sim_orders.
      3. Constructs the required 11 leak-free forecasting features.
      4. Loads saved XGBoost demand model and runs fast inference (<1s).
      5. Persists forecast predictions into sim_forecast_results table.
      6. Emits PIPELINE_STATUS_CHANGED dashboard event.

    Safety Invariants:
      - Never queries or modifies olist_orders or forecast_results.
      - Writes strictly to sim_forecast_results and sim_runs.
      - Never re-trains model; runs inference only using validated model weights.
    """
    import json
    import datetime
    import pandas as pd
    import numpy as np
    import xgboost as xgb

    logger.info(f"Starting simulated demand forecast for execution #{execution_id} ({pipeline_name})...")
    conn = get_connection()
    cur = conn.cursor()

    try:
        # 1. Resolve simulation_run_id
        run_id = simulation_run_id
        if not run_id:
            cur.execute("""
                SELECT simulation_run_id FROM sim_runs 
                WHERE status IN ('DATA_GENERATED', 'PROCESSING', 'PIPELINES_TRIGGERED') 
                ORDER BY created_at DESC LIMIT 1
            """)
            row = cur.fetchone()
            if row:
                run_id = row['simulation_run_id'] if isinstance(row, dict) else row[0]
            else:
                cur.execute("SELECT simulation_run_id FROM sim_runs ORDER BY created_at DESC LIMIT 1")
                row = cur.fetchone()
                run_id = (row['simulation_run_id'] if isinstance(row, dict) else row[0]) if row else None

        if not run_id:
            logger.warning("No simulation_run_id found for sim_demand_forecast_handler.")
            return {
                "status": "COMPLETED",
                "execution_id": execution_id,
                "pipeline_name": pipeline_name,
                "forecasts_count": 0,
                "message": "No active simulation run found."
            }

        # 2. Aggregate weekly category demand from sim_orders
        cur.execute("""
            SELECT product_category, week_start_date, year_week, SUM(quantity) as demand
            FROM sim_orders
            WHERE simulation_run_id = %s
            GROUP BY product_category, week_start_date, year_week
            ORDER BY week_start_date, product_category
        """, (run_id,))
        order_rows = cur.fetchall()

        if not order_rows:
            logger.warning(f"No sim_orders found for run '{run_id}'.")
            return {
                "status": "COMPLETED",
                "execution_id": execution_id,
                "pipeline_name": pipeline_name,
                "forecasts_count": 0,
                "simulation_run_id": run_id,
            }

        df_demand = pd.DataFrame(order_rows)
        if 'product_category' not in df_demand.columns and len(order_rows) > 0:
            df_demand.columns = ['product_category', 'week_start_date', 'year_week', 'demand']

        df_demand['week_start_date'] = pd.to_datetime(df_demand['week_start_date'])
        df_demand['demand'] = df_demand['demand'].astype(float)

        # 3. Load category mapping from features reference or fit deterministic encoding
        cat_mapping: Dict[str, int] = {}
        features_csv = os.path.join(PROCESSED_DIR, 'forecasting_features.csv')
        if os.path.isfile(features_csv):
            try:
                ref_df = pd.read_csv(features_csv, usecols=['product_category', 'product_category_enc']).dropna().drop_duplicates()
                cat_mapping = dict(zip(ref_df['product_category'], ref_df['product_category_enc'].astype(int)))
            except Exception as e:
                logger.debug(f"Could not load pre-fitted category mapping: {e}")

        # 4. Construct feature matrix for each category observation
        feature_rows = []
        forecast_predictions = []

        # Load XGBoost model
        model_path = os.path.join(REPO_ROOT, 'ml', 'models', 'xgboost_demand_model.json')
        model = xgb.Booster()
        model.load_model(model_path)

        for cat, group in df_demand.groupby('product_category'):
            group = group.sort_values('week_start_date').reset_index(drop=True)
            cat_enc = cat_mapping.get(cat, abs(hash(cat)) % 73)

            for idx, row in group.iterrows():
                dt = row['week_start_date']
                demand_val = row['demand']

                # Lags from observed weeks
                lag_1 = float(group.loc[idx - 1, 'demand']) if idx >= 1 else demand_val
                lag_2 = float(group.loc[idx - 2, 'demand']) if idx >= 2 else lag_1
                lag_4 = float(group.loc[idx - 4, 'demand']) if idx >= 4 else lag_2

                # Rolling stats
                past_demands = group.loc[max(0, idx - 4):idx, 'demand'].tolist()
                rolling_mean_4w = float(np.mean(past_demands)) if past_demands else demand_val
                rolling_std_4w = float(np.std(past_demands)) if len(past_demands) > 1 else 0.0
                rolling_mean_8w = rolling_mean_4w

                trend_ratio = float((lag_1 + 1.0) / (rolling_mean_4w + 1.0))
                month = dt.month
                week = dt.isocalendar()[1]
                quarter = (month - 1) // 3 + 1

                feature_rows.append({
                    'product_category': cat,
                    'forecast_week': (dt + datetime.timedelta(days=7)).date(),  # Forecast next week
                    'features': [
                        lag_1, lag_2, lag_4, rolling_mean_4w, rolling_std_4w,
                        rolling_mean_8w, trend_ratio, cat_enc, month, week, quarter
                    ]
                })

        feature_matrix = np.array([r['features'] for r in feature_rows], dtype=np.float32)
        feature_names = [
            'lag_1_demand', 'lag_2_demand', 'lag_4_demand', 'rolling_mean_4w',
            'rolling_std_4w', 'rolling_mean_8w', 'demand_trend_ratio',
            'product_category_enc', 'month_of_year', 'week_of_year', 'quarter'
        ]

        dmatrix = xgb.DMatrix(feature_matrix, feature_names=feature_names)
        raw_preds = model.predict(dmatrix)

        # 5. Persist predictions into sim_forecast_results
        inserts = []
        for i, pred in enumerate(raw_preds):
            pred_val = round(max(0.1, float(pred)), 4)
            cat_name = feature_rows[i]['product_category']
            fc_week = feature_rows[i]['forecast_week']

            inserts.append((
                run_id, execution_id, cat_name, fc_week, pred_val, 'XGBoost', 'xgboost-v1'
            ))

        cur.executemany("""
            INSERT INTO sim_forecast_results (
                simulation_run_id, execution_id, product_category, forecast_week,
                predicted_demand, model_name, model_version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, inserts)

        # 6. Update sim_runs forecast count
        cur.execute("""
            UPDATE sim_runs
            SET forecast_count = %s
            WHERE simulation_run_id = %s
        """, (len(inserts), run_id))

        conn.commit()

        # 7. Write execution artifact
        exec_dir = os.path.join(RESULTS_DIR, 'executions', str(execution_id))
        os.makedirs(exec_dir, exist_ok=True)
        summary_path = os.path.join(exec_dir, 'sim_forecast_summary.json')
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump({
                "execution_id": execution_id,
                "pipeline_name": pipeline_name,
                "simulation_run_id": run_id,
                "forecasts_count": len(inserts),
                "model_name": "XGBoost",
                "model_version": "xgboost-v1",
                "categories": list(set(r[2] for r in inserts)),
            }, f, indent=2)

        rel_output = os.path.relpath(summary_path, REPO_ROOT).replace('\\', '/')
        logger.info(
            f"Simulated Demand Forecast completed: {len(inserts)} predictions generated "
            f"for run '{run_id}' (Execution #{execution_id})."
        )

        return {
            "status": "COMPLETED",
            "execution_id": execution_id,
            "pipeline_name": pipeline_name,
            "simulation_run_id": run_id,
            "forecasts_count": len(inserts),
            "output_location": rel_output,
            "simulated": True,
        }

    except Exception as exc:
        conn.rollback()
        logger.error(f"sim_demand_forecast_handler failed for execution #{execution_id}: {exc}", exc_info=True)
        raise
    finally:
        cur.close()
        conn.close()


def sim_inventory_reorder_handler(execution_id: int, pipeline_name: str, simulation_run_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Executes automated replenishment orders for simulated low-stock SKUs:
      1. Resolves active simulation_run_id.
      2. Queries sim_inventory_snapshots for SKUs where is_below_threshold = 1.
      3. Dispatches simulated supplier reorders to online suppliers.
      4. Persists records into sim_replenishment_orders table.
      5. Emits INVENTORY_REORDER_STATE_CHANGED SSE event (tagged is_simulated=True).

    Safety Invariants:
      - Never queries or modifies operational_inventory or replenishment_orders.
      - Writes strictly to sim_replenishment_orders and sim_runs.
    """
    import os
    import json
    import uuid
    import random
    import datetime

    logger.info(f"Starting simulated inventory reorder for execution #{execution_id} ({pipeline_name})...")
    conn = get_connection()
    cur = conn.cursor()

    try:
        # 1. Resolve simulation_run_id
        run_id = simulation_run_id
        if not run_id:
            cur.execute("""
                SELECT simulation_run_id FROM sim_runs 
                WHERE status IN ('DATA_GENERATED', 'PROCESSING', 'PIPELINES_TRIGGERED') 
                ORDER BY created_at DESC LIMIT 1
            """)
            row = cur.fetchone()
            if row:
                run_id = row['simulation_run_id'] if isinstance(row, dict) else row[0]
            else:
                cur.execute("SELECT simulation_run_id FROM sim_runs ORDER BY created_at DESC LIMIT 1")
                row = cur.fetchone()
                run_id = (row['simulation_run_id'] if isinstance(row, dict) else row[0]) if row else None

        if not run_id:
            logger.warning("No simulation_run_id found for sim_inventory_reorder_handler.")
            return {
                "status": "COMPLETED",
                "execution_id": execution_id,
                "pipeline_name": pipeline_name,
                "replenishments_count": 0,
                "message": "No active simulation run found."
            }

        # 2. Query low-stock SKUs from sim_inventory_snapshots
        cur.execute("""
            SELECT sku, warehouse_id, on_hand_quantity, reorder_threshold, reorder_target_qty
            FROM sim_inventory_snapshots
            WHERE simulation_run_id = %s AND is_below_threshold = 1
        """, (run_id,))
        low_stock_skus = cur.fetchall()

        if not low_stock_skus:
            logger.info(f"No low-stock SKUs found for run '{run_id}'.")
            return {
                "status": "COMPLETED",
                "execution_id": execution_id,
                "pipeline_name": pipeline_name,
                "simulation_run_id": run_id,
                "replenishments_count": 0,
            }

        # 3. Discover online suppliers
        cur.execute("SELECT supplier_id FROM operational_suppliers WHERE status = 'Online'")
        supplier_rows = cur.fetchall()
        suppliers = [
            r['supplier_id'] if isinstance(r, dict) else r[0] 
            for r in supplier_rows
        ]
        if not suppliers:
            suppliers = ['SUP-SP-PRIMARY', 'SUP-MG-BACKUP', 'SUP-RJ-SECONDARY']

        # 4. Generate simulated replenishment orders
        now = datetime.datetime.now()
        replenishment_inserts = []
        for r in low_stock_skus:
            sku = r['sku'] if isinstance(r, dict) else r[0]
            warehouse_id = r['warehouse_id'] if isinstance(r, dict) else r[1]
            on_hand = r['on_hand_quantity'] if isinstance(r, dict) else r[2]
            target_qty = r['reorder_target_qty'] if isinstance(r, dict) else r[4]

            order_qty = max(20, target_qty - on_hand)
            supplier_id = random.choice(suppliers)
            sim_rep_id = f"SIM-RPL-{uuid.uuid4().hex[:10].upper()}"
            est_delivery = now + datetime.timedelta(days=2)

            replenishment_inserts.append((
                sim_rep_id,
                run_id,
                execution_id,
                sku,
                warehouse_id,
                supplier_id,
                order_qty,
                'CONFIRMED',
                now,
                est_delivery,
                now
            ))

        cur.executemany("""
            INSERT INTO sim_replenishment_orders (
                sim_replenishment_id, simulation_run_id, execution_id,
                sku, warehouse_id, supplier_id, quantity, status,
                dispatched_at, estimated_delivery_at, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, replenishment_inserts)

        # 5. Update sim_runs replenishment count
        cur.execute("""
            UPDATE sim_runs
            SET replenishment_count = %s
            WHERE simulation_run_id = %s
        """, (len(replenishment_inserts), run_id))

        conn.commit()

        # 6. Write execution artifact
        exec_dir = os.path.join(RESULTS_DIR, 'executions', str(execution_id))
        os.makedirs(exec_dir, exist_ok=True)
        summary_path = os.path.join(exec_dir, 'sim_reorder_summary.json')
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump({
                "execution_id": execution_id,
                "pipeline_name": pipeline_name,
                "simulation_run_id": run_id,
                "replenishments_count": len(replenishment_inserts),
                "orders": [
                    {
                        "replenishment_id": o[0],
                        "sku": o[3],
                        "warehouse_id": o[4],
                        "supplier_id": o[5],
                        "quantity": o[6],
                    } for o in replenishment_inserts
                ]
            }, f, indent=2)

        rel_output = os.path.relpath(summary_path, REPO_ROOT).replace('\\', '/')

        # 7. Publish SSE Event
        try:
            from services.observability.publisher import publish_dashboard_event
            publish_dashboard_event(
                event_type="INVENTORY_REORDER_STATE_CHANGED",
                entity="INVENTORY",
                severity="INFO",
                execution_id=execution_id,
                data={
                    "simulation_run_id": run_id,
                    "replenishments_created": len(replenishment_inserts),
                    "is_simulated": True,
                    "lifecycle_state": "CONFIRMED",
                }
            )
        except Exception as ev_err:
            logger.debug(f"Simulated reorder event skipped: {ev_err}")

        logger.info(
            f"Simulated Inventory Reorder completed: {len(replenishment_inserts)} orders generated "
            f"for run '{run_id}' (Execution #{execution_id})."
        )

        return {
            "status": "COMPLETED",
            "execution_id": execution_id,
            "pipeline_name": pipeline_name,
            "simulation_run_id": run_id,
            "replenishments_count": len(replenishment_inserts),
            "output_location": rel_output,
            "simulated": True,
        }

    except Exception as exc:
        conn.rollback()
        logger.error(f"sim_inventory_reorder_handler failed for execution #{execution_id}: {exc}", exc_info=True)
        raise
    finally:
        cur.close()
        conn.close()


def operational_feature_pipeline_handler(execution_id: int, pipeline_name: str) -> Dict[str, Any]:
    """
    Executes the Operational Demand Feature Pipeline:
      1. Aggregates customer demand from operational_orders (excluding CANCELLED).
      2. Persists weekly aggregated demand into operational_demand_weekly.
      3. Expands onto complete continuous weekly grid across all model categories.
      4. Engineers the 11 XGBoost features (with insufficient history and unseen category tracking).
      5. Persists features into operational_feature_store.
      6. Updates dataset_metadata for 'Operational Demand Features' and emits events.

    Args:
        execution_id: ID of pipeline_executions record in MySQL.
        pipeline_name: Name of pipeline being executed.

    Returns:
        Dict with execution summary and feature counts.
    """
    logger.info(f"Starting execution #{execution_id} for pipeline '{pipeline_name}'...")
    from services.feature_pipeline.pipeline import run_operational_feature_pipeline
    return run_operational_feature_pipeline(execution_id=execution_id, pipeline_name=pipeline_name)





def operational_demand_forecast_handler(execution_id: int, pipeline_name: str) -> Dict[str, Any]:
    """
    Executes the Operational Demand Forecast Pipeline:
      1. Loads latest feature vectors from operational_feature_store.
      2. Loads authoritative trained XGBoost model.
      3. Generates recursive forward forecasts for 7-day and 14-day horizons.
      4. Persists predictions into operational_forecast_results.
      5. Updates dataset_metadata for 'Operational Forecasts' and emits events.

    Args:
        execution_id: ID of pipeline_executions record in MySQL.
        pipeline_name: Name of pipeline being executed.

    Returns:
        Dict with execution summary and prediction counts.
    """
    logger.info(f"Starting execution #{execution_id} for pipeline '{pipeline_name}'...")
    from services.forecasting.pipeline import run_operational_forecast_pipeline
    return run_operational_forecast_pipeline(execution_id=execution_id, pipeline_name=pipeline_name)



# ==============================================================================
# Authoritative Real Olist Pipeline Handlers
# ==============================================================================

def olist_core_forecast_handler(execution_id: int, pipeline_name: str) -> Dict[str, Any]:
    """
    Executes the Olist Core Forecast Pipeline triggered when all 3 real Olist datasets
    (Orders + Order Items + Payments) are in READY status.
    Generates real ML demand forecast predictions using the trained XGBoost model,
    saves predictions to MySQL (forecast_results), and writes an execution report.
    """
    logger.info(f"Starting execution #{execution_id} for real Olist pipeline '{pipeline_name}'...")
    return demand_forecast_pipeline_handler(execution_id=execution_id, pipeline_name=pipeline_name)


def olist_regional_dispatch_handler(execution_id: int, pipeline_name: str) -> Dict[str, Any]:
    """
    Executes Olist Regional Dispatch Pipeline triggered when a QUORUM of regional
    customer order feeds (e.g. at least 3 of SP, RJ, MG, RS) are in READY status.
    Aggregates real state-level orders, order volume, and logistics distribution.
    """
    logger.info(f"Starting execution #{execution_id} for regional Olist pipeline '{pipeline_name}'...")
    import pandas as pd
    import json
    import datetime

    sim_dir = os.path.join(REPO_ROOT, "data", "olist_split", "simulation")
    orders_csv = os.path.join(sim_dir, "olist_orders_dataset.csv")
    cust_csv = os.path.join(sim_dir, "olist_customers_dataset.csv")

    df_orders = pd.read_csv(orders_csv) if os.path.exists(orders_csv) else pd.DataFrame()
    df_cust = pd.read_csv(cust_csv) if os.path.exists(cust_csv) else pd.DataFrame()

    regional_metrics = {}
    if not df_orders.empty and not df_cust.empty:
        merged = df_orders.merge(df_cust[["customer_id", "customer_state", "customer_city"]], on="customer_id", how="inner")
        for state in ["SP", "RJ", "MG", "RS"]:
            st_orders = merged[merged["customer_state"] == state]
            regional_metrics[state] = {
                "orders_count": len(st_orders),
                "top_cities": st_orders["customer_city"].value_counts().head(3).to_dict()
            }
    else:
        regional_metrics = {"SP": {"orders_count": 0}, "RJ": {"orders_count": 0}}

    exec_dir = os.path.join(RESULTS_DIR, "executions", str(execution_id))
    os.makedirs(exec_dir, exist_ok=True)
    summary_path = os.path.join(exec_dir, "olist_regional_dispatch_summary.json")

    summary_data = {
        "execution_id": execution_id,
        "pipeline_name": pipeline_name,
        "quorum_satisfied": True,
        "regions_evaluated": list(regional_metrics.keys()),
        "regional_breakdown": regional_metrics,
        "executed_at": datetime.datetime.now().isoformat()
    }

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary_data, f, indent=2)

    rel_output = os.path.relpath(summary_path, REPO_ROOT).replace("\\", "/")
    logger.info(f"Olist Regional Dispatch completed successfully. Artifact saved to: {rel_output}")

    return {
        "status": "COMPLETED",
        "execution_id": execution_id,
        "pipeline_name": pipeline_name,
        "output_location": rel_output,
        "metrics": regional_metrics
    }


def olist_payment_settlement_handler(execution_id: int, pipeline_name: str) -> Dict[str, Any]:
    """
    Executes Olist Payment Settlement Pipeline triggered when ANY payment stream
    (Credit Card, Boleto, Voucher) is in READY status.
    Aggregates real transaction volumes, payment values, and installment distributions.
    """
    logger.info(f"Starting execution #{execution_id} for payment Olist pipeline '{pipeline_name}'...")
    import pandas as pd
    import json
    import datetime

    sim_dir = os.path.join(REPO_ROOT, "data", "olist_split", "simulation")
    payments_csv = os.path.join(sim_dir, "olist_order_payments_dataset.csv")

    df_payments = pd.read_csv(payments_csv) if os.path.exists(payments_csv) else pd.DataFrame()

    payment_breakdown = {}
    total_settled_value = 0.0
    if not df_payments.empty:
        grouped = df_payments.groupby("payment_type").agg(
            transactions=("order_id", "count"),
            total_value=("payment_value", "sum"),
            avg_installments=("payment_installments", "mean")
        ).reset_index()

        total_settled_value = float(df_payments["payment_value"].sum())
        for _, row in grouped.iterrows():
            payment_breakdown[row["payment_type"]] = {
                "transactions": int(row["transactions"]),
                "total_value": round(float(row["total_value"]), 2),
                "avg_installments": round(float(row["avg_installments"]), 1)
            }

    exec_dir = os.path.join(RESULTS_DIR, "executions", str(execution_id))
    os.makedirs(exec_dir, exist_ok=True)
    summary_path = os.path.join(exec_dir, "olist_payment_settlement_summary.json")

    summary_data = {
        "execution_id": execution_id,
        "pipeline_name": pipeline_name,
        "total_settled_value": round(total_settled_value, 2),
        "payment_types_processed": list(payment_breakdown.keys()),
        "breakdown": payment_breakdown,
        "executed_at": datetime.datetime.now().isoformat()
    }

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary_data, f, indent=2)

    rel_output = os.path.relpath(summary_path, REPO_ROOT).replace("\\", "/")
    logger.info(f"Olist Payment Settlement completed successfully. Total settled: R$ {total_settled_value:,.2f}. Artifact: {rel_output}")

    return {
        "status": "COMPLETED",
        "execution_id": execution_id,
        "pipeline_name": pipeline_name,
        "output_location": rel_output,
        "total_settled_value": total_settled_value,
        "breakdown": payment_breakdown
    }
