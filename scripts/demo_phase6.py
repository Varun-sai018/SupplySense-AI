"""
SupplySense AI - Phase 6 Demo Script: Downstream Forecast Results Persistence

Demonstrates:
  1. Triggering the Demand Forecast Pipeline when dependencies are satisfied.
  2. Execution lifecycle: RUNNING -> COMPLETED with output_location.
  3. Automatic persistence of out-of-sample test predictions into the `forecast_results` MySQL table.
  4. Querying forecast predictions from MySQL by Execution ID, Product Category, and Forecast Week.
  5. Generating summary analytics of the stored forecasts directly from the database.
"""

import os
import sys
import importlib
import json

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection

event_gen = importlib.import_module('services.event-generator.main')
dep_engine = importlib.import_module('services.dependency-engine.main')
forecast_repo = importlib.import_module('services.pipeline-runner.forecast_repository')


def run_phase6_demo():
    conn = get_connection()
    cur = conn.cursor()

    print("=" * 80)
    print("   SUPPLYSENSE AI - PHASE 6 FORECAST RESULTS PERSISTENCE DEMO")
    print("=" * 80)

    # 1. Reset dependency state for Olist datasets
    print("\n[Step 0] Initializing Dependency Configuration (Condition: ALL)...")
    cur.execute("UPDATE dataset_metadata SET status = 'WAITING' WHERE dataset_name IN ('Orders', 'Products', 'Sellers')")
    cur.execute("UPDATE pipeline_dependencies SET condition_type = 'ALL', required_count = NULL WHERE pipeline_name = 'Demand Forecast Pipeline'")
    conn.commit()

    # 2. Progressively generate events to satisfy all dependencies
    print("\n" + "=" * 80)
    print("STEP 1: GENERATING EVENTS & TRIGGERING PIPELINE")
    print("=" * 80)

    print("1a. Updating 'Orders' dataset (1/3 READY)...")
    event_gen.generate_event('Orders', 30)
    cur.execute("SELECT * FROM dataset_events WHERE dataset_name = 'Orders' ORDER BY event_id DESC LIMIT 1")
    event_orders = cur.fetchone()
    decision_1, reason_1 = dep_engine.process_event(event_orders, conn)
    print(f"    Decision: {decision_1} | Reason: {reason_1}")

    print("1b. Updating 'Products' dataset (2/3 READY)...")
    event_gen.generate_event('Products', 15)
    cur.execute("SELECT * FROM dataset_events WHERE dataset_name = 'Products' ORDER BY event_id DESC LIMIT 1")
    event_products = cur.fetchone()
    decision_2, reason_2 = dep_engine.process_event(event_products, conn)
    print(f"    Decision: {decision_2} | Reason: {reason_2}")

    print("1c. Updating 'Sellers' dataset (3/3 READY -> TRIGGER)...")
    event_gen.generate_event('Sellers', 10)
    cur.execute("SELECT * FROM dataset_events WHERE dataset_name = 'Sellers' ORDER BY event_id DESC LIMIT 1")
    event_sellers = cur.fetchone()
    decision_3, reason_3 = dep_engine.process_event(event_sellers, conn)
    print(f"    Decision: {decision_3} | Reason: {reason_3}")

    # 3. Retrieve Execution Record
    print("\n" + "=" * 80)
    print("STEP 2: VERIFYING PIPELINE EXECUTION LIFECYCLE")
    print("=" * 80)
    cur.execute("""
        SELECT execution_id, pipeline_name, decision_id, triggering_event_id, status, started_at, completed_at, output_location, error_message
        FROM pipeline_executions
        WHERE triggering_event_id = %s
    """, (event_sellers['event_id'],))
    exec_record = cur.fetchone()

    for k, v in exec_record.items():
        print(f"  {k:<22}: {v}")

    assert exec_record['status'] == 'COMPLETED', f"Expected COMPLETED, got {exec_record['status']}"
    execution_id = exec_record['execution_id']

    # 4. Query MySQL Forecast Results
    print("\n" + "=" * 80)
    print(f"STEP 3: QUERYING PERSISTED FORECAST RESULTS (Execution ID #{execution_id})")
    print("=" * 80)

    # 4a. Execution Summary Aggregate
    summary = forecast_repo.get_forecast_summary_by_execution(execution_id, connection=conn)
    print("Forecast Summary Aggregate:")
    print(f"  Total Forecast Rows    : {summary.get('total_forecasts')}")
    print(f"  Distinct Categories    : {summary.get('distinct_categories')}")
    print(f"  Forecast Period Range  : {summary.get('min_week')} to {summary.get('max_week')}")
    print(f"  Average Demand Forecast: {float(summary.get('avg_predicted_demand', 0)):.4f}")
    print(f"  Model Name & Version   : {summary.get('model_name')} ({summary.get('model_version')})")

    # 4b. Sample Category Forecasts
    print("\nSample Category Forecasts ('cama_mesa_banho'):")
    cat_rows = forecast_repo.get_forecasts_by_category("cama_mesa_banho", limit=5, connection=conn)
    for r in cat_rows:
        print(f"  Week: {r['forecast_week']} | Category: {r['product_category']:<18} | Predicted Demand: {float(r['predicted_demand']):.4f} | Model: {r['model_name']} {r['model_version']}")

    # 4c. Sample Week Forecasts
    sample_week = summary.get('min_week')
    if sample_week:
        print(f"\nSample Week Multi-Category Forecasts ({sample_week}):")
        week_rows = forecast_repo.get_forecasts_by_week(sample_week, limit=5, connection=conn)
        for r in week_rows:
            print(f"  Week: {r['forecast_week']} | Category: {r['product_category']:<25} | Predicted Demand: {float(r['predicted_demand']):.4f}")

    conn.close()
    print("\n" + "=" * 80)
    print("      PHASE 6 DEMO COMPLETED SUCCESSFULLY")
    print("=" * 80)


if __name__ == '__main__':
    run_phase6_demo()
