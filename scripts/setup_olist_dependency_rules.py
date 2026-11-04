"""
SupplySense AI - Setup Authoritative Olist Dependency Rules & Pipelines

Configures real Olist-driven pipeline dependencies in MySQL:
  1. Olist Core Forecast Pipeline (ALL):
     - Requires: Olist_Orders, Olist_Order_Items, Olist_Payments
     - Action: Aggregates real items, runs XGBoost demand forecasting, persists to forecast_results.
  2. Olist Regional Dispatch Pipeline (QUORUM: 3 of 4):
     - Requires: Olist_Orders_SP, Olist_Orders_RJ, Olist_Orders_MG, Olist_Orders_RS
     - Action: Consolidates state-level order distributions from SP, RJ, MG, RS, calculates logistics metrics.
  3. Olist Payment Settlement Pipeline (ANY):
     - Requires: Olist_Payments_CreditCard, Olist_Payments_Boleto, Olist_Payments_Voucher
     - Action: Settles orders by real payment method, updates allocation status.
"""

import os
import sys
import logging

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SetupOlistRules")

DATASETS = [
    # Core triad
    ("Olist_Orders", "olist_orders", "OLIST_STREAM"),
    ("Olist_Order_Items", "olist_order_items", "OLIST_STREAM"),
    ("Olist_Payments", "olist_order_payments", "OLIST_STREAM"),
    # Regional state feeds
    ("Olist_Orders_SP", "olist_orders", "OLIST_REGIONAL"),
    ("Olist_Orders_RJ", "olist_orders", "OLIST_REGIONAL"),
    ("Olist_Orders_MG", "olist_orders", "OLIST_REGIONAL"),
    ("Olist_Orders_RS", "olist_orders", "OLIST_REGIONAL"),
    # Payment method streams
    ("Olist_Payments_CreditCard", "olist_order_payments", "OLIST_PAYMENT"),
    ("Olist_Payments_Boleto", "olist_order_payments", "OLIST_PAYMENT"),
    ("Olist_Payments_Voucher", "olist_order_payments", "OLIST_PAYMENT"),
]

PIPELINES = [
    {
        "name": "Olist Core Forecast Pipeline",
        "condition_type": "ALL",
        "required_count": None,
        "datasets": ["Olist_Orders", "Olist_Order_Items", "Olist_Payments"]
    },
    {
        "name": "Olist Regional Dispatch Pipeline",
        "condition_type": "QUORUM",
        "required_count": 3,
        "datasets": ["Olist_Orders_SP", "Olist_Orders_RJ", "Olist_Orders_MG", "Olist_Orders_RS"]
    },
    {
        "name": "Olist Payment Settlement Pipeline",
        "condition_type": "ANY",
        "required_count": None,
        "datasets": ["Olist_Payments_CreditCard", "Olist_Payments_Boleto", "Olist_Payments_Voucher"]
    }
]


def setup_rules():
    conn = get_connection()
    cur = conn.cursor()

    logger.info("Registering real Olist datasets in dataset_metadata...")
    dataset_id_map = {}
    for d_name, tbl, src in DATASETS:
        cur.execute("""
            INSERT INTO dataset_metadata (dataset_name, source_type, table_name, current_version, row_count, status)
            VALUES (%s, %s, %s, 1, 0, 'WAITING')
            ON DUPLICATE KEY UPDATE table_name = VALUES(table_name), source_type = VALUES(source_type)
        """, (d_name, src, tbl))
        cur.execute("SELECT dataset_id FROM dataset_metadata WHERE dataset_name = %s", (d_name,))
        d_id = cur.fetchone()["dataset_id"]
        dataset_id_map[d_name] = d_id
        logger.info(f"  Dataset: '{d_name}' (ID: {d_id}, Table: {tbl}, Source: {src})")

    logger.info("Configuring pipeline dependencies...")
    for pipe in PIPELINES:
        p_name = pipe["name"]
        cond = pipe["condition_type"]
        req_cnt = pipe["required_count"]

        # Insert or update pipeline_dependencies
        cur.execute("""
            INSERT INTO pipeline_dependencies (pipeline_name, condition_type, required_count)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE condition_type = VALUES(condition_type), required_count = VALUES(required_count)
        """, (p_name, cond, req_cnt))

        cur.execute("SELECT dependency_id FROM pipeline_dependencies WHERE pipeline_name = %s", (p_name,))
        dep_id = cur.fetchone()["dependency_id"]

        # Clear existing mappings for clean state
        cur.execute("DELETE FROM dependency_datasets WHERE dependency_id = %s", (dep_id,))

        # Insert dependency_datasets
        for d_name in pipe["datasets"]:
            d_id = dataset_id_map[d_name]
            cur.execute("""
                INSERT INTO dependency_datasets (dependency_id, dataset_id)
                VALUES (%s, %s)
            """, (dep_id, d_id))

        logger.info(f"  Pipeline: '{p_name}' (ID: {dep_id}, Condition: {cond}, Quorum: {req_cnt}) -> Datasets: {pipe['datasets']}")

    conn.commit()
    conn.close()
    logger.info("Olist pipeline dependency rules successfully configured in MySQL!")


if __name__ == "__main__":
    setup_rules()
