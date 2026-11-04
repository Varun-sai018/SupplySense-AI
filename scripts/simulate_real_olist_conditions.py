"""
SupplySense AI - Real Olist Data Condition Simulation & Execution

Simulates and evaluates dependency conditions using 100% authoritative Olist data
from the Part 2 simulation dataset (data/olist_split/simulation/):
  - NO synthetic or fake placeholder data.
  - Condition 1 (ALL): Real Orders, Order Items, and Order Payments synchronizing to trigger XGBoost demand forecasting.
  - Condition 2 (QUORUM: 3 of 4): Real Regional Customer Orders from São Paulo (SP), Rio de Janeiro (RJ),
    Minas Gerais (MG), and Rio Grande do Sul (RS) requiring quorum consensus before regional dispatch.
  - Condition 3 (ANY): Real Multi-Channel Payment Streams (Credit Card, Boleto, Voucher) where ANY arrival
    immediately triggers payment settlement and order allocation.
"""

import os
import sys
import time
import json
import logging
import pandas as pd
from datetime import datetime

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from services.dependency_engine.main import process_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("RealOlistConditions")

SIM_DIR = os.path.join(REPO_ROOT, "data", "olist_split", "simulation")


def print_banner(title: str):
    logger.info("\n" + "=" * 75)
    logger.info(f"  {title.upper()}")
    logger.info("=" * 75)


def run_real_olist_simulation():
    conn = get_connection()
    cur = conn.cursor()

    base_eid = int(time.time() * 10) % 900000 + 200000

    # Verify real Olist simulation files exist
    orders_csv = os.path.join(SIM_DIR, "olist_orders_dataset.csv")
    items_csv = os.path.join(SIM_DIR, "olist_order_items_dataset.csv")
    payments_csv = os.path.join(SIM_DIR, "olist_order_payments_dataset.csv")
    cust_csv = os.path.join(SIM_DIR, "olist_customers_dataset.csv")

    for path, name in [(orders_csv, "Orders"), (items_csv, "Items"), (payments_csv, "Payments"), (cust_csv, "Customers")]:
        assert os.path.exists(path), f"Missing real Olist file: {path}"

    df_orders = pd.read_csv(orders_csv)
    df_items = pd.read_csv(items_csv)
    df_payments = pd.read_csv(payments_csv)
    df_cust = pd.read_csv(cust_csv)

    logger.info(f"Loaded Real Olist Simulation Data:")
    logger.info(f"  Orders: {len(df_orders):,} | Items: {len(df_items):,} | Payments: {len(df_payments):,} | Customers: {len(df_cust):,}")

    # =========================================================================
    # CONDITION 1: ALL (Olist Core Forecast Pipeline)
    # =========================================================================
    print_banner("1. REAL OLIST CONDITION: ALL (Orders + Items + Payments)")
    logger.info("Dependency Rule: ALL 3 real datasets must arrive to trigger Demand Forecasting.")

    # Reset all 3 to WAITING
    cur.execute("UPDATE dataset_metadata SET status = 'WAITING' WHERE dataset_name IN ('Olist_Orders', 'Olist_Order_Items', 'Olist_Payments')")
    conn.commit()

    # Step 1A: Ingest real Olist Orders batch
    sample_orders = df_orders.head(50)
    logger.info(f"--> Step 1A: Streaming batch of {len(sample_orders)} real Olist orders (Span: {sample_orders['order_purchase_timestamp'].min()} to {sample_orders['order_purchase_timestamp'].max()})...")
    cur.execute("UPDATE dataset_metadata SET status = 'READY', current_version = current_version + 1, row_count = %s WHERE dataset_name = 'Olist_Orders'", (len(sample_orders),))
    conn.commit()

    event_1a = {
        "event_id": base_eid + 1,
        "dataset_name": "Olist_Orders",
        "pipeline_name": "Olist Core Forecast Pipeline",
        "dataset_version": 2,
        "rows_changed": len(sample_orders)
    }
    dec_1a, reason_1a = process_event(event_1a, conn)
    logger.info(f"Decision 1A -> [{dec_1a}] | Reason: {reason_1a}")
    assert dec_1a == "BLOCK"

    # Step 1B: Ingest real Olist Order Items batch
    sample_items = df_items[df_items["order_id"].isin(sample_orders["order_id"])]
    logger.info(f"--> Step 1B: Streaming {len(sample_items)} matching real Olist order items...")
    cur.execute("UPDATE dataset_metadata SET status = 'READY', current_version = current_version + 1, row_count = %s WHERE dataset_name = 'Olist_Order_Items'", (len(sample_items),))
    conn.commit()

    event_1b = {
        "event_id": base_eid + 2,
        "dataset_name": "Olist_Order_Items",
        "pipeline_name": "Olist Core Forecast Pipeline",
        "dataset_version": 2,
        "rows_changed": len(sample_items)
    }
    dec_1b, reason_1b = process_event(event_1b, conn)
    logger.info(f"Decision 1B -> [{dec_1b}] | Reason: {reason_1b}")
    assert dec_1b == "BLOCK"

    # Step 1C: Ingest real Olist Payments batch
    sample_pay = df_payments[df_payments["order_id"].isin(sample_orders["order_id"])]
    total_val = float(sample_pay["payment_value"].sum())
    logger.info(f"--> Step 1C: Streaming {len(sample_pay)} real Olist payments (Total Value: R$ {total_val:,.2f})...")
    cur.execute("UPDATE dataset_metadata SET status = 'READY', current_version = current_version + 1, row_count = %s WHERE dataset_name = 'Olist_Payments'", (len(sample_pay),))
    conn.commit()

    event_1c = {
        "event_id": base_eid + 3,
        "dataset_name": "Olist_Payments",
        "pipeline_name": "Olist Core Forecast Pipeline",
        "dataset_version": 2,
        "rows_changed": len(sample_pay)
    }
    dec_1c, reason_1c = process_event(event_1c, conn)
    logger.info(f"Decision 1C -> [{dec_1c}] | Reason: {reason_1c}")
    assert dec_1c == "TRIGGER"
    logger.info("[PASS] Real Olist Condition 'ALL' verified: Correctly synchronized real orders, items, and payments to trigger forecasting!")

    # =========================================================================
    # CONDITION 2: QUORUM (Olist Regional Dispatch Pipeline)
    # =========================================================================
    print_banner("2. REAL OLIST CONDITION: QUORUM (Regional Consensus 3 of 4 States)")
    logger.info("Dependency Rule: At least 3 of 4 real Brazilian states (SP, RJ, MG, RS) must report regional orders.")

    reg_datasets = ["Olist_Orders_SP", "Olist_Orders_RJ", "Olist_Orders_MG", "Olist_Orders_RS"]
    cur.execute(f"UPDATE dataset_metadata SET status = 'WAITING' WHERE dataset_name IN ({','.join(['%s']*4)})", tuple(reg_datasets))
    conn.commit()

    merged_cust = df_orders.merge(df_cust[["customer_id", "customer_state"]], on="customer_id", how="inner")

    # Step 2A: São Paulo (SP)
    sp_count = len(merged_cust[merged_cust["customer_state"] == "SP"])
    logger.info(f"--> Step 2A: Ingesting São Paulo (SP) orders ({sp_count:,} real orders)...")
    cur.execute("UPDATE dataset_metadata SET status = 'READY', row_count = %s WHERE dataset_name = 'Olist_Orders_SP'", (sp_count,))
    conn.commit()
    event_2a = {"event_id": base_eid + 21, "dataset_name": "Olist_Orders_SP", "pipeline_name": "Olist Regional Dispatch Pipeline", "dataset_version": 1}
    dec_2a, _ = process_event(event_2a, conn)
    logger.info(f"Quorum Step 1 (SP) -> [{dec_2a}] (1/3 Quorum reached)")
    assert dec_2a == "BLOCK"

    # Step 2B: Rio de Janeiro (RJ)
    rj_count = len(merged_cust[merged_cust["customer_state"] == "RJ"])
    logger.info(f"--> Step 2B: Ingesting Rio de Janeiro (RJ) orders ({rj_count:,} real orders)...")
    cur.execute("UPDATE dataset_metadata SET status = 'READY', row_count = %s WHERE dataset_name = 'Olist_Orders_RJ'", (rj_count,))
    conn.commit()
    event_2b = {"event_id": base_eid + 22, "dataset_name": "Olist_Orders_RJ", "pipeline_name": "Olist Regional Dispatch Pipeline", "dataset_version": 1}
    dec_2b, _ = process_event(event_2b, conn)
    logger.info(f"Quorum Step 2 (RJ) -> [{dec_2b}] (2/3 Quorum reached)")
    assert dec_2b == "BLOCK"

    # Step 2C: Minas Gerais (MG) -> REACHES 3 OF 4 QUORUM!
    mg_count = len(merged_cust[merged_cust["customer_state"] == "MG"])
    logger.info(f"--> Step 2C: Ingesting Minas Gerais (MG) orders ({mg_count:,} real orders)...")
    cur.execute("UPDATE dataset_metadata SET status = 'READY', row_count = %s WHERE dataset_name = 'Olist_Orders_MG'", (mg_count,))
    conn.commit()
    event_2c = {"event_id": base_eid + 23, "dataset_name": "Olist_Orders_MG", "pipeline_name": "Olist Regional Dispatch Pipeline", "dataset_version": 1}
    dec_2c, _ = process_event(event_2c, conn)
    logger.info(f"Quorum Step 3 (MG) -> [{dec_2c}] (3/3 Quorum REACHED!)")
    assert dec_2c == "TRIGGER"
    logger.info("[PASS] Real Olist Condition 'QUORUM' verified: Regional orders from SP, RJ, MG triggered regional dispatch!")

    # =========================================================================
    # CONDITION 3: ANY (Olist Payment Settlement Pipeline)
    # =========================================================================
    print_banner("3. REAL OLIST CONDITION: ANY (Payment Channel Settlement)")
    logger.info("Dependency Rule: As soon as ANY real payment stream (Credit Card, Boleto, Voucher) arrives, trigger settlement.")

    pay_datasets = ["Olist_Payments_CreditCard", "Olist_Payments_Boleto", "Olist_Payments_Voucher"]
    cur.execute(f"UPDATE dataset_metadata SET status = 'WAITING' WHERE dataset_name IN ({','.join(['%s']*3)})", tuple(pay_datasets))
    conn.commit()

    # Ingest Real Boleto payment stream
    boleto_df = df_payments[df_payments["payment_type"] == "boleto"]
    boleto_total = float(boleto_df["payment_value"].sum())
    logger.info(f"--> Ingesting real Boleto payments stream ({len(boleto_df):,} payments, Value: R$ {boleto_total:,.2f})...")
    cur.execute("UPDATE dataset_metadata SET status = 'READY', row_count = %s WHERE dataset_name = 'Olist_Payments_Boleto'", (len(boleto_df),))
    conn.commit()

    event_3 = {
        "event_id": base_eid + 30,
        "dataset_name": "Olist_Payments_Boleto",
        "pipeline_name": "Olist Payment Settlement Pipeline",
        "dataset_version": 1,
        "rows_changed": len(boleto_df)
    }
    dec_3, reason_3 = process_event(event_3, conn)
    logger.info(f"Decision ANY (Boleto) -> [{dec_3}] | Reason: {reason_3}")
    assert dec_3 == "TRIGGER"
    logger.info("[PASS] Real Olist Condition 'ANY' verified: Real Boleto stream immediately triggered settlement and allocation!")

    conn.close()
    print_banner("ALL REAL OLIST CONDITIONS VERIFIED SUCCESSFULLY WITHOUT SYNTHETIC DATA!")


if __name__ == "__main__":
    run_real_olist_simulation()
