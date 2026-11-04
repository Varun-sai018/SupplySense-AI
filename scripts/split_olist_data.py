"""
SupplySense AI - Olist Dataset Splitter

Chronologically splits the raw Olist dataset into two parts:
  Part 1: Historical Data (< 2018-05-28) -> Used for training and validating XGBoost
  Part 2: Simulation Data (>= 2018-05-28) -> Used for real-time Kafka event streaming
"""

import os
import sys
import shutil
import json
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OLIST_RAW_DIR = os.path.join(REPO_ROOT, "Olist")
SPLIT_BASE_DIR = os.path.join(REPO_ROOT, "data", "olist_split")
HISTORICAL_DIR = os.path.join(SPLIT_BASE_DIR, "historical")
SIMULATION_DIR = os.path.join(SPLIT_BASE_DIR, "simulation")

CUTOFF_TIMESTAMP = "2018-05-28 00:00:00"


def split_olist_datasets(cutoff: str = CUTOFF_TIMESTAMP):
    logger.info("==================================================")
    logger.info("SupplySense AI: Splitting Olist Data")
    logger.info(f"Source Directory: {OLIST_RAW_DIR}")
    logger.info(f"Cutoff Timestamp: {cutoff}")
    logger.info("==================================================")

    if not os.path.isdir(OLIST_RAW_DIR):
        raise FileNotFoundError(f"Raw Olist directory not found: {OLIST_RAW_DIR}")

    os.makedirs(HISTORICAL_DIR, exist_ok=True)
    os.makedirs(SIMULATION_DIR, exist_ok=True)

    # 1. Load orders
    orders_csv = os.path.join(OLIST_RAW_DIR, "olist_orders_dataset.csv")
    logger.info(f"Reading {orders_csv}...")
    df_orders = pd.read_csv(orders_csv)
    df_orders["parsed_ts"] = pd.to_datetime(df_orders["order_purchase_timestamp"])

    hist_orders = df_orders[df_orders["parsed_ts"] < cutoff].copy()
    sim_orders = df_orders[df_orders["parsed_ts"] >= cutoff].copy()

    logger.info(f"Total Orders: {len(df_orders):,}")
    logger.info(f"Part 1 (Historical) Orders: {len(hist_orders):,} (Min: {hist_orders['parsed_ts'].min()} -> Max: {hist_orders['parsed_ts'].max()})")
    logger.info(f"Part 2 (Simulation) Orders: {len(sim_orders):,} (Min: {sim_orders['parsed_ts'].min()} -> Max: {sim_orders['parsed_ts'].max()})")

    # Drop parsed_ts before saving to match schema exactly
    hist_orders.drop(columns=["parsed_ts"]).to_csv(os.path.join(HISTORICAL_DIR, "olist_orders_dataset.csv"), index=False)
    sim_orders.drop(columns=["parsed_ts"]).to_csv(os.path.join(SIMULATION_DIR, "olist_orders_dataset.csv"), index=False)

    hist_order_ids = set(hist_orders["order_id"])
    sim_order_ids = set(sim_orders["order_id"])

    # 2. Split order items
    items_csv = os.path.join(OLIST_RAW_DIR, "olist_order_items_dataset.csv")
    logger.info(f"Reading {items_csv}...")
    df_items = pd.read_csv(items_csv)
    hist_items = df_items[df_items["order_id"].isin(hist_order_ids)].copy()
    sim_items = df_items[df_items["order_id"].isin(sim_order_ids)].copy()

    logger.info(f"Total Order Items: {len(df_items):,}")
    logger.info(f"Part 1 (Historical) Items: {len(hist_items):,}")
    logger.info(f"Part 2 (Simulation) Items: {len(sim_items):,}")

    hist_items.to_csv(os.path.join(HISTORICAL_DIR, "olist_order_items_dataset.csv"), index=False)
    sim_items.to_csv(os.path.join(SIMULATION_DIR, "olist_order_items_dataset.csv"), index=False)

    # 3. Split order payments
    payments_csv = os.path.join(OLIST_RAW_DIR, "olist_order_payments_dataset.csv")
    if os.path.exists(payments_csv):
        df_payments = pd.read_csv(payments_csv)
        hist_payments = df_payments[df_payments["order_id"].isin(hist_order_ids)].copy()
        sim_payments = df_payments[df_payments["order_id"].isin(sim_order_ids)].copy()
        hist_payments.to_csv(os.path.join(HISTORICAL_DIR, "olist_order_payments_dataset.csv"), index=False)
        sim_payments.to_csv(os.path.join(SIMULATION_DIR, "olist_order_payments_dataset.csv"), index=False)
        logger.info(f"Split Payments -> Hist: {len(hist_payments):,}, Sim: {len(sim_payments):,}")

    # 4. Split order reviews
    reviews_csv = os.path.join(OLIST_RAW_DIR, "olist_order_reviews_dataset.csv")
    if os.path.exists(reviews_csv):
        df_reviews = pd.read_csv(reviews_csv)
        hist_reviews = df_reviews[df_reviews["order_id"].isin(hist_order_ids)].copy()
        sim_reviews = df_reviews[df_reviews["order_id"].isin(sim_order_ids)].copy()
        hist_reviews.to_csv(os.path.join(HISTORICAL_DIR, "olist_order_reviews_dataset.csv"), index=False)
        sim_reviews.to_csv(os.path.join(SIMULATION_DIR, "olist_order_reviews_dataset.csv"), index=False)
        logger.info(f"Split Reviews -> Hist: {len(hist_reviews):,}, Sim: {len(sim_reviews):,}")

    # 5. Shared reference catalogs (products, sellers, customers, translation)
    shared_files = [
        "olist_products_dataset.csv",
        "olist_sellers_dataset.csv",
        "olist_customers_dataset.csv",
        "product_category_name_translation.csv",
    ]
    for filename in shared_files:
        src = os.path.join(OLIST_RAW_DIR, filename)
        if os.path.exists(src):
            shutil.copy2(src, os.path.join(HISTORICAL_DIR, filename))
            shutil.copy2(src, os.path.join(SIMULATION_DIR, filename))
            logger.info(f"Copied shared catalog: {filename}")

    manifest = {
        "cutoff_timestamp": cutoff,
        "historical": {
            "dir": HISTORICAL_DIR,
            "orders_count": len(hist_orders),
            "items_count": len(hist_items),
            "date_range": [str(hist_orders["parsed_ts"].min()), str(hist_orders["parsed_ts"].max())]
        },
        "simulation": {
            "dir": SIMULATION_DIR,
            "orders_count": len(sim_orders),
            "items_count": len(sim_items),
            "date_range": [str(sim_orders["parsed_ts"].min()), str(sim_orders["parsed_ts"].max())]
        }
    }

    manifest_path = os.path.join(SPLIT_BASE_DIR, "split_manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    logger.info(f"Dataset split complete! Manifest saved to: {manifest_path}")
    return manifest


if __name__ == "__main__":
    split_olist_datasets()
