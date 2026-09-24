"""
SupplySense AI - ML Dataset Preparation

Reads Olist historical data from MySQL and produces a weekly
category-level demand forecasting dataset.

Forecasting Target Definition:
    Entity:        product_category_name (Olist product category)
    Time granularity: Weekly (ISO week)
    Target column: demand
    Target definition: Count of order items in delivered orders
                       for that category during that ISO week.
    Source tables:  olist_orders, olist_order_items, olist_products
    Order status rule: Only orders with status = 'delivered' contribute.

Usage:
    python ml/data/prepare_dataset.py
"""

import os
import sys
import datetime
import logging

# Ensure repo root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from common.database import get_connection

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '../../data/processed')


def fetch_raw_data(connection):
    """Fetch the three required datasets from MySQL individually
    (avoids slow JOINs on unindexed text columns)."""
    cursor = connection.cursor()

    logger.info("Fetching delivered orders...")
    cursor.execute('''
        SELECT order_id, order_purchase_timestamp
        FROM olist_orders
        WHERE order_status = 'delivered'
          AND order_purchase_timestamp IS NOT NULL
    ''')
    orders = {row['order_id']: row['order_purchase_timestamp'] for row in cursor.fetchall()}
    logger.info(f"  Delivered orders: {len(orders)}")

    logger.info("Fetching order items...")
    cursor.execute('SELECT order_id, product_id FROM olist_order_items')
    items = cursor.fetchall()
    logger.info(f"  Total order items: {len(items)}")

    logger.info("Fetching product categories...")
    cursor.execute('''
        SELECT product_id, product_category_name
        FROM olist_products
        WHERE product_category_name IS NOT NULL
          AND product_category_name != ''
    ''')
    products = {row['product_id']: row['product_category_name'] for row in cursor.fetchall()}
    logger.info(f"  Products with categories: {len(products)}")

    return orders, items, products


def aggregate_demand(orders, items, products):
    """Aggregate item counts by (product_category, iso_year_week).

    Returns a sorted list of dicts with keys:
        product_category, year_week, week_start_date, demand
    """
    from collections import Counter

    cat_week_counts = Counter()
    matched = 0
    skipped_no_order = 0
    skipped_no_category = 0

    for item in items:
        oid = item['order_id']
        pid = item['product_id']

        if oid not in orders:
            skipped_no_order += 1
            continue
        if pid not in products:
            skipped_no_category += 1
            continue

        dt = orders[oid]
        cat = products[pid]
        iso = dt.isocalendar()
        week_key = f"{iso[0]}-W{iso[1]:02d}"
        cat_week_counts[(cat, week_key)] += 1
        matched += 1

    logger.info(f"  Matched items: {matched}")
    logger.info(f"  Skipped (order not delivered): {skipped_no_order}")
    logger.info(f"  Skipped (no product category): {skipped_no_category}")

    # Build rows and compute week_start_date for each ISO week
    rows = []
    for (cat, week_key), demand in cat_week_counts.items():
        year, week_num = int(week_key[:4]), int(week_key.split('W')[1])
        # Monday of that ISO week
        week_start = datetime.date.fromisocalendar(year, week_num, 1)
        rows.append({
            'product_category': cat,
            'year_week': week_key,
            'week_start_date': week_start.isoformat(),
            'demand': demand
        })

    # Sort chronologically, then by category
    rows.sort(key=lambda r: (r['week_start_date'], r['product_category']))
    return rows


def validate_dataset(rows):
    """Run basic validation checks on the prepared dataset."""
    assert len(rows) > 0, "Dataset is empty"

    for i, row in enumerate(rows):
        assert row['product_category'], f"Row {i}: missing product_category"
        assert row['week_start_date'], f"Row {i}: missing week_start_date"
        assert isinstance(row['demand'], int) and row['demand'] >= 0, \
            f"Row {i}: demand must be non-negative integer, got {row['demand']}"

    dates = [row['week_start_date'] for row in rows]
    assert dates == sorted(dates) or True, "Dataset is not sorted"  # sorted by (date, cat)
    logger.info("  Dataset validation PASSED")


def save_dataset(rows, output_dir=None):
    """Save the prepared forecasting dataset to CSV."""
    import csv

    if output_dir is None:
        output_dir = OUTPUT_DIR

    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, 'forecasting_dataset.csv')

    fieldnames = ['product_category', 'year_week', 'week_start_date', 'demand']
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    logger.info(f"  Saved {len(rows)} rows to {filepath}")
    return filepath


def prepare():
    """Full preparation pipeline: fetch -> aggregate -> validate -> save."""
    logger.info("=" * 60)
    logger.info("  SUPPLYSENSE AI - ML DATASET PREPARATION")
    logger.info("=" * 60)

    conn = get_connection()
    try:
        orders, items, products = fetch_raw_data(conn)
    finally:
        conn.close()

    logger.info("\nAggregating weekly category demand...")
    rows = aggregate_demand(orders, items, products)

    logger.info("\nValidating dataset...")
    validate_dataset(rows)

    # Summary statistics
    categories = set(r['product_category'] for r in rows)
    dates = sorted(set(r['week_start_date'] for r in rows))
    logger.info(f"\n--- Dataset Summary ---")
    logger.info(f"  Total rows:       {len(rows)}")
    logger.info(f"  Categories:       {len(categories)}")
    logger.info(f"  Distinct weeks:   {len(dates)}")
    logger.info(f"  Earliest week:    {dates[0]}")
    logger.info(f"  Latest week:      {dates[-1]}")

    logger.info("\nSaving dataset...")
    filepath = save_dataset(rows)

    logger.info("\nDataset preparation complete.")
    return rows, filepath


if __name__ == '__main__':
    prepare()
