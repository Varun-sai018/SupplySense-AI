"""
SupplySense AI - Purge Dummy / Showcase-Only Datasets & Pipelines
Purges:
  - 'Demand Forecast Pipeline' (dependency_id 1)
  - Unused static dummy datasets: 'Orders' (1), 'Order Items' (2), 'Products' (3), 'Sellers' (4), 'Inventory' (5)
Retains ONLY the 100% active, real Olist stream and operational pipelines and datasets.
"""

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection

def purge_showcase_datasets():
    conn = get_connection()
    cur = conn.cursor()

    dummy_dataset_ids = (1, 2, 3, 4, 5)

    print("1. Removing dependency_datasets links for dummy pipeline dependency_id = 1...")
    cur.execute("DELETE FROM dependency_datasets WHERE dependency_id = 1 OR dataset_id IN %s", (dummy_dataset_ids,))
    print(f"   Deleted {cur.rowcount} links from dependency_datasets.")

    print("2. Removing dummy pipeline 'Demand Forecast Pipeline' (dependency_id = 1)...")
    cur.execute("DELETE FROM pipeline_dependencies WHERE dependency_id = 1")
    print(f"   Deleted {cur.rowcount} pipeline(s) from pipeline_dependencies.")

    print("3. Removing old dummy events from dataset_events...")
    cur.execute("DELETE FROM dataset_events WHERE dataset_id IN %s", (dummy_dataset_ids,))
    print(f"   Deleted {cur.rowcount} events from dataset_events.")

    print("4. Removing dummy datasets from dataset_metadata...")
    cur.execute("DELETE FROM dataset_metadata WHERE dataset_id IN %s", (dummy_dataset_ids,))
    print(f"   Deleted {cur.rowcount} dummy dataset(s) from dataset_metadata.")

    conn.commit()

    # Verify authoritative remaining datasets
    cur.execute("SELECT dataset_id, dataset_name, source_type, status FROM dataset_metadata ORDER BY dataset_id")
    remaining_datasets = cur.fetchall()
    print(f"\nRemaining Authoritative Datasets ({len(remaining_datasets)}):")
    for d in remaining_datasets:
        print(f"  - #{d['dataset_id']} | {d['dataset_name']} ({d['source_type']}) [{d['status']}]")

    # Verify authoritative remaining pipelines
    cur.execute("SELECT dependency_id, pipeline_name, condition_type FROM pipeline_dependencies ORDER BY dependency_id")
    remaining_pipelines = cur.fetchall()
    print(f"\nRemaining Authoritative Pipelines ({len(remaining_pipelines)}):")
    for p in remaining_pipelines:
        print(f"  - #{p['dependency_id']} | {p['pipeline_name']} [{p['condition_type']}]")

    cur.close()
    conn.close()
    print("\nDatabase cleanup complete. Only 100% real active datasets and pipelines remain!")

if __name__ == "__main__":
    purge_showcase_datasets()
