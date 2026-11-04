"""
SupplySense AI - Cleanup Unwanted Virtual/Dummy Datasets and Dependencies

Removes all synthetic placeholder pipelines and virtual feeds from MySQL:
  - Deletes SIM-ALL-Pipeline, SIM-ANY-Pipeline, SIM-QUORUM-Pipeline, SIM-AST-Pipeline,
    SIM-FORECAST-Pipeline, SIM-REORDER-Pipeline from pipeline_dependencies & dependency_datasets.
  - Deletes all dummy SIM_% datasets from dataset_metadata.
  - Preserves authoritative real Olist, Operational, and Demand Forecasting pipelines.
"""

import os
import sys
import logging

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("CleanupUnwanted")

UNWANTED_PIPELINES = [
    "SIM-ALL-Pipeline",
    "SIM-ANY-Pipeline",
    "SIM-QUORUM-Pipeline",
    "SIM-AST-Pipeline",
    "SIM-FORECAST-Pipeline",
    "SIM-REORDER-Pipeline",
]


def cleanup():
    conn = get_connection()
    cur = conn.cursor()

    logger.info("1. Removing unwanted synthetic pipelines...")
    for pipe_name in UNWANTED_PIPELINES:
        cur.execute("SELECT dependency_id FROM pipeline_dependencies WHERE pipeline_name = %s", (pipe_name,))
        row = cur.fetchone()
        if row:
            dep_id = row["dependency_id"]
            cur.execute("DELETE FROM dependency_datasets WHERE dependency_id = %s", (dep_id,))
            cur.execute("DELETE FROM pipeline_dependencies WHERE dependency_id = %s", (dep_id,))
            logger.info(f"   Deleted pipeline dependency: '{pipe_name}' (ID: {dep_id})")

    logger.info("2. Removing unwanted virtual dummy datasets (SIM_% prefix)...")
    cur.execute("""
        SELECT dataset_id, dataset_name FROM dataset_metadata
        WHERE dataset_name LIKE 'SIM_%' OR source_type = 'SIMULATED'
    """)
    unwanted_datasets = cur.fetchall()
    for d in unwanted_datasets:
        d_id = d["dataset_id"]
        d_name = d["dataset_name"]
        # Delete any remaining dependency associations
        cur.execute("DELETE FROM dependency_datasets WHERE dataset_id = %s", (d_id,))
        cur.execute("DELETE FROM dataset_metadata WHERE dataset_id = %s", (d_id,))
        logger.info(f"   Deleted dataset: '{d_name}' (ID: {d_id})")

    conn.commit()

    # Verify remaining pipelines
    cur.execute("""
        SELECT pd.dependency_id, pd.pipeline_name, pd.condition_type, pd.required_count,
               GROUP_CONCAT(dm.dataset_name ORDER BY dm.dataset_name SEPARATOR ', ') as datasets
        FROM pipeline_dependencies pd
        LEFT JOIN dependency_datasets dd ON pd.dependency_id = dd.dependency_id
        LEFT JOIN dataset_metadata dm ON dd.dataset_id = dm.dataset_id
        GROUP BY pd.dependency_id, pd.pipeline_name, pd.condition_type, pd.required_count
        ORDER BY pd.dependency_id
    """)
    remaining_pipes = cur.fetchall()
    logger.info("\n--- Authoritative Pipelines Remaining in MySQL ---")
    for p in remaining_pipes:
        logger.info(f"  #{p['dependency_id']} | {p['pipeline_name']} [{p['condition_type']}] -> {p['datasets']}")

    # Verify remaining datasets
    cur.execute("SELECT dataset_id, dataset_name, table_name, source_type, status, current_version, row_count FROM dataset_metadata ORDER BY dataset_id")
    remaining_datasets = cur.fetchall()
    logger.info(f"\n--- Authoritative Tracked Datasets Remaining: {len(remaining_datasets)} ---")
    for d in remaining_datasets:
        logger.info(f"  #{d['dataset_id']} | {d['dataset_name']} ({d['source_type']}) -> Status: {d['status']} | Rows: {d['row_count']}")

    conn.close()
    logger.info("\nCleanup of unwanted datasets & pipelines complete!")


if __name__ == "__main__":
    cleanup()
