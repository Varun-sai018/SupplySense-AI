"""
SupplySense AI - Simulated Dependency Configuration Seeder (Phase 1)

Defines an idempotent helper to configure simulated pipeline dependency structures:
  1. SIM-ALL-Pipeline (Strategy: ALL)
  2. SIM-ANY-Pipeline (Strategy: ANY)
  3. SIM-QUORUM-Pipeline (Strategy: QUORUM, Threshold: 3)
  4. SIM-AST-Pipeline (Strategy: AST)

Safety Invariants:
  - Idempotent: uses SELECT checks before INSERT; never duplicates or overwrites.
  - Non-destructive: never touches operational datasets (Orders, Products, Sellers, Inventory)
    or operational pipeline 'Demand Forecast Pipeline'.
  - No hardcoded IDs: uses database-generated AUTO_INCREMENT primary keys.
  - Schema-compliant: table_name uses virtual identifier ("sim_*") since table_name is NOT NULL;
    source_type is set to 'SIMULATED'.
  - Transaction-safe: executes within a single transaction with automatic rollback on error.
  - NOT executed automatically: must be called explicitly by an authorized runner/script.
"""

import os
import sys
import logging
from typing import Dict, Any, List, Optional

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection

logger = logging.getLogger(__name__)

# Canonical specifications for simulated pipelines and their synthetic dependencies
SIMULATED_PIPELINE_SPECS = [
    {
        "pipeline_name": "SIM-ALL-Pipeline",
        "condition_type": "ALL",
        "required_count": None,
        "datasets": [
            "SIM_Raw_Orders",
            "SIM_Catalog_Feed",
            "SIM_Pricing_Index",
        ],
        "ast_expression": None,
    },
    {
        "pipeline_name": "SIM-ANY-Pipeline",
        "condition_type": "ANY",
        "required_count": None,
        "datasets": [
            "SIM_Primary_Stream",
            "SIM_Backup_Stream",
        ],
        "ast_expression": None,
    },
    {
        "pipeline_name": "SIM-QUORUM-Pipeline",
        "condition_type": "QUORUM",
        "required_count": 3,
        "datasets": [
            "SIM_Regional_Node_A",
            "SIM_Regional_Node_B",
            "SIM_Regional_Node_C",
            "SIM_Regional_Node_D",
        ],
        "ast_expression": None,
    },
    {
        "pipeline_name": "SIM-AST-Pipeline",
        "condition_type": "AST",
        "required_count": None,
        "datasets": [
            "SIM_Inventory_Feed",
            "SIM_Supplier_Feed",
        ],
        "ast_expression": "AND(SIM_Inventory_Feed == 'READY', SIM_Supplier_Feed == 'READY')",
    },
    {
        "pipeline_name": "SIM-FORECAST-Pipeline",
        "condition_type": "ALL",
        "required_count": None,
        "datasets": [
            "SIM_Orders_Feed",
        ],
        "ast_expression": None,
    },
    {
        "pipeline_name": "SIM-REORDER-Pipeline",
        "condition_type": "ALL",
        "required_count": None,
        "datasets": [
            "SIM_Inventory_Snapshot",
        ],
        "ast_expression": None,
    },
]


def ensure_simulated_dependencies(connection=None, dry_run: bool = False) -> Dict[str, Any]:
    """
    Idempotently configures simulated pipeline definitions and datasets in MySQL.

    Args:
        connection: Optional open PyMySQL connection. When None, opens and manages its own.
        dry_run: When True, inspects what would be created without performing any writes.

    Returns:
        Dict summarizing pipelines created/existing, datasets created/existing,
        and dependency mappings created/existing.
    """
    should_close_conn = False
    if connection is None:
        connection = get_connection()
        should_close_conn = True

    cursor = connection.cursor()

    summary = {
        "dry_run": dry_run,
        "pipelines_created": [],
        "pipelines_existing": [],
        "datasets_created": [],
        "datasets_existing": [],
        "mappings_created": 0,
        "mappings_existing": 0,
    }

    try:
        # Step 1: Ensure simulated datasets exist in dataset_metadata
        dataset_id_map: Dict[str, int] = {}

        # Collect all unique simulated dataset names
        all_dataset_names = set()
        for spec in SIMULATED_PIPELINE_SPECS:
            all_dataset_names.update(spec["datasets"])

        for ds_name in sorted(all_dataset_names):
            cursor.execute(
                "SELECT dataset_id, status FROM dataset_metadata WHERE dataset_name = %s",
                (ds_name,)
            )
            existing_row = cursor.fetchone()

            if existing_row:
                ds_id = existing_row["dataset_id"] if isinstance(existing_row, dict) else existing_row[0]
                dataset_id_map[ds_name] = ds_id
                summary["datasets_existing"].append(ds_name)
            else:
                if not dry_run:
                    # Note: table_name is NOT NULL in schema, so we assign a safe virtual identifier
                    virtual_table = f"sim_{ds_name.lower()}"
                    cursor.execute("""
                        INSERT INTO dataset_metadata (
                            dataset_name, source_type, table_name,
                            current_version, row_count, status
                        ) VALUES (%s, %s, %s, 1, 0, 'WAITING')
                    """, (ds_name, "SIMULATED", virtual_table))
                    ds_id = cursor.lastrowid
                    dataset_id_map[ds_name] = ds_id
                    summary["datasets_created"].append(ds_name)
                else:
                    summary["datasets_created"].append(f"[DRY_RUN] {ds_name}")

        # Step 2: Ensure simulated pipelines exist in pipeline_dependencies
        for spec in SIMULATED_PIPELINE_SPECS:
            p_name = spec["pipeline_name"]
            cond_type = spec["condition_type"]
            req_count = spec["required_count"]

            cursor.execute(
                "SELECT dependency_id FROM pipeline_dependencies WHERE pipeline_name = %s",
                (p_name,)
            )
            existing_dep = cursor.fetchone()

            if existing_dep:
                dep_id = existing_dep["dependency_id"] if isinstance(existing_dep, dict) else existing_dep[0]
                summary["pipelines_existing"].append(p_name)
            else:
                if not dry_run:
                    cursor.execute("""
                        INSERT INTO pipeline_dependencies (
                            pipeline_name, condition_type, required_count
                        ) VALUES (%s, %s, %s)
                    """, (p_name, cond_type, req_count))
                    dep_id = cursor.lastrowid
                    summary["pipelines_created"].append(p_name)
                else:
                    dep_id = -1
                    summary["pipelines_created"].append(f"[DRY_RUN] {p_name}")

            # Step 3: Ensure dependency_datasets mappings exist
            for ds_name in spec["datasets"]:
                ds_id = dataset_id_map.get(ds_name)

                if ds_id and dep_id != -1:
                    cursor.execute("""
                        SELECT id FROM dependency_datasets
                        WHERE dependency_id = %s AND dataset_id = %s
                    """, (dep_id, ds_id))
                    existing_mapping = cursor.fetchone()

                    if existing_mapping:
                        summary["mappings_existing"] += 1
                    else:
                        if not dry_run:
                            cursor.execute("""
                                INSERT INTO dependency_datasets (dependency_id, dataset_id)
                                VALUES (%s, %s)
                            """, (dep_id, ds_id))
                            summary["mappings_created"] += 1
                        else:
                            summary["mappings_created"] += 1
                elif dry_run:
                    summary["mappings_created"] += 1

        if not dry_run:
            connection.commit()
            logger.info(
                f"Simulated dependencies ensured: {len(summary['pipelines_created'])} pipelines created, "
                f"{len(summary['datasets_created'])} datasets created, "
                f"{summary['mappings_created']} mappings created."
            )
        else:
            logger.info("Dry-run inspection completed. Zero database writes performed.")

        return summary

    except Exception as exc:
        if not dry_run:
            connection.rollback()
        logger.error(f"Error seeding simulated dependencies: {exc}")
        raise
    finally:
        cursor.close()
        if should_close_conn:
            connection.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Seed simulated pipeline dependencies.")
    parser.add_argument("--confirm", action="store_true", help="Explicitly confirm database write.")
    args = parser.parse_args()

    if not args.confirm:
        print("Dry-run mode (use --confirm to apply writes):")
        res = ensure_simulated_dependencies(dry_run=True)
        print(res)
    else:
        print("Applying simulated dependencies to database...")
        res = ensure_simulated_dependencies(dry_run=False)
        print(res)
