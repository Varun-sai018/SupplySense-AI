"""
SupplySense AI - Deterministic Operational Inventory Seeding (Phase 2A)

Seeds initial baseline operational inventory balances, suppliers, and catalog availability.
Uses real reference SKUs from the Olist product catalog, labeled as 'HISTORICAL_SEED'.
Safe to re-run: Uses non-destructive upserts that preserve modified quantities and status changes.
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

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Deterministic reference SKUs from Olist catalog
SEED_INVENTORY_ITEMS = [
    {
        "sku": "aca2eb7d00ea1a7b8ebd4e68314663af",  # moveis_decoracao
        "warehouse_id": "WH-SP-01",
        "on_hand_quantity": 80,
        "reserved_quantity": 0,
        "reorder_threshold": 20,
        "reorder_target_quantity": 100,
        "safety_stock_buffer": 10,
        "data_origin": "HISTORICAL_SEED",
    },
    {
        "sku": "99a4788cb24856965c36a24e339b6058",  # cama_mesa_banho
        "warehouse_id": "WH-SP-01",
        "on_hand_quantity": 65,
        "reserved_quantity": 0,
        "reorder_threshold": 20,
        "reorder_target_quantity": 100,
        "safety_stock_buffer": 10,
        "data_origin": "HISTORICAL_SEED",
    },
    {
        "sku": "422879e10f46682990de24d770e7f83d",  # ferramentas_jardim
        "warehouse_id": "WH-SP-01",
        "on_hand_quantity": 50,
        "reserved_quantity": 0,
        "reorder_threshold": 20,
        "reorder_target_quantity": 80,
        "safety_stock_buffer": 10,
        "data_origin": "HISTORICAL_SEED",
    },
    {
        "sku": "389d119b48cf3043d311335e499d9c6b",  # ferramentas_jardim
        "warehouse_id": "WH-SP-01",
        "on_hand_quantity": 40,
        "reserved_quantity": 0,
        "reorder_threshold": 15,
        "reorder_target_quantity": 60,
        "safety_stock_buffer": 5,
        "data_origin": "HISTORICAL_SEED",
    },
    {
        "sku": "368c6c730842d78016ad823897a372db",  # ferramentas_jardim
        "warehouse_id": "WH-SP-01",
        "on_hand_quantity": 30,
        "reserved_quantity": 0,
        "reorder_threshold": 15,
        "reorder_target_quantity": 50,
        "safety_stock_buffer": 5,
        "data_origin": "HISTORICAL_SEED",
    },
]

# Deterministic suppliers with diverse operational health states
SEED_SUPPLIERS = [
    {
        "supplier_id": "SUP-SP-PRIMARY",
        "supplier_name": "São Paulo Logistics & Supply Co.",
        "status": "Online",
        "api_endpoint": "https://api.supplier-sp.local/v1",
        "reliability_score": 0.985,
        "average_lead_time_days": 2,
    },
    {
        "supplier_id": "SUP-RJ-SECONDARY",
        "supplier_name": "Rio Industrial Wholesalers",
        "status": "Offline",
        "api_endpoint": "https://api.supplier-rj.local/v1",
        "reliability_score": 0.880,
        "average_lead_time_days": 4,
    },
    {
        "supplier_id": "SUP-MG-BACKUP",
        "supplier_name": "Minas Distribution Hub",
        "status": "Degraded",
        "api_endpoint": "https://api.supplier-mg.local/v1",
        "reliability_score": 0.920,
        "average_lead_time_days": 3,
    },
]

# Catalog mappings between suppliers and SKUs
SEED_SUPPLIER_CATALOG = [
    # Primary Supplier (Online) carries all 5 items
    {
        "supplier_id": "SUP-SP-PRIMARY",
        "sku": "aca2eb7d00ea1a7b8ebd4e68314663af",
        "supply_availability": "IN_STOCK",
        "unit_reorder_cost": 45.00,
        "min_reorder_quantity": 10,
        "lead_time_hours": 48,
    },
    {
        "supplier_id": "SUP-SP-PRIMARY",
        "sku": "99a4788cb24856965c36a24e339b6058",
        "supply_availability": "IN_STOCK",
        "unit_reorder_cost": 55.00,
        "min_reorder_quantity": 10,
        "lead_time_hours": 48,
    },
    {
        "supplier_id": "SUP-SP-PRIMARY",
        "sku": "422879e10f46682990de24d770e7f83d",
        "supply_availability": "IN_STOCK",
        "unit_reorder_cost": 38.00,
        "min_reorder_quantity": 15,
        "lead_time_hours": 48,
    },
    {
        "supplier_id": "SUP-SP-PRIMARY",
        "sku": "389d119b48cf3043d311335e499d9c6b",
        "supply_availability": "IN_STOCK",
        "unit_reorder_cost": 32.00,
        "min_reorder_quantity": 15,
        "lead_time_hours": 48,
    },
    {
        "supplier_id": "SUP-SP-PRIMARY",
        "sku": "368c6c730842d78016ad823897a372db",
        "supply_availability": "IN_STOCK",
        "unit_reorder_cost": 28.00,
        "min_reorder_quantity": 10,
        "lead_time_hours": 48,
    },
    # Secondary Supplier (Offline) carries key items
    {
        "supplier_id": "SUP-RJ-SECONDARY",
        "sku": "aca2eb7d00ea1a7b8ebd4e68314663af",
        "supply_availability": "LOW_STOCK",
        "unit_reorder_cost": 42.50,
        "min_reorder_quantity": 25,
        "lead_time_hours": 96,
    },
    {
        "supplier_id": "SUP-RJ-SECONDARY",
        "sku": "99a4788cb24856965c36a24e339b6058",
        "supply_availability": "OUT_OF_STOCK",
        "unit_reorder_cost": 50.00,
        "min_reorder_quantity": 20,
        "lead_time_hours": 96,
    },
    # Backup Supplier (Degraded)
    {
        "supplier_id": "SUP-MG-BACKUP",
        "sku": "422879e10f46682990de24d770e7f83d",
        "supply_availability": "IN_STOCK",
        "unit_reorder_cost": 40.00,
        "min_reorder_quantity": 10,
        "lead_time_hours": 72,
    },
]


def seed_operational_inventory(connection=None) -> Dict[str, int]:
    """
    Executes idempotent seeding of operational inventory, suppliers, and catalog availability.
    Preserves existing operational quantities or status modifications on subsequent runs.
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    counts = {
        "inventory_processed": len(SEED_INVENTORY_ITEMS),
        "suppliers_processed": len(SEED_SUPPLIERS),
        "catalog_processed": len(SEED_SUPPLIER_CATALOG),
        "inventory_seeded": 0,
        "suppliers_seeded": 0,
        "catalog_seeded": 0,
    }

    try:
        cursor = connection.cursor()

        # 1. Seed Operational Inventory (preserve existing on_hand/reserved if row already exists)
        for item in SEED_INVENTORY_ITEMS:
            cursor.execute("""
                INSERT INTO operational_inventory (
                    sku, warehouse_id, on_hand_quantity, reserved_quantity,
                    reorder_threshold, reorder_target_quantity, safety_stock_buffer,
                    data_origin, version
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 0)
                ON DUPLICATE KEY UPDATE
                    reorder_threshold = VALUES(reorder_threshold),
                    reorder_target_quantity = VALUES(reorder_target_quantity),
                    safety_stock_buffer = VALUES(safety_stock_buffer)
            """, (
                item["sku"],
                item["warehouse_id"],
                item["on_hand_quantity"],
                item["reserved_quantity"],
                item["reorder_threshold"],
                item["reorder_target_quantity"],
                item["safety_stock_buffer"],
                item["data_origin"],
            ))
            counts["inventory_seeded"] += 1

        # 2. Seed Operational Suppliers (preserve existing status if row already exists)
        for sup in SEED_SUPPLIERS:
            cursor.execute("""
                INSERT INTO operational_suppliers (
                    supplier_id, supplier_name, status, api_endpoint,
                    reliability_score, average_lead_time_days
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    supplier_name = VALUES(supplier_name),
                    reliability_score = VALUES(reliability_score),
                    average_lead_time_days = VALUES(average_lead_time_days)
            """, (
                sup["supplier_id"],
                sup["supplier_name"],
                sup["status"],
                sup["api_endpoint"],
                sup["reliability_score"],
                sup["average_lead_time_days"],
            ))
            counts["suppliers_seeded"] += 1

        # 3. Seed Supplier Catalog Availability
        for cat in SEED_SUPPLIER_CATALOG:
            cursor.execute("""
                INSERT INTO supplier_catalog_availability (
                    supplier_id, sku, supply_availability, unit_reorder_cost,
                    min_reorder_quantity, lead_time_hours
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    unit_reorder_cost = VALUES(unit_reorder_cost),
                    min_reorder_quantity = VALUES(min_reorder_quantity),
                    lead_time_hours = VALUES(lead_time_hours)
            """, (
                cat["supplier_id"],
                cat["sku"],
                cat["supply_availability"],
                cat["unit_reorder_cost"],
                cat["min_reorder_quantity"],
                cat["lead_time_hours"],
            ))
            counts["catalog_seeded"] += 1

        connection.commit()
        logger.info(
            f"Operational inventory seeding completed: {counts['inventory_seeded']} inventory rows, "
            f"{counts['suppliers_seeded']} suppliers, {counts['catalog_seeded']} catalog entries."
        )
        return counts
    except Exception as e:
        logger.error(f"Error during operational inventory seeding: {e}")
        connection.rollback()
        raise
    finally:
        if should_close:
            connection.close()


if __name__ == "__main__":
    seed_operational_inventory()
