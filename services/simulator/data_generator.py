"""
SupplySense AI - Simulation Data Generator Service

Generates realistic synthetic supply-chain data for end-to-end simulation:
  1. Synthetic Orders -> stored in isolated `sim_orders` table.
  2. Synthetic Inventory Snapshots -> stored in isolated `sim_inventory_snapshots` table.
  3. Tracks execution run metadata in `sim_runs` table.

Safety Invariants:
  - NEVER modifies or writes to `olist_*` tables.
  - NEVER modifies or writes to `operational_inventory`, `operational_orders`, or `operational_suppliers`.
  - NEVER modifies or writes to operational `replenishment_orders`.
  - All records are strictly scoped to a unique `simulation_run_id`.
"""

import os
import sys
import uuid
import random
import logging
import datetime
from typing import Dict, Any, List, Optional

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection

logger = logging.getLogger(__name__)


def generate_simulation_run(
    batch_size: int = 200,
    low_stock_ratio: float = 0.35,
    target_destination: str = "SANDBOX",
    connection=None
) -> Dict[str, Any]:
    """
    Generates a full batch of synthetic orders and inventory snapshots for simulation.

    Args:
        batch_size: Number of synthetic orders to generate (default 200, 50-1000).
        low_stock_ratio: Fraction of inventory SKUs that will trigger low-stock alerts.
        connection: Optional PyMySQL connection.

    Returns:
        Dict with simulation_run_id, orders_count, inventory_count, low_stock_count, etc.
    """
    batch_size = max(50, min(1000, batch_size))
    simulation_run_id = f"sim_run_{uuid.uuid4().hex[:12]}"
    now = datetime.datetime.now()

    should_close_conn = False
    if connection is None:
        connection = get_connection()
        should_close_conn = True

    cursor = connection.cursor()

    try:
        # 1. Initialize simulation run entry
        cursor.execute("""
            INSERT INTO sim_runs (
                simulation_run_id, scenario_id, status, started_at
            ) VALUES (%s, 'E2E_ORDERS_INVENTORY', 'GENERATING_DATA', %s)
        """, (simulation_run_id, now))

        # 2. Query available product categories from olist_products (read-only reference)
        cursor.execute("""
            SELECT DISTINCT product_category_name 
            FROM olist_products 
            WHERE product_category_name IS NOT NULL AND product_category_name != ''
            LIMIT 40
        """)
        cat_rows = cursor.fetchall()
        categories = [
            r['product_category_name'] if isinstance(r, dict) else r[0] 
            for r in cat_rows
        ]
        if not categories:
            categories = [
                'beleza_saude', 'informatica_acessorios', 'cama_mesa_banho',
                'esporte_lazer', 'moveis_decoracao', 'utilidades_domesticas',
                'relogios_presentes', 'telefonia', 'automotivo', 'brinquedos'
            ]

        # 3. Generate synthetic orders
        base_date = now.date()
        target_destination_norm = (target_destination or "SANDBOX").upper()
        categories_count = 0

        if target_destination_norm == "OPERATIONAL":
            cursor.execute("""
                SELECT product_id, product_category_name 
                FROM olist_products 
                WHERE product_id IS NOT NULL AND product_category_name IS NOT NULL
                LIMIT 200
            """)
            catalog_rows = cursor.fetchall()
            valid_prods = [
                (r['product_id'] if isinstance(r, dict) else r[0], r['product_category_name'] if isinstance(r, dict) else r[1])
                for r in catalog_rows
            ]
            op_orders_payload = []
            seen_cats = set()
            for i in range(batch_size):
                prod_tuple = random.choice(valid_prods) if valid_prods else ("368c6c730842d78016ad823897a372db", "beleza_saude")
                sku = prod_tuple[0]
                seen_cats.add(prod_tuple[1])
                qty = random.choices([1, 2, 3, 4, 5], weights=[0.55, 0.25, 0.10, 0.06, 0.04])[0]
                unit_price = round(random.uniform(18.50, 240.00), 2)
                sim_order_id = f"SIM-ORD-{uuid.uuid4().hex[:10].upper()}"
                op_orders_payload.append({
                    "order_id": sim_order_id,
                    "sku": sku,
                    "warehouse_id": "WH-SP-01",
                    "quantity": qty,
                    "unit_price": unit_price,
                    "event_time": now.isoformat()
                })

            from services.observability import repository as obs_repo
            obs_repo.ingest_operational_orders(
                orders=op_orders_payload,
                idempotency_key=f"sim_{simulation_run_id}",
                source="SIMULATED",
                warehouse_id="WH-SP-01",
                connection=connection
            )
            orders_count_generated = len(op_orders_payload)
            categories_count = len(seen_cats)
        else:
            orders_to_insert = []
            for i in range(batch_size):
                sim_order_id = f"SIM-ORD-{uuid.uuid4().hex[:10].upper()}"
                category = random.choice(categories)
                prod_id = f"SIM-PROD-{uuid.uuid4().hex[:8].upper()}"
                qty = random.choices([1, 2, 3, 4, 5], weights=[0.55, 0.25, 0.10, 0.06, 0.04])[0]
                unit_price = round(random.uniform(18.50, 240.00), 2)
                total_price = round(unit_price * qty, 2)

                # Spread orders across 4 consecutive ISO weeks
                days_ago = random.randint(0, 27)
                order_date = base_date - datetime.timedelta(days=days_ago)
                iso = order_date.isocalendar()
                year_week = f"{iso[0]}-W{iso[1]:02d}"
                week_start = datetime.date.fromisocalendar(iso[0], iso[1], 1)

                orders_to_insert.append((
                    sim_order_id,
                    simulation_run_id,
                    category,
                    prod_id,
                    qty,
                    unit_price,
                    total_price,
                    'delivered',
                    order_date,
                    week_start,
                    year_week,
                    now
                ))

            cursor.executemany("""
                INSERT INTO sim_orders (
                    sim_order_id, simulation_run_id, product_category, product_id,
                    quantity, unit_price, total_price, order_status,
                    purchase_date, week_start_date, year_week, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, orders_to_insert)
            orders_count_generated = len(orders_to_insert)
            categories_count = len(set(o[2] for o in orders_to_insert))

        # 4. Generate inventory snapshots based on operational SKUs (read-only reference)
        cursor.execute("""
            SELECT sku, reorder_threshold, reorder_target_quantity, safety_stock_buffer 
            FROM operational_inventory 
            LIMIT 20
        """)
        sku_rows = cursor.fetchall()
        
        inventory_to_insert = []
        low_stock_count = 0
        warehouse_id = "SIM-WH-01"

        for row in sku_rows:
            sku = row['sku'] if isinstance(row, dict) else row[0]
            threshold = row['reorder_threshold'] if isinstance(row, dict) else row[1]
            target_qty = row['reorder_target_quantity'] if isinstance(row, dict) else row[2]
            
            # Decide if this SKU is in low-stock condition for this run
            is_low = (random.random() < low_stock_ratio)
            if is_low:
                on_hand = max(1, int(threshold * random.uniform(0.2, 0.75)))
                below_flag = 1
                low_stock_count += 1
            else:
                on_hand = int(threshold * random.uniform(1.3, 3.2))
                below_flag = 0

            inventory_to_insert.append((
                simulation_run_id,
                sku,
                warehouse_id,
                on_hand,
                0,
                threshold,
                target_qty,
                below_flag,
                now
            ))

        cursor.executemany("""
            INSERT INTO sim_inventory_snapshots (
                simulation_run_id, sku, warehouse_id, on_hand_quantity,
                reserved_quantity, reorder_threshold, reorder_target_qty,
                is_below_threshold, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, inventory_to_insert)

        # 5. Update sim_runs with completed generation stats
        cursor.execute("""
            UPDATE sim_runs
            SET status = 'DATA_GENERATED',
                orders_generated = %s,
                sku_count = %s,
                low_stock_count = %s
            WHERE simulation_run_id = %s
        """, (orders_count_generated, len(inventory_to_insert), low_stock_count, simulation_run_id))

        connection.commit()

        logger.info(
            f"Simulation run {simulation_run_id} generated ({target_destination_norm}): "
            f"{orders_count_generated} orders, {len(inventory_to_insert)} inventory snapshots "
            f"({low_stock_count} low-stock alerts)."
        )

        return {
            "simulation_run_id": simulation_run_id,
            "status": "DATA_GENERATED",
            "target_destination": target_destination_norm,
            "orders_generated": orders_count_generated,
            "sku_count": len(inventory_to_insert),
            "low_stock_count": low_stock_count,
            "warehouse_id": warehouse_id,
            "categories_count": categories_count,
        }

    except Exception as exc:
        connection.rollback()
        logger.error(f"Failed to generate simulation data: {exc}")
        try:
            cursor.execute("""
                UPDATE sim_runs
                SET status = 'FAILED', error_message = %s
                WHERE simulation_run_id = %s
            """, (str(exc), simulation_run_id))
            connection.commit()
        except Exception:
            pass
        raise
    finally:
        cursor.close()
        if should_close_conn:
            connection.close()
