"""
SupplySense AI - Operational Inventory Foundation Tests (Phase 2A)

Tests the operational inventory database foundation, deterministic seeding,
inventory arithmetic, constraint validation, version incrementing, error handling,
rollback behavior, and strict isolation from historical Olist and ML assets.
"""

import os
import sys
import unittest
from typing import Dict, Any

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import importlib
from common.database import get_connection

seed_module = importlib.import_module("database.seeds.002_seed_operational_inventory")
seed_operational_inventory = seed_module.seed_operational_inventory
SEED_INVENTORY_ITEMS = seed_module.SEED_INVENTORY_ITEMS
SEED_SUPPLIERS = seed_module.SEED_SUPPLIERS

from services.dependency_engine.repository import (
    get_inventory,
    list_inventory,
    get_supplier_availability,
    apply_inventory_delta,
    InventoryNotFoundError,
    InventoryInvariantError
)


class TestOperationalInventorySchemaAndIntegrity(unittest.TestCase):
    """Validates MySQL schema creation, keys, constraints, and relationships for Migration 015."""

    @classmethod
    def setUpClass(cls):
        cls.conn = get_connection()

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def test_01_all_five_tables_exist(self):
        """1. Verify that all 5 operational inventory tables exist in MySQL."""
        expected_tables = {
            "operational_inventory",
            "operational_orders",
            "operational_suppliers",
            "supplier_catalog_availability",
            "replenishment_orders"
        }
        with self.conn.cursor() as cur:
            cur.execute("SHOW TABLES")
            existing = {list(r.values())[0] for r in cur.fetchall()}
            for tbl in expected_tables:
                self.assertIn(tbl, existing, f"Table '{tbl}' was not created by Migration 015")

    def test_02_primary_and_unique_keys(self):
        """1b. Verify primary and unique key definitions across operational tables."""
        with self.conn.cursor() as cur:
            # Check operational_inventory composite PK (sku, warehouse_id)
            cur.execute("""
                SELECT COLUMN_NAME
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'operational_inventory'
                  AND CONSTRAINT_NAME = 'PRIMARY'
                ORDER BY ORDINAL_POSITION
            """)
            pk_cols = [r["COLUMN_NAME"] for r in cur.fetchall()]
            self.assertEqual(pk_cols, ["sku", "warehouse_id"])

            # Check operational_orders unique idempotency_key
            cur.execute("""
                SELECT COLUMN_NAME
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'operational_orders'
                  AND CONSTRAINT_NAME = 'idx_unique_order_idempotency'
            """)
            idemp_cols = [r["COLUMN_NAME"] for r in cur.fetchall()]
            self.assertEqual(idemp_cols, ["idempotency_key"])

            # Check replenishment_orders unique action_id
            cur.execute("""
                SELECT COLUMN_NAME
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'replenishment_orders'
                  AND CONSTRAINT_NAME = 'idx_replenish_action_id'
            """)
            action_uk_cols = [r["COLUMN_NAME"] for r in cur.fetchall()]
            self.assertEqual(action_uk_cols, ["action_id"])

    def test_03_foreign_keys_and_types(self):
        """1c. Verify foreign keys reference business_rule_actions and pipeline_decisions."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT CONSTRAINT_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'replenishment_orders'
                  AND REFERENCED_TABLE_NAME IS NOT NULL
            """)
            fks = {r["CONSTRAINT_NAME"]: r for r in cur.fetchall()}
            self.assertIn("fk_replenish_action", fks)
            self.assertEqual(fks["fk_replenish_action"]["REFERENCED_TABLE_NAME"], "business_rule_actions")
            self.assertEqual(fks["fk_replenish_action"]["REFERENCED_COLUMN_NAME"], "action_id")

            self.assertIn("fk_replenish_decision", fks)
            self.assertEqual(fks["fk_replenish_decision"]["REFERENCED_TABLE_NAME"], "pipeline_decisions")
            self.assertEqual(fks["fk_replenish_decision"]["REFERENCED_COLUMN_NAME"], "decision_id")

            self.assertIn("fk_replenish_supplier", fks)
            self.assertEqual(fks["fk_replenish_supplier"]["REFERENCED_TABLE_NAME"], "operational_suppliers")
            self.assertEqual(fks["fk_replenish_supplier"]["REFERENCED_COLUMN_NAME"], "supplier_id")


class TestOperationalInventorySeeding(unittest.TestCase):
    """Validates deterministic seed execution, idempotency, and provenance tagging."""

    def test_04_seed_script_is_deterministic_and_idempotent(self):
        """2 & 3. Verify seeding runs without error and is safe to execute multiple times."""
        # First run
        counts1 = seed_operational_inventory()
        self.assertGreater(counts1["inventory_seeded"], 0)
        self.assertGreater(counts1["suppliers_seeded"], 0)

        # Second run (should not crash and should preserve existing rows)
        counts2 = seed_operational_inventory()
        self.assertIsInstance(counts2, dict)

        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) as cnt FROM operational_inventory")
                total_inv = cur.fetchone()["cnt"]
                self.assertGreaterEqual(total_inv, len(SEED_INVENTORY_ITEMS))

                # Check data_origin provenance tag
                cur.execute("SELECT DISTINCT data_origin FROM operational_inventory WHERE data_origin = 'HISTORICAL_SEED'")
                provenance_rows = cur.fetchall()
                self.assertTrue(len(provenance_rows) > 0)
                self.assertEqual(provenance_rows[0]["data_origin"], "HISTORICAL_SEED")
        finally:
            conn.close()


class TestOperationalInventoryDataAccessLayer(unittest.TestCase):
    """Validates arithmetic, constraints, versioning, rollback, and concurrency invariants."""

    @classmethod
    def setUpClass(cls):
        cls.conn = get_connection()
        cls.test_sku = "test_sku_foundation_001"
        cls.test_wh = "WH-TEST-01"

        # Setup isolated test record
        with cls.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO operational_inventory (
                    sku, warehouse_id, on_hand_quantity, reserved_quantity,
                    reorder_threshold, reorder_target_quantity, safety_stock_buffer,
                    data_origin, version
                )
                VALUES (%s, %s, 100, 20, 25, 120, 10, 'SIMULATED', 0)
                ON DUPLICATE KEY UPDATE
                    on_hand_quantity = 100,
                    reserved_quantity = 20,
                    version = 0
            """, (cls.test_sku, cls.test_wh))
        cls.conn.commit()

    @classmethod
    def tearDownClass(cls):
        with cls.conn.cursor() as cur:
            cur.execute("DELETE FROM operational_inventory WHERE sku = %s", (cls.test_sku,))
        cls.conn.commit()
        cls.conn.close()

    def test_05_inventory_arithmetic_generated_column(self):
        """4. Verify available_quantity equals on_hand minus reserved."""
        inv = get_inventory(self.test_sku, self.test_wh, connection=self.conn)
        self.assertIsNotNone(inv)
        self.assertEqual(inv["on_hand_quantity"], 100)
        self.assertEqual(inv["reserved_quantity"], 20)
        # 100 - 20 = 80
        self.assertEqual(inv["available_quantity"], 80)

    def test_06_version_increment_on_update(self):
        """6. Verify that an update increments version by 1."""
        initial = get_inventory(self.test_sku, self.test_wh, connection=self.conn)
        init_ver = initial["version"]

        # Reserve 5 more units
        updated = apply_inventory_delta(
            sku=self.test_sku,
            warehouse_id=self.test_wh,
            delta_on_hand=0,
            delta_reserved=5,
            connection=self.conn
        )
        self.assertEqual(updated["version"], init_ver + 1)
        self.assertEqual(updated["reserved_quantity"], 25)
        self.assertEqual(updated["available_quantity"], 75)

    def test_07_invalid_negative_quantities_rejected(self):
        """5. Verify that reducing on-hand stock below 0 is rejected."""
        curr = get_inventory(self.test_sku, self.test_wh, connection=self.conn)
        on_hand = curr["on_hand_quantity"]

        # Attempt to subtract more than on_hand
        with self.assertRaises(InventoryInvariantError):
            apply_inventory_delta(
                sku=self.test_sku,
                warehouse_id=self.test_wh,
                delta_on_hand=-(on_hand + 50),
                connection=self.conn
            )

    def test_08_reserved_exceeding_on_hand_rejected(self):
        """5b. Verify that reserving more than on_hand is rejected (no negative available stock)."""
        curr = get_inventory(self.test_sku, self.test_wh, connection=self.conn)
        available = curr["available_quantity"]

        with self.assertRaises(InventoryInvariantError):
            apply_inventory_delta(
                sku=self.test_sku,
                warehouse_id=self.test_wh,
                delta_reserved=available + 10,
                connection=self.conn
            )

    def test_09_missing_inventory_record_raises_error(self):
        """7. Verify missing inventory record raises InventoryNotFoundError."""
        with self.assertRaises(InventoryNotFoundError):
            apply_inventory_delta(
                sku="NON_EXISTENT_SKU_99999",
                warehouse_id="WH-SP-01",
                delta_on_hand=10,
                connection=self.conn
            )

    def test_10_transaction_failure_rollback(self):
        """8. Verify that invariant failure rolls back completely without altering state."""
        before = get_inventory(self.test_sku, self.test_wh, connection=self.conn)

        try:
            apply_inventory_delta(
                sku=self.test_sku,
                warehouse_id=self.test_wh,
                delta_on_hand=-500,  # Invalid
                connection=self.conn
            )
        except InventoryInvariantError:
            pass

        after = get_inventory(self.test_sku, self.test_wh, connection=self.conn)
        self.assertEqual(before["on_hand_quantity"], after["on_hand_quantity"])
        self.assertEqual(before["reserved_quantity"], after["reserved_quantity"])
        self.assertEqual(before["version"], after["version"])

    def test_11_list_inventory_pagination(self):
        """Verify list_inventory returns bounded results with pagination."""
        items = list_inventory(limit=3, offset=0, connection=self.conn)
        self.assertIsInstance(items, list)
        self.assertLessEqual(len(items), 3)

    def test_12_get_supplier_availability(self):
        """Verify supplier availability read function."""
        sup = get_supplier_availability("SUP-SP-PRIMARY", connection=self.conn)
        self.assertIsNotNone(sup)
        self.assertEqual(sup["supplier_id"], "SUP-SP-PRIMARY")
        self.assertEqual(sup["status"], "Online")


class TestHistoricalDataAndArtifactIsolation(unittest.TestCase):
    """Guarantees operational schema and seeds did not mutate historical Olist or ML assets."""

    @classmethod
    def setUpClass(cls):
        cls.conn = get_connection()

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def test_13_historical_olist_tables_unmutated(self):
        """10. Verify Olist orders, items, products, and sellers counts are intact."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) as cnt FROM olist_orders")
            orders_cnt = cur.fetchone()["cnt"]
            self.assertGreaterEqual(orders_cnt, 99000, "Historical orders count corrupted")

            cur.execute("SELECT COUNT(*) as cnt FROM olist_order_items")
            items_cnt = cur.fetchone()["cnt"]
            self.assertGreaterEqual(items_cnt, 110000, "Historical order items count corrupted")

            cur.execute("SELECT COUNT(*) as cnt FROM olist_products")
            products_cnt = cur.fetchone()["cnt"]
            self.assertGreaterEqual(products_cnt, 32000, "Historical products count corrupted")

    def test_14_protected_executions_and_artifacts_intact(self):
        """10b. Verify benchmark #36, historical #99586, and latest #99653 remain intact."""
        protected_ids = (36, 99586, 99653)
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT execution_id, pipeline_name, status, output_location
                FROM pipeline_executions
                WHERE execution_id IN %s
            """, (protected_ids,))
            rows = {r["execution_id"]: r for r in cur.fetchall()}

            for eid in protected_ids:
                self.assertIn(eid, rows, f"Protected execution #{eid} missing from database")
                self.assertEqual(rows[eid]["status"], "COMPLETED")

                # Verify artifact file exists on disk
                art_loc = rows[eid]["output_location"]
                self.assertIsNotNone(art_loc)
                full_path = os.path.join(REPO_ROOT, art_loc.replace('/', os.sep))
                self.assertTrue(os.path.isfile(full_path), f"Artifact for #{eid} missing at {full_path}")
                self.assertGreater(os.path.getsize(full_path), 0)


if __name__ == "__main__":
    unittest.main()
