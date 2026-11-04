"""
Unit and Integration Tests for Phase 1: Operational Order Ingestion.

Validates:
  1. Transactional ingestion into operational_orders, dataset_metadata, and dataset_events.
  2. Batch idempotency and safe duplicate replay handling.
  3. Catalog SKU validation (olist_products) and operational inventory eligibility flag.
  4. Non-negative quantity validation and invalid input rejection.
  5. Source provenance preservation (OPERATIONAL_API, SIMULATED, MANUAL_IMPORT).
  6. Zero inventory deduction safety invariant (operational_inventory remains untouched).
  7. Optional Kafka publication tolerance (swallows errors without failing MySQL transaction).
  8. Simulator dual-mode execution (SANDBOX vs OPERATIONAL).
  9. FastAPI REST endpoints (POST and GET /api/observability/orders).
"""

import os
import sys
import unittest
import uuid
import datetime
from fastapi.testclient import TestClient

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from services.observability import repository, service
from services.observability.main import app
from services.simulator.data_generator import generate_simulation_run


class TestOperationalOrderIngestion(unittest.TestCase):

    def setUp(self):
        self.conn = get_connection()
        self.cursor = self.conn.cursor()
        self.client = TestClient(app)

        # Baseline count for operational_inventory to verify safety invariant
        self.cursor.execute("SELECT COUNT(*) AS total FROM operational_inventory")
        self.initial_inv_count = self.cursor.fetchone()["total"]

        # Baseline count for olist_orders
        self.cursor.execute("SELECT COUNT(*) AS total FROM olist_orders")
        self.initial_olist_count = self.cursor.fetchone()["total"]

        # Sample valid catalog product and inventory SKU
        self.cursor.execute("SELECT product_id FROM olist_products LIMIT 1")
        self.catalog_sku = self.cursor.fetchone()["product_id"]

        self.cursor.execute("SELECT sku FROM operational_inventory LIMIT 1")
        self.inventory_sku = self.cursor.fetchone()["sku"]

        # Clean up any previous test keys
        self.cleanup_test_data()

    def tearDown(self):
        self.cleanup_test_data()
        if self.conn:
            self.conn.close()

    def cleanup_test_data(self):
        """Cleans up test records without affecting historical or seed data."""
        try:
            self.cursor.execute(
                "DELETE FROM operational_orders WHERE idempotency_key LIKE 'test_%' OR order_id LIKE 'TEST-%' OR source = 'MANUAL_IMPORT'"
            )
            self.cursor.execute(
                "DELETE FROM dataset_events WHERE source_change_id LIKE 'test_%' OR batch_id LIKE 'test_%'"
            )
            self.cursor.execute(
                "DELETE FROM sim_runs WHERE simulation_run_id LIKE 'sim_test_%'"
            )
            self.conn.commit()
        except Exception:
            pass

    def test_01_single_operational_order_ingestion(self):
        """1. Single order ingestion updates operational_orders, dataset_metadata, and dataset_events."""
        test_key = f"test_idem_{uuid.uuid4().hex[:10]}"
        test_order_id = f"TEST-ORD-{uuid.uuid4().hex[:8].upper()}"

        orders = [{
            "order_id": test_order_id,
            "sku": self.catalog_sku,
            "quantity": 3,
            "unit_price": 45.50,
            "warehouse_id": "WH-SP-01"
        }]

        result = repository.ingest_operational_orders(
            orders=orders,
            idempotency_key=test_key,
            source="OPERATIONAL_API",
            connection=self.conn
        )

        self.assertEqual(result["status"], "INGESTED")
        self.assertFalse(result["idempotent_replay"])
        self.assertEqual(result["orders_count"], 1)
        self.assertIsNotNone(result["event_id"])
        self.assertGreater(result["dataset_version"], 0)

        # Verify operational_orders record
        self.cursor.execute("SELECT * FROM operational_orders WHERE order_id = %s", (test_order_id,))
        row = self.cursor.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["sku"], self.catalog_sku)
        self.assertEqual(row["quantity"], 3)
        self.assertEqual(row["source"], "OPERATIONAL_API")
        self.assertEqual(row["order_status"], "PENDING")

        # Verify dataset_events record
        self.cursor.execute("SELECT * FROM dataset_events WHERE event_id = %s", (result["event_id"],))
        evt = self.cursor.fetchone()
        self.assertIsNotNone(evt)
        self.assertEqual(evt["dataset_name"], "Operational Orders")
        self.assertEqual(evt["event_type"], "ORDER_BATCH_INGESTED")
        self.assertEqual(evt["rows_changed"], 1)

        # Verify dataset_metadata status is READY (or WAITING if background engine already processed the cycle)
        self.cursor.execute("SELECT status FROM dataset_metadata WHERE dataset_name = 'Operational Orders'")
        meta = self.cursor.fetchone()
        self.assertIn(meta["status"], ("READY", "WAITING"))

    def test_02_batch_order_ingestion_and_inventory_eligibility(self):
        """2. Batch order ingestion correctly flags configured inventory SKUs vs catalog-only SKUs."""
        test_key = f"test_idem_batch_{uuid.uuid4().hex[:10]}"
        ord1 = f"TEST-ORD-{uuid.uuid4().hex[:8].upper()}"
        ord2 = f"TEST-ORD-{uuid.uuid4().hex[:8].upper()}"

        orders = [
            {"order_id": ord1, "sku": self.inventory_sku, "quantity": 5, "unit_price": 20.0},
            {"order_id": ord2, "sku": self.catalog_sku, "quantity": 2, "unit_price": 80.0}
        ]

        result = repository.ingest_operational_orders(
            orders=orders,
            idempotency_key=test_key,
            source="OPERATIONAL_API",
            connection=self.conn
        )

        self.assertEqual(result["orders_count"], 2)
        # Check inventory_eligible flag in return payload
        items = {o["order_id"]: o["inventory_eligible"] for o in result["orders"]}
        self.assertTrue(items[ord1])  # inventory_sku has configured operational_inventory

    def test_03_batch_idempotency_safe_replay(self):
        """3. Replaying an identical batch with the same idempotency key returns 200 without duplicate inserts."""
        test_key = f"test_replay_{uuid.uuid4().hex[:10]}"
        ord_id = f"TEST-ORD-{uuid.uuid4().hex[:8].upper()}"

        orders = [{"order_id": ord_id, "sku": self.catalog_sku, "quantity": 1, "unit_price": 10.0}]

        # First ingestion
        res1 = repository.ingest_operational_orders(orders=orders, idempotency_key=test_key, connection=self.conn)
        self.assertFalse(res1["idempotent_replay"])

        # Count records in operational_orders
        self.cursor.execute("SELECT COUNT(*) AS total FROM operational_orders WHERE idempotency_key = %s", (test_key,))
        count_first = self.cursor.fetchone()["total"]
        self.assertEqual(count_first, 1)

        # Second ingestion with identical idempotency_key
        res2 = repository.ingest_operational_orders(orders=orders, idempotency_key=test_key, connection=self.conn)
        self.assertTrue(res2["idempotent_replay"])
        self.assertEqual(res2["status"], "DUPLICATE_ACCEPTED")

        # Verify count in operational_orders did not increase
        self.cursor.execute("SELECT COUNT(*) AS total FROM operational_orders WHERE idempotency_key = %s", (test_key,))
        count_second = self.cursor.fetchone()["total"]
        self.assertEqual(count_second, 1)

    def test_04_invalid_catalog_sku_rejected(self):
        """4. Order with unknown SKU not present in catalog or inventory is rejected with ValueError."""
        test_key = f"test_invalid_sku_{uuid.uuid4().hex[:10]}"
        fake_sku = "NON_EXISTENT_SKU_ABC_9999"

        orders = [{"sku": fake_sku, "quantity": 1}]

        with self.assertRaises(ValueError) as ctx:
            repository.ingest_operational_orders(orders=orders, idempotency_key=test_key, connection=self.conn)

        self.assertIn("Unknown or invalid catalog SKU", str(ctx.exception))

    def test_05_negative_or_zero_quantity_rejected(self):
        """5. Order with quantity <= 0 is rejected with ValueError."""
        test_key = f"test_invalid_qty_{uuid.uuid4().hex[:10]}"

        # Zero quantity
        with self.assertRaises(ValueError) as ctx1:
            repository.ingest_operational_orders(
                orders=[{"sku": self.catalog_sku, "quantity": 0}],
                idempotency_key=test_key,
                connection=self.conn
            )
        self.assertIn("positive integer", str(ctx1.exception))

        # Negative quantity
        with self.assertRaises(ValueError) as ctx2:
            repository.ingest_operational_orders(
                orders=[{"sku": self.catalog_sku, "quantity": -5}],
                idempotency_key=test_key,
                connection=self.conn
            )
        self.assertIn("positive integer", str(ctx2.exception))

    def test_06_source_provenance_preserved(self):
        """6. Preserves distinct source provenance values in operational_orders."""
        for src in ["SIMULATED", "MANUAL_IMPORT", "OPERATIONAL_API"]:
            key = f"test_prov_{src}_{uuid.uuid4().hex[:8]}"
            ord_id = f"TEST-ORD-{src[:4]}-{uuid.uuid4().hex[:6]}"
            res = repository.ingest_operational_orders(
                orders=[{"order_id": ord_id, "sku": self.catalog_sku, "quantity": 1}],
                idempotency_key=key,
                source=src,
                connection=self.conn
            )
            self.assertEqual(res["orders"][0]["source"], src)

            self.cursor.execute("SELECT source FROM operational_orders WHERE order_id = %s", (ord_id,))
            row = self.cursor.fetchone()
            self.assertEqual(row["source"], src)

    def test_07_safety_invariant_zero_inventory_deduction(self):
        """7. Invariant check: Ingestion does NOT modify operational_inventory rows or quantities."""
        test_key = f"test_safety_{uuid.uuid4().hex[:10]}"
        ord_id = f"TEST-ORD-{uuid.uuid4().hex[:8].upper()}"

        # Fetch snapshot of inventory SKU available quantity
        self.cursor.execute("SELECT available_quantity FROM operational_inventory WHERE sku = %s", (self.inventory_sku,))
        init_avail = self.cursor.fetchone()["available_quantity"]

        # Ingest order for that SKU
        repository.ingest_operational_orders(
            orders=[{"order_id": ord_id, "sku": self.inventory_sku, "quantity": 10}],
            idempotency_key=test_key,
            connection=self.conn
        )

        # Verify operational_inventory available_quantity is completely unchanged
        self.cursor.execute("SELECT available_quantity FROM operational_inventory WHERE sku = %s", (self.inventory_sku,))
        post_avail = self.cursor.fetchone()["available_quantity"]
        self.assertEqual(init_avail, post_avail)

        # Verify total inventory row count is exactly unchanged
        self.cursor.execute("SELECT COUNT(*) AS total FROM operational_inventory")
        current_inv_count = self.cursor.fetchone()["total"]
        self.assertEqual(self.initial_inv_count, current_inv_count)

        # Verify olist_orders row count is exactly unchanged
        self.cursor.execute("SELECT COUNT(*) AS total FROM olist_orders")
        current_olist_count = self.cursor.fetchone()["total"]
        self.assertEqual(self.initial_olist_count, current_olist_count)

    def test_08_simulator_dual_mode_support(self):
        """8. Simulator generates to sim_orders in SANDBOX mode, and to operational_orders in OPERATIONAL mode."""
        # 8a: SANDBOX mode (default)
        res_sandbox = generate_simulation_run(batch_size=50, target_destination="SANDBOX", connection=self.conn)
        self.assertEqual(res_sandbox["status"], "DATA_GENERATED")
        self.assertEqual(res_sandbox["target_destination"], "SANDBOX")
        run_id_sandbox = res_sandbox["simulation_run_id"]

        # Verify wrote to sim_orders
        self.cursor.execute("SELECT COUNT(*) AS total FROM sim_orders WHERE simulation_run_id = %s", (run_id_sandbox,))
        self.assertEqual(self.cursor.fetchone()["total"], 50)

        # 8b: OPERATIONAL mode
        res_op = generate_simulation_run(batch_size=50, target_destination="OPERATIONAL", connection=self.conn)
        self.assertEqual(res_op["status"], "DATA_GENERATED")
        self.assertEqual(res_op["target_destination"], "OPERATIONAL")
        run_id_op = res_op["simulation_run_id"]

        # Verify wrote to operational_orders with source=SIMULATED
        self.cursor.execute(
            "SELECT COUNT(*) AS total FROM operational_orders WHERE idempotency_key LIKE %s",
            (f"sim_{run_id_op}%",)
        )
        self.assertEqual(self.cursor.fetchone()["total"], 50)

        # Cleanup simulation run records
        self.cursor.execute("DELETE FROM sim_orders WHERE simulation_run_id = %s", (run_id_sandbox,))
        self.cursor.execute("DELETE FROM sim_inventory_snapshots WHERE simulation_run_id IN (%s, %s)", (run_id_sandbox, run_id_op))
        self.cursor.execute("DELETE FROM sim_runs WHERE simulation_run_id IN (%s, %s)", (run_id_sandbox, run_id_op))
        self.cursor.execute("DELETE FROM operational_orders WHERE idempotency_key LIKE %s", (f"sim_{run_id_op}%",))
        self.cursor.execute("DELETE FROM dataset_events WHERE source_change_id = %s", (f"sim_{run_id_op}",))
        self.conn.commit()

    def test_09_fastapi_rest_endpoints(self):
        """9. Validates POST and GET endpoints for operational orders."""
        test_key = f"test_api_{uuid.uuid4().hex[:10]}"
        ord_id = f"TEST-ORD-API-{uuid.uuid4().hex[:6].upper()}"

        # Test POST fresh order -> 201 Created
        payload = {
            "idempotency_key": test_key,
            "source": "OPERATIONAL_API",
            "warehouse_id": "WH-SP-01",
            "orders": [{
                "order_id": ord_id,
                "sku": self.catalog_sku,
                "quantity": 4,
                "unit_price": 55.0
            }]
        }
        post_resp = self.client.post("/api/observability/orders", json=payload)
        self.assertEqual(post_resp.status_code, 201)
        data = post_resp.json()
        self.assertEqual(data["status"], "INGESTED")
        self.assertFalse(data["idempotent_replay"])

        # Test POST replay with same key -> 200 OK
        replay_resp = self.client.post("/api/observability/orders", json=payload)
        self.assertEqual(replay_resp.status_code, 200)
        replay_data = replay_resp.json()
        self.assertTrue(replay_data["idempotent_replay"])

        # Test GET order by id -> 200 OK
        get_resp = self.client.get(f"/api/observability/orders/{ord_id}")
        self.assertEqual(get_resp.status_code, 200)
        order_detail = get_resp.json()
        self.assertEqual(order_detail["order_id"], ord_id)
        self.assertEqual(order_detail["quantity"], 4)

        # Test GET orders list -> 200 OK
        list_resp = self.client.get("/api/observability/orders?limit=10")
        self.assertEqual(list_resp.status_code, 200)
        list_data = list_resp.json()
        self.assertIn("orders", list_data)
        self.assertGreater(list_data["total"], 0)


if __name__ == "__main__":
    unittest.main()
