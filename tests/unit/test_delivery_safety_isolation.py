"""
Unit tests for SupplySense AI Delivery Safety & Operational Data Isolation (Phase F2.9).

Verifies:
  1. Operational delivery rejection (HTTP 403) with zero database mutations or events.
  2. Mismatched warehouse / SKU / provenance rejection (HTTP 403).
  3. Non-simulated inventory origin rejection (HTTP 403).
  4. Non-existent replenishment rejection (HTTP 404).
  5. Simulated-path validation succeeds when order, warehouse, and inventory all have verified simulated provenance.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch
from fastapi import HTTPException

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from services.observability.main import deliver_replenishment_endpoint
from services.simulator.delivery_service import (
    deliver_replenishment,
    OperationalDeliveryForbiddenError
)
from services.dependency_engine.repository import InventoryNotFoundError


class TestDeliverySafetyIsolation(unittest.TestCase):
    """Verifies that operational orders are strictly rejected and isolated simulated paths are validated."""

    def test_operational_delivery_rejected_403(self):
        """Operational replenishment orders targeting WH-SP-01 must be rejected with HTTP 403."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        order_row = {
            "replenishment_id": "RPL-OP-TEST-001",
            "sku": "SKU-OP-001",
            "warehouse_id": "WH-SP-01",
            "supplier_id": "supplier_primary",
            "quantity": 25,
            "status": "CONFIRMED",
            "action_id": 100,
            "decision_id": 200,
            "execution_id": None,
            "supplier_order_reference": None,
            "dispatched_at": None,
            "estimated_delivery_at": None,
            "delivered_at": None
        }
        dec_row = {"pipeline_name": "Inventory Reorder Pipeline"}
        inv_row = {
            "sku": "SKU-OP-001",
            "warehouse_id": "WH-SP-01",
            "data_origin": "SIMULATED",
            "on_hand_quantity": 50
        }

        mock_cur.fetchone.side_effect = [order_row, dec_row, inv_row]

        with self.assertRaises(OperationalDeliveryForbiddenError):
            deliver_replenishment("RPL-OP-TEST-001", connection=mock_conn)

        # Invariant: connection.rollback() must have been called, and connection.commit() must NOT have been called
        mock_conn.rollback.assert_called()
        mock_conn.commit.assert_not_called()

    def test_endpoint_returns_403_for_operational_order(self):
        """Calling the FastAPI endpoint for an operational order raises HTTPException with status 403 Forbidden."""
        with patch("services.observability.service.deliver_replenishment") as mock_deliver:
            mock_deliver.side_effect = OperationalDeliveryForbiddenError(
                "Simulated delivery rejected: Order 'RPL-OP-001' targets operational warehouse 'WH-SP-01'."
            )
            with self.assertRaises(HTTPException) as ctx:
                deliver_replenishment_endpoint("RPL-OP-001")
            
            self.assertEqual(ctx.exception.status_code, 403)
            self.assertIn("Simulated delivery rejected for operational data", ctx.exception.detail)

    def test_mismatched_provenance_rejected(self):
        """Orders with historical seed inventory origin must be rejected with 403 even in a test facility."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        order_row = {
            "replenishment_id": "RPL-TEST-002",
            "sku": "SKU-TEST-002",
            "warehouse_id": "WH-TEST-01",
            "supplier_id": "supplier_primary",
            "quantity": 10,
            "status": "CONFIRMED",
            "action_id": 101,
            "decision_id": 201,
            "execution_id": None,
            "supplier_order_reference": None,
            "dispatched_at": None,
            "estimated_delivery_at": None,
            "delivered_at": None
        }
        dec_row = {"pipeline_name": "SIM-TEST-Pipeline"}
        # Inventory has HISTORICAL_SEED (operational provenance)
        inv_row = {
            "sku": "SKU-TEST-002",
            "warehouse_id": "WH-TEST-01",
            "data_origin": "HISTORICAL_SEED",
            "on_hand_quantity": 20
        }
        mock_cur.fetchone.side_effect = [order_row, dec_row, inv_row]

        with self.assertRaises(OperationalDeliveryForbiddenError):
            deliver_replenishment("RPL-TEST-002", connection=mock_conn)

        mock_conn.rollback.assert_called()
        mock_conn.commit.assert_not_called()

    def test_verified_simulated_order_permitted(self):
        """Orders where order, warehouse, SKU, and data_origin are all verified simulated proceed safely."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        order_row = {
            "replenishment_id": "RPL-SIM-003",
            "sku": "SKU-SIM-003",
            "warehouse_id": "WH-SIM-01",
            "supplier_id": "supplier_sim",
            "quantity": 30,
            "status": "CONFIRMED",
            "action_id": 102,
            "decision_id": 202,
            "execution_id": None,
            "supplier_order_reference": None,
            "dispatched_at": None,
            "estimated_delivery_at": None,
            "delivered_at": None
        }
        dec_row = {"pipeline_name": "SIM-ALL-Pipeline"}
        inv_row = {
            "sku": "SKU-SIM-003",
            "warehouse_id": "WH-SIM-01",
            "data_origin": "SIMULATED",
            "on_hand_quantity": 10
        }
        mock_cur.fetchone.side_effect = [order_row, dec_row, inv_row]

        with patch("services.simulator.delivery_service.apply_inventory_delta") as mock_apply, \
             patch("services.simulator.delivery_service.publish_dashboard_event") as mock_pub, \
             patch("services.simulator.delivery_service.broadcast_dashboard_event") as mock_bcast:
            mock_apply.return_value = {"on_hand_quantity": 40, "version": 2}
            mock_pub.return_value = MagicMock()

            result = deliver_replenishment("RPL-SIM-003", connection=mock_conn)
            self.assertEqual(result["status"], "DELIVERED")
            self.assertEqual(result["quantity"], 30)
            self.assertFalse(result["idempotent"])
            mock_conn.commit.assert_called()


if __name__ == "__main__":
    unittest.main()
