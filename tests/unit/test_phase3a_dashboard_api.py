"""
Unit and integration tests for SupplySense AI Phase 3A: Real-Time Operational Dashboard API.

Verifies:
  1. GET /api/observability/inventory (listing, search, low-stock filter, warehouse filter)
  2. GET /api/observability/suppliers (listing, status filter, catalog nesting)
  3. GET /api/observability/replenishments (listing, status filter, sku filter, execution traceability)
  4. GET /api/observability/operational-summary (aggregations, health status, SKU counts)
  5. GET /api/observability/events/recent (event feed, limit, event_type filter)
  6. Repository error handling & fallback responses
"""

import os
import sys
import unittest
from unittest.mock import patch
from fastapi.testclient import TestClient

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from services.observability.main import app


class TestPhase3ADashboardAPI(unittest.TestCase):
    """Test suite for Phase 3A Operational Dashboard API endpoints."""

    def setUp(self):
        self.client = TestClient(app)

    # 1. Operational Inventory Endpoint Tests
    def test_get_inventory_success(self):
        """Test GET /api/observability/inventory returns items and pagination metadata."""
        response = self.client.get("/api/observability/inventory?limit=10")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("items", data)
        self.assertIn("total_count", data)
        self.assertIn("limit", data)
        self.assertIn("offset", data)
        self.assertIsInstance(data["items"], list)
        if data["items"]:
            item = data["items"][0]
            self.assertIn("sku", item)
            self.assertIn("warehouse_id", item)
            self.assertIn("on_hand_quantity", item)
            self.assertIn("reserved_quantity", item)
            self.assertIn("available_quantity", item)
            self.assertIn("reorder_threshold", item)
            self.assertIn("reorder_target_quantity", item)
            self.assertIn("safety_stock_buffer", item)
            self.assertIn("version", item)
            self.assertIn("is_low_stock", item)

    def test_get_inventory_low_stock_filter(self):
        """Test GET /api/observability/inventory with low_stock_only=true."""
        response = self.client.get("/api/observability/inventory?low_stock_only=true")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        for item in data["items"]:
            self.assertTrue(item["is_low_stock"])
            self.assertLess(item["available_quantity"], item["reorder_threshold"])

    def test_get_inventory_warehouse_filter(self):
        """Test GET /api/observability/inventory filtering by warehouse_id."""
        response = self.client.get("/api/observability/inventory?warehouse_id=WH-SP-01")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        for item in data["items"]:
            self.assertEqual(item["warehouse_id"], "WH-SP-01")

    def test_get_inventory_search(self):
        """Test GET /api/observability/inventory with search string."""
        response = self.client.get("/api/observability/inventory?search=SKU")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data["items"], list)

    # 2. Operational Suppliers Endpoint Tests
    def test_get_suppliers_success(self):
        """Test GET /api/observability/suppliers returns suppliers and catalog availability."""
        response = self.client.get("/api/observability/suppliers")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("items", data)
        self.assertIn("total_count", data)
        if data["items"]:
            supp = data["items"][0]
            self.assertIn("supplier_id", supp)
            self.assertIn("supplier_name", supp)
            self.assertIn("status", supp)
            self.assertIn("is_online", supp)
            self.assertIn("catalog_items", supp)
            self.assertIsInstance(supp["catalog_items"], list)

    def test_get_suppliers_status_filter(self):
        """Test GET /api/observability/suppliers filtering by status."""
        response = self.client.get("/api/observability/suppliers?status=Online")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        for supp in data["items"]:
            self.assertEqual(supp["status"], "Online")

    # 3. Replenishment Orders Endpoint Tests
    def test_get_replenishments_success(self):
        """Test GET /api/observability/replenishments returns orders with lifecycle metadata."""
        response = self.client.get("/api/observability/replenishments?limit=10")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("items", data)
        self.assertIn("total_count", data)
        self.assertIn("active_count", data)
        self.assertIn("delivered_count", data)
        if data["items"]:
            order = data["items"][0]
            self.assertIn("replenishment_id", order)
            self.assertIn("sku", order)
            self.assertIn("warehouse_id", order)
            self.assertIn("supplier_id", order)
            self.assertIn("quantity", order)
            self.assertIn("status", order)
            self.assertIn("action_id", order)
            self.assertIn("decision_id", order)
            self.assertIn("execution_id", order)

    def test_get_replenishments_status_filter(self):
        """Test GET /api/observability/replenishments filtering by status."""
        response = self.client.get("/api/observability/replenishments?status=DELIVERED")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        for order in data["items"]:
            self.assertEqual(order["status"], "DELIVERED")

    # 4. Operational Summary Endpoint Tests
    def test_get_operational_summary_success(self):
        """Test GET /api/observability/operational-summary returns aggregated metrics."""
        response = self.client.get("/api/observability/operational-summary")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("inventory", data)
        self.assertIn("replenishments", data)
        self.assertIn("suppliers", data)
        self.assertIn("data_provenance", data)
        self.assertIn("timestamp", data)

        inv = data["inventory"]
        self.assertIn("total_skus", inv)
        self.assertIn("low_stock_skus", inv)
        self.assertIn("total_on_hand", inv)
        self.assertIn("total_reserved", inv)
        self.assertIn("total_available", inv)

        rep = data["replenishments"]
        self.assertIn("total_replenishments", rep)
        self.assertIn("active_replenishments", rep)
        self.assertIn("delivered_replenishments", rep)

        sup = data["suppliers"]
        self.assertIn("total_suppliers", sup)
        self.assertIn("online_suppliers", sup)
        self.assertIn("offline_suppliers", sup)
        self.assertIn("degraded_suppliers", sup)

    # 5. Recent Events Endpoint Tests
    def test_get_recent_events_success(self):
        """Test GET /api/observability/events/recent returns recent dashboard events."""
        response = self.client.get("/api/observability/events/recent?limit=5")
        self.assertEqual(response.status_code, 200)
        events = response.json()
        self.assertIsInstance(events, list)
        self.assertLessEqual(len(events), 5)
        if events:
            ev = events[0]
            self.assertIn("id", ev)
            self.assertIn("event_id", ev)
            self.assertIn("event_type", ev)
            self.assertIn("data", ev)
            self.assertIn("timestamp", ev)

    def test_get_recent_events_filter_type(self):
        """Test GET /api/observability/events/recent with event_type filter."""
        response = self.client.get("/api/observability/events/recent?event_type=DELIVERY_COMPLETED")
        self.assertEqual(response.status_code, 200)
        events = response.json()
        self.assertIsInstance(events, list)
        for ev in events:
            self.assertEqual(ev["event_type"], "DELIVERY_COMPLETED")

    # 6. Fallback and Error Handling Tests
    @patch("services.observability.repository.get_operational_inventory", side_effect=Exception("DB connection dropped"))
    def test_get_inventory_db_error_fallback(self, mock_repo):
        """Test GET /api/observability/inventory returns 500 when repository fails."""
        response = self.client.get("/api/observability/inventory")
        self.assertEqual(response.status_code, 500)
        self.assertIn("detail", response.json())


if __name__ == "__main__":
    unittest.main()
