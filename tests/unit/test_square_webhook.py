import base64
import hashlib
import hmac
import json
import os
import sys
import unittest
from unittest.mock import patch, MagicMock

from fastapi.testclient import TestClient

# Ensure repository root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

import importlib
main_module = importlib.import_module('services.realtime-api.main')
square_adapter = importlib.import_module('services.realtime-api.square_adapter')

app = main_module.app


def compute_square_signature(raw_body: bytes, signature_key: str, notification_url: str) -> str:
    data = notification_url + raw_body.decode('utf-8')
    computed_hash = hmac.new(
        signature_key.encode('utf-8'),
        data.encode('utf-8'),
        hashlib.sha256
    ).digest()
    return base64.b64encode(computed_hash).decode('utf-8')


class TestSquareWebhook(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        self.test_key = "test_signature_key_12345"
        self.sample_payload = {
            "merchant_id": "ML1234567",
            "type": "inventory.count.updated",
            "event_id": "evt_test_9999",
            "created_at": "2026-09-24T12:00:00Z",
            "data": {
                "type": "inventory_count",
                "id": "cnt_12345",
                "object": {
                    "inventory_counts": [
                        {
                            "catalog_object_id": "OBJ_PRODUCT_1",
                            "catalog_object_type": "ITEM_VARIATION",
                            "state": "IN_STOCK",
                            "location_id": "LM2QZQNXN9P7F",
                            "quantity": "50",
                            "calculated_at": "2026-09-24T12:00:00Z"
                        }
                    ]
                }
            }
        }

    def test_health_check(self):
        """1. Test GET /health returns 200 OK with expected fields."""
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "ok")
        self.assertIn("service", data)

    @patch("config.settings.SQUARE_WEBHOOK_SIGNATURE_KEY", "test_key_secret")
    def test_missing_webhook_signature(self):
        """2. Test missing signature header when key is configured raises 403."""
        response = self.client.post(
            "/webhooks/square",
            json=self.sample_payload
        )
        self.assertEqual(response.status_code, 403)
        self.assertIn("Invalid Square Webhook Signature", response.json()["detail"])

    @patch("config.settings.SQUARE_WEBHOOK_SIGNATURE_KEY", "test_key_secret")
    def test_invalid_webhook_signature(self):
        """3. Test invalid signature header raises 403."""
        headers = {"x-square-hmacsha256-signature": "invalid_signature_hash=="}
        response = self.client.post(
            "/webhooks/square",
            json=self.sample_payload,
            headers=headers
        )
        self.assertEqual(response.status_code, 403)

    @patch("config.settings.SQUARE_WEBHOOK_SIGNATURE_KEY", "test_key_secret")
    def test_valid_webhook_signature(self):
        """4. Test valid webhook signature is accepted and calls persist_and_publish_event."""
        with patch.object(main_module, "persist_and_publish_event") as mock_persist:
            mock_persist.return_value = {
                "status": "PROCESSED",
                "event_id": 101,
                "dataset_name": "Inventory",
                "dataset_version": 1,
                "published_to_kafka": True
            }
            
            raw_bytes = json.dumps(self.sample_payload).encode('utf-8')
            notification_url = "http://testserver/webhooks/square"
            valid_sig = compute_square_signature(raw_bytes, "test_key_secret", notification_url)

            headers = {
                "x-square-hmacsha256-signature": valid_sig,
                "content-type": "application/json"
            }

            response = self.client.post(
                "/webhooks/square",
                content=raw_bytes,
                headers=headers
            )

            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["status"], "SUCCESS")
            self.assertTrue(mock_persist.called)


    @patch("config.settings.SQUARE_WEBHOOK_SIGNATURE_KEY", None)
    def test_malformed_json_payload(self):
        """5. Test malformed JSON returns 400 Bad Request."""
        headers = {"content-type": "application/json"}
        response = self.client.post(
            "/webhooks/square",
            content=b"{ invalid json syntax ",
            headers=headers
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("Malformed JSON payload", response.json()["detail"])

    def test_normalization_inventory_count_updated(self):
        """6. Test normalization adapter for inventory.count.updated."""
        normalized = square_adapter.normalize_square_event(self.sample_payload)
        self.assertTrue(normalized["supported"])
        self.assertEqual(normalized["dataset_name"], "Inventory")
        self.assertEqual(normalized["event_type"], "DATA_UPDATED")
        self.assertEqual(normalized["external_event_id"], "evt_test_9999")
        self.assertEqual(normalized["rows_changed"], 1)

    @patch("config.settings.SQUARE_WEBHOOK_SIGNATURE_KEY", None)
    def test_unsupported_event_type(self):
        """10. Test unsupported event type returns HTTP 200 IGNORED."""
        with patch.object(main_module, "persist_and_publish_event") as mock_persist:
            unsupported_payload = {
                "merchant_id": "ML1234567",
                "type": "order.updated",
                "event_id": "evt_order_123"
            }
            response = self.client.post(
                "/webhooks/square",
                json=unsupported_payload
            )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["status"], "IGNORED")
            self.assertFalse(mock_persist.called)


    def test_signature_verification_utility(self):
        """Direct verification of signature helper function."""
        raw_body = b'{"test":"data"}'
        key = "secret_key_999"
        url = "http://localhost:8000/webhooks/square"
        sig = compute_square_signature(raw_body, key, url)

        self.assertTrue(square_adapter.verify_square_signature(raw_body, sig, key, url))
        self.assertFalse(square_adapter.verify_square_signature(raw_body, "bad_sig", key, url))
        self.assertFalse(square_adapter.verify_square_signature(raw_body, sig, "wrong_key", url))


if __name__ == "__main__":
    unittest.main()
