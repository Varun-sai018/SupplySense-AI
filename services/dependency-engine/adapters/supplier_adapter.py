"""
SupplySense AI - Supplier API Action Adapter (Rule B)

Handles inventory reorder placement and supplier availability status queries
with timeout protection, idempotency enforcement, error classification,
and mock support for offline unit testing.
"""

import os
import sys
import logging
from typing import Dict, Any, Optional
import requests

logger = logging.getLogger(__name__)


class SupplierAPIAdapter:
    """
    Adapter for communicating with the Supplier REST API.
    Distinguishes eligible, dispatched, confirmed, rejected, and failed reorder outcomes.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout_seconds: float = 3.0,
        api_key: Optional[str] = None,
        mock_handler: Optional[Any] = None
    ):
        self.base_url = base_url or os.getenv("SUPPLIER_API_BASE_URL", "")
        self.timeout_seconds = timeout_seconds
        self.api_key = api_key or os.getenv("SUPPLIER_API_KEY", "")
        self.mock_handler = mock_handler

    def get_supplier_status(self, supplier_id: str) -> str:
        """
        Queries current status of the supplier.
        Returns 'Online', 'Offline', or 'Degraded'.
        """
        if self.mock_handler and hasattr(self.mock_handler, "get_supplier_status"):
            return self.mock_handler.get_supplier_status(supplier_id)

        if not self.base_url:
            # Simulation default
            return "Online"

        try:
            url = f"{self.base_url.rstrip('/')}/suppliers/{supplier_id}/status"
            headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}
            resp = requests.get(url, headers=headers, timeout=self.timeout_seconds)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("status", "Offline")
            return "Offline"
        except Exception as e:
            logger.warning(f"Error checking supplier {supplier_id} status: {e}")
            return "Offline"

    def place_reorder(
        self,
        product_id: str,
        quantity: int,
        idempotency_key: str,
        supplier_id: str = "default_supplier"
    ) -> Dict[str, Any]:
        """
        Dispatches reorder request to supplier.
        Returns a structured outcome dict with status:
          - 'CONFIRMED': Order accepted and confirmed by supplier with order_id.
          - 'REJECTED': Supplier explicitly rejected the reorder (non-retryable).
          - 'FAILED': Network timeout or server failure.
        """
        if self.mock_handler and hasattr(self.mock_handler, "place_reorder"):
            return self.mock_handler.place_reorder(product_id, quantity, idempotency_key, supplier_id)

        if not self.base_url:
            # Deterministic simulation mode when no external API is configured
            logger.info(f"[SupplierAdapter MOCK] Simulated order placed for product '{product_id}', qty {quantity}")
            return {
                "status": "CONFIRMED",
                "supplier_order_id": f"ORD-SUP-{abs(hash(idempotency_key)) % 1000000:06d}",
                "product_id": product_id,
                "quantity": quantity,
                "supplier_id": supplier_id,
                "idempotency_key": idempotency_key,
                "message": "Order successfully confirmed by supplier simulation"
            }

        url = f"{self.base_url.rstrip('/')}/reorders"
        headers = {
            "Content-Type": "application/json",
            "X-Idempotency-Key": idempotency_key
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        payload = {
            "product_id": product_id,
            "quantity": quantity,
            "supplier_id": supplier_id,
            "idempotency_key": idempotency_key
        }

        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=self.timeout_seconds)

            if resp.status_code in (200, 201):
                data = resp.json()
                return {
                    "status": "CONFIRMED",
                    "supplier_order_id": data.get("order_id") or data.get("supplier_order_id"),
                    "product_id": product_id,
                    "quantity": quantity,
                    "supplier_id": supplier_id,
                    "idempotency_key": idempotency_key,
                    "raw_response": data
                }

            if resp.status_code in (400, 422):
                err_msg = f"Reorder rejected by supplier (HTTP {resp.status_code}): {resp.text[:300]}"
                logger.error(err_msg)
                return {
                    "status": "REJECTED",
                    "error": err_msg,
                    "http_status": resp.status_code,
                    "retryable": False
                }

            err_msg = f"Supplier API returned unexpected error HTTP {resp.status_code}: {resp.text[:300]}"
            logger.error(err_msg)
            return {
                "status": "FAILED",
                "error": err_msg,
                "http_status": resp.status_code,
                "retryable": True
            }

        except requests.Timeout as t_err:
            err_msg = f"Supplier API timeout after {self.timeout_seconds}s: {t_err}"
            logger.error(err_msg)
            return {
                "status": "FAILED",
                "error": err_msg,
                "error_type": "TIMEOUT",
                "retryable": True
            }
        except Exception as exc:
            err_msg = f"Supplier API connection failure: {exc}"
            logger.error(err_msg)
            return {
                "status": "FAILED",
                "error": err_msg,
                "error_type": "CONNECTION_ERROR",
                "retryable": True
            }
