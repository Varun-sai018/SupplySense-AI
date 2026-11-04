"""
SupplySense AI - Marketing Webhook Action Adapter (Rule A)

Handles dispatching cart-abandonment trigger notifications to the marketing
funnel webhook with bounded retries, exponential backoff, timeout protection,
and mock support for offline unit testing.
"""

import os
import sys
import time
import logging
from typing import Dict, Any, Optional
import requests

logger = logging.getLogger(__name__)


class MarketingWebhookError(Exception):
    """Raised when marketing webhook dispatch fails after retries."""
    pass


class MarketingWebhookAdapter:
    """
    Adapter for marketing funnel webhook dispatch.
    Supports real HTTP requests with timeouts and bounded retries,
    or mocked responses for test isolation.
    """

    def __init__(
        self,
        webhook_url: Optional[str] = None,
        timeout_seconds: float = 3.0,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        mock_handler: Optional[Any] = None
    ):
        self.webhook_url = webhook_url or os.getenv("MARKETING_WEBHOOK_URL", "")
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.mock_handler = mock_handler

    def dispatch(
        self,
        payload: Dict[str, Any],
        idempotency_key: str
    ) -> Dict[str, Any]:
        """
        Dispatches payload to the marketing webhook.
        Returns a dict with dispatch outcome, status code, and response body.
        """
        # If mock handler provided or in mock mode without URL
        if self.mock_handler is not None:
            return self.mock_handler(payload, idempotency_key)

        if not self.webhook_url:
            # Deterministic local simulation mode when no external webhook is configured
            logger.info(f"[MarketingAdapter MOCK] Simulated webhook call for key '{idempotency_key}': {payload}")
            return {
                "status": "CONFIRMED",
                "http_status": 200,
                "response": {"message": "Simulated webhook delivered successfully", "idempotency_key": idempotency_key},
                "attempts": 1,
                "mode": "SIMULATION"
            }

        headers = {
            "Content-Type": "application/json",
            "X-Idempotency-Key": idempotency_key,
            "User-Agent": "SupplySense-RuleEngine/1.0"
        }

        attempts = 0
        last_error = None

        while attempts < self.max_retries:
            attempts += 1
            try:
                logger.info(f"Dispatching marketing webhook (attempt {attempts}/{self.max_retries}) to {self.webhook_url}...")
                resp = requests.post(
                    self.webhook_url,
                    json=payload,
                    headers=headers,
                    timeout=self.timeout_seconds
                )

                if resp.status_code in (200, 201, 202, 204):
                    try:
                        resp_data = resp.json()
                    except Exception:
                        resp_data = {"raw_text": resp.text[:500]}
                    return {
                        "status": "CONFIRMED",
                        "http_status": resp.status_code,
                        "response": resp_data,
                        "attempts": attempts,
                        "mode": "HTTP"
                    }

                # 4xx Client Errors are generally non-retryable
                if 400 <= resp.status_code < 500:
                    err_msg = f"Marketing webhook rejected with HTTP {resp.status_code}: {resp.text[:300]}"
                    logger.error(err_msg)
                    return {
                        "status": "FAILED",
                        "http_status": resp.status_code,
                        "error": err_msg,
                        "attempts": attempts,
                        "retryable": False
                    }

                # 5xx Server Errors are retryable
                err_msg = f"Marketing webhook server error HTTP {resp.status_code}: {resp.text[:300]}"
                last_error = MarketingWebhookError(err_msg)
                logger.warning(f"Attempt {attempts} failed: {err_msg}")

            except (requests.Timeout, requests.ConnectionError) as req_err:
                last_error = req_err
                logger.warning(f"Marketing webhook network error on attempt {attempts}: {req_err}")

            if attempts < self.max_retries:
                sleep_time = self.backoff_factor * (2 ** (attempts - 1))
                time.sleep(sleep_time)

        final_err = f"Marketing webhook failed after {attempts} attempts: {last_error}"
        logger.error(final_err)
        return {
            "status": "FAILED",
            "http_status": 0,
            "error": final_err,
            "attempts": attempts,
            "retryable": True
        }
