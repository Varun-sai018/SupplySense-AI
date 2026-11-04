"""
SupplySense AI - Unit Tests for Live State Synchronization & SSE (Module 3)

Verifies:
  1. DashboardEvent versioned envelope validation and schema constraints.
  2. Text/event-stream formatting with IDs, event types, and JSON payloads.
  3. EventHub subscription tracking and disconnect cleanup.
  4. Memory-bounded subscriber queues and slow client overflow protection.
  5. Last-Event-ID replay from in-memory ring buffer.
  6. Expired or invalid Last-Event-ID REST_SNAPSHOT_REQUIRED fallback.
  7. Heartbeat keep-alive comments.
  8. Safe publisher helper integration with rule engine and pipeline runner.
  9. FastAPI SSE stream endpoint headers and replay behavior.
"""

import os
import sys
import json
import unittest
import asyncio
from unittest.mock import MagicMock, patch

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import services
from services.observability.event_hub import (
    DashboardEvent,
    EventHub,
    SCHEMA_VERSION,
    hub
)
from services.observability.publisher import publish_dashboard_event


class TestLiveStateSync(unittest.TestCase):
    """Unit test suite for EventHub, SSE formatting, and event publishing."""

    def setUp(self):
        self.test_hub = EventHub(buffer_size=10)

    def test_event_envelope_validation_success(self):
        ev = DashboardEvent(
            event_id="evt_test_001",
            event_type="RULE_DECISION_EVALUATED",
            entity="RULE_A_CART_ABANDONMENT",
            severity="WARNING",
            data={"dropoff_rate": 0.35, "decision": "TRIGGER"}
        )
        ev.validate()
        self.assertEqual(ev.schema_version, "1.0")
        self.assertEqual(ev.severity, "WARNING")

    def test_event_envelope_rejects_empty_id(self):
        ev = DashboardEvent(
            event_id="",
            event_type="RULE_DECISION_EVALUATED",
            entity="RULE_A"
        )
        with self.assertRaises(ValueError):
            ev.validate()

    def test_event_envelope_rejects_invalid_type(self):
        ev = DashboardEvent(
            event_id="evt_002",
            event_type="UNSUPPORTED_TYPE_XYZ",
            entity="RULE_A"
        )
        with self.assertRaises(ValueError):
            ev.validate()

    def test_event_envelope_rejects_invalid_severity(self):
        ev = DashboardEvent(
            event_id="evt_003",
            event_type="RULE_DECISION_EVALUATED",
            entity="RULE_A",
            severity="SUPER_CRITICAL"
        )
        with self.assertRaises(ValueError):
            ev.validate()

    def test_sse_message_formatting(self):
        ev = DashboardEvent(
            event_id="evt_test_100",
            event_type="PAYMENT_GATEWAY_ALERT_TRIGGERED",
            entity="PAYMENT_GATEWAY",
            severity="CRITICAL",
            data={"success_rate": 0.88}
        )
        msg = ev.to_sse_format()
        self.assertTrue(msg.startswith("id: evt_test_100\n"))
        self.assertIn("event: PAYMENT_GATEWAY_ALERT_TRIGGERED\n", msg)
        self.assertIn('"success_rate": 0.88', msg)
        self.assertTrue(msg.endswith("\n\n"))

    def test_event_hub_subscribe_and_unsubscribe(self):
        self.assertEqual(self.test_hub.subscriber_count(), 0)
        q = self.test_hub.subscribe()
        self.assertEqual(self.test_hub.subscriber_count(), 1)
        self.test_hub.unsubscribe(q)
        self.assertEqual(self.test_hub.subscriber_count(), 0)

    def test_event_hub_broadcast_to_multiple_subscribers(self):
        q1 = self.test_hub.subscribe()
        q2 = self.test_hub.subscribe()

        ev = DashboardEvent(
            event_id="evt_bcast_01",
            event_type="INVENTORY_REORDER_STATE_CHANGED",
            entity="INVENTORY",
            severity="INFO",
            data={"product_id": "P1", "quantity": 50}
        )
        self.test_hub.publish(ev)

        self.assertFalse(q1.empty())
        self.assertFalse(q2.empty())

        r1 = q1.get_nowait()
        r2 = q2.get_nowait()
        self.assertEqual(r1.event_id, "evt_bcast_01")
        self.assertEqual(r2.event_id, "evt_bcast_01")

        self.test_hub.unsubscribe(q1)
        self.test_hub.unsubscribe(q2)

    def test_slow_client_bounded_queue_overflow_protection(self):
        # Create a small queue maxsize=2
        q = self.test_hub.subscribe(maxsize=2)

        ev1 = DashboardEvent(event_id="e1", event_type="HEARTBEAT", entity="SYS", data={})
        ev2 = DashboardEvent(event_id="e2", event_type="HEARTBEAT", entity="SYS", data={})
        ev3 = DashboardEvent(event_id="e3", event_type="HEARTBEAT", entity="SYS", data={})

        self.test_hub.publish(ev1)
        self.test_hub.publish(ev2)
        # 3rd event should drop oldest (ev1) without blocking or error
        self.test_hub.publish(ev3)

        self.assertEqual(q.qsize(), 2)
        first = q.get_nowait()
        second = q.get_nowait()
        self.assertEqual(first.event_id, "e2")
        self.assertEqual(second.event_id, "e3")

        self.test_hub.unsubscribe(q)

    def test_replay_with_valid_last_event_id(self):
        ev1 = DashboardEvent(event_id="e_seq_1", event_type="HEARTBEAT", entity="SYS", data={})
        ev2 = DashboardEvent(event_id="e_seq_2", event_type="RULE_DECISION_EVALUATED", entity="R1", data={})
        ev3 = DashboardEvent(event_id="e_seq_3", event_type="PIPELINE_STATUS_CHANGED", entity="P1", data={})

        self.test_hub.publish(ev1)
        self.test_hub.publish(ev2)
        self.test_hub.publish(ev3)

        found, replay = self.test_hub.get_replay_events("e_seq_1")
        self.assertTrue(found)
        self.assertEqual(len(replay), 2)
        self.assertEqual(replay[0].event_id, "e_seq_2")
        self.assertEqual(replay[1].event_id, "e_seq_3")

    def test_replay_with_expired_or_invalid_id_returns_false(self):
        ev1 = DashboardEvent(event_id="e_seq_10", event_type="HEARTBEAT", entity="SYS", data={})
        self.test_hub.publish(ev1)

        found, replay = self.test_hub.get_replay_events("non_existent_id_999")
        self.assertFalse(found)
        self.assertEqual(replay, [])

    def test_snapshot_required_event_formatting(self):
        snap_msg = self.test_hub.format_snapshot_required()
        self.assertIn("event: REST_SNAPSHOT_REQUIRED\n", snap_msg)
        self.assertIn('"action": "FETCH_REST_SNAPSHOT"', snap_msg)
        self.assertIn("/api/observability/summary", snap_msg)

    def test_heartbeat_formatting(self):
        hb = self.test_hub.format_heartbeat()
        self.assertTrue(hb.startswith(": keep-alive"))
        self.assertTrue(hb.endswith("\n\n"))

    def test_publisher_helper_safely_emits_event(self):
        # Test helper using global hub
        q = hub.subscribe()
        try:
            ev = publish_dashboard_event(
                event_type="PIPELINE_STATUS_CHANGED",
                entity="PIPELINE",
                severity="INFO",
                execution_id=12345,
                data={"pipeline_name": "Test Pipeline", "status": "COMPLETED"}
            )
            self.assertIsNotNone(ev)
            self.assertEqual(ev.execution_id, 12345)
            self.assertEqual(ev.event_type, "PIPELINE_STATUS_CHANGED")

            # Check that subscriber received it
            received = q.get_nowait()
            self.assertEqual(received.event_id, ev.event_id)
        finally:
            hub.unsubscribe(q)


class TestSSEFastAPIEndpoint(unittest.IsolatedAsyncioTestCase):
    """Async unit tests verifying FastAPI SSE endpoint stream behavior and headers."""

    async def test_sse_endpoint_stream_and_replay(self):
        from services.observability.main import stream_events

        # Publish a test event into the global hub
        test_ev = hub.publish_event(
            event_type="PAYMENT_GATEWAY_ALERT_TRIGGERED",
            entity="PAYMENT_GATEWAY",
            severity="CRITICAL",
            data={"success_rate": 0.85}
        )

        # Mock request with disconnect check returning False then True
        mock_request = MagicMock()
        mock_request.is_disconnected.side_effect = [False, True]

        # Call endpoint with Last-Event-ID of a prior ID
        # First publish dummy prior ID
        prior_ev = hub.publish_event(
            event_type="HEARTBEAT",
            entity="SYS",
            data={}
        )
        new_ev = hub.publish_event(
            event_type="RULE_DECISION_EVALUATED",
            entity="RULE_A",
            data={"decision": "TRIGGER"}
        )

        response = await stream_events(
            request=mock_request,
            last_event_id=prior_ev.event_id
        )

        self.assertEqual(response.media_type, "text/event-stream")
        self.assertEqual(response.headers.get("Cache-Control"), "no-cache, no-transform")
        self.assertEqual(response.headers.get("Connection"), "keep-alive")

        # Iterate generator
        chunks = []
        async for chunk in response.body_iterator:
            chunks.append(chunk)

        # Verified that replayed new_ev was yielded
        combined = "".join(chunks)
        self.assertIn(new_ev.event_id, combined)
        self.assertIn("RULE_DECISION_EVALUATED", combined)


if __name__ == "__main__":
    unittest.main()
