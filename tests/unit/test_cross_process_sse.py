"""
Unit and integration tests for Cross-Process SSE Event Synchronization.

Tests:
1. Journaling DashboardEvent to MySQL dashboard_events table.
2. Fetching events created by an external process after given row ID.
3. Cross-process poller forwarding database events into active EventHub.
4. Ring-buffer deduplication preventing double-broadcast.
5. Database-backed replay when Last-Event-ID has fallen out of memory ring buffer.
6. POST /api/observability/events/publish endpoint.
"""

import unittest
import asyncio
import time
import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from services.observability.event_hub import hub, DashboardEvent
from services.observability.publisher import publish_dashboard_event
from services.observability.event_journal import (
    record_event_to_db,
    fetch_events_after_id,
    fetch_replay_events_by_event_id,
    get_latest_event_row_id,
    prune_old_events
)


class TestCrossProcessSSE(unittest.TestCase):

    def setUp(self):
        self.conn = get_connection()
        self.cur = self.conn.cursor()

    def tearDown(self):
        self.cur.close()
        self.conn.close()

    def test_record_and_fetch_event_journal(self):
        """Verifies an event is persisted to MySQL and can be fetched by ID."""
        test_event_id = f"test_ev_{int(time.time()*1000)}"
        event = DashboardEvent(
            event_id=test_event_id,
            event_type="SYSTEM_NOTIFICATION",
            entity="TEST_RUNNER",
            severity="INFO",
            data={"message": "Cross-process test event"},
            correlation_id="corr-test-123"
        )

        row_id = record_event_to_db(event, connection=self.conn)
        self.assertIsNotNone(row_id)
        self.assertGreater(row_id, 0)

        # Fetch events after row_id - 1
        fetched = fetch_events_after_id(after_id=row_id - 1, limit=10, connection=self.conn)
        self.assertTrue(any(ev.event_id == test_event_id for _, ev in fetched))

    def test_idempotent_event_journal_insert(self):
        """Duplicate inserts with same event_id must be ignored gracefully."""
        test_event_id = f"test_idem_{int(time.time()*1000)}"
        event = DashboardEvent(
            event_id=test_event_id,
            event_type="RULE_DECISION_EVALUATED",
            entity="DEPENDENCY_ENGINE",
            data={"rule": "Rule A"}
        )
        row1 = record_event_to_db(event, connection=self.conn)
        row2 = record_event_to_db(event, connection=self.conn)
        self.assertIsNotNone(row1)
        # INSERT IGNORE does not fail on duplicate
        self.cur.execute("SELECT COUNT(*) AS cnt FROM dashboard_events WHERE event_id = %s", (test_event_id,))
        count = self.cur.fetchone()["cnt"]
        self.assertEqual(count, 1)

    def test_database_backed_replay(self):
        """Replay retrieves events after last_event_id directly from MySQL."""
        base_ts = int(time.time() * 1000)
        ev1 = DashboardEvent(
            event_id=f"replay_1_{base_ts}",
            event_type="PIPELINE_STATUS_CHANGED",
            entity="PIPELINE",
            data={"step": 1}
        )
        ev2 = DashboardEvent(
            event_id=f"replay_2_{base_ts}",
            event_type="PIPELINE_STATUS_CHANGED",
            entity="PIPELINE",
            data={"step": 2}
        )
        record_event_to_db(ev1, connection=self.conn)
        record_event_to_db(ev2, connection=self.conn)

        found, replay_list = fetch_replay_events_by_event_id(
            last_event_id=ev1.event_id,
            limit=10,
            connection=self.conn
        )
        self.assertTrue(found)
        self.assertTrue(any(e.event_id == ev2.event_id for e in replay_list))

    def test_publisher_writes_to_hub_and_journal(self):
        """publish_dashboard_event broadcasts to in-process hub and writes to MySQL."""
        queue = hub.subscribe()
        try:
            test_id = f"pub_test_{int(time.time()*1000)}"
            ev = publish_dashboard_event(
                event_type="INVENTORY_REORDER_STATE_CHANGED",
                entity="INVENTORY",
                severity="INFO",
                data={"product": "TEST-SKU", "stock": 5},
                event_id=test_id
            )
            self.assertIsNotNone(ev)
            self.assertEqual(ev.event_id, test_id)

            # In-process queue should have received it
            self.assertFalse(queue.empty())
            received_ev = queue.get_nowait()
            self.assertEqual(received_ev.event_id, test_id)

            # Database should also contain it
            self.cur.execute("SELECT event_id FROM dashboard_events WHERE event_id = %s", (test_id,))
            row = self.cur.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["event_id"], test_id)
        finally:
            hub.unsubscribe(queue)

    def test_ring_buffer_deduplication(self):
        """Publishing the same event_id twice must not queue duplicate to subscriber."""
        queue = hub.subscribe()
        try:
            unique_id = f"dedup_test_{int(time.time()*1000)}"
            ev = DashboardEvent(
                event_id=unique_id,
                event_type="SYSTEM_NOTIFICATION",
                entity="SYSTEM",
                data={"v": 1}
            )
            hub.publish(ev)
            # Second attempt with identical event_id
            hub.publish(ev)

            # Queue must contain exactly 1 event
            self.assertEqual(queue.qsize(), 1)
            queued_item = queue.get_nowait()
            self.assertEqual(queued_item.event_id, unique_id)
            self.assertTrue(queue.empty())
        finally:
            hub.unsubscribe(queue)

    def test_database_to_poller_to_hub_sse_pipeline(self):
        """
        Verifies complete path:
        Database persistence -> Poller fetch -> EventHub publish -> SSE queue delivery.
        Ensures newly generated events with id > last_seen_id are received with exact SSE formatting.
        """
        queue = hub.subscribe()
        test_corr_id = f"TEST-PROBE-E2E-{int(time.time()*1000)}"
        test_ev_id = f"evt_probe_{int(time.time()*1000)}"
        try:
            # 1. Establish baseline last_seen_id
            cursor_baseline = get_latest_event_row_id(connection=self.conn)

            # 2. Persist a newly generated non-operational diagnostic event to MySQL
            diag_event = DashboardEvent(
                event_id=test_ev_id,
                event_type="SYSTEM_NOTIFICATION",
                entity="DIAGNOSTIC_PROBE",
                severity="INFO",
                data={"probe_key": "live_visibility_check", "status": "ok"},
                correlation_id=test_corr_id
            )
            inserted_row_id = record_event_to_db(diag_event, connection=self.conn)
            self.assertIsNotNone(inserted_row_id)
            self.assertGreater(inserted_row_id, cursor_baseline)

            # 3. Simulate poller retrieval for new records
            new_records = fetch_events_after_id(after_id=cursor_baseline, limit=50, connection=self.conn)
            matching = [ev for r_id, ev in new_records if ev.event_id == test_ev_id]
            self.assertEqual(len(matching), 1, "Poller query must find newly inserted record")

            # 4. Dispatch to EventHub (as cross_process_event_poller does)
            for r_id, ev in new_records:
                hub.publish(ev)

            # 5. Verify delivered to SSE subscriber queue
            self.assertFalse(queue.empty())
            received = queue.get_nowait()
            self.assertEqual(received.event_id, test_ev_id)
            self.assertEqual(received.event_type, "SYSTEM_NOTIFICATION")
            self.assertEqual(received.correlation_id, test_corr_id)

            # 6. Verify SSE wire format conforms to EventSource specification
            sse_wire = received.to_sse_format()
            self.assertIn(f"id: {test_ev_id}\n", sse_wire)
            self.assertIn("event: SYSTEM_NOTIFICATION\n", sse_wire)
            self.assertIn('"probe_key": "live_visibility_check"', sse_wire)
        finally:
            hub.unsubscribe(queue)
            # Safe cleanup of probe record
            self.cur.execute("DELETE FROM dashboard_events WHERE correlation_id = %s", (test_corr_id,))
            self.conn.commit()


class TestPublishHTTPRoute(unittest.TestCase):

    def test_http_publish_endpoint(self):
        """POST /api/observability/events/publish forwards to hub and returns 201."""
        from fastapi.testclient import TestClient
        from services.observability.main import app

        client = TestClient(app)
        queue = hub.subscribe()
        try:
            payload = {
                "event_type": "CART_ABANDONMENT_TRIGGERED",
                "entity": "MARKETING_FUNNEL",
                "severity": "WARNING",
                "data": {"dropoff_rate": 0.32, "sessions": 50}
            }
            res = client.post("/api/observability/events/publish", json=payload)
            self.assertEqual(res.status_code, 201)
            body = res.json()
            self.assertEqual(body["status"], "published")
            self.assertEqual(body["event_type"], "CART_ABANDONMENT_TRIGGERED")
            self.assertIn("event_id", body)

            # Verify subscriber received it
            self.assertFalse(queue.empty())
            ev = queue.get_nowait()
            self.assertEqual(ev.event_id, body["event_id"])
        finally:
            hub.unsubscribe(queue)


if __name__ == "__main__":
    unittest.main()
