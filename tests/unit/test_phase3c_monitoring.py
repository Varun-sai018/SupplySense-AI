"""
Phase 3C — Operational Monitoring & Failure Resilience Tests
============================================================

Coverage:
  - sanitize_error_message()
  - get_operational_alerts() service logic (mocked repo)
  - get_event_processing_health() via TestClient (mocked repo)
  - /api/observability/alerts endpoint (filters, pagination)
  - /api/observability/event-health endpoint
  - /api/observability/executions pagination + action_id linkage
  - Overdue vs stuck alert deduplication
  - Empty-input edge cases
  - Real MySQL integration (skipped when DB unavailable)
"""

import os
import sys
import datetime
import unittest
from unittest.mock import patch, MagicMock

# ── Path bootstrap ──────────────────────────────────────────────────────────────
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# Try to detect database availability early (used for skip decorators)
_DB_AVAILABLE = False
try:
    from services.observability import repository as _repo_probe
    conn = _repo_probe._get_connection()
    conn.close()
    _DB_AVAILABLE = True
except Exception:
    pass


# ══════════════════════════════════════════════════════════════════════════════
# 1. sanitize_error_message unit tests
# ══════════════════════════════════════════════════════════════════════════════

from services.observability.service import sanitize_error_message


class TestSanitizeErrorMessage(unittest.TestCase):
    """sanitize_error_message() must redact credentials and DB URLs."""

    def test_redacts_password_keyword(self):
        msg = "Connection failed: password=s3cr3t host=localhost"
        result = sanitize_error_message(msg)
        self.assertNotIn("s3cr3t", result)
        self.assertIn("[REDACTED]", result)

    def test_redacts_token_keyword(self):
        msg = "Auth error: token=abcXYZ123"
        result = sanitize_error_message(msg)
        self.assertNotIn("abcXYZ123", result)
        self.assertIn("[REDACTED]", result)

    def test_redacts_db_url(self):
        msg = "mysql://user:secretpass@localhost:3306/db timed out"
        result = sanitize_error_message(msg)
        self.assertNotIn("secretpass", result)
        self.assertIn("[REDACTED]", result)

    def test_redacts_pymysql_url(self):
        msg = "mysql+pymysql://admin:hunter2@db.internal/mydb"
        result = sanitize_error_message(msg)
        self.assertNotIn("hunter2", result)
        self.assertIn("[REDACTED]", result)

    def test_passes_clean_message(self):
        msg = "Execution timed out after 300 seconds"
        result = sanitize_error_message(msg)
        self.assertEqual(result, msg)

    def test_none_input_returns_none(self):
        self.assertIsNone(sanitize_error_message(None))

    def test_empty_string_returns_empty(self):
        self.assertEqual(sanitize_error_message(""), "")


# ══════════════════════════════════════════════════════════════════════════════
# 2. get_operational_alerts service unit tests (mocked repository)
# ══════════════════════════════════════════════════════════════════════════════

from services.observability import service as obs_service


def _empty_raw():
    return {
        "problem_suppliers": [],
        "catalog_failures": [],
        "failed_replenishments": [],
        "overdue_replenishments": [],
        "stuck_replenishments": [],
        "failed_executions": [],
    }


class TestGetOperationalAlerts(unittest.TestCase):
    """get_operational_alerts() with fully mocked repository data."""

    def _make_supplier(self, supplier_id, status, name="Test Supplier"):
        return {
            "supplier_id": supplier_id,
            "supplier_name": name,
            "status": status,
            "reliability_score": 0.9,
            "average_lead_time_days": 5,
            "last_health_check_at": datetime.datetime(2026, 10, 29, 12, 0, 0),
        }

    def _make_replenishment(self, rep_id, sku, status, **kwargs):
        base = {
            "replenishment_id": rep_id,
            "sku": sku,
            "supplier_id": "SUP-TEST",
            "quantity": 50,
            "status": status,
            "dispatched_at": datetime.datetime(2026, 10, 28, 8, 0, 0),
            "estimated_delivery_at": None,
            "action_id": 1,
            "decision_id": 1,
            "execution_id": 100,
        }
        base.update(kwargs)
        return base

    @patch("services.observability.repository.get_operational_alerts_raw")
    def test_returns_summary_structure(self, mock_raw):
        mock_raw.return_value = _empty_raw()
        result = obs_service.get_operational_alerts(connection=None)
        self.assertIn("summary", result)
        self.assertIn("alerts", result)
        self.assertIn("total_alerts", result["summary"])
        self.assertIn("critical_count", result["summary"])
        self.assertIn("warning_count", result["summary"])
        self.assertIn("by_category", result["summary"])

    @patch("services.observability.repository.get_operational_alerts_raw")
    def test_offline_supplier_is_critical(self, mock_raw):
        raw = _empty_raw()
        raw["problem_suppliers"] = [self._make_supplier("SUP-OFFLINE", "Offline")]
        mock_raw.return_value = raw
        result = obs_service.get_operational_alerts(connection=None)
        critical = [a for a in result["alerts"] if a["category"] == "SUPPLIER" and a["severity"] == "CRITICAL"]
        self.assertEqual(len(critical), 1)
        self.assertIn("OFFLINE", critical[0]["alert_id"])

    @patch("services.observability.repository.get_operational_alerts_raw")
    def test_degraded_supplier_is_warning(self, mock_raw):
        raw = _empty_raw()
        raw["problem_suppliers"] = [self._make_supplier("SUP-DEG", "Degraded")]
        mock_raw.return_value = raw
        result = obs_service.get_operational_alerts(connection=None)
        warnings = [a for a in result["alerts"] if a["category"] == "SUPPLIER" and a["severity"] == "WARNING"]
        self.assertEqual(len(warnings), 1)

    @patch("services.observability.repository.get_operational_alerts_raw")
    def test_failed_replenishment_is_critical(self, mock_raw):
        raw = _empty_raw()
        raw["failed_replenishments"] = [self._make_replenishment("REP-001", "SKU-A", "FAILED")]
        mock_raw.return_value = raw
        result = obs_service.get_operational_alerts(connection=None)
        crit = [a for a in result["alerts"] if a["category"] == "REPLENISHMENT" and a["severity"] == "CRITICAL"]
        self.assertEqual(len(crit), 1)

    @patch("services.observability.repository.get_operational_alerts_raw")
    def test_cancelled_replenishment_is_warning(self, mock_raw):
        raw = _empty_raw()
        raw["failed_replenishments"] = [self._make_replenishment("REP-002", "SKU-B", "CANCELLED")]
        mock_raw.return_value = raw
        result = obs_service.get_operational_alerts(connection=None)
        warns = [a for a in result["alerts"] if a["category"] == "REPLENISHMENT" and a["severity"] == "WARNING"]
        self.assertEqual(len(warns), 1)

    @patch("services.observability.repository.get_operational_alerts_raw")
    def test_filter_by_severity_critical(self, mock_raw):
        raw = _empty_raw()
        raw["problem_suppliers"] = [
            self._make_supplier("SUP-OFFLINE", "Offline"),
            self._make_supplier("SUP-DEG", "Degraded"),
        ]
        mock_raw.return_value = raw
        result = obs_service.get_operational_alerts(severity="CRITICAL", connection=None)
        for alert in result["alerts"]:
            self.assertEqual(alert["severity"], "CRITICAL")
        # summary totals are computed on full unfiltered list
        self.assertGreater(result["summary"]["warning_count"], 0)

    @patch("services.observability.repository.get_operational_alerts_raw")
    def test_filter_by_category_supplier(self, mock_raw):
        raw = _empty_raw()
        raw["problem_suppliers"] = [self._make_supplier("SUP-OFFLINE", "Offline")]
        raw["failed_replenishments"] = [self._make_replenishment("REP-003", "SKU-C", "FAILED")]
        mock_raw.return_value = raw
        result = obs_service.get_operational_alerts(category="SUPPLIER", connection=None)
        for alert in result["alerts"]:
            self.assertEqual(alert["category"], "SUPPLIER")

    @patch("services.observability.repository.get_operational_alerts_raw")
    def test_no_alerts_when_everything_clean(self, mock_raw):
        mock_raw.return_value = _empty_raw()
        result = obs_service.get_operational_alerts(connection=None)
        self.assertEqual(result["summary"]["total_alerts"], 0)
        self.assertEqual(len(result["alerts"]), 0)

    @patch("services.observability.repository.get_operational_alerts_raw")
    def test_overdue_and_stuck_deduplication(self, mock_raw):
        """A replenishment that is BOTH overdue AND stuck must appear once."""
        raw = _empty_raw()
        overdue_rep = self._make_replenishment(
            "REP-OVER-001", "SKU-D", "DISPATCHED",
            overdue_hours=10,
            estimated_delivery_at=datetime.datetime(2026, 10, 28, 6, 0, 0),
        )
        stuck_rep = self._make_replenishment(
            "REP-OVER-001", "SKU-D", "DISPATCHED",
            stuck_hours=15,
        )
        raw["overdue_replenishments"] = [overdue_rep]
        raw["stuck_replenishments"] = [stuck_rep]
        mock_raw.return_value = raw
        result = obs_service.get_operational_alerts(connection=None)
        rep_alerts = [a for a in result["alerts"] if a["identifiers"].get("replenishment_id") == "REP-OVER-001"]
        # Should be deduplicated to 1 alert (overdue wins, stuck is skipped)
        self.assertEqual(len(rep_alerts), 1, f"Expected 1 alert for REP-OVER-001, got {len(rep_alerts)}")

    @patch("services.observability.repository.get_operational_alerts_raw")
    def test_pipeline_alert_sanitizes_error_message(self, mock_raw):
        raw = _empty_raw()
        raw["failed_executions"] = [{
            "execution_id": 9999,
            "pipeline_name": "Inventory Reorder Pipeline",
            "error_message": "Connection refused: password=topsecret@db host=prod",
            "retry_count": 3,
            "max_retries": 3,
            "started_at": datetime.datetime(2026, 10, 29, 8, 0, 0),
            "completed_at": datetime.datetime(2026, 10, 29, 8, 1, 0),
            "decision_id": None,
            "action_id": None,
        }]
        mock_raw.return_value = raw
        result = obs_service.get_operational_alerts(connection=None)
        pipeline_alerts = [a for a in result["alerts"] if a["category"] == "PIPELINE"]
        self.assertEqual(len(pipeline_alerts), 1)
        self.assertNotIn("topsecret", pipeline_alerts[0]["condition"])
        self.assertIn("[REDACTED]", pipeline_alerts[0]["condition"])

    @patch("services.observability.repository.get_operational_alerts_raw")
    def test_limit_applied(self, mock_raw):
        raw = _empty_raw()
        raw["problem_suppliers"] = [self._make_supplier(f"SUP-{i}", "Offline") for i in range(10)]
        mock_raw.return_value = raw
        result = obs_service.get_operational_alerts(limit=3, connection=None)
        self.assertLessEqual(len(result["alerts"]), 3)

    @patch("services.observability.repository.get_operational_alerts_raw")
    def test_alerts_sorted_critical_first(self, mock_raw):
        raw = _empty_raw()
        raw["problem_suppliers"] = [
            self._make_supplier("SUP-DEG", "Degraded"),
            self._make_supplier("SUP-OFFLINE", "Offline"),
        ]
        mock_raw.return_value = raw
        result = obs_service.get_operational_alerts(connection=None)
        severities = [a["severity"] for a in result["alerts"]]
        # CRITICAL must come before WARNING in sorted order
        if "CRITICAL" in severities and "WARNING" in severities:
            self.assertLess(severities.index("CRITICAL"), severities.index("WARNING"))


# ══════════════════════════════════════════════════════════════════════════════
# 3. FastAPI TestClient — endpoint-level tests
# ══════════════════════════════════════════════════════════════════════════════

from fastapi.testclient import TestClient
from services.observability.main import app

_EMPTY_ALERTS_RETURN = {
    "summary": {"total_alerts": 0, "critical_count": 0, "warning_count": 0, "info_count": 0, "by_category": {}},
    "alerts": [],
    "count": 0,
    "total_count": 0,
    "timestamp": "2026-10-29T00:00:00+00:00",
}

_MOCK_HEALTH_RETURN = {
    "status": "HEALTHY",
    "reasons": [],
    "sse": {"active_subscribers": 0, "ring_buffer_size": 0, "ring_buffer_capacity": 128,
            "schema_version": "1.0", "heartbeat_interval_seconds": 30,
            "oldest_buffered_event_id": None, "newest_buffered_event_id": None},
    "journal": {"total_journaled_events": 390, "events_last_24h": 390,
                "latest_timestamp": "2026-10-29T19:08:07", "by_type": {}, "by_severity": {}},
    "timestamp": "2026-10-29T00:00:00+00:00",
}


class TestAlertsEndpoint(unittest.TestCase):
    """Tests for /api/observability/alerts via TestClient."""

    def setUp(self):
        self.client = TestClient(app, raise_server_exceptions=False)

    @patch("services.observability.service.get_operational_alerts")
    def test_alerts_endpoint_returns_200(self, mock_svc):
        mock_svc.return_value = _EMPTY_ALERTS_RETURN
        resp = self.client.get("/api/observability/alerts")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertIn("summary", body)
        self.assertIn("alerts", body)

    @patch("services.observability.service.get_operational_alerts")
    def test_alerts_endpoint_severity_filter_passed_through(self, mock_svc):
        mock_svc.return_value = _EMPTY_ALERTS_RETURN
        resp = self.client.get("/api/observability/alerts?severity=CRITICAL")
        self.assertEqual(resp.status_code, 200)
        call_kwargs = mock_svc.call_args
        # The endpoint forwards severity as a keyword argument to the service
        actual_severity = call_kwargs.kwargs.get("severity")
        self.assertEqual(actual_severity, "CRITICAL",
                         f"Expected severity='CRITICAL' forwarded to service, got: {call_kwargs}")

    @patch("services.observability.service.get_operational_alerts")
    def test_alerts_endpoint_category_filter_passed_through(self, mock_svc):
        mock_svc.return_value = _EMPTY_ALERTS_RETURN
        resp = self.client.get("/api/observability/alerts?category=PIPELINE")
        self.assertEqual(resp.status_code, 200)
        call_kwargs = mock_svc.call_args
        called_with_cat = (
            call_kwargs.kwargs.get("category") == "PIPELINE"
            or (len(call_kwargs.args) > 1 and call_kwargs.args[1] == "PIPELINE")
        )
        self.assertTrue(called_with_cat, f"Expected category=PIPELINE in call, got: {call_kwargs}")

    @patch("services.observability.service.get_operational_alerts")
    def test_alerts_endpoint_limit_clamp(self, mock_svc):
        """limit > 100 should be rejected with 422."""
        mock_svc.return_value = _EMPTY_ALERTS_RETURN
        resp = self.client.get("/api/observability/alerts?limit=200")
        self.assertEqual(resp.status_code, 422)


class TestEventHealthEndpoint(unittest.TestCase):
    """Tests for /api/observability/event-health via TestClient."""

    def setUp(self):
        self.client = TestClient(app, raise_server_exceptions=False)

    @patch("services.observability.service.get_event_processing_health")
    def test_event_health_returns_200(self, mock_svc):
        mock_svc.return_value = _MOCK_HEALTH_RETURN
        resp = self.client.get("/api/observability/event-health")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertIn("status", body)
        self.assertIn("sse", body)
        self.assertIn("journal", body)

    @patch("services.observability.service.get_event_processing_health")
    def test_event_health_status_field(self, mock_svc):
        mock_svc.return_value = _MOCK_HEALTH_RETURN
        resp = self.client.get("/api/observability/event-health")
        body = resp.json()
        self.assertIn(body["status"], ["HEALTHY", "DEGRADED", "UNKNOWN"])


class TestExecutionsPagination(unittest.TestCase):
    """Tests for /api/observability/executions pagination and action_id."""

    def setUp(self):
        self.client = TestClient(app, raise_server_exceptions=False)

    @patch("services.observability.service.get_recent_executions")
    def test_pagination_offset_param(self, mock_svc):
        mock_svc.return_value = []
        resp = self.client.get("/api/observability/executions?limit=5&offset=0")
        self.assertEqual(resp.status_code, 200)
        # Verify offset was forwarded to service
        call_kwargs = mock_svc.call_args.kwargs
        self.assertIn("offset", call_kwargs)
        self.assertEqual(call_kwargs["offset"], 0)

    @patch("services.observability.service.get_recent_executions")
    def test_pagination_offset_nonzero(self, mock_svc):
        mock_svc.return_value = []
        resp = self.client.get("/api/observability/executions?limit=10&offset=20")
        self.assertEqual(resp.status_code, 200)
        call_kwargs = mock_svc.call_args.kwargs
        self.assertEqual(call_kwargs["offset"], 20)

    @patch("services.observability.service.get_recent_executions")
    def test_status_filter_failed(self, mock_svc):
        mock_svc.return_value = []
        resp = self.client.get("/api/observability/executions?status=FAILED")
        self.assertEqual(resp.status_code, 200)
        call_kwargs = mock_svc.call_args.kwargs
        self.assertEqual(call_kwargs.get("status"), "FAILED")

    @patch("services.observability.service.get_recent_executions")
    def test_action_id_present_in_response(self, mock_svc):
        mock_svc.return_value = [{
            "execution_id": 42,
            "pipeline_name": "Inventory Reorder Pipeline",
            "status": "COMPLETED",
            "action_id": 7,
            "decision_id": 3,
            "started_at": "2026-10-29T10:00:00",
            "completed_at": "2026-10-29T10:01:00",
            "duration_seconds": 60.0,
            "error_message": None,
            "retry_count": 0,
        }]
        resp = self.client.get("/api/observability/executions")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertIsInstance(data, list)
        self.assertEqual(data[0].get("action_id"), 7)


# ══════════════════════════════════════════════════════════════════════════════
# 4. Service-level edge case: duration null when no started_at
# ══════════════════════════════════════════════════════════════════════════════

from services.observability.service import _calculate_duration


class TestCalculateDuration(unittest.TestCase):
    def test_duration_null_when_no_started_at(self):
        result = _calculate_duration(None, None, "FAILED")
        self.assertIsNone(result)

    def test_duration_computed_when_both_present(self):
        start = datetime.datetime(2026, 10, 29, 8, 0, 0)
        end = datetime.datetime(2026, 10, 29, 8, 1, 30)
        result = _calculate_duration(start, end, "COMPLETED")
        self.assertAlmostEqual(result, 90.0, places=1)

    def test_duration_running_uses_now(self):
        start = datetime.datetime.now() - datetime.timedelta(seconds=45)
        result = _calculate_duration(start, None, "RUNNING")
        self.assertIsNotNone(result)
        self.assertGreater(result, 40)


# ══════════════════════════════════════════════════════════════════════════════
# 5. Real MySQL integration tests (skipped when DB unavailable)
# ══════════════════════════════════════════════════════════════════════════════

@unittest.skipUnless(_DB_AVAILABLE, "MySQL database not available in this environment")
class TestAlertsRealMySQL(unittest.TestCase):
    """Validates that alert counts from the service match direct DB queries."""

    def setUp(self):
        from services.observability import repository
        self.conn = repository._get_connection()
        self.cursor = self.conn.cursor()

    def tearDown(self):
        self.cursor.close()
        self.conn.close()

    def test_critical_alert_count_matches_db_offline_suppliers(self):
        from services.observability.service import get_operational_alerts
        result = get_operational_alerts(connection=self.conn)

        # Count CRITICAL supplier alerts from service
        svc_critical_suppliers = sum(
            1 for a in result["alerts"]
            if a["category"] == "SUPPLIER" and a["severity"] == "CRITICAL"
        )

        # Direct DB query for Offline suppliers
        self.cursor.execute("SELECT COUNT(*) FROM operational_suppliers WHERE status = 'Offline'")
        db_offline = self.cursor.fetchone()[0]

        self.assertEqual(svc_critical_suppliers, db_offline,
                         f"Service reports {svc_critical_suppliers} critical supplier alerts "
                         f"but DB has {db_offline} Offline suppliers")

    def test_failed_replenishment_alert_count_matches_db(self):
        from services.observability.service import get_operational_alerts
        result = get_operational_alerts(connection=self.conn)

        svc_failed_reps = sum(
            1 for a in result["alerts"]
            if a["category"] == "REPLENISHMENT" and a["severity"] == "CRITICAL"
        )

        self.cursor.execute("SELECT COUNT(*) FROM replenishment_orders WHERE status = 'FAILED'")
        db_failed = self.cursor.fetchone()[0]

        self.assertEqual(svc_failed_reps, db_failed,
                         f"Service reports {svc_failed_reps} critical replenishment alerts "
                         f"but DB has {db_failed} FAILED replenishments")

    def test_alert_total_nonzero_with_known_data(self):
        """With known seeded data, total_alerts must be > 0."""
        from services.observability.service import get_operational_alerts
        result = get_operational_alerts(connection=self.conn)
        self.assertGreater(result["summary"]["total_alerts"], 0,
                           "Expected at least 1 alert with seeded operational data")

    def test_event_journal_total_nonzero(self):
        from services.observability import repository
        metrics = repository.get_event_processing_metrics(connection=self.conn)
        self.assertGreater(metrics.get("total_journaled_events", 0), 0,
                           "Expected journaled events in dashboard_events table")

    def test_error_messages_sanitized_in_pipeline_alerts(self):
        """No raw DB credential strings should leak into pipeline alert conditions."""
        from services.observability.service import get_operational_alerts
        result = get_operational_alerts(connection=self.conn)
        pipeline_alerts = [a for a in result["alerts"] if a["category"] == "PIPELINE"]
        for alert in pipeline_alerts:
            condition = alert.get("condition", "")
            self.assertNotIn("password=", condition.lower())
            self.assertNotIn("://", condition)


if __name__ == "__main__":
    unittest.main(verbosity=2)
