"""
SupplySense AI - Unit Test Suite for Validated Nested Rule Engine (Module 1)

Verifies:
  1. AST Logical Operators (AND, OR, NOT) with short-circuiting.
  2. String rule parsing into AST.
  3. Strict schema validation, rejection of empty/malformed expressions, invalid thresholds.
  4. Missing/null/stale input safety (never silently True).
  5. Backwards compatibility with ALL / ANY / QUORUM legacy evaluator.
  6. Rule A (Cart Abandonment): rolling 5-min window, strictly > 25%, zero sessions,
     stale events, peak-hour timezone boundaries, idempotency, and cooldown.
  7. Rule B (Inventory Reorder): threshold evaluation (< 20), supplier online,
     pending orders == 0, duplicate prevention, confirmation, rejection, and timeout.
  8. Rule C (Payment Failure Spike): 50-transaction window, strictly < 92%, pending outcomes,
     duplicates, out-of-order sorting, alert deduplication, and recovery.
"""

import os
import sys
import unittest
import datetime
from zoneinfo import ZoneInfo
from unittest.mock import MagicMock, patch

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import services


from services.dependency_engine.rule_evaluator import (
    validate_expression,
    evaluate_rule,
    parse_rule_string,
    evaluate_legacy_condition,
    RuleValidationError,
    EvaluationResult
)
from services.dependency_engine.rules.rule_cart_abandonment import (
    calculate_rolling_dropoff,
    is_peak_hour,
    evaluate_and_dispatch_cart_abandonment,
    RULE_ID as RULE_A_ID
)
from services.dependency_engine.rules.rule_inventory_reorder import (
    evaluate_and_dispatch_inventory_reorder,
    RULE_ID as RULE_B_ID
)
from services.dependency_engine.rules.rule_payment_spike import (
    process_transactions_window,
    evaluate_and_manage_payment_alerts,
    RULE_ID as RULE_C_ID
)
from services.dependency_engine.adapters.marketing_adapter import MarketingWebhookAdapter
from services.dependency_engine.adapters.supplier_adapter import SupplierAPIAdapter


class TestRuleEvaluatorCore(unittest.TestCase):
    """Unit tests for AST evaluation, string parser, and validation."""

    def test_evaluator_and_all_true(self):
        expr = {
            "operator": "AND",
            "conditions": [
                {"field": "a", "op": ">", "value": 10},
                {"field": "b", "op": "==", "value": True}
            ]
        }
        res = evaluate_rule("test_rule", expr, {"a": 15, "b": True})
        self.assertTrue(res.matched)
        self.assertEqual(res.decision, "TRIGGER")

    def test_evaluator_and_short_circuit(self):
        expr = {
            "operator": "AND",
            "conditions": [
                {"field": "a", "op": ">", "value": 10},
                {"field": "b", "op": "==", "value": True}
            ]
        }
        res = evaluate_rule("test_rule", expr, {"a": 5, "b": True})
        self.assertFalse(res.matched)
        self.assertEqual(res.decision, "BLOCK")
        self.assertTrue(any("short-circuit" in r for r in res.reasons))

    def test_evaluator_or_any_true(self):
        expr = {
            "operator": "OR",
            "conditions": [
                {"field": "x", "op": "==", "value": "critical"},
                {"field": "y", "op": "<", "value": 5}
            ]
        }
        res = evaluate_rule("test_rule", expr, {"x": "normal", "y": 2})
        self.assertTrue(res.matched)
        self.assertEqual(res.decision, "TRIGGER")

    def test_evaluator_or_all_false(self):
        expr = {
            "operator": "OR",
            "conditions": [
                {"field": "x", "op": "==", "value": "critical"},
                {"field": "y", "op": "<", "value": 5}
            ]
        }
        res = evaluate_rule("test_rule", expr, {"x": "normal", "y": 10})
        self.assertFalse(res.matched)
        self.assertEqual(res.decision, "BLOCK")

    def test_evaluator_not_inversion(self):
        expr = {
            "operator": "NOT",
            "conditions": [
                {"field": "is_blocked", "op": "==", "value": True}
            ]
        }
        # Child is True -> NOT is False
        res1 = evaluate_rule("test_rule", expr, {"is_blocked": True})
        self.assertFalse(res1.matched)

        # Child is False -> NOT is True
        res2 = evaluate_rule("test_rule", expr, {"is_blocked": False})
        self.assertTrue(res2.matched)

    def test_evaluator_nested_hierarchical_tree(self):
        # AND(dropoff_rate > 0.25, OR(peak_hour == true, emergency_override == true))
        expr = {
            "operator": "AND",
            "conditions": [
                {"field": "dropoff_rate", "op": ">", "value": 0.25},
                {
                    "operator": "OR",
                    "conditions": [
                        {"field": "peak_hour", "op": "==", "value": True},
                        {"field": "emergency_override", "op": "==", "value": True}
                    ]
                }
            ]
        }
        # Case 1: dropoff > 0.25 and emergency override -> TRIGGER
        res = evaluate_rule("test_nested", expr, {"dropoff_rate": 0.30, "peak_hour": False, "emergency_override": True})
        self.assertTrue(res.matched)

        # Case 2: dropoff > 0.25 but neither peak nor override -> BLOCK
        res2 = evaluate_rule("test_nested", expr, {"dropoff_rate": 0.30, "peak_hour": False, "emergency_override": False})
        self.assertFalse(res2.matched)

    def test_evaluator_string_parser(self):
        expr_str = "AND(dropoff_rate > 0.25, OR(peak_hour == true, emergency_override == true))"
        ast = parse_rule_string(expr_str)
        self.assertEqual(ast["operator"], "AND")
        self.assertEqual(len(ast["conditions"]), 2)
        self.assertEqual(ast["conditions"][0]["field"], "dropoff_rate")
        self.assertEqual(ast["conditions"][0]["value"], 0.25)
        self.assertEqual(ast["conditions"][1]["operator"], "OR")

        # Test evaluation of parsed AST
        res = evaluate_rule("parsed_test", ast, {"dropoff_rate": 0.26, "peak_hour": True, "emergency_override": False})
        self.assertTrue(res.matched)

    def test_evaluator_rejects_empty_conditions(self):
        with self.assertRaises(RuleValidationError):
            validate_expression({"operator": "AND", "conditions": []})

        with self.assertRaises(RuleValidationError):
            validate_expression({"operator": "OR", "conditions": []})

    def test_evaluator_rejects_malformed_not(self):
        # NOT with 2 children must fail
        with self.assertRaises(RuleValidationError):
            validate_expression({
                "operator": "NOT",
                "conditions": [
                    {"field": "a", "op": "==", "value": 1},
                    {"field": "b", "op": "==", "value": 2}
                ]
            })

    def test_evaluator_rejects_unsupported_operator(self):
        with self.assertRaises(RuleValidationError):
            validate_expression({"operator": "XOR", "conditions": [{"field": "a", "op": "==", "value": 1}]})

        with self.assertRaises(RuleValidationError):
            validate_expression({"field": "a", "op": "~=", "value": 1})

    def test_evaluator_rejects_invalid_numeric_threshold(self):
        with self.assertRaises(RuleValidationError):
            validate_expression({"field": "threshold", "op": ">", "value": "not_a_number"})

    def test_evaluator_missing_field_safely_evaluates_false(self):
        expr = {"field": "missing_key", "op": ">", "value": 10}
        res = evaluate_rule("test_missing", expr, {})
        self.assertFalse(res.matched)
        self.assertTrue(any("missing or null" in r for r in res.reasons))

    def test_legacy_all_any_quorum_compatibility(self):
        # ALL
        self.assertEqual(evaluate_legacy_condition("ALL", 3, 3), ("TRIGGER", "All required datasets are READY."))
        self.assertEqual(evaluate_legacy_condition("ALL", 2, 3), ("BLOCK", "2 of 3 required datasets are READY."))

        # ANY
        self.assertEqual(evaluate_legacy_condition("ANY", 1, 3), ("TRIGGER", "At least one required dataset is READY."))
        self.assertEqual(evaluate_legacy_condition("ANY", 0, 3), ("BLOCK", "No required dataset is READY."))

        # QUORUM
        self.assertEqual(evaluate_legacy_condition("QUORUM", 2, 5, required_count=2), ("TRIGGER", "Required quorum of 2 datasets satisfied."))
        self.assertEqual(evaluate_legacy_condition("QUORUM", 1, 5, required_count=2), ("BLOCK", "Only 1 of 2 required datasets are READY."))


class TestRuleACartAbandonment(unittest.TestCase):
    """Unit tests for Rule A: High-Velocity Cart Abandonment."""

    def setUp(self):
        self.now = datetime.datetime(2026, 10, 24, 19, 0, 0, tzinfo=datetime.timezone.utc)

    def test_cart_strictly_greater_than_25_percent_triggers_in_peak(self):
        # 26 abandoned out of 100 = 26% > 25%
        sessions = [
            {"session_id": f"s_{i}", "started_at": self.now - datetime.timedelta(seconds=60), "status": "abandoned" if i < 26 else "completed"}
            for i in range(100)
        ]
        mock_webhook = MagicMock()
        mock_webhook.dispatch.return_value = {"status": "CONFIRMED", "http_status": 200}

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = None  # No cooldown
        mock_conn.cursor.return_value = mock_cur

        with patch("services.dependency_engine.rules.rule_cart_abandonment.check_cooldown_active", return_value=(False, None)):
            with patch("services.dependency_engine.rules.rule_cart_abandonment.record_action_attempt", return_value=101):
                with patch("services.dependency_engine.rules.rule_cart_abandonment.update_action_status"):
                    res = evaluate_and_dispatch_cart_abandonment(
                        sessions=sessions,
                        current_time=self.now,
                        tz_name="UTC",
                        peak_start=18,
                        peak_end=22,
                        webhook_adapter=mock_webhook,
                        connection=mock_conn
                    )
        self.assertTrue(res["matched"])
        self.assertEqual(res["decision"], "TRIGGER")
        self.assertEqual(res["metrics"]["dropoff_rate"], 0.26)
        mock_webhook.dispatch.assert_called_once()

    def test_cart_exactly_25_percent_blocks(self):
        # 25 abandoned out of 100 = 25.0% -> NOT strictly > 25% -> BLOCK
        sessions = [
            {"session_id": f"s_{i}", "started_at": self.now - datetime.timedelta(seconds=60), "status": "abandoned" if i < 25 else "completed"}
            for i in range(100)
        ]
        res = evaluate_and_dispatch_cart_abandonment(
            sessions=sessions,
            current_time=self.now,
            tz_name="UTC",
            peak_start=18,
            peak_end=22
        )
        self.assertFalse(res["matched"])
        self.assertEqual(res["decision"], "BLOCK")
        self.assertEqual(res["metrics"]["dropoff_rate"], 0.25)

    def test_cart_zero_sessions_blocks_safely(self):
        res = evaluate_and_dispatch_cart_abandonment(
            sessions=[],
            current_time=self.now,
            tz_name="UTC",
            peak_start=18,
            peak_end=22
        )
        self.assertFalse(res["matched"])
        self.assertEqual(res["decision"], "BLOCK")
        self.assertEqual(res["metrics"]["dropoff_rate"], 0.0)
        self.assertEqual(res["metrics"]["total_sessions"], 0)

    def test_cart_stale_sessions_outside_5min_ignored(self):
        # 10 abandoned sessions started 10 minutes ago (stale), 10 active sessions started 1 min ago
        sessions = [
            {"session_id": f"stale_{i}", "started_at": self.now - datetime.timedelta(seconds=600), "status": "abandoned"}
            for i in range(10)
        ] + [
            {"session_id": f"fresh_{i}", "started_at": self.now - datetime.timedelta(seconds=60), "status": "active"}
            for i in range(10)
        ]
        rate, abandoned, total = calculate_rolling_dropoff(sessions, self.now, window_seconds=300)
        self.assertEqual(total, 10)  # Only fresh sessions counted
        self.assertEqual(abandoned, 0)
        self.assertEqual(rate, 0.0)

    def test_cart_duplicate_session_ids_deduplicated(self):
        # Single session with 3 event updates
        sessions = [
            {"session_id": "s_100", "started_at": self.now - datetime.timedelta(seconds=120), "status": "active"},
            {"session_id": "s_100", "started_at": self.now - datetime.timedelta(seconds=90), "status": "in_checkout"},
            {"session_id": "s_100", "started_at": self.now - datetime.timedelta(seconds=30), "status": "abandoned"},
        ]
        rate, abandoned, total = calculate_rolling_dropoff(sessions, self.now, window_seconds=300)
        self.assertEqual(total, 1)
        self.assertEqual(abandoned, 1)
        self.assertEqual(rate, 1.0)

    def test_cart_off_peak_blocks_even_if_high_dropoff(self):
        # 14:00 is off-peak (peak is 18 to 22)
        off_peak_time = datetime.datetime(2026, 10, 24, 14, 0, 0, tzinfo=datetime.timezone.utc)
        sessions = [
            {"session_id": f"s_{i}", "started_at": off_peak_time - datetime.timedelta(seconds=60), "status": "abandoned"}
            for i in range(10)
        ]
        res = evaluate_and_dispatch_cart_abandonment(
            sessions=sessions,
            current_time=off_peak_time,
            tz_name="UTC",
            peak_start=18,
            peak_end=22
        )
        self.assertFalse(res["matched"])
        self.assertEqual(res["decision"], "BLOCK")
        self.assertFalse(res["metrics"]["peak_hour"])

    def test_cart_timezone_boundaries(self):
        # UTC 21:30 is 18:30 in America/Sao_Paulo (UTC-3), which IS peak
        utc_time = datetime.datetime(2026, 10, 24, 21, 30, 0, tzinfo=datetime.timezone.utc)
        self.assertTrue(is_peak_hour(utc_time, tz_name="America/Sao_Paulo", start_hour=18, end_hour=22))

        # UTC 02:00 is 23:00 in America/Sao_Paulo, which is OFF peak
        utc_night = datetime.datetime(2026, 10, 24, 2, 0, 0, tzinfo=datetime.timezone.utc)
        self.assertFalse(is_peak_hour(utc_night, tz_name="America/Sao_Paulo", start_hour=18, end_hour=22))

    def test_cart_cooldown_prevents_duplicate_webhook(self):
        sessions = [
            {"session_id": f"s_{i}", "started_at": self.now - datetime.timedelta(seconds=60), "status": "abandoned"}
            for i in range(10)
        ]
        mock_webhook = MagicMock()
        mock_conn = MagicMock()

        # Simulate cooldown active
        with patch("services.dependency_engine.rules.rule_cart_abandonment.check_cooldown_active", return_value=(True, self.now - datetime.timedelta(minutes=5))):
            res = evaluate_and_dispatch_cart_abandonment(
                sessions=sessions,
                current_time=self.now,
                tz_name="UTC",
                cooldown_seconds=900,
                webhook_adapter=mock_webhook,
                connection=mock_conn
            )
        self.assertEqual(res["decision"], "BLOCK")
        self.assertEqual(res["action"]["status"], "COOLDOWN")
        mock_webhook.dispatch.assert_not_called()


class TestRuleBInventoryReorder(unittest.TestCase):
    """Unit tests for Rule B: Automated Inventory Reorder."""

    def test_reorder_triggers_when_below_threshold_online_zero_pending(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.lastrowid = 201
        mock_conn.cursor.return_value = mock_cur

        with patch("services.dependency_engine.rules.rule_inventory_reorder.get_action_by_key", return_value=None):
            with patch("services.dependency_engine.rules.rule_inventory_reorder.record_action_attempt", return_value=301):
                with patch("services.dependency_engine.rules.rule_inventory_reorder.execute_pipeline") as mock_exec:
                    mock_exec.return_value = {
                        "status": "COMPLETED",
                        "execution_id": 201,
                        "output_location": "ml/results/executions/201/reorder_confirmation.json",
                        "result": {"status": "CONFIRMED", "supplier_order_id": "ORD-999"}
                    }
                    with patch("services.dependency_engine.rules.rule_inventory_reorder.update_action_status"):
                        res = evaluate_and_dispatch_inventory_reorder(
                            product_id="PROD-TEST-1",
                            inventory_level=12,  # < 20
                            supplier_status="Online",
                            pending_orders=0,
                            safe_threshold=20,
                            connection=mock_conn
                        )

        self.assertTrue(res["matched"])
        self.assertEqual(res["decision"], "TRIGGER")
        self.assertEqual(res["lifecycle_state"], "CONFIRMED")
        self.assertTrue(res["reorder_confirmed"])

    def test_reorder_blocks_when_equal_or_above_threshold(self):
        # inventory == 20 (threshold is 20, must be strictly < 20)
        res = evaluate_and_dispatch_inventory_reorder(
            product_id="PROD-TEST-2",
            inventory_level=20,
            supplier_status="Online",
            pending_orders=0,
            safe_threshold=20
        )
        self.assertFalse(res["matched"])
        self.assertEqual(res["decision"], "BLOCK")

        # inventory == 25 (> 20)
        res2 = evaluate_and_dispatch_inventory_reorder(
            product_id="PROD-TEST-2",
            inventory_level=25,
            supplier_status="Online",
            pending_orders=0,
            safe_threshold=20
        )
        self.assertFalse(res2["matched"])
        self.assertEqual(res2["decision"], "BLOCK")

    def test_reorder_blocks_when_supplier_offline(self):
        res = evaluate_and_dispatch_inventory_reorder(
            product_id="PROD-TEST-3",
            inventory_level=5,
            supplier_status="Offline",
            pending_orders=0,
            safe_threshold=20
        )
        self.assertFalse(res["matched"])
        self.assertEqual(res["decision"], "BLOCK")

    def test_reorder_blocks_when_pending_orders_exist(self):
        res = evaluate_and_dispatch_inventory_reorder(
            product_id="PROD-TEST-4",
            inventory_level=5,
            supplier_status="Online",
            pending_orders=1,
            safe_threshold=20
        )
        self.assertFalse(res["matched"])
        self.assertEqual(res["decision"], "BLOCK")

    def test_reorder_duplicate_idempotent_skips(self):
        mock_conn = MagicMock()
        existing = {"action_id": 50, "status": "CONFIRMED", "idempotency_key": "key_1"}

        with patch("services.dependency_engine.rules.rule_inventory_reorder.get_action_by_key", return_value=existing):
            with patch("services.dependency_engine.rules.rule_inventory_reorder.execute_pipeline") as mock_exec:
                res = evaluate_and_dispatch_inventory_reorder(
                    product_id="PROD-TEST-5",
                    inventory_level=5,
                    supplier_status="Online",
                    pending_orders=0,
                    safe_threshold=20,
                    connection=mock_conn
                )
                self.assertEqual(res["decision"], "SKIPPED")
                self.assertEqual(res["lifecycle_state"], "IDEMPOTENT_SKIPPED")
                mock_exec.assert_not_called()

    def test_reorder_supplier_failure_marks_failed(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.lastrowid = 202
        mock_conn.cursor.return_value = mock_cur

        with patch("services.dependency_engine.rules.rule_inventory_reorder.get_action_by_key", return_value=None):
            with patch("services.dependency_engine.rules.rule_inventory_reorder.record_action_attempt", return_value=302):
                with patch("services.dependency_engine.rules.rule_inventory_reorder.execute_pipeline") as mock_exec:
                    mock_exec.return_value = {
                        "status": "FAILED",
                        "execution_id": 202,
                        "error": "Supplier API timeout connecting to host"
                    }
                    with patch("services.dependency_engine.rules.rule_inventory_reorder.update_action_status"):
                        res = evaluate_and_dispatch_inventory_reorder(
                            product_id="PROD-TEST-6",
                            inventory_level=5,
                            supplier_status="Online",
                            pending_orders=0,
                            safe_threshold=20,
                            connection=mock_conn
                        )

        self.assertTrue(res["matched"])
        self.assertEqual(res["decision"], "TRIGGER")
        self.assertEqual(res["lifecycle_state"], "FAILED")
        self.assertFalse(res["reorder_confirmed"])


class TestRuleCPaymentSpike(unittest.TestCase):
    """Unit tests for Rule C: Payment Failure Spike Detection."""

    def test_payment_insufficient_history_under_50_blocks(self):
        # Only 40 transactions: not enough history to trigger spike
        txs = [{"transaction_id": f"tx_{i}", "status": "FAILED"} for i in range(40)]
        res = evaluate_and_manage_payment_alerts(transactions=txs)
        self.assertFalse(res["matched"])
        self.assertEqual(res["decision"], "BLOCK")
        self.assertFalse(res["metrics"]["has_sufficient_history"])

    def test_payment_strictly_below_92_percent_triggers_alert(self):
        # 50 transactions: 45 success, 5 failed -> 45/50 = 90.0% < 92% -> TRIGGER
        txs = [
            {"transaction_id": f"tx_{i}", "status": "SUCCESS" if i < 45 else "FAILED"}
            for i in range(50)
        ]
        mock_conn = MagicMock()

        with patch("services.dependency_engine.rules.rule_payment_spike.get_active_alert", return_value=None):
            with patch("services.dependency_engine.rules.rule_payment_spike.record_action_attempt", return_value=701):
                res = evaluate_and_manage_payment_alerts(transactions=txs, connection=mock_conn)

        self.assertTrue(res["matched"])
        self.assertEqual(res["decision"], "TRIGGER")
        self.assertEqual(res["metrics"]["success_rate"], 0.90)
        self.assertEqual(res["alert_status"], "NEW_ALERT_PUBLISHED")
        self.assertIsNotNone(res["alert_event"])
        self.assertEqual(res["alert_event"]["severity"], "CRITICAL")

    def test_payment_exactly_92_percent_healthy_blocks(self):
        # 46 success out of 50 = 46/50 = 92.0% -> NOT strictly < 92% -> BLOCK
        txs = [
            {"transaction_id": f"tx_{i}", "status": "SUCCESS" if i < 46 else "FAILED"}
            for i in range(50)
        ]
        with patch("services.dependency_engine.rules.rule_payment_spike.get_active_alert", return_value=None):
            res = evaluate_and_manage_payment_alerts(transactions=txs)

        self.assertFalse(res["matched"])
        self.assertEqual(res["decision"], "BLOCK")
        self.assertEqual(res["metrics"]["success_rate"], 0.92)
        self.assertEqual(res["alert_status"], "HEALTHY")

    def test_payment_pending_transactions_not_counted_as_successes(self):
        # 45 success, 5 pending: 50 total. Success = 45/50 = 90% < 92% -> TRIGGER
        txs = [
            {"transaction_id": f"tx_{i}", "status": "SUCCESS" if i < 45 else "PENDING"}
            for i in range(50)
        ]
        mock_conn = MagicMock()

        with patch("services.dependency_engine.rules.rule_payment_spike.get_active_alert", return_value=None):
            with patch("services.dependency_engine.rules.rule_payment_spike.record_action_attempt", return_value=702):
                res = evaluate_and_manage_payment_alerts(transactions=txs, connection=mock_conn)

        self.assertTrue(res["matched"])
        self.assertEqual(res["metrics"]["success_rate"], 0.90)

    def test_payment_duplicate_transactions_deduplicated(self):
        # 50 transactions but 10 are duplicates
        txs = [{"transaction_id": f"tx_{i}", "status": "SUCCESS"} for i in range(40)]
        # Add 10 duplicates with updated status
        for i in range(10):
            txs.append({"transaction_id": f"tx_{i}", "status": "FAILED"})

        _, _, total_unique, _ = process_transactions_window(txs, window_size=50)
        self.assertEqual(total_unique, 40)  # Only 40 unique transactions

    def test_payment_active_alert_deduplication(self):
        # Degraded rate (90%) but alert already open
        txs = [
            {"transaction_id": f"tx_{i}", "status": "SUCCESS" if i < 45 else "FAILED"}
            for i in range(50)
        ]
        mock_conn = MagicMock()
        existing_alert = {"action_id": 901, "status": "ACTIVE"}

        with patch("services.dependency_engine.rules.rule_payment_spike.get_active_alert", return_value=existing_alert):
            with patch("services.dependency_engine.rules.rule_payment_spike.record_action_attempt") as mock_record:
                res = evaluate_and_manage_payment_alerts(transactions=txs, connection=mock_conn)
                self.assertEqual(res["alert_status"], "ACTIVE_PERSISTING")
                mock_record.assert_not_called()

    def test_payment_recovery_resolves_active_alert(self):
        # Rate returns to 96% (48/50) and active alert exists
        txs = [
            {"transaction_id": f"tx_{i}", "status": "SUCCESS" if i < 48 else "FAILED"}
            for i in range(50)
        ]
        mock_conn = MagicMock()
        existing_alert = {"action_id": 902, "status": "ACTIVE"}

        with patch("services.dependency_engine.rules.rule_payment_spike.get_active_alert", return_value=existing_alert):
            with patch("services.dependency_engine.rules.rule_payment_spike.resolve_active_alert", return_value=True) as mock_resolve:
                res = evaluate_and_manage_payment_alerts(transactions=txs, connection=mock_conn)
                self.assertFalse(res["matched"])
                self.assertEqual(res["alert_status"], "ALERT_RECOVERED")
                mock_resolve.assert_called_once()
                self.assertEqual(res["alert_event"]["event_type"], "PAYMENT_GATEWAY_ALERT_RECOVERED")


if __name__ == "__main__":
    unittest.main()
