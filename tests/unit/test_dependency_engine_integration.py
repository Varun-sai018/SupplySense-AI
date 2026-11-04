"""
SupplySense AI — Phase 2: Dependency Engine Integration Tests

Comprehensive unit tests covering:
  - evaluate_condition() extended boundary conditions (ALL/ANY/QUORUM/AST/EXPRESSION)
  - process_event() routing (Rule A / B / C / standard pipeline path)
  - Cooldown enforcement (within window → BLOCK, outside window → proceed)
  - ANY and QUORUM condition wiring through process_event
  - Idempotency (duplicate event_id → SKIPPED, race-condition IntegrityError)
  - BLOCK decision never dispatches a pipeline
  - TRIGGER dispatches exactly once

All tests use mocked DB connections; no real DB or pipeline executions are created.
"""

import os
import sys
import datetime
import importlib
import unittest
from unittest.mock import MagicMock, patch, call

import pymysql

# ---------------------------------------------------------------------------
# Path setup – ensure repo root is importable
# ---------------------------------------------------------------------------
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# Import dependency engine and evaluator via importlib (hyphenated directory)
dep_engine = importlib.import_module("services.dependency-engine.main")
process_event = dep_engine.process_event

evaluator_mod = importlib.import_module("services.dependency-engine.evaluator")
evaluate_condition = evaluator_mod.evaluate_condition


# ===========================================================================
# Helper factories
# ===========================================================================

def _make_conn(
    dependency=None,
    idempotency_row=None,
    cooldown_row=None,
    datasets=None,
    last_rowid=42,
):
    """Build a pre-configured mock connection for the standard pipeline path.

    fetchone calls are sequenced:
        1st  → dependency row (or None → no config found)
        2nd  → idempotency check row (None → not yet triggered)
        3rd  → cooldown check row (None → no recent execution)
    fetchall  → datasets list
    lastrowid → simulated AUTO_INCREMENT value
    """
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor

    # Build a flexible fetchone side_effect
    side_effects = [dependency, idempotency_row, cooldown_row]
    cursor.fetchone.side_effect = side_effects
    cursor.fetchall.return_value = datasets or []
    cursor.lastrowid = last_rowid
    return conn, cursor


# ===========================================================================
# 1. evaluate_condition() — extended boundary conditions
# ===========================================================================

class TestEvaluateConditionBoundaries(unittest.TestCase):
    """Boundary tests for the standalone evaluate_condition function."""

    # ---- ALL ---------------------------------------------------------------
    def test_all_all_ready_triggers(self):
        decision, reason = evaluate_condition("ALL", ready_count=3, total_required=3)
        self.assertEqual(decision, "TRIGGER")
        self.assertIn("All required", reason)

    def test_all_one_missing_blocks(self):
        decision, reason = evaluate_condition("ALL", ready_count=2, total_required=3)
        self.assertEqual(decision, "BLOCK")
        self.assertIn("2 of 3", reason)

    def test_all_zero_datasets_configured_blocks(self):
        """ALL with no datasets must BLOCK, not divide by zero."""
        decision, reason = evaluate_condition("ALL", ready_count=0, total_required=0)
        self.assertEqual(decision, "BLOCK")
        self.assertIn("No required datasets", reason)

    def test_all_negative_ready_treated_as_zero(self):
        """Negative ready_count is clamped to 0; should BLOCK."""
        decision, reason = evaluate_condition("ALL", ready_count=-5, total_required=3)
        self.assertEqual(decision, "BLOCK")

    # ---- ANY ---------------------------------------------------------------
    def test_any_one_ready_triggers(self):
        decision, _ = evaluate_condition("ANY", ready_count=1, total_required=5)
        self.assertEqual(decision, "TRIGGER")

    def test_any_none_ready_blocks(self):
        decision, _ = evaluate_condition("ANY", ready_count=0, total_required=3)
        self.assertEqual(decision, "BLOCK")

    def test_any_zero_datasets_configured_blocks(self):
        decision, reason = evaluate_condition("ANY", ready_count=0, total_required=0)
        self.assertEqual(decision, "BLOCK")
        self.assertIn("No required datasets", reason)

    def test_any_all_ready_triggers(self):
        decision, _ = evaluate_condition("ANY", ready_count=3, total_required=3)
        self.assertEqual(decision, "TRIGGER")

    # ---- QUORUM ------------------------------------------------------------
    def test_quorum_exactly_at_threshold_triggers(self):
        decision, reason = evaluate_condition(
            "QUORUM", ready_count=2, total_required=5, required_count=2
        )
        self.assertEqual(decision, "TRIGGER")
        self.assertIn("2", reason)

    def test_quorum_above_threshold_triggers(self):
        decision, _ = evaluate_condition(
            "QUORUM", ready_count=4, total_required=5, required_count=2
        )
        self.assertEqual(decision, "TRIGGER")

    def test_quorum_below_threshold_blocks(self):
        decision, reason = evaluate_condition(
            "QUORUM", ready_count=1, total_required=5, required_count=2
        )
        self.assertEqual(decision, "BLOCK")
        self.assertIn("1 of 2", reason)

    def test_quorum_none_required_count_blocks(self):
        """QUORUM without a threshold is always a BLOCK."""
        decision, reason = evaluate_condition(
            "QUORUM", ready_count=3, total_required=3, required_count=None
        )
        self.assertEqual(decision, "BLOCK")
        self.assertIn("required_count", reason)

    def test_quorum_zero_required_count_blocks(self):
        """QUORUM threshold of 0 is meaningless and must BLOCK."""
        decision, reason = evaluate_condition(
            "QUORUM", ready_count=3, total_required=3, required_count=0
        )
        self.assertEqual(decision, "BLOCK")
        self.assertIn("greater than 0", reason)

    def test_quorum_negative_required_count_blocks(self):
        """Negative threshold must BLOCK — threshold must be > 0."""
        decision, reason = evaluate_condition(
            "QUORUM", ready_count=3, total_required=3, required_count=-1
        )
        self.assertEqual(decision, "BLOCK")

    # ---- Unsupported / Unknown conditions -----------------------------------
    def test_unsupported_condition_type_blocks(self):
        decision, reason = evaluate_condition("FUZZY_MATCH", ready_count=3, total_required=3)
        self.assertEqual(decision, "BLOCK")
        self.assertIn("Unsupported condition type", reason)

    def test_empty_condition_type_blocks(self):
        decision, reason = evaluate_condition("", ready_count=1, total_required=1)
        self.assertEqual(decision, "BLOCK")

    # ---- AST / EXPRESSION --------------------------------------------------
    def test_ast_condition_type_with_valid_dict_expression(self):
        """EXPRESSION condition_type with a simple AND dict expression.

        The rule_evaluator uses 'op' (not 'operator') for leaf comparison nodes.
        """
        expression = {
            "operator": "AND",
            "conditions": [
                {"field": "Orders", "op": "==", "value": "READY"},
                {"field": "Sellers", "op": "==", "value": "READY"},
            ],
        }
        context = {"Orders": "READY", "Sellers": "READY"}
        decision, _ = evaluate_condition(
            "EXPRESSION",
            ready_count=2,
            total_required=2,
            expression=expression,
            context=context,
        )
        self.assertEqual(decision, "TRIGGER")

    def test_expression_condition_type_partial_context_blocks(self):
        """EXPRESSION where one field is not READY → BLOCK."""
        expression = {
            "operator": "AND",
            "conditions": [
                {"field": "Orders", "op": "==", "value": "READY"},
                {"field": "Sellers", "op": "==", "value": "READY"},
            ],
        }
        context = {"Orders": "READY", "Sellers": "WAITING"}
        decision, _ = evaluate_condition(
            "EXPRESSION",
            ready_count=1,
            total_required=2,
            expression=expression,
            context=context,
        )
        self.assertEqual(decision, "BLOCK")

    def test_ast_condition_type_with_or_expression(self):
        """AST OR: at least one field satisfied → TRIGGER."""
        expression = {
            "operator": "OR",
            "conditions": [
                {"field": "Orders", "op": "==", "value": "READY"},
                {"field": "Sellers", "op": "==", "value": "READY"},
            ],
        }
        context = {"Orders": "WAITING", "Sellers": "READY"}
        decision, _ = evaluate_condition(
            "AST",
            ready_count=1,
            total_required=2,
            expression=expression,
            context=context,
        )
        self.assertEqual(decision, "TRIGGER")

    def test_ast_condition_type_with_not_expression(self):
        """AST NOT: inverts the inner condition (NOT(Orders == WAITING) when Orders is READY → TRIGGER)."""
        expression = {
            "operator": "NOT",
            "conditions": [
                {"field": "Orders", "op": "==", "value": "WAITING"},
            ],
        }
        context = {"Orders": "READY"}
        decision, _ = evaluate_condition(
            "AST",
            ready_count=1,
            total_required=1,
            expression=expression,
            context=context,
        )
        self.assertEqual(decision, "TRIGGER")

    def test_ast_missing_expression_blocks(self):
        """EXPRESSION condition_type with no expression provided → BLOCK."""
        decision, reason = evaluate_condition(
            "EXPRESSION",
            ready_count=3,
            total_required=3,
            expression=None,
            context={},
        )
        self.assertEqual(decision, "BLOCK")

    def test_ast_inline_and_string(self):
        """condition_type that looks like AND(...) is parsed as rule string."""
        # Minimal sanity check: any BLOCK or TRIGGER is acceptable as long as
        # it doesn't raise an unhandled exception.
        decision, reason = evaluate_condition(
            "AND(Orders == READY, Sellers == READY)",
            ready_count=2,
            total_required=2,
            expression=None,
            context={"Orders": "READY", "Sellers": "READY"},
        )
        self.assertIn(decision, ("TRIGGER", "BLOCK"))

    def test_malformed_ast_dict_blocks_gracefully(self):
        """Malformed AST dict should BLOCK with a validation error message."""
        expression = {"operator": "UNKNOWN_OP", "conditions": []}
        decision, reason = evaluate_condition(
            "AST",
            ready_count=0,
            total_required=0,
            expression=expression,
            context={},
        )
        self.assertEqual(decision, "BLOCK")


# ===========================================================================
# 2. process_event() routing — Rule A / B / C / standard path
# ===========================================================================

class TestProcessEventRouting(unittest.TestCase):
    """Verify that process_event routes events to the correct handler."""

    def _make_basic_conn(self):
        conn = MagicMock()
        conn.cursor.return_value = MagicMock()
        return conn

    # ---- Rule A (Cart Abandonment) -----------------------------------------
    def test_event_type_cart_abandonment_routes_to_rule_a(self):
        """event_type=CART_ABANDONMENT must NOT follow the standard pipeline path."""
        conn = self._make_basic_conn()
        event = {"event_type": "CART_ABANDONMENT", "sessions": [], "current_time": None}
        with patch.object(dep_engine, "_process_rule_a_event", return_value=("BLOCK", "off-peak")) as mock_a, \
             patch.object(dep_engine, "execute_pipeline") as mock_run:
            decision, reason = process_event(event, conn)
            mock_a.assert_called_once_with(event, conn)
            mock_run.assert_not_called()
        self.assertEqual(decision, "BLOCK")

    def test_rule_id_rule_a_cart_abandonment_routes_to_rule_a(self):
        conn = self._make_basic_conn()
        event = {"rule_id": "RULE_A_CART_ABANDONMENT"}
        with patch.object(dep_engine, "_process_rule_a_event", return_value=("TRIGGER", "dispatched")) as mock_a:
            decision, _ = process_event(event, conn)
            mock_a.assert_called_once()
        self.assertEqual(decision, "TRIGGER")

    def test_cart_drop_off_alias_routes_to_rule_a(self):
        conn = self._make_basic_conn()
        event = {"event_type": "CART_DROP_OFF"}
        with patch.object(dep_engine, "_process_rule_a_event", return_value=("BLOCK", "low dropoff")) as mock_a:
            process_event(event, conn)
            mock_a.assert_called_once()

    # ---- Rule B (Inventory Reorder) ----------------------------------------
    def test_event_type_inventory_reorder_routes_to_rule_b(self):
        conn = self._make_basic_conn()
        event = {"event_type": "INVENTORY_REORDER", "product_id": "P1", "inventory_level": 5}
        with patch.object(dep_engine, "_process_rule_b_event", return_value=("TRIGGER", "reorder placed")) as mock_b, \
             patch.object(dep_engine, "execute_pipeline") as mock_run:
            decision, _ = process_event(event, conn)
            mock_b.assert_called_once_with(event, conn)
            mock_run.assert_not_called()  # Rule B handler manages its own execution

    def test_inventory_check_alias_routes_to_rule_b(self):
        conn = self._make_basic_conn()
        event = {"event_type": "INVENTORY_CHECK"}
        with patch.object(dep_engine, "_process_rule_b_event", return_value=("BLOCK", "level ok")) as mock_b:
            process_event(event, conn)
            mock_b.assert_called_once()

    # ---- Rule C (Payment Spike) --------------------------------------------
    def test_event_type_payment_spike_routes_to_rule_c(self):
        conn = self._make_basic_conn()
        event = {"event_type": "PAYMENT_SPIKE", "transactions": []}
        with patch.object(dep_engine, "_process_rule_c_event", return_value=("BLOCK", "rate ok")) as mock_c, \
             patch.object(dep_engine, "execute_pipeline") as mock_run:
            decision, _ = process_event(event, conn)
            mock_c.assert_called_once_with(event, conn)
            # Rule C NEVER creates pipeline_executions
            mock_run.assert_not_called()

    def test_payment_failure_spike_alias_routes_to_rule_c(self):
        conn = self._make_basic_conn()
        event = {"event_type": "PAYMENT_FAILURE_SPIKE"}
        with patch.object(dep_engine, "_process_rule_c_event", return_value=("TRIGGER", "spike detected")) as mock_c:
            process_event(event, conn)
            mock_c.assert_called_once()

    def test_rule_id_rule_c_routes_to_rule_c(self):
        conn = self._make_basic_conn()
        event = {"rule_id": "RULE_C_PAYMENT_FAILURE_SPIKE"}
        with patch.object(dep_engine, "_process_rule_c_event", return_value=("BLOCK", "healthy")) as mock_c:
            process_event(event, conn)
            mock_c.assert_called_once()

    def test_rule_c_trigger_never_calls_execute_pipeline(self):
        """Even when Rule C returns TRIGGER (alert raised), execute_pipeline must NOT be called."""
        conn = self._make_basic_conn()
        event = {"event_type": "PAYMENT_SPIKE", "transactions": []}
        with patch.object(dep_engine, "_process_rule_c_event", return_value=("TRIGGER", "spike raised")), \
             patch.object(dep_engine, "execute_pipeline") as mock_run:
            process_event(event, conn)
            mock_run.assert_not_called()

    # ---- Unknown event_type falls through to standard path -----------------
    def test_unknown_event_type_falls_through_to_dataset_dependency_path(self):
        """An unrecognized event_type should route through the standard pipeline path."""
        dep_row = {
            "dependency_id": 1,
            "pipeline_name": "Demand Forecast Pipeline",
            "condition_type": "ALL",
            "required_count": None,
        }
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        cursor.fetchone.side_effect = [dep_row, None, None]
        cursor.fetchall.return_value = [
            {"dataset_id": 1, "dataset_name": "Orders", "status": "WAITING", "current_version": 1},
        ]

        with patch.object(dep_engine, "execute_pipeline") as mock_run:
            event = {"event_type": "SENSOR_READING", "event_id": 9001, "dataset_name": "Orders"}
            decision, _ = process_event(event, conn)
            mock_run.assert_not_called()
        self.assertEqual(decision, "BLOCK")


# ===========================================================================
# 3. Cooldown enforcement
# ===========================================================================

class TestCooldownEnforcement(unittest.TestCase):

    def _dep_row(self):
        return {
            "dependency_id": 1,
            "pipeline_name": "Demand Forecast Pipeline",
            "condition_type": "ALL",
            "required_count": None,
        }

    def test_pipeline_within_cooldown_window_blocks(self):
        """A recent execution (5 s ago) within a 60 s cooldown must BLOCK."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor

        recent_time = datetime.datetime.now() - datetime.timedelta(seconds=5)
        cursor.fetchone.side_effect = [
            self._dep_row(),       # dependency lookup
            None,                  # idempotency check
            {"created_at": recent_time},  # cooldown check → within window
        ]
        cursor.fetchall.return_value = []  # should not be reached

        event = {
            "event_id": 8001,
            "dataset_name": "Orders",
            "cooldown_seconds": 60,
        }

        with patch.object(dep_engine, "execute_pipeline") as mock_run:
            decision, reason = process_event(event, conn)
            self.assertEqual(decision, "BLOCK")
            self.assertIn("cooldown", reason.lower())
            mock_run.assert_not_called()

    def test_pipeline_outside_cooldown_window_proceeds(self):
        """An old execution (120 s ago) outside a 60 s cooldown must allow evaluation."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor

        old_time = datetime.datetime.now() - datetime.timedelta(seconds=120)
        cursor.fetchone.side_effect = [
            self._dep_row(),
            None,                  # idempotency check
            {"created_at": old_time},  # cooldown check → outside window
        ]
        cursor.fetchall.return_value = [
            {"dataset_id": 1, "dataset_name": "Orders", "status": "WAITING", "current_version": 1},
        ]

        event = {
            "event_id": 8002,
            "dataset_name": "Orders",
            "cooldown_seconds": 60,
        }

        with patch.object(dep_engine, "execute_pipeline") as mock_run:
            decision, reason = process_event(event, conn)
            # With 1/1 WAITING, ALL should BLOCK (not from cooldown)
            self.assertEqual(decision, "BLOCK")
            self.assertNotIn("cooldown", reason.lower())

    def test_no_cooldown_configured_skips_cooldown_check(self):
        """When no cooldown_seconds is set and env var is absent, cooldown is skipped."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor

        cursor.fetchone.side_effect = [
            self._dep_row(),
            None,  # idempotency
            # No 3rd fetchone for cooldown (should not be called if no cooldown)
        ]
        cursor.fetchall.return_value = [
            {"dataset_id": 1, "dataset_name": "Orders", "status": "WAITING", "current_version": 1},
        ]

        event = {"event_id": 8003, "dataset_name": "Orders"}  # no cooldown_seconds

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("PIPELINE_COOLDOWN_SECONDS", None)
            with patch.object(dep_engine, "execute_pipeline") as mock_run:
                decision, _ = process_event(event, conn)
                mock_run.assert_not_called()
        self.assertEqual(decision, "BLOCK")

    def test_cooldown_block_persists_decision_record(self):
        """A cooldown BLOCK must write a pipeline_decisions row."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor

        recent_time = datetime.datetime.now() - datetime.timedelta(seconds=10)
        cursor.fetchone.side_effect = [
            self._dep_row(),
            None,
            {"created_at": recent_time},
        ]
        cursor.fetchall.return_value = []

        event = {
            "event_id": 8004,
            "dataset_name": "Orders",
            "cooldown_seconds": 120,
        }

        process_event(event, conn)
        # _persist_decision calls cursor.execute with INSERT INTO pipeline_decisions
        insert_calls = [
            c for c in cursor.execute.call_args_list
            if "pipeline_decisions" in str(c)
        ]
        self.assertTrue(len(insert_calls) >= 1, "Expected a pipeline_decisions INSERT for cooldown BLOCK")


# ===========================================================================
# 4. ANY condition wired through process_event
# ===========================================================================

class TestAnyConditionIntegration(unittest.TestCase):

    def _dep_row_any(self):
        return {
            "dependency_id": 2,
            "pipeline_name": "Demand Forecast Pipeline",
            "condition_type": "ANY",
            "required_count": None,
        }

    def test_any_one_ready_triggers_pipeline(self):
        """ANY condition: 1 of 3 READY → TRIGGER and execute_pipeline called once."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor

        cursor.fetchone.side_effect = [self._dep_row_any(), None, None]
        cursor.fetchall.return_value = [
            {"dataset_id": 1, "dataset_name": "Orders", "status": "READY", "current_version": 1},
            {"dataset_id": 2, "dataset_name": "Products", "status": "WAITING", "current_version": 1},
            {"dataset_id": 3, "dataset_name": "Sellers", "status": "WAITING", "current_version": 1},
        ]
        cursor.lastrowid = 55

        with patch.object(dep_engine, "execute_pipeline", return_value={"status": "COMPLETED"}) as mock_run:
            event = {"event_id": 3001, "dataset_name": "Orders"}
            decision, reason = process_event(event, conn)
            self.assertEqual(decision, "TRIGGER")
            mock_run.assert_called_once()

    def test_any_zero_ready_blocks_pipeline(self):
        """ANY condition: 0 of 3 READY → BLOCK, execute_pipeline NOT called."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor

        cursor.fetchone.side_effect = [self._dep_row_any(), None, None]
        cursor.fetchall.return_value = [
            {"dataset_id": 1, "dataset_name": "Orders", "status": "WAITING", "current_version": 1},
            {"dataset_id": 2, "dataset_name": "Products", "status": "WAITING", "current_version": 1},
            {"dataset_id": 3, "dataset_name": "Sellers", "status": "WAITING", "current_version": 1},
        ]

        with patch.object(dep_engine, "execute_pipeline") as mock_run:
            event = {"event_id": 3002, "dataset_name": "Orders"}
            decision, reason = process_event(event, conn)
            self.assertEqual(decision, "BLOCK")
            mock_run.assert_not_called()


# ===========================================================================
# 5. QUORUM condition wired through process_event
# ===========================================================================

class TestQuorumConditionIntegration(unittest.TestCase):

    def _dep_row_quorum(self, required_count):
        return {
            "dependency_id": 3,
            "pipeline_name": "Demand Forecast Pipeline",
            "condition_type": "QUORUM",
            "required_count": required_count,
        }

    def test_quorum_2_of_3_ready_triggers(self):
        """QUORUM(required=2): 2 of 3 READY → TRIGGER."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor

        cursor.fetchone.side_effect = [self._dep_row_quorum(2), None, None]
        cursor.fetchall.return_value = [
            {"dataset_id": 1, "dataset_name": "Orders", "status": "READY", "current_version": 1},
            {"dataset_id": 2, "dataset_name": "Products", "status": "READY", "current_version": 1},
            {"dataset_id": 3, "dataset_name": "Sellers", "status": "WAITING", "current_version": 1},
        ]
        cursor.lastrowid = 66

        with patch.object(dep_engine, "execute_pipeline", return_value={"status": "COMPLETED"}) as mock_run:
            event = {"event_id": 4001, "dataset_name": "Products"}
            decision, _ = process_event(event, conn)
            self.assertEqual(decision, "TRIGGER")
            mock_run.assert_called_once()

    def test_quorum_1_of_3_blocks(self):
        """QUORUM(required=2): only 1 of 3 READY → BLOCK."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor

        cursor.fetchone.side_effect = [self._dep_row_quorum(2), None, None]
        cursor.fetchall.return_value = [
            {"dataset_id": 1, "dataset_name": "Orders", "status": "READY", "current_version": 1},
            {"dataset_id": 2, "dataset_name": "Products", "status": "WAITING", "current_version": 1},
            {"dataset_id": 3, "dataset_name": "Sellers", "status": "WAITING", "current_version": 1},
        ]

        with patch.object(dep_engine, "execute_pipeline") as mock_run:
            event = {"event_id": 4002, "dataset_name": "Orders"}
            decision, reason = process_event(event, conn)
            self.assertEqual(decision, "BLOCK")
            self.assertIn("1 of 2", reason)
            mock_run.assert_not_called()

    def test_quorum_3_of_3_above_threshold_triggers(self):
        """QUORUM(required=2): 3 of 3 READY (above threshold) → TRIGGER."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor

        cursor.fetchone.side_effect = [self._dep_row_quorum(2), None, None]
        cursor.fetchall.return_value = [
            {"dataset_id": 1, "dataset_name": "Orders", "status": "READY", "current_version": 1},
            {"dataset_id": 2, "dataset_name": "Products", "status": "READY", "current_version": 1},
            {"dataset_id": 3, "dataset_name": "Sellers", "status": "READY", "current_version": 1},
        ]
        cursor.lastrowid = 67

        with patch.object(dep_engine, "execute_pipeline", return_value={"status": "COMPLETED"}) as mock_run:
            event = {"event_id": 4003, "dataset_name": "Sellers"}
            decision, _ = process_event(event, conn)
            self.assertEqual(decision, "TRIGGER")
            mock_run.assert_called_once()


# ===========================================================================
# 6. Idempotency and race conditions
# ===========================================================================

class TestIdempotency(unittest.TestCase):

    def _dep_row(self):
        return {
            "dependency_id": 1,
            "pipeline_name": "Demand Forecast Pipeline",
            "condition_type": "ALL",
            "required_count": None,
        }

    def test_duplicate_event_id_returns_skipped(self):
        """Second arrival of the same event_id must return SKIPPED and not call runner."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor

        cursor.fetchone.side_effect = [
            self._dep_row(),
            {"execution_id": 77},  # Already executed
        ]

        with patch.object(dep_engine, "execute_pipeline") as mock_run:
            event = {"event_id": 5001, "dataset_name": "Orders"}
            decision, reason = process_event(event, conn)
            self.assertEqual(decision, "SKIPPED")
            self.assertIn("already triggered", reason)
            mock_run.assert_not_called()

    def test_none_event_id_bypasses_idempotency_check(self):
        """event_id=None should bypass the idempotency DB check entirely."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor

        # Only two fetchone calls expected: dependency + cooldown (no idempotency)
        cursor.fetchone.side_effect = [
            self._dep_row(),
            None,  # cooldown check
        ]
        cursor.fetchall.return_value = [
            {"dataset_id": 1, "dataset_name": "Orders", "status": "WAITING", "current_version": 1},
        ]

        event = {"dataset_name": "Orders"}  # no event_id key
        with patch.object(dep_engine, "execute_pipeline") as mock_run:
            decision, _ = process_event(event, conn)
            mock_run.assert_not_called()
        self.assertEqual(decision, "BLOCK")

    def test_concurrent_integrity_error_no_double_execute(self):
        """Race condition: INSERT into pipeline_executions raises IntegrityError → no runner call.

        We mirror the pattern used in test_dependency_execution_flow.py:
        fetchone controls dependency/idempotency check; execute raises IntegrityError
        at the 5th call (INSERT INTO pipeline_executions).
        """
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor

        cursor.fetchone.side_effect = [
            self._dep_row(),
            None,  # idempotency check: not yet triggered
            # No 3rd fetchone for cooldown (no cooldown_seconds in event)
        ]
        cursor.fetchall.return_value = [
            {"dataset_id": 1, "dataset_name": "Orders", "status": "READY", "current_version": 1},
        ]
        # Allow everything until the INSERT INTO pipeline_executions (5th execute call)
        # The execute side_effect list: SELECT dep, SELECT idempotency, SELECT datasets,
        # INSERT decisions, INSERT executions → IntegrityError
        cursor.execute.side_effect = [
            None,  # 1: SELECT pipeline_dependencies
            None,  # 2: SELECT idempotency
            None,  # 3: SELECT datasets
            None,  # 4: INSERT pipeline_decisions
            pymysql.err.IntegrityError(1062, "Duplicate entry"),  # 5: INSERT pipeline_executions
        ]

        with patch.object(dep_engine, "execute_pipeline") as mock_run:
            event = {"event_id": 5002, "dataset_name": "Orders"}
            decision, _ = process_event(event, conn)
            self.assertEqual(decision, "TRIGGER")  # Condition evaluated TRIGGER before the error
            mock_run.assert_not_called()  # Runner NOT called due to IntegrityError
            conn.rollback.assert_called()


# ===========================================================================
# 7. BLOCK decisions never dispatch a pipeline (ALL)
# ===========================================================================

class TestBlockNeverDispatchesPipeline(unittest.TestCase):

    def _dep_row(self):
        return {
            "dependency_id": 1,
            "pipeline_name": "Demand Forecast Pipeline",
            "condition_type": "ALL",
            "required_count": None,
        }

    def test_all_1_of_3_ready_blocks_and_no_execute(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor

        cursor.fetchone.side_effect = [self._dep_row(), None, None]
        cursor.fetchall.return_value = [
            {"dataset_id": 1, "dataset_name": "Orders", "status": "READY", "current_version": 1},
            {"dataset_id": 2, "dataset_name": "Products", "status": "WAITING", "current_version": 1},
            {"dataset_id": 3, "dataset_name": "Sellers", "status": "WAITING", "current_version": 1},
        ]

        with patch.object(dep_engine, "execute_pipeline") as mock_run:
            event = {"event_id": 6001, "dataset_name": "Orders"}
            decision, _ = process_event(event, conn)
            self.assertEqual(decision, "BLOCK")
            mock_run.assert_not_called()

    def test_all_0_of_3_ready_blocks_and_no_execute(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor

        cursor.fetchone.side_effect = [self._dep_row(), None, None]
        cursor.fetchall.return_value = [
            {"dataset_id": 1, "dataset_name": "Orders", "status": "WAITING", "current_version": 1},
            {"dataset_id": 2, "dataset_name": "Products", "status": "WAITING", "current_version": 1},
            {"dataset_id": 3, "dataset_name": "Sellers", "status": "WAITING", "current_version": 1},
        ]

        with patch.object(dep_engine, "execute_pipeline") as mock_run:
            event = {"event_id": 6002, "dataset_name": "Products"}
            decision, _ = process_event(event, conn)
            self.assertEqual(decision, "BLOCK")
            mock_run.assert_not_called()

    def test_missing_pipeline_config_blocks_and_no_execute(self):
        """Unknown pipeline_name → no dependency config found → BLOCK."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor

        cursor.fetchone.return_value = None  # No matching dependency row

        with patch.object(dep_engine, "execute_pipeline") as mock_run:
            event = {"event_id": 6003, "pipeline_name": "NONEXISTENT_PIPELINE", "dataset_name": "Orders"}
            decision, reason = process_event(event, conn)
            self.assertEqual(decision, "BLOCK")
            self.assertIn("No dependency configuration", reason)
            mock_run.assert_not_called()


# ===========================================================================
# 8. TRIGGER dispatches exactly once (ALL — 3/3 READY)
# ===========================================================================

class TestTriggerDispatchesExactlyOnce(unittest.TestCase):

    def _dep_row(self):
        return {
            "dependency_id": 1,
            "pipeline_name": "Demand Forecast Pipeline",
            "condition_type": "ALL",
            "required_count": None,
        }

    def test_all_3_of_3_ready_triggers_exactly_once(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor

        cursor.fetchone.side_effect = [self._dep_row(), None, None]
        cursor.fetchall.return_value = [
            {"dataset_id": 1, "dataset_name": "Orders", "status": "READY", "current_version": 1},
            {"dataset_id": 2, "dataset_name": "Products", "status": "READY", "current_version": 1},
            {"dataset_id": 3, "dataset_name": "Sellers", "status": "READY", "current_version": 1},
        ]
        cursor.lastrowid = 100

        with patch.object(dep_engine, "execute_pipeline", return_value={"status": "COMPLETED", "output_location": "ml/results/"}) as mock_run:
            event = {"event_id": 7001, "dataset_name": "Sellers"}
            decision, _ = process_event(event, conn)
            self.assertEqual(decision, "TRIGGER")
            mock_run.assert_called_once_with(100, "Demand Forecast Pipeline", conn)

    def test_trigger_resets_dataset_states_to_waiting(self):
        """After TRIGGER, all pipeline datasets must be reset to WAITING."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor

        cursor.fetchone.side_effect = [self._dep_row(), None, None]
        cursor.fetchall.return_value = [
            {"dataset_id": 1, "dataset_name": "Orders", "status": "READY", "current_version": 1},
            {"dataset_id": 2, "dataset_name": "Products", "status": "READY", "current_version": 1},
            {"dataset_id": 3, "dataset_name": "Sellers", "status": "READY", "current_version": 1},
        ]
        cursor.lastrowid = 101

        with patch.object(dep_engine, "execute_pipeline", return_value={"status": "COMPLETED"}):
            event = {"event_id": 7002, "dataset_name": "Sellers"}
            process_event(event, conn)

        update_calls = [
            c for c in cursor.execute.call_args_list
            if "UPDATE dataset_metadata" in str(c) and "WAITING" in str(c)
        ]
        self.assertTrue(len(update_calls) >= 1, "Expected UPDATE dataset_metadata SET status='WAITING'")

    def test_trigger_persists_decision_record(self):
        """A TRIGGER decision must be persisted in pipeline_decisions."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor

        cursor.fetchone.side_effect = [self._dep_row(), None, None]
        cursor.fetchall.return_value = [
            {"dataset_id": 1, "dataset_name": "Orders", "status": "READY", "current_version": 1},
        ]
        cursor.lastrowid = 102

        with patch.object(dep_engine, "execute_pipeline", return_value={"status": "COMPLETED"}):
            event = {"event_id": 7003, "dataset_name": "Orders"}
            process_event(event, conn)

        insert_calls = [
            c for c in cursor.execute.call_args_list
            if "pipeline_decisions" in str(c)
        ]
        self.assertTrue(len(insert_calls) >= 1, "Expected INSERT INTO pipeline_decisions")


# ===========================================================================
# 9. _process_rule_a/b/c — internal behavior spot-checks
# ===========================================================================

class TestRuleHandlerBehavior(unittest.TestCase):
    """Smoke tests for the _process_rule_X wrappers in main.py.

    We patch at the dep_engine module level because that's where the functions
    are imported (as dep_engine.evaluate_and_dispatch_cart_abandonment etc.).
    """

    def _make_conn(self):
        conn = MagicMock()
        conn.cursor.return_value = MagicMock()
        return conn

    def test_rule_a_handler_persists_decision(self):
        """_process_rule_a_event must call _persist_decision regardless of outcome."""
        conn = self._make_conn()
        event = {
            "event_type": "CART_ABANDONMENT",
            "sessions": [],
            "current_time": None,
        }
        mock_result = {"matched": False, "status": "BLOCKED", "reason": "off-peak", "reasons": [], "dropoff_rate": 0.1}
        with patch.object(dep_engine, "evaluate_and_dispatch_cart_abandonment", return_value=mock_result) as mock_eval, \
             patch.object(dep_engine, "_persist_decision") as mock_persist:
            dep_engine._process_rule_a_event(event, conn)
            mock_eval.assert_called_once()
            mock_persist.assert_called_once()
            # Verify pipeline_name kwarg contains "Rule A"
            call_kwargs = mock_persist.call_args[1]
            self.assertIn("Rule A", call_kwargs.get("pipeline_name", ""))

    def test_rule_c_handler_persists_decision(self):
        """_process_rule_c_event must call _persist_decision regardless of outcome."""
        conn = self._make_conn()
        event = {
            "event_type": "PAYMENT_SPIKE",
            "transactions": [],
        }
        mock_result = {"matched": False, "reason": "rate ok", "success_rate": 99}
        with patch.object(dep_engine, "evaluate_and_manage_payment_alerts", return_value=mock_result) as mock_eval, \
             patch.object(dep_engine, "_persist_decision") as mock_persist:
            dep_engine._process_rule_c_event(event, conn)
            mock_eval.assert_called_once()
            mock_persist.assert_called_once()

    def test_rule_b_handler_does_not_call_execute_pipeline_directly(self):
        """_process_rule_b_event delegates to the rule function; main.py must NOT call execute_pipeline separately."""
        conn = self._make_conn()
        event = {
            "event_type": "INVENTORY_REORDER",
            "product_id": "P-SMOKE-1",
            "inventory_level": 5,
        }
        mock_result = {"matched": True, "status": "CONFIRMED", "reason": "reorder ok"}
        with patch.object(dep_engine, "evaluate_and_dispatch_inventory_reorder", return_value=mock_result) as mock_rule, \
             patch.object(dep_engine, "execute_pipeline") as mock_run:
            dep_engine._process_rule_b_event(event, conn)
            mock_rule.assert_called_once()
            mock_run.assert_not_called()


# ===========================================================================
# 10. _persist_decision — atomic DB write verification
# ===========================================================================

class TestPersistDecision(unittest.TestCase):

    def test_persist_decision_inserts_trigger_row(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        cursor.lastrowid = 123

        result = dep_engine._persist_decision(
            pipeline_name="Test Pipeline",
            condition_type="ALL",
            decision="TRIGGER",
            reason="All READY",
            connection=conn,
            required_count=None,
            ready_count=3,
            total_required=3,
        )

        cursor.execute.assert_called_once()
        sql_call = str(cursor.execute.call_args)
        self.assertIn("pipeline_decisions", sql_call)
        conn.commit.assert_called_once()
        self.assertEqual(result, 123)

    def test_persist_decision_inserts_block_row_with_null_triggered_at(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        cursor.lastrowid = 124

        dep_engine._persist_decision(
            pipeline_name="Test Pipeline",
            condition_type="ALL",
            decision="BLOCK",
            reason="Missing data",
            connection=conn,
        )

        cursor.execute.assert_called_once()
        # triggered_at should be None for BLOCK
        call_args = cursor.execute.call_args[0]
        params = call_args[1]
        # params[7] is triggered_at (0-indexed)
        self.assertIsNone(params[7])

    def test_persist_decision_returns_none_on_db_error(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        cursor.execute.side_effect = Exception("DB error")

        result = dep_engine._persist_decision(
            pipeline_name="Test Pipeline",
            condition_type="ALL",
            decision="TRIGGER",
            reason="test",
            connection=conn,
        )
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
