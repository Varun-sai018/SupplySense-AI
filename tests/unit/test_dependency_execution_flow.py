"""
Unit tests for Dependency Engine with Pipeline Runner integration (Phase 4).
Tests BLOCK vs TRIGGER runner invocation, idempotency, and error handling.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch
import pymysql

# Ensure repo root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import importlib
dep_engine = importlib.import_module('services.dependency-engine.main')
process_event = dep_engine.process_event


class TestDependencyExecutionFlow(unittest.TestCase):

    def setUp(self):
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value = self.mock_cursor

    def test_block_decision_never_invokes_runner(self):
        """1. When evaluator returns BLOCK, pipeline runner must NEVER be invoked."""
        # 1. Pipeline dependency setup
        self.mock_cursor.fetchone.side_effect = [
            {"dependency_id": 1, "pipeline_name": "Demand Forecast Pipeline", "condition_type": "ALL", "required_count": None}, # dependency
            None, # Idempotency check: not already triggered
        ]
        # 2. Datasets: only 1 of 3 READY
        self.mock_cursor.fetchall.return_value = [
            {"dataset_id": 1, "dataset_name": "Orders", "status": "READY", "current_version": 1},
            {"dataset_id": 2, "dataset_name": "Products", "status": "WAITING", "current_version": 1},
            {"dataset_id": 3, "dataset_name": "Sellers", "status": "WAITING", "current_version": 1},
        ]

        with patch.object(dep_engine, 'execute_pipeline') as mock_execute_pipeline:
            event = {"event_id": 501, "dataset_name": "Orders", "dataset_version": 1, "rows_changed": 10}
            decision, reason = process_event(event, self.mock_conn)

            self.assertEqual(decision, "BLOCK")
            mock_execute_pipeline.assert_not_called()
            self.mock_conn.commit.assert_called()

    def test_trigger_decision_invokes_runner_and_resets_cycle(self):
        """2. When evaluator returns TRIGGER, execution record is created and runner is invoked exactly once."""
        self.mock_cursor.fetchone.side_effect = [
            {"dependency_id": 1, "pipeline_name": "Demand Forecast Pipeline", "condition_type": "ALL", "required_count": None},
            None, # Idempotency check: not triggered
        ]
        self.mock_cursor.fetchall.return_value = [
            {"dataset_id": 1, "dataset_name": "Orders", "status": "READY", "current_version": 1},
            {"dataset_id": 2, "dataset_name": "Products", "status": "READY", "current_version": 1},
            {"dataset_id": 3, "dataset_name": "Sellers", "status": "READY", "current_version": 1},
        ]
        self.mock_cursor.lastrowid = 101  # execution_id / decision_id

        with patch.object(dep_engine, 'execute_pipeline') as mock_execute_pipeline:
            mock_execute_pipeline.return_value = {
                "execution_id": 101,
                "pipeline_name": "Demand Forecast Pipeline",
                "status": "COMPLETED",
                "output_location": "ml/results/baseline_metrics.json"
            }

            event = {"event_id": 502, "dataset_name": "Sellers", "dataset_version": 1, "rows_changed": 10}
            decision, reason = process_event(event, self.mock_conn)

            self.assertEqual(decision, "TRIGGER")
            mock_execute_pipeline.assert_called_once_with(101, "Demand Forecast Pipeline", self.mock_conn)

    def test_duplicate_event_skips_and_never_invokes_runner(self):
        """3. Duplicate event (already executed in DB) returns SKIPPED and does not invoke runner."""
        self.mock_cursor.fetchone.side_effect = [
            {"dependency_id": 1, "pipeline_name": "Demand Forecast Pipeline", "condition_type": "ALL", "required_count": None},
            {"execution_id": 99}, # Already executed
        ]

        with patch.object(dep_engine, 'execute_pipeline') as mock_execute_pipeline:
            event = {"event_id": 503, "dataset_name": "Orders", "dataset_version": 1, "rows_changed": 10}
            decision, reason = process_event(event, self.mock_conn)

            self.assertEqual(decision, "SKIPPED")
            self.assertIn("already triggered", reason)
            mock_execute_pipeline.assert_not_called()

    def test_concurrent_duplicate_integrity_error_handled(self):
        """4. Race condition leading to MySQL IntegrityError aborts execution without calling runner."""
        self.mock_cursor.fetchone.side_effect = [
            {"dependency_id": 1, "pipeline_name": "Demand Forecast Pipeline", "condition_type": "ALL", "required_count": None},
            None, # Pre-check passes
        ]
        self.mock_cursor.fetchall.return_value = [
            {"dataset_id": 1, "dataset_name": "Orders", "status": "READY", "current_version": 1},
        ]
        # Simulate race condition: INSERT INTO pipeline_executions throws IntegrityError
        self.mock_cursor.execute.side_effect = [
            None, # SELECT dependency
            None, # SELECT idempotency
            None, # SELECT datasets
            None, # INSERT pipeline_decisions
            pymysql.err.IntegrityError(1062, "Duplicate entry for key 'idx_unique_pipeline_event'")
        ]

        with patch.object(dep_engine, 'execute_pipeline') as mock_execute_pipeline:
            event = {"event_id": 504, "dataset_name": "Orders", "dataset_version": 1, "rows_changed": 10}
            decision, reason = process_event(event, self.mock_conn)

            self.assertEqual(decision, "TRIGGER")
            mock_execute_pipeline.assert_not_called()
            self.mock_conn.rollback.assert_called()


if __name__ == '__main__':
    unittest.main()
