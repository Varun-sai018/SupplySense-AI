"""
Unit tests for Pipeline Execution Reaper / Reconciliation Service (Task 8.5).
Tests detection, conditional atomic transitions, race condition safety, and connection lifecycle.
"""

import os
import sys
import unittest
import datetime
from unittest.mock import MagicMock, patch

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import importlib
reaper_mod = importlib.import_module('services.pipeline-runner.reaper')
reap_stale_executions = reaper_mod.reap_stale_executions


class TestExecutionReaper(unittest.TestCase):

    def setUp(self):
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value = self.mock_cursor

    def test_stale_execution_reaped(self):
        """1. A RUNNING execution older than timeout is marked FAILED."""
        now = datetime.datetime.now()
        stale_time = now - datetime.timedelta(seconds=2000)

        # Mock SELECT result returning 1 stale execution
        self.mock_cursor.fetchall.return_value = [
            {
                "execution_id": 101,
                "pipeline_name": "Demand Forecast Pipeline",
                "status": "RUNNING",
                "started_at": stale_time,
                "created_at": stale_time
            }
        ]
        self.mock_cursor.rowcount = 1

        stats = reap_stale_executions(timeout_seconds=1800, connection=self.mock_conn)

        self.assertEqual(stats["scanned"], 1)
        self.assertEqual(stats["stale_found"], 1)
        self.assertEqual(stats["reconciled"], 1)
        self.assertEqual(stats["skipped"], 0)

        # Verify UPDATE query was executed with status = 'FAILED'
        update_call = self.mock_cursor.execute.call_args_list[1]
        query_sql = update_call[0][0]
        params = update_call[0][1]

        self.assertIn("UPDATE pipeline_executions", query_sql)
        self.assertIn("status = 'FAILED'", query_sql)
        self.assertIn("WHERE execution_id = %s", query_sql)
        self.assertIn("AND status = 'RUNNING'", query_sql)
        self.assertIn("Execution timed out and marked FAILED by reaper", params[1])
        self.assertEqual(params[-1], 101)
        self.mock_conn.commit.assert_called_once()

    def test_recent_running_execution_untouched(self):
        """2. A RUNNING execution younger than timeout is NOT reaped."""
        now = datetime.datetime.now()
        recent_time = now - datetime.timedelta(seconds=60)  # Only 1 min old

        self.mock_cursor.fetchall.return_value = [
            {
                "execution_id": 102,
                "pipeline_name": "Demand Forecast Pipeline",
                "status": "RUNNING",
                "started_at": recent_time,
                "created_at": recent_time
            }
        ]

        stats = reap_stale_executions(timeout_seconds=1800, connection=self.mock_conn)

        self.assertEqual(stats["scanned"], 1)
        self.assertEqual(stats["stale_found"], 0)
        self.assertEqual(stats["reconciled"], 0)
        self.assertEqual(stats["skipped"], 0)

        # Ensure only SELECT was executed, NO UPDATE
        self.assertEqual(self.mock_cursor.execute.call_count, 1)
        self.mock_conn.commit.assert_called_once()

    def test_completed_or_failed_executions_ignored(self):
        """3. Non-RUNNING executions are excluded by query filter."""
        # Query specifically requests WHERE status = 'RUNNING'
        self.mock_cursor.fetchall.return_value = []

        stats = reap_stale_executions(timeout_seconds=1800, connection=self.mock_conn)

        self.assertEqual(stats["scanned"], 0)
        self.assertEqual(stats["stale_found"], 0)
        self.assertEqual(stats["reconciled"], 0)
        self.assertEqual(stats["skipped"], 0)
        self.assertEqual(self.mock_cursor.execute.call_count, 1)

    def test_race_condition_safety_concurrent_completion(self):
        """4. If execution finishes concurrently, rowcount=0 and skipped count increments."""
        now = datetime.datetime.now()
        stale_time = now - datetime.timedelta(seconds=3600)

        self.mock_cursor.fetchall.return_value = [
            {
                "execution_id": 103,
                "pipeline_name": "Demand Forecast Pipeline",
                "status": "RUNNING",
                "started_at": stale_time,
                "created_at": stale_time
            }
        ]
        # Simulate UPDATE matched 0 rows because status changed to COMPLETED before UPDATE
        self.mock_cursor.rowcount = 0

        stats = reap_stale_executions(timeout_seconds=1800, connection=self.mock_conn)

        self.assertEqual(stats["scanned"], 1)
        self.assertEqual(stats["stale_found"], 1)
        self.assertEqual(stats["reconciled"], 0)
        self.assertEqual(stats["skipped"], 1)
        self.mock_conn.commit.assert_called_once()

    def test_custom_timeout_override(self):
        """5. Custom timeout parameter is respected."""
        now = datetime.datetime.now()
        # 120 seconds old -> stale if timeout=60, not stale if default timeout=1800
        mid_time = now - datetime.timedelta(seconds=120)

        self.mock_cursor.fetchall.return_value = [
            {
                "execution_id": 104,
                "pipeline_name": "Demand Forecast Pipeline",
                "status": "RUNNING",
                "started_at": mid_time,
                "created_at": mid_time
            }
        ]
        self.mock_cursor.rowcount = 1

        stats = reap_stale_executions(timeout_seconds=60, connection=self.mock_conn)

        self.assertEqual(stats["scanned"], 1)
        self.assertEqual(stats["stale_found"], 1)
        self.assertEqual(stats["reconciled"], 1)

    def test_fallback_to_created_at_when_started_at_null(self):
        """6. Stale execution with started_at=None falls back to created_at."""
        now = datetime.datetime.now()
        stale_created = now - datetime.timedelta(seconds=2500)

        self.mock_cursor.fetchall.return_value = [
            {
                "execution_id": 105,
                "pipeline_name": "Demand Forecast Pipeline",
                "status": "RUNNING",
                "started_at": None,
                "created_at": stale_created
            }
        ]
        self.mock_cursor.rowcount = 1

        stats = reap_stale_executions(timeout_seconds=1800, connection=self.mock_conn)

        self.assertEqual(stats["scanned"], 1)
        self.assertEqual(stats["stale_found"], 1)
        self.assertEqual(stats["reconciled"], 1)

    def test_multiple_mixed_executions(self):
        """7. Handles multiple executions with mixed states correctly."""
        now = datetime.datetime.now()
        stale_time1 = now - datetime.timedelta(seconds=4000)
        fresh_time = now - datetime.timedelta(seconds=100)
        stale_time2 = now - datetime.timedelta(seconds=5000)

        self.mock_cursor.fetchall.return_value = [
            {"execution_id": 1, "pipeline_name": "Pipeline A", "status": "RUNNING", "started_at": stale_time1},
            {"execution_id": 2, "pipeline_name": "Pipeline B", "status": "RUNNING", "started_at": fresh_time},
            {"execution_id": 3, "pipeline_name": "Pipeline C", "status": "RUNNING", "started_at": stale_time2},
        ]

        # First stale update succeeds (rowcount=1), second stale update encounters race condition (rowcount=0)
        self.mock_cursor.rowcount = 1
        call_count = 0

        def mock_execute(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 3:  # Execution #3 update
                self.mock_cursor.rowcount = 0
            else:
                self.mock_cursor.rowcount = 1

        self.mock_cursor.execute.side_effect = mock_execute

        stats = reap_stale_executions(timeout_seconds=1800, connection=self.mock_conn)

        self.assertEqual(stats["scanned"], 3)
        self.assertEqual(stats["stale_found"], 2)
        self.assertEqual(stats["reconciled"], 1)
        self.assertEqual(stats["skipped"], 1)

    def test_connection_created_and_closed_when_none(self):
        """8. Creates and closes connection if connection=None."""
        mock_auto_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_auto_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []

        with patch.object(reaper_mod, 'get_connection', return_value=mock_auto_conn):
            stats = reap_stale_executions(timeout_seconds=1800, connection=None)

        mock_auto_conn.commit.assert_called_once()
        mock_auto_conn.close.assert_called_once()
        self.assertEqual(stats["scanned"], 0)

    def test_database_error_rolls_back_and_raises(self):
        """9. Database exceptions trigger rollback and bubble up."""
        self.mock_cursor.execute.side_effect = RuntimeError("DB connection lost")

        with self.assertRaises(RuntimeError):
            reap_stale_executions(timeout_seconds=1800, connection=self.mock_conn)

        self.mock_conn.rollback.assert_called_once()


if __name__ == '__main__':
    unittest.main()
