"""
Integration tests for Phase 8 Task 8.5 - Pipeline Execution Reconciliation / Reaper.
Verifies against live MySQL database that stale RUNNING executions are safely marked FAILED,
while recent RUNNING, COMPLETED, and FAILED executions remain untouched.
"""

import os
import sys
import unittest
import datetime

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import importlib
from common.database import get_connection

reaper_mod = importlib.import_module('services.pipeline-runner.reaper')
reap_stale_executions = reaper_mod.reap_stale_executions


class TestPhase8ReaperIntegration(unittest.TestCase):

    def setUp(self):
        self.conn = get_connection()
        self.cur = self.conn.cursor()
        self.test_exec_ids = []
        self.test_decision_ids = []

    def tearDown(self):
        if self.test_exec_ids:
            format_strings = ','.join(['%s'] * len(self.test_exec_ids))
            self.cur.execute(
                f"DELETE FROM pipeline_executions WHERE execution_id IN ({format_strings})",
                tuple(self.test_exec_ids)
            )
        if self.test_decision_ids:
            format_strings = ','.join(['%s'] * len(self.test_decision_ids))
            self.cur.execute(
                f"DELETE FROM pipeline_decisions WHERE decision_id IN ({format_strings})",
                tuple(self.test_decision_ids)
            )
        self.conn.commit()
        self.conn.close()

    def _create_decision(self) -> int:
        self.cur.execute("""
            INSERT INTO pipeline_decisions (
                pipeline_name, condition_type, decision, ready_count, total_required, reason
            ) VALUES (
                'Demand Forecast Pipeline', 'ALL', 'TRIGGER', 3, 3, 'Test trigger for reaper'
            )
        """)
        d_id = self.cur.lastrowid
        self.test_decision_ids.append(d_id)
        return d_id

    def test_live_stale_execution_reaping(self):
        """Verify live MySQL transitions for stale, fresh, completed, and failed executions."""
        now = datetime.datetime.now()
        stale_time = now - datetime.timedelta(seconds=3600)  # 1 hour ago
        fresh_time = now - datetime.timedelta(seconds=60)    # 1 minute ago

        # 1. Insert Stale RUNNING execution
        d1 = self._create_decision()
        self.cur.execute("""
            INSERT INTO pipeline_executions (
                pipeline_name, decision_id, triggering_event_id, status, started_at, created_at
            ) VALUES (
                'Demand Forecast Pipeline', %s, 99991, 'RUNNING', %s, %s
            )
        """, (d1, stale_time, stale_time))
        stale_exec_id = self.cur.lastrowid
        self.test_exec_ids.append(stale_exec_id)

        # 2. Insert Fresh RUNNING execution
        d2 = self._create_decision()
        self.cur.execute("""
            INSERT INTO pipeline_executions (
                pipeline_name, decision_id, triggering_event_id, status, started_at, created_at
            ) VALUES (
                'Demand Forecast Pipeline', %s, 99992, 'RUNNING', %s, %s
            )
        """, (d2, fresh_time, fresh_time))
        fresh_exec_id = self.cur.lastrowid
        self.test_exec_ids.append(fresh_exec_id)

        # 3. Insert COMPLETED execution
        d3 = self._create_decision()
        self.cur.execute("""
            INSERT INTO pipeline_executions (
                pipeline_name, decision_id, triggering_event_id, status, started_at, completed_at, created_at
            ) VALUES (
                'Demand Forecast Pipeline', %s, 99993, 'COMPLETED', %s, %s, %s
            )
        """, (d3, stale_time, now, stale_time))
        completed_exec_id = self.cur.lastrowid
        self.test_exec_ids.append(completed_exec_id)

        # 4. Insert FAILED execution
        d4 = self._create_decision()
        self.cur.execute("""
            INSERT INTO pipeline_executions (
                pipeline_name, decision_id, triggering_event_id, status, started_at, completed_at, error_message, created_at
            ) VALUES (
                'Demand Forecast Pipeline', %s, 99994, 'FAILED', %s, %s, 'Original error', %s
            )
        """, (d4, stale_time, now, stale_time))
        failed_exec_id = self.cur.lastrowid
        self.test_exec_ids.append(failed_exec_id)

        self.conn.commit()

        # Run Reaper with 1800s (30m) timeout threshold
        stats = reap_stale_executions(timeout_seconds=1800, connection=self.conn)

        self.assertGreaterEqual(stats["scanned"], 2)
        self.assertGreaterEqual(stats["stale_found"], 1)
        self.assertGreaterEqual(stats["reconciled"], 1)

        # Verify Stale execution is now FAILED
        self.cur.execute("SELECT * FROM pipeline_executions WHERE execution_id = %s", (stale_exec_id,))
        stale_row = self.cur.fetchone()
        self.assertEqual(stale_row["status"], "FAILED")
        self.assertIsNotNone(stale_row["completed_at"])
        self.assertIn("Execution timed out and marked FAILED by reaper", stale_row["error_message"])

        # Verify Fresh execution remains RUNNING
        self.cur.execute("SELECT * FROM pipeline_executions WHERE execution_id = %s", (fresh_exec_id,))
        fresh_row = self.cur.fetchone()
        self.assertEqual(fresh_row["status"], "RUNNING")
        self.assertIsNone(fresh_row["completed_at"])

        # Verify COMPLETED execution remains COMPLETED
        self.cur.execute("SELECT * FROM pipeline_executions WHERE execution_id = %s", (completed_exec_id,))
        completed_row = self.cur.fetchone()
        self.assertEqual(completed_row["status"], "COMPLETED")

        # Verify FAILED execution remains FAILED with original error
        self.cur.execute("SELECT * FROM pipeline_executions WHERE execution_id = %s", (failed_exec_id,))
        failed_row = self.cur.fetchone()
        self.assertEqual(failed_row["status"], "FAILED")
        self.assertEqual(failed_row["error_message"], "Original error")


if __name__ == '__main__':
    unittest.main()
