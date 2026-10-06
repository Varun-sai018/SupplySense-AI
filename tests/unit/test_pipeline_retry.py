"""
Unit tests for Pipeline Execution Retry Policy, Backoff, and Retry Service (Phase 9).
Tests error classification, backoff calculation, atomic claiming, lifecycle transitions,
idempotency, and retry limits.
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
policy_mod = importlib.import_module('services.pipeline-runner.retry_policy')
retry_service_mod = importlib.import_module('services.pipeline-runner.retry_service')

is_retryable_error = policy_mod.is_retryable_error
classify_error_type = policy_mod.classify_error_type
calculate_backoff_delay = policy_mod.calculate_backoff_delay
retry_execution = retry_service_mod.retry_execution
process_retry_candidates = retry_service_mod.process_retry_candidates


class TestPipelineRetryPolicy(unittest.TestCase):

    def test_retryable_database_errors(self):
        """1. Database connection errors and timeouts are classified as retryable."""
        self.assertTrue(is_retryable_error("MySQL connection timed out"))
        self.assertTrue(is_retryable_error("DB Connection Lost"))
        self.assertTrue(is_retryable_error("OperationalError: (2006, 'MySQL server has gone away')"))
        self.assertTrue(is_retryable_error("Lock wait timeout exceeded; try restarting transaction"))
        self.assertTrue(is_retryable_error("Deadlock found when trying to get lock"))
        self.assertEqual(classify_error_type("MySQL connection timed out"), "TRANSIENT_DB")

    def test_retryable_network_and_timeout_errors(self):
        """2. Network and socket timeouts are classified as retryable."""
        self.assertTrue(is_retryable_error("Kafka broker not available / request timed out"))
        self.assertTrue(is_retryable_error("ConnectionRefusedError: [Errno 111] Connection refused"))
        self.assertTrue(is_retryable_error("503 Service Unavailable: Gateway Timeout"))
        self.assertEqual(classify_error_type("Kafka broker not available"), "TRANSIENT_NETWORK")
        self.assertEqual(classify_error_type("Request timeout after 30s"), "TRANSIENT_TIMEOUT")

    def test_retryable_reaper_timeout(self):
        """3. Reaper-marked stale executions are classified as retryable."""
        reaper_msg = "Execution timed out and marked FAILED by reaper after exceeding stale timeout of 1800s (started at 2026-10-06 08:00:00)."
        self.assertTrue(is_retryable_error(reaper_msg))
        self.assertEqual(classify_error_type(reaper_msg), "TRANSIENT_REAPER")

    def test_non_retryable_input_and_file_errors(self):
        """4. Missing input files / FileNotFoundError are non-retryable."""
        self.assertFalse(is_retryable_error("Missing required prepared dataset input(s): train.csv", FileNotFoundError("train.csv")))
        self.assertFalse(is_retryable_error(exception=FileNotFoundError("model.json")))
        self.assertEqual(classify_error_type(exception=FileNotFoundError()), "DETERMINISTIC_INPUT")

    def test_non_retryable_deterministic_application_errors(self):
        """5. ValueError, KeyError, TypeError, SyntaxError are non-retryable."""
        self.assertFalse(is_retryable_error("ValueError: invalid literal for int()", ValueError("invalid literal")))
        self.assertFalse(is_retryable_error("KeyError: 'target_column'", KeyError("target_column")))
        self.assertFalse(is_retryable_error("TypeError: unsupported operand", TypeError("unsupported operand")))
        self.assertEqual(classify_error_type(exception=ValueError()), "DETERMINISTIC_APPLICATION")

    def test_non_retryable_config_and_integrity_errors(self):
        """6. Unknown pipeline and constraint violations are non-retryable."""
        self.assertFalse(is_retryable_error("Unknown pipeline 'Invalid Pipeline'"))
        self.assertFalse(is_retryable_error("IntegrityError: Duplicate entry '101' for key 'PRIMARY'"))
        self.assertEqual(classify_error_type("Unknown pipeline 'X'"), "DETERMINISTIC_CONFIG")
        self.assertEqual(classify_error_type("Duplicate entry for key"), "DETERMINISTIC_INTEGRITY")


class TestPipelineRetryBackoff(unittest.TestCase):

    def test_exponential_backoff_progression(self):
        """7. Exponential backoff increases correctly with retry_count."""
        # base = 5, max = 60
        self.assertEqual(calculate_backoff_delay(0, base_delay=5, max_delay=60), 5)   # 5 * 2^0 = 5
        self.assertEqual(calculate_backoff_delay(1, base_delay=5, max_delay=60), 10)  # 5 * 2^1 = 10
        self.assertEqual(calculate_backoff_delay(2, base_delay=5, max_delay=60), 20)  # 5 * 2^2 = 20
        self.assertEqual(calculate_backoff_delay(3, base_delay=5, max_delay=60), 40)  # 5 * 2^3 = 40

    def test_exponential_backoff_ceiling_cap(self):
        """8. Backoff delay respects maximum delay cap."""
        self.assertEqual(calculate_backoff_delay(4, base_delay=5, max_delay=60), 60)  # 5 * 2^4 = 80 -> capped at 60
        self.assertEqual(calculate_backoff_delay(10, base_delay=5, max_delay=60), 60) # Capped at 60


class TestPipelineRetryService(unittest.TestCase):

    def setUp(self):
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value = self.mock_cursor

    def test_successful_retry_lifecycle(self):
        """9. Failed execution is claimed, executed, and transitions to COMPLETED."""
        # Initial state: execution #201 is FAILED with retry_count=0
        self.mock_cursor.fetchone.return_value = {
            "execution_id": 201,
            "pipeline_name": "Demand Forecast Pipeline",
            "status": "FAILED",
            "retry_count": 0,
            "max_retries": 3,
            "error_message": "MySQL connection timed out",
            "triggering_event_id": 501
        }
        self.mock_cursor.rowcount = 1  # Atomic claim update matches 1 row

        with patch.object(retry_service_mod, 'get_pipeline_handler') as mock_get_handler:
            mock_handler = MagicMock(return_value={"output_location": "ml/results/test.json"})
            mock_get_handler.return_value = mock_handler

            result = retry_execution(201, connection=self.mock_conn)

            self.assertEqual(result["status"], "COMPLETED")
            self.assertEqual(result["retry_count"], 1)
            self.assertEqual(result["output_location"], "ml/results/test.json")
            mock_handler.assert_called_once_with(201, "Demand Forecast Pipeline")

    def test_failed_retry_reschedules_with_backoff(self):
        """10. Failed retry increments count and schedules next retry if under limit."""
        self.mock_cursor.fetchone.return_value = {
            "execution_id": 202,
            "pipeline_name": "Demand Forecast Pipeline",
            "status": "FAILED",
            "retry_count": 0,
            "max_retries": 3,
            "error_message": "DB Connection Lost",
            "triggering_event_id": 502
        }
        self.mock_cursor.rowcount = 1

        with patch.object(retry_service_mod, 'get_pipeline_handler') as mock_get_handler:
            mock_handler = MagicMock(side_effect=RuntimeError("Transient database timeout"))
            mock_get_handler.return_value = mock_handler

            result = retry_execution(202, connection=self.mock_conn)

            self.assertEqual(result["status"], "FAILED")
            self.assertEqual(result["retry_count"], 1)
            self.assertFalse(result["retry_exhausted"])
            self.assertIsNotNone(result["next_retry_at"])

    def test_retry_exhaustion_at_max_retries(self):
        """11. Execution with retry_count >= max_retries is not retried."""
        self.mock_cursor.fetchone.return_value = {
            "execution_id": 203,
            "pipeline_name": "Demand Forecast Pipeline",
            "status": "FAILED",
            "retry_count": 3,
            "max_retries": 3,
            "error_message": "DB Connection Lost",
            "triggering_event_id": 503
        }

        result = retry_execution(203, connection=self.mock_conn)

        self.assertEqual(result["status"], "EXHAUSTED")
        self.assertEqual(result["retry_count"], 3)

    def test_non_retryable_error_skipped(self):
        """12. Execution with deterministic non-retryable error is not retried."""
        self.mock_cursor.fetchone.return_value = {
            "execution_id": 204,
            "pipeline_name": "Demand Forecast Pipeline",
            "status": "FAILED",
            "retry_count": 0,
            "max_retries": 3,
            "error_message": "Missing required prepared dataset input(s): train.csv",
            "triggering_event_id": 504
        }

        result = retry_execution(204, connection=self.mock_conn)

        self.assertEqual(result["status"], "NON_RETRYABLE")

    def test_atomic_claim_concurrency_conflict_skipped(self):
        """13. If another worker claims the execution, rowcount=0 and retry is skipped."""
        self.mock_cursor.fetchone.return_value = {
            "execution_id": 205,
            "pipeline_name": "Demand Forecast Pipeline",
            "status": "FAILED",
            "retry_count": 0,
            "max_retries": 3,
            "error_message": "MySQL connection timed out",
            "triggering_event_id": 505
        }
        # Simulate race condition: another worker changed status to RUNNING right before UPDATE
        self.mock_cursor.rowcount = 0

        result = retry_execution(205, connection=self.mock_conn)

        self.assertEqual(result["status"], "SKIPPED")
        self.assertIn("Concurrent claim", result["reason"])

    def test_non_failed_status_skipped(self):
        """14. Execution not in FAILED status (e.g. COMPLETED) is skipped."""
        self.mock_cursor.fetchone.return_value = {
            "execution_id": 206,
            "pipeline_name": "Demand Forecast Pipeline",
            "status": "COMPLETED",
            "retry_count": 0,
            "max_retries": 3,
            "error_message": None,
            "triggering_event_id": 506
        }

        result = retry_execution(206, connection=self.mock_conn)

        self.assertEqual(result["status"], "SKIPPED")
        self.assertIn("expected 'FAILED'", result["reason"])


if __name__ == '__main__':
    unittest.main()
