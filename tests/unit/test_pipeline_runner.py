"""
Unit tests for Pipeline Runner Service (Phase 4).
Tests registry, execution transitions (RUNNING -> COMPLETED / FAILED), error handling, and ML baseline invocation.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# Ensure repo root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import importlib
runner_module = importlib.import_module('services.pipeline-runner.runner')
registry_module = importlib.import_module('services.pipeline-runner.registry')
handlers_module = importlib.import_module('services.pipeline-runner.handlers')

execute_pipeline = runner_module.execute_pipeline
register_pipeline = registry_module.register_pipeline
get_pipeline_handler = registry_module.get_pipeline_handler
list_registered_pipelines = registry_module.list_registered_pipelines
demand_forecast_pipeline_handler = handlers_module.demand_forecast_pipeline_handler


class TestPipelineRunner(unittest.TestCase):

    def setUp(self):
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value = self.mock_cursor

    def test_registry_contains_demand_forecast_pipeline(self):
        """1. Pipeline registry has 'Demand Forecast Pipeline' registered."""
        pipelines = list_registered_pipelines()
        self.assertIn("Demand Forecast Pipeline", pipelines)
        handler = get_pipeline_handler("Demand Forecast Pipeline")
        self.assertIsNotNone(handler)
        self.assertEqual(handler, demand_forecast_pipeline_handler)

    def test_registry_custom_registration(self):
        """2. Can register and retrieve custom pipeline handlers."""
        def dummy_handler(exec_id, pipe_name):
            return {"output_location": "dummy/path.json"}

        register_pipeline("Custom Pipeline", dummy_handler)
        self.assertEqual(get_pipeline_handler("Custom Pipeline"), dummy_handler)
        self.assertIn("Custom Pipeline", list_registered_pipelines())

    def test_unknown_pipeline_fails_safely(self):
        """3. Unknown pipeline name marks execution FAILED without throwing uncaught exceptions."""
        result = execute_pipeline(
            execution_id=999,
            pipeline_name="Nonexistent Pipeline",
            connection=self.mock_conn
        )

        self.assertEqual(result["status"], "FAILED")
        self.assertIn("Unknown pipeline", result["error"])
        
        # Verify database update to FAILED
        self.mock_cursor.execute.assert_any_call(
            """
                UPDATE pipeline_executions
                SET status = 'FAILED', completed_at = %s, error_message = %s
                WHERE execution_id = %s
            """,
            unittest.mock.ANY
        )
        self.mock_conn.commit.assert_called()

    def test_successful_execution_marks_completed(self):
        """4. Successful handler execution marks record as COMPLETED with output_location."""
        mock_handler = MagicMock()
        mock_handler.return_value = {
            "output_location": "ml/results/baseline_metrics.json",
            "metrics": {"validation": {"mae": 7.98}, "test": {"mae": 9.46}}
        }
        with patch.object(runner_module, 'get_pipeline_handler', return_value=mock_handler):
            result = execute_pipeline(
                execution_id=101,
                pipeline_name="Demand Forecast Pipeline",
                connection=self.mock_conn
            )

        self.assertEqual(result["status"], "COMPLETED")
        self.assertEqual(result["output_location"], "ml/results/baseline_metrics.json")
        self.assertIn("metrics", result["result"])

        # Check DB update for COMPLETED
        self.mock_cursor.execute.assert_any_call(
            """
            UPDATE pipeline_executions
            SET status = 'COMPLETED',
                completed_at = %s,
                output_location = %s,
                error_message = NULL
            WHERE execution_id = %s
        """,
            unittest.mock.ANY
        )
        self.mock_conn.commit.assert_called()

    def test_failed_handler_execution_marks_failed(self):
        """5. Exception in pipeline handler marks execution FAILED with error message."""
        mock_handler = MagicMock()
        mock_handler.side_effect = RuntimeError("Forecast computation failed")
        with patch.object(runner_module, 'get_pipeline_handler', return_value=mock_handler):
            result = execute_pipeline(
                execution_id=102,
                pipeline_name="Demand Forecast Pipeline",
                connection=self.mock_conn
            )

        self.assertEqual(result["status"], "FAILED")
        self.assertIn("Forecast computation failed", result["error"])

        # Check DB update for FAILED
        self.mock_cursor.execute.assert_any_call(
            """
                UPDATE pipeline_executions
                SET status = 'FAILED', completed_at = %s, error_message = %s
                WHERE execution_id = %s
            """,
            unittest.mock.ANY
        )
        self.mock_conn.commit.assert_called()

    @patch('os.path.isfile')
    def test_handler_missing_input_datasets_raises_filenotfound(self, mock_isfile):
        """6. Missing prepared ML input files raises FileNotFoundError."""
        mock_isfile.return_value = False  # Simulate missing files

        with self.assertRaises(FileNotFoundError) as ctx:
            demand_forecast_pipeline_handler(103, "Demand Forecast Pipeline")
        self.assertIn("Missing required prepared dataset", str(ctx.exception))

    @patch('os.path.isfile')
    def test_handler_successful_invocation(self, mock_isfile):
        """7. Handler successfully invokes naive baseline and returns structured output."""
        mock_isfile.return_value = True
        mock_res = {
            "validation": {"mae": 7.98, "rmse": 15.31, "r2": 0.89},
            "test": {"mae": 9.46, "rmse": 17.99, "r2": 0.83}
        }
        with patch.object(handlers_module, 'run_baseline', return_value=mock_res) as mock_run_baseline:
            result = demand_forecast_pipeline_handler(104, "Demand Forecast Pipeline")
            self.assertEqual(result["output_location"], "ml/results/baseline_metrics.json")
            self.assertIn("metrics", result)
            self.assertIn("artifacts", result)
            mock_run_baseline.assert_called_once()

    def test_db_connection_exception_handled_safely(self):
        """8. Database failure inside execute_pipeline returns structured FAILED dict."""
        self.mock_cursor.execute.side_effect = Exception("DB Connection Lost")

        result = execute_pipeline(
            execution_id=105,
            pipeline_name="Demand Forecast Pipeline",
            connection=self.mock_conn
        )

        self.assertEqual(result["status"], "FAILED")
        self.assertIn("Database tracking error", result["error"])
        self.mock_conn.rollback.assert_called()


if __name__ == '__main__':
    unittest.main()
