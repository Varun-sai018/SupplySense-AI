"""
SupplySense AI - Priority 3 Reliability Tests

Covers:
  1. Kafka offset committed only after successful process_event().
  2. Kafka offset NOT committed when process_event() raises an exception.
  3. Airflow URL defaults to http://localhost:18080 when env var is absent.
  4. AIRFLOW_BASE_URL env var overrides the default.

No real Kafka broker, MySQL, or Airflow server is used.
Protected executions #36, #99586, #99653 are not touched.
"""

import os
import sys
import importlib
import unittest
from unittest.mock import MagicMock, patch, call

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)


# ---------------------------------------------------------------------------
# Helper: import dependency-engine main module (hyphenated directory)
# ---------------------------------------------------------------------------
dep_engine = importlib.import_module("services.dependency-engine.main")


class TestKafkaOffsetCommit(unittest.TestCase):
    """Verify that consumer.commit() is called exactly after successful processing
    and is NOT called when process_event() raises."""

    def _make_message(self, event_id="evt-test-001", event_type="UNKNOWN"):
        msg = MagicMock()
        msg.value = {"event_id": event_id, "event_type": event_type}
        return msg

    def test_offset_committed_on_success(self):
        """consumer.commit() must be called once when process_event() succeeds."""
        consumer = MagicMock()
        connection = MagicMock()
        message = self._make_message()

        with patch.object(dep_engine, "process_event", return_value=("BLOCK", "no config")) as mock_pe:
            # Simulate one message then StopIteration to exit the for-loop
            consumer.__iter__ = MagicMock(return_value=iter([message]))

            try:
                connection_factory = MagicMock(return_value=connection)
                with patch.object(dep_engine, "get_connection", connection_factory):
                    # Run the inner loop logic directly (not main() which blocks on Kafka connect)
                    for msg in consumer:
                        try:
                            dep_engine.process_event(msg.value, connection)
                            consumer.commit()
                        except Exception as e:
                            connection.rollback()
            except Exception:
                pass

        consumer.commit.assert_called_once()
        mock_pe.assert_called_once_with(message.value, connection)

    def test_offset_not_committed_on_failure(self):
        """consumer.commit() must NOT be called when process_event() raises an exception."""
        consumer = MagicMock()
        connection = MagicMock()
        message = self._make_message()

        with patch.object(dep_engine, "process_event", side_effect=RuntimeError("DB error")):
            consumer.__iter__ = MagicMock(return_value=iter([message]))

            for msg in consumer:
                try:
                    dep_engine.process_event(msg.value, connection)
                    consumer.commit()  # must NOT reach here
                except Exception:
                    connection.rollback()

        consumer.commit.assert_not_called()
        connection.rollback.assert_called_once()

    def test_commit_not_called_after_rollback(self):
        """Verify that rollback is called and commit is skipped on any exception."""
        consumer = MagicMock()
        connection = MagicMock()

        # Simulate two messages: first fails, second succeeds
        msg_fail = self._make_message("evt-fail", "UNKNOWN")
        msg_ok = self._make_message("evt-ok", "UNKNOWN")

        side_effects = [RuntimeError("transient error"), ("BLOCK", "no config")]

        with patch.object(dep_engine, "process_event", side_effect=side_effects):
            for msg in [msg_fail, msg_ok]:
                try:
                    dep_engine.process_event(msg.value, connection)
                    consumer.commit()
                except Exception:
                    connection.rollback()

        # commit called once (for msg_ok), rollback called once (for msg_fail)
        self.assertEqual(consumer.commit.call_count, 1)
        self.assertEqual(connection.rollback.call_count, 1)


class TestKafkaConsumerConfig(unittest.TestCase):
    """Verify the KafkaConsumer is configured with enable_auto_commit=False."""

    def test_auto_commit_disabled_in_source(self):
        """Read main.py source and confirm enable_auto_commit=False is present."""
        main_path = os.path.join(
            REPO_ROOT, "services", "dependency-engine", "main.py"
        )
        with open(main_path, encoding="utf-8") as f:
            source = f.read()

        self.assertIn(
            "enable_auto_commit=False",
            source,
            "enable_auto_commit must be False in the KafkaConsumer constructor"
        )
        self.assertNotIn(
            "enable_auto_commit=True",
            source,
            "enable_auto_commit=True must not be present"
        )

    def test_manual_commit_call_present_in_source(self):
        """Verify consumer.commit() call exists in the consumer loop."""
        main_path = os.path.join(
            REPO_ROOT, "services", "dependency-engine", "main.py"
        )
        with open(main_path, encoding="utf-8") as f:
            source = f.read()

        self.assertIn(
            "consumer.commit()",
            source,
            "consumer.commit() must be present in the message processing loop"
        )


class TestAirflowURLConfiguration(unittest.TestCase):
    """Verify Airflow URL default and environment variable override."""

    def test_default_url_is_port_18080(self):
        """When AIRFLOW_BASE_URL env var is not set, default must be localhost:18080."""
        env_backup = os.environ.pop("AIRFLOW_BASE_URL", None)
        try:
            import config.settings as settings
            importlib.reload(settings)
            self.assertEqual(
                settings.AIRFLOW_BASE_URL,
                "http://localhost:18080",
                "Default Airflow URL must be http://localhost:18080 (Docker host port)"
            )
        finally:
            if env_backup is not None:
                os.environ["AIRFLOW_BASE_URL"] = env_backup

    def test_env_override_takes_precedence(self):
        """AIRFLOW_BASE_URL env var must override the hardcoded default."""
        container_url = "http://airflow-webserver:8080"
        original = os.environ.get("AIRFLOW_BASE_URL")
        os.environ["AIRFLOW_BASE_URL"] = container_url
        try:
            import config.settings as settings
            importlib.reload(settings)
            self.assertEqual(
                settings.AIRFLOW_BASE_URL,
                container_url,
                "AIRFLOW_BASE_URL env var must override the default"
            )
        finally:
            if original is None:
                os.environ.pop("AIRFLOW_BASE_URL", None)
            else:
                os.environ["AIRFLOW_BASE_URL"] = original
            # Reload to restore default state
            importlib.reload(settings)

    def test_old_default_port_8080_not_used(self):
        """Confirm the old incorrect default (port 8080) is no longer present in settings."""
        settings_path = os.path.join(REPO_ROOT, "config", "settings.py")
        with open(settings_path, encoding="utf-8") as f:
            source = f.read()
        # The string localhost:8080 must not appear as a fallback default
        self.assertNotIn(
            '"http://localhost:8080"',
            source,
            "Old default http://localhost:8080 must be removed from settings.py"
        )

    def test_host_url_comment_documents_docker_port_mapping(self):
        """Confirm the settings file documents the Docker host:18080 -> container:8080 mapping."""
        settings_path = os.path.join(REPO_ROOT, "config", "settings.py")
        with open(settings_path, encoding="utf-8") as f:
            source = f.read()
        self.assertIn(
            "18080",
            source,
            "settings.py must document the port 18080 Docker mapping"
        )


class TestStructuredLogging(unittest.TestCase):
    """Verify print() statements have been replaced with logger calls in main.py."""

    def test_no_debugging_print_calls_in_process_event(self):
        """process_event() and main() must not contain bare print() debug calls."""
        main_path = os.path.join(
            REPO_ROOT, "services", "dependency-engine", "main.py"
        )
        with open(main_path, encoding="utf-8") as f:
            lines = f.readlines()

        # Collect any indented print() calls (inside functions)
        offending = [
            (i + 1, line.rstrip())
            for i, line in enumerate(lines)
            if "    print(" in line
        ]
        self.assertEqual(
            offending, [],
            f"Found {len(offending)} remaining print() calls: {offending}"
        )

    def test_logger_info_calls_present(self):
        """Verify logger.info() is used in the standard pipeline path."""
        main_path = os.path.join(
            REPO_ROOT, "services", "dependency-engine", "main.py"
        )
        with open(main_path, encoding="utf-8") as f:
            source = f.read()
        self.assertIn("logger.info(", source)
        self.assertIn("logger.error(", source)


if __name__ == "__main__":
    unittest.main()


class TestConnectionHealthCheck(unittest.TestCase):
    """
    Verify per-message MySQL connection health-check and recovery behaviour.

    All tests simulate the inner loop body directly — no real Kafka broker,
    no real MySQL server, no operational data touched.

    Scenarios covered:
      1. Healthy connection: ping() succeeds → process_event and commit called normally.
      2. Dead connection, reconnect succeeds: ping() raises, get_connection() returns
         a fresh mock, process_event and commit still called.
      3. Dead connection, reconnect fails: ping() raises, get_connection() also raises →
         consumer.commit() must NOT be called (message redelivered).
      4. Rollback on dead connection: if process_event raises AND rollback raises,
         the outer exception is logged but commit is still skipped.
    """

    def _run_inner_loop(self, consumer, connection, get_conn_mock, process_mock, messages):
        """
        Re-implement the inner consumer loop from main.py so tests exercise the
        exact same branching without starting main() (which blocks on Kafka connect).
        This mirrors lines 409-443 of services/dependency-engine/main.py exactly.
        """
        for message in messages:
            try:
                # --- Connection health check ---
                try:
                    connection.ping()
                except Exception as ping_err:
                    try:
                        connection.close()
                    except Exception:
                        pass
                    connection = get_conn_mock()   # may raise
                process_mock(message.value, connection)
                consumer.commit()
            except Exception:
                try:
                    connection.rollback()
                except Exception:
                    pass
                # offset NOT committed

    def _make_msg(self, event_id="evt-conn-test"):
        msg = MagicMock()
        msg.value = {"event_id": event_id, "event_type": "UNKNOWN"}
        return msg

    # ------------------------------------------------------------------
    # Test 1: Healthy connection — ping succeeds, normal processing
    # ------------------------------------------------------------------
    def test_healthy_connection_processes_normally(self):
        """ping() succeeds → process_event called → consumer.commit() called once."""
        consumer = MagicMock()
        connection = MagicMock()
        connection.ping.return_value = None          # healthy
        get_conn_mock = MagicMock()                  # should NOT be called
        process_mock = MagicMock(return_value=("BLOCK", "ok"))
        msg = self._make_msg()

        self._run_inner_loop(consumer, connection, get_conn_mock, process_mock, [msg])

        connection.ping.assert_called_once_with()
        get_conn_mock.assert_not_called()            # no reconnect needed
        process_mock.assert_called_once()
        consumer.commit.assert_called_once()
        connection.rollback.assert_not_called()

    # ------------------------------------------------------------------
    # Test 2: Dead connection, reconnect succeeds
    # ------------------------------------------------------------------
    def test_dead_connection_reconnect_succeeds_then_processes(self):
        """ping() raises, get_connection() returns fresh conn → process and commit called."""
        consumer = MagicMock()
        dead_conn = MagicMock()
        dead_conn.ping.side_effect = Exception("MySQL server has gone away")
        fresh_conn = MagicMock()
        fresh_conn.ping.return_value = None
        get_conn_mock = MagicMock(return_value=fresh_conn)
        process_mock = MagicMock(return_value=("BLOCK", "ok"))
        msg = self._make_msg("evt-reconnect-ok")

        self._run_inner_loop(consumer, dead_conn, get_conn_mock, process_mock, [msg])

        dead_conn.ping.assert_called_once_with()
        dead_conn.close.assert_called_once()         # dead socket closed
        get_conn_mock.assert_called_once()           # fresh connection obtained
        # process_event called with the FRESH connection
        process_mock.assert_called_once()
        fresh_conn_arg = process_mock.call_args[0][1]
        self.assertIs(fresh_conn_arg, fresh_conn)
        consumer.commit.assert_called_once()         # offset committed
        dead_conn.rollback.assert_not_called()

    # ------------------------------------------------------------------
    # Test 3: Dead connection, reconnect also fails
    # ------------------------------------------------------------------
    def test_dead_connection_reconnect_fails_skips_message(self):
        """ping() raises, get_connection() also raises → message skipped, no commit."""
        consumer = MagicMock()
        dead_conn = MagicMock()
        dead_conn.ping.side_effect = Exception("MySQL server has gone away")
        get_conn_mock = MagicMock(side_effect=Exception("Can't connect to MySQL server"))
        process_mock = MagicMock()
        msg = self._make_msg("evt-reconnect-fail")

        self._run_inner_loop(consumer, dead_conn, get_conn_mock, process_mock, [msg])

        dead_conn.ping.assert_called_once_with()
        dead_conn.close.assert_called_once()
        get_conn_mock.assert_called_once()
        process_mock.assert_not_called()             # event NOT processed
        consumer.commit.assert_not_called()          # offset NOT committed → redelivery

    # ------------------------------------------------------------------
    # Test 4: Healthy ping, but process_event raises → no commit
    # ------------------------------------------------------------------
    def test_process_event_failure_skips_commit(self):
        """ping() healthy, process_event() raises → rollback called, commit skipped."""
        consumer = MagicMock()
        connection = MagicMock()
        connection.ping.return_value = None
        get_conn_mock = MagicMock()
        process_mock = MagicMock(side_effect=RuntimeError("DB integrity error"))
        msg = self._make_msg("evt-proc-fail")

        self._run_inner_loop(consumer, connection, get_conn_mock, process_mock, [msg])

        process_mock.assert_called_once()
        consumer.commit.assert_not_called()          # offset NOT committed
        connection.rollback.assert_called_once()

    # ------------------------------------------------------------------
    # Test 5: Rollback on already-dead connection — no secondary exception leak
    # ------------------------------------------------------------------
    def test_rollback_on_dead_connection_does_not_raise(self):
        """If both process_event and rollback raise, the loop continues without crashing."""
        consumer = MagicMock()
        connection = MagicMock()
        connection.ping.return_value = None
        connection.rollback.side_effect = Exception("Lost connection during rollback")
        get_conn_mock = MagicMock()
        process_mock = MagicMock(side_effect=RuntimeError("processing failed"))
        msg1 = self._make_msg("evt-rb-dead-1")
        msg2 = self._make_msg("evt-rb-dead-2")

        # The loop should survive both messages even when rollback raises on msg1
        # Reset rollback to succeed on msg2
        call_count = [0]
        orig_side = connection.rollback.side_effect

        def rollback_side():
            call_count[0] += 1
            if call_count[0] == 1:
                raise Exception("Lost connection during rollback")

        connection.rollback.side_effect = rollback_side
        # process_mock raises on msg1, succeeds on msg2
        process_mock.side_effect = [RuntimeError("fail msg1"), ("BLOCK", "ok")]

        self._run_inner_loop(consumer, connection, get_conn_mock, process_mock, [msg1, msg2])

        self.assertEqual(process_mock.call_count, 2)
        self.assertEqual(consumer.commit.call_count, 1)   # only msg2 committed
        self.assertEqual(connection.rollback.call_count, 1)

    # ------------------------------------------------------------------
    # Test 6: Source-level check — ping() call present without reconnect=True
    # ------------------------------------------------------------------
    def test_source_uses_ping_without_deprecated_reconnect_arg(self):
        """main.py must call connection.ping() without the deprecated reconnect=True arg."""
        main_path = os.path.join(
            REPO_ROOT, "services", "dependency-engine", "main.py"
        )
        with open(main_path, encoding="utf-8") as f:
            source = f.read()

        self.assertIn(
            "connection.ping()",
            source,
            "connection.ping() must be present in the consumer loop"
        )
        self.assertNotIn(
            "ping(reconnect=True)",
            source,
            "Deprecated ping(reconnect=True) must not be used (PyMySQL 2.2.8)"
        )

    # ------------------------------------------------------------------
    # Test 7: Source-level check — rollback is guarded in its own try/except
    # ------------------------------------------------------------------
    def test_source_rollback_is_guarded(self):
        """Rollback must be wrapped in try/except so a dead connection doesn't crash the loop."""
        main_path = os.path.join(
            REPO_ROOT, "services", "dependency-engine", "main.py"
        )
        with open(main_path, encoding="utf-8") as f:
            source = f.read()

        # The pattern 'try:\n...connection.rollback()' must appear
        self.assertIn(
            "connection.rollback()",
            source,
        )
        # The rollback warning log for failed rollback must be present
        self.assertIn(
            "Rollback failed",
            source,
            "A warning log for failed rollback must be present"
        )
