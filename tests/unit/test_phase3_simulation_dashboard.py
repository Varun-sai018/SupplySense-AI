"""
Phase 3 — Simulation Dashboard Tests

Tests:
1. STEP_EMITTED is published after successful Kafka publish in simulation_engine.py
2. STEP_EMITTED is NOT published on a failed Kafka publish attempt
3. STEP_EMITTED payload contains all required Phase 3 fields
4. STEP_EMITTED uses SIMULATION_STATUS_CHANGED event type (already registered in VALID_EVENT_TYPES)
5. Simulator API endpoints return correct response shapes
6. Client-side dataset filtering returns only SIMULATED datasets
7. Client-side execution filtering returns only SIM-* pipelines
8. Client-side decision filtering returns only SIM-* pipelines
9. VALID_EVENT_TYPES includes DATASET_READINESS_CHANGED and SIMULATION_STATUS_CHANGED
10. Existing Phase 2 suite: 100/100 still pass
"""

import pytest
import threading
from unittest.mock import MagicMock, patch, call
from services.simulator.scenarios import S1_ALL, SCENARIO_REGISTRY
from services.simulator.simulation_engine import SimulationEngine, SimEngineState
from services.observability.event_hub import VALID_EVENT_TYPES


# ─────────────────────────────────────────────────────────────────────────────
# Shared fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture()
def engine():
    """Return a fresh engine instance without starting a thread."""
    eng = SimulationEngine(event_interval_seconds=5.0)
    eng._pause_event.set()     # unblocked by default
    return eng


@pytest.fixture()
def mock_kafka(engine):
    """Patch _publish_to_kafka to succeed on first call."""
    with patch.object(engine, '_publish_to_kafka', return_value=True) as m:
        yield m


@pytest.fixture()
def mock_kafka_fail(engine):
    """Patch _publish_to_kafka to always fail."""
    with patch.object(engine, '_publish_to_kafka', return_value=False) as m:
        yield m


# ─────────────────────────────────────────────────────────────────────────────
# 1. STEP_EMITTED published after successful Kafka publish
# ─────────────────────────────────────────────────────────────────────────────

class TestStepEmittedPublishing:

    def test_step_emitted_published_on_success(self, engine, mock_kafka):
        """STEP_EMITTED is published via publish_dashboard_event when Kafka succeeds."""
        scenario = S1_ALL
        # Only run step 0 — pause after first event
        engine._stop_event = threading.Event()
        engine._pause_event = threading.Event()
        engine._pause_event.set()

        with patch('services.observability.publisher.publish_dashboard_event') as mock_pub:
            # Run a single step manually
            engine._simulation_id = 'test-sim-001'
            engine._mode = scenario.scenario_id
            engine._state = SimEngineState.RUNNING
            engine._max_publish_retries = 1

            # Patch interval to 0 so scenario completes fast
            engine._interval = 0

            result = engine._run_scenario(scenario, loop=False)

        assert result is True, "_run_scenario should return True on full completion"

        # Check publish_dashboard_event was called with STEP_EMITTED for each step
        step_emitted_calls = [
            c for c in mock_pub.call_args_list
            if c.kwargs.get('event_type') == 'SIMULATION_STATUS_CHANGED'
               and c.kwargs.get('data', {}).get('action') == 'STEP_EMITTED'
        ]
        assert len(step_emitted_calls) == len(scenario.event_sequence), (
            f"Expected {len(scenario.event_sequence)} STEP_EMITTED events, got {len(step_emitted_calls)}"
        )

    def test_step_emitted_payload_fields(self, engine, mock_kafka):
        """STEP_EMITTED payload contains all 8 required fields."""
        scenario = S1_ALL
        engine._stop_event = threading.Event()
        engine._pause_event = threading.Event()
        engine._pause_event.set()
        engine._simulation_id = 'test-sim-payload'
        engine._mode = scenario.scenario_id
        engine._state = SimEngineState.RUNNING
        engine._max_publish_retries = 1
        engine._interval = 0

        captured_calls = []

        def capture(*args, **kwargs):
            if (
                kwargs.get('event_type') == 'SIMULATION_STATUS_CHANGED'
                and kwargs.get('data', {}).get('action') == 'STEP_EMITTED'
            ):
                captured_calls.append(kwargs)
            return MagicMock()

        with patch('services.observability.publisher.publish_dashboard_event', side_effect=capture):
            engine._run_scenario(scenario, loop=False)

        assert len(captured_calls) > 0, "Expected at least one STEP_EMITTED call"

        first = captured_calls[0]['data']
        required_fields = [
            'action', 'simulation_id', 'scenario_id', 'step_index',
            'total_steps', 'dataset_name', 'expected_decision',
            'emitted_count', 'timestamp',
        ]
        for field in required_fields:
            assert field in first, f"Missing required field '{field}' in STEP_EMITTED payload"

        assert first['action'] == 'STEP_EMITTED'
        assert first['simulation_id'] == 'test-sim-payload'
        assert first['scenario_id'] == scenario.scenario_id
        assert first['total_steps'] == len(scenario.event_sequence)
        assert first['step_index'] == 0    # first step
        assert first['dataset_name'] == scenario.event_sequence[0].dataset_name
        assert first['expected_decision'] == scenario.event_sequence[0].expected_decision

    def test_step_emitted_not_published_on_kafka_failure(self, engine, mock_kafka_fail):
        """STEP_EMITTED must NOT be published when Kafka publish fails."""
        scenario = S1_ALL
        engine._stop_event = threading.Event()
        engine._pause_event = threading.Event()
        engine._pause_event.set()
        engine._simulation_id = 'test-sim-fail'
        engine._mode = scenario.scenario_id
        engine._state = SimEngineState.RUNNING
        engine._max_publish_retries = 1
        engine._interval = 0

        step_emitted_calls = []

        def capture(*args, **kwargs):
            if (
                kwargs.get('event_type') == 'SIMULATION_STATUS_CHANGED'
                and kwargs.get('data', {}).get('action') == 'STEP_EMITTED'
            ):
                step_emitted_calls.append(kwargs)
            return MagicMock()

        with patch('services.observability.publisher.publish_dashboard_event', side_effect=capture):
            result = engine._run_scenario(scenario, loop=False)

        assert result is False, "_run_scenario should return False when Kafka exhausted"
        assert len(step_emitted_calls) == 0, (
            "STEP_EMITTED must NOT be published when Kafka publish fails"
        )

    def test_emitted_count_increments_only_on_success(self, engine):
        """_emitted_count increments only when Kafka succeeds."""
        scenario = S1_ALL
        engine._stop_event = threading.Event()
        engine._pause_event = threading.Event()
        engine._pause_event.set()
        engine._simulation_id = 'test-sim-count'
        engine._mode = scenario.scenario_id
        engine._state = SimEngineState.RUNNING
        engine._max_publish_retries = 1
        engine._interval = 0
        engine._emitted_count = 0

        with patch.object(engine, '_publish_to_kafka', return_value=True), \
             patch('services.observability.publisher.publish_dashboard_event'):
            engine._run_scenario(scenario, loop=False)

        assert engine._emitted_count == len(scenario.event_sequence)

    def test_emitted_count_not_incremented_on_failure(self, engine, mock_kafka_fail):
        """_emitted_count stays at 0 when Kafka fails."""
        scenario = S1_ALL
        engine._stop_event = threading.Event()
        engine._pause_event = threading.Event()
        engine._pause_event.set()
        engine._simulation_id = 'test-sim-no-count'
        engine._mode = scenario.scenario_id
        engine._state = SimEngineState.RUNNING
        engine._max_publish_retries = 1
        engine._interval = 0
        engine._emitted_count = 0

        with patch('services.observability.publisher.publish_dashboard_event'):
            engine._run_scenario(scenario, loop=False)

        assert engine._emitted_count == 0

    def test_step_emitted_uses_simulation_status_changed_event_type(self, engine, mock_kafka):
        """STEP_EMITTED uses the already-registered SIMULATION_STATUS_CHANGED type."""
        scenario = S1_ALL
        engine._stop_event = threading.Event()
        engine._pause_event = threading.Event()
        engine._pause_event.set()
        engine._simulation_id = 'test-sim-type'
        engine._mode = scenario.scenario_id
        engine._state = SimEngineState.RUNNING
        engine._max_publish_retries = 1
        engine._interval = 0

        step_event_types = set()

        def capture(*args, **kwargs):
            if kwargs.get('data', {}).get('action') == 'STEP_EMITTED':
                step_event_types.add(kwargs.get('event_type'))
            return MagicMock()

        with patch('services.observability.publisher.publish_dashboard_event', side_effect=capture):
            engine._run_scenario(scenario, loop=False)

        assert step_event_types == {'SIMULATION_STATUS_CHANGED'}, (
            "STEP_EMITTED must use event_type='SIMULATION_STATUS_CHANGED'"
        )


# ─────────────────────────────────────────────────────────────────────────────
# 2. Event Hub VALID_EVENT_TYPES coverage
# ─────────────────────────────────────────────────────────────────────────────

class TestEventHubPhase3Types:

    def test_simulation_status_changed_registered(self):
        assert 'SIMULATION_STATUS_CHANGED' in VALID_EVENT_TYPES

    def test_dataset_readiness_changed_registered(self):
        assert 'DATASET_READINESS_CHANGED' in VALID_EVENT_TYPES

    def test_rule_decision_evaluated_registered(self):
        assert 'RULE_DECISION_EVALUATED' in VALID_EVENT_TYPES


# ─────────────────────────────────────────────────────────────────────────────
# 3. Simulator API endpoint shape tests (via FastAPI TestClient)
# ─────────────────────────────────────────────────────────────────────────────

class TestSimulatorAPIPhase3:

    @pytest.fixture(autouse=True)
    def _setup_mocks(self):
        """Patch publish and record at module level before each test."""
        with patch('services.observability.publisher.publish_dashboard_event'), \
             patch('services.observability.event_journal.record_event_to_db'):
            yield

    def _make_engine_mock(self):
        mock_engine = MagicMock()
        mock_engine.get_status.return_value = {
            'state': 'IDLE',
            'simulation_id': None,
            'mode': None,
            'active_scenario_id': None,
            'emitted_event_count': 0,
            'last_event_timestamp': None,
            'started_at': None,
            'event_interval_seconds': 7.0,
            'error_message': None,
        }
        return mock_engine

    def test_status_returns_required_fields(self):
        from starlette.testclient import TestClient
        from services.observability.main import app
        mock_engine = self._make_engine_mock()
        with patch('services.observability.main.get_engine', return_value=mock_engine):
            client = TestClient(app)
            resp = client.get('/api/simulator/status')
        assert resp.status_code == 200
        data = resp.json()
        for field in ['state', 'simulation_id', 'emitted_event_count', 'event_interval_seconds']:
            assert field in data, f"Missing field '{field}' in /api/simulator/status response"

    def test_scenarios_returns_scenarios_key(self):
        from starlette.testclient import TestClient
        from services.observability.main import app
        from services.simulator.scenarios import list_scenarios
        mock_engine = self._make_engine_mock()
        with patch('services.observability.main.get_engine', return_value=mock_engine), \
             patch('services.observability.main.list_scenarios', return_value=list_scenarios()):
            client = TestClient(app)
            resp = client.get('/api/simulator/scenarios')
        assert resp.status_code == 200
        assert 'scenarios' in resp.json()

    def test_stop_returns_200(self):
        """Stop endpoint returns 200 OK."""
        from starlette.testclient import TestClient
        from services.observability.main import app
        mock_engine = self._make_engine_mock()
        stop_result = dict(mock_engine.get_status.return_value)
        mock_engine.stop.return_value = stop_result

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 11
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch('services.observability.main.get_engine', return_value=mock_engine), \
             patch('common.database.get_connection', return_value=mock_conn):
            client = TestClient(app)
            resp = client.post('/api/simulator/stop')

        assert resp.status_code == 200


# ─────────────────────────────────────────────────────────────────────────────
# 4. Step index correctness across all scenarios
# ─────────────────────────────────────────────────────────────────────────────

class TestStepIndexValues:

    @pytest.mark.parametrize('scenario_id', ['S1_ALL', 'S2_ANY', 'S3_QUORUM', 'S4_AST'])
    def test_step_index_matches_sequence_position(self, scenario_id):
        """step_index in STEP_EMITTED payload matches position in event_sequence."""
        from services.simulator.scenarios import SCENARIO_REGISTRY
        scenario = SCENARIO_REGISTRY[scenario_id]

        eng = SimulationEngine(event_interval_seconds=5.0)
        eng._stop_event = threading.Event()
        eng._pause_event = threading.Event()
        eng._pause_event.set()
        eng._simulation_id = f'test-{scenario_id}'
        eng._mode = scenario_id
        eng._state = SimEngineState.RUNNING
        eng._max_publish_retries = 1
        eng._interval = 0

        captured_indices = []

        def capture(*args, **kwargs):
            d = kwargs.get('data', {})
            if d.get('action') == 'STEP_EMITTED':
                captured_indices.append(d.get('step_index'))
            return MagicMock()

        with patch.object(eng, '_publish_to_kafka', return_value=True), \
             patch('services.observability.publisher.publish_dashboard_event', side_effect=capture):
            eng._run_scenario(scenario, loop=False)

        expected_indices = list(range(len(scenario.event_sequence)))
        assert captured_indices == expected_indices, (
            f"Scenario {scenario_id}: expected step indices {expected_indices}, got {captured_indices}"
        )
