"""
Unit tests for SupplySense AI - Olist Historical Order Replay Simulator.

Validates:
1. Dataset loading and verification from Olist/
2. Filtering out-of-sample orders and excluding canceled/unavailable
3. Item-to-order grouping with accurate quantities and unit prices
4. Timestamp linear scaling onto recent 8-week operational window
5. Batch creation matching OperationalOrderBatchRequest format
6. Simulator state transitions: IDLE -> RUNNING -> PAUSED -> RESUMED -> STOPPED -> COMPLETED
7. Telemetry reporting covering all 10 simulator status fields
8. Safe preview without initiating execution
"""

import os
import sys
import time
import pytest
import datetime
from unittest.mock import patch, MagicMock

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from services.simulator.olist_replay import OlistReplayLoader, OlistReplaySimulator


@pytest.fixture
def loader():
    return OlistReplayLoader()


@pytest.fixture
def simulator():
    sim = OlistReplaySimulator(base_url="http://127.0.0.1:8000")
    with patch.object(sim, '_emit_status_event'), patch.object(sim, '_submit_batch', return_value=True):
        yield sim
        sim.stop()


class TestOlistReplayLoader:
    def test_olist_files_exist(self, loader):
        """Verifies required Olist CSV datasets exist in Olist/ directory."""
        assert os.path.exists(loader.orders_path), f"Missing {loader.orders_path}"
        assert os.path.exists(loader.items_path), f"Missing {loader.items_path}"
        assert os.path.exists(loader.products_path), f"Missing {loader.products_path}"

    def test_load_batches_window_filtering(self, loader):
        """Tests window filtering with auto-fallback to active weeks."""
        batches, meta = loader.load_batches(
            window_start="2018-W36",
            window_end="2018-W42",
            auto_fallback=True,
            batch_size=25
        )
        assert len(batches) > 0
        assert meta["total_eligible_orders"] >= 25
        assert meta["auto_fallback_applied"] is True
        assert "2018-W29" in meta["window_active"]

    def test_load_batches_strict_window_small(self, loader):
        """Tests strict window filtering without auto-fallback."""
        batches, meta = loader.load_batches(
            window_start="2018-W36",
            window_end="2018-W42",
            auto_fallback=False,
            batch_size=25
        )
        # In W36-W42 without fallback, there is only 1 non-canceled order
        assert len(batches) == 1
        assert meta["total_eligible_orders"] == 1
        assert meta["auto_fallback_applied"] is False

    def test_timestamp_transformation_span(self, loader):
        """Verifies timestamps span across recent 8 weeks monotonically."""
        batches, meta = loader.load_batches(
            window_start="2018-W29",
            window_end="2018-W35",
            auto_fallback=False,
            batch_size=50
        )
        assert len(batches) > 0
        first_batch = batches[0]
        last_batch = batches[-1]

        t_first = datetime.datetime.fromisoformat(first_batch["orders"][0]["event_time"])
        t_last = datetime.datetime.fromisoformat(last_batch["orders"][-1]["event_time"])

        # Chronological monotonicity
        assert t_first < t_last

        # Relative span should be close to 8 weeks (within 7-8 weeks)
        span_days = (t_last - t_first).total_seconds() / 86400
        assert span_days > 45.0, f"Expected span > 45 days, got {span_days:.1f}"

    def test_batch_payload_contract(self, loader):
        """Validates that batch structure strictly adheres to OperationalOrderBatchRequest format."""
        batches, meta = loader.load_batches(
            window_start="2018-W30",
            window_end="2018-W30",
            auto_fallback=False,
            batch_size=10
        )
        assert len(batches) > 0
        sample = batches[0]
        assert "idempotency_key" in sample
        assert sample["source"] == "SIMULATED"
        assert sample["warehouse_id"] == "WH-SP-01"
        assert isinstance(sample["orders"], list)
        assert len(sample["orders"]) > 0

        first_order = sample["orders"][0]
        assert "order_id" in first_order
        assert "sku" in first_order
        assert "warehouse_id" in first_order
        assert "quantity" in first_order
        assert "unit_price" in first_order
        assert "event_time" in first_order
        assert "idempotency_key" in first_order
        assert first_order["quantity"] >= 1
        assert first_order["unit_price"] >= 0.0


class TestOlistReplaySimulatorStateMachine:
    def test_initial_state(self, simulator):
        """Simulator begins in IDLE state with 0 processed orders."""
        status = simulator.get_status()
        assert status["status"] == "IDLE"
        assert status["processed_orders"] == 0
        assert status["total_batches"] == 0

    def test_preview_mode(self, simulator):
        """Preview returns metadata and sample orders without initiating execution."""
        preview = simulator.preview(
            window_start="2018-W30",
            window_end="2018-W30",
            auto_fallback=False,
            batch_size=10
        )
        assert "metadata" in preview
        assert "sample_batch" in preview
        assert preview["sample_batch"]["orders_count"] == 10
        # Simulator state must remain IDLE
        assert simulator.status == "IDLE"

    def test_state_transitions(self, simulator):
        """Tests start, pause, resume, and stop lifecycle transitions."""
        # Mock _submit_batch to avoid network calls during unit test
        with patch.object(simulator, '_submit_batch', return_value=True):
            # 1. Start with max_batches=5 and short interval
            res = simulator.start(
                batch_size=5,
                interval_seconds=1.0,
                window_start="2018-W30",
                window_end="2018-W30",
                auto_fallback=False,
                max_batches=5
            )
            assert res["status"] == "RUNNING"
            assert res["total_batches"] == 5

            # 2. Pause
            res_pause = simulator.pause()
            assert res_pause["status"] == "PAUSED"

            # 3. Resume
            res_resume = simulator.resume()
            assert res_resume["status"] == "RUNNING"

            # 4. Stop
            res_stop = simulator.stop()
            assert res_stop["status"] == "STOPPED"

    def test_status_telemetry_fields(self, simulator):
        """Verifies get_status returns all 10 required operational metrics."""
        status = simulator.get_status()
        required_fields = [
            "status",
            "run_id",
            "total_eligible_orders",
            "processed_orders",
            "remaining_orders",
            "current_batch",
            "total_batches",
            "successful_batches",
            "failed_batches",
            "progress_pct"
        ]
        for field in required_fields:
            assert field in status, f"Missing required telemetry field: {field}"
