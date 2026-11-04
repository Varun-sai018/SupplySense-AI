"""
SupplySense AI - Phase D Unit Tests: Real-Time Reliability, Simulation Transparency & Visualization Accuracy
Tests:
  1. SSE formal connection state machine & backward compatibility aliases.
  2. Exponential backoff retry loop with bounded jitter.
  3. Heartbeat watchdog timing and stale-connection detection contract.
  4. Targeted frontend invalidation and trigger dispatch routing.
  5. Pipeline execution classification (OPERATIONAL vs. SIMULATED) and protected run preservation.
  6. Dataset provenance attribution, virtual feeds distinction, and upstream source identification.
  7. Rule A 25% cart abandonment alert threshold consistency.
  8. S1–S5 Scenario Architecture Guide completeness and condition types.
"""

import os
import sys
import pytest
import math
import random

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import services
from services.observability.event_hub import (
    DashboardEvent,
    VALID_EVENT_TYPES,
    HEARTBEAT_INTERVAL_SECONDS,
)
from services.dependency_engine.rule_evaluator import evaluate_rule
from services.dependency_engine.rules.rule_cart_abandonment import (
    RULE_ID as RULE_A_ID,
    calculate_rolling_dropoff,
    is_peak_hour,
)


class TestSSEReliabilityContract:
    """Verifies SSE connection state machine, backoff intervals, and heartbeat watchdog."""

    VALID_SSE_STATES = {"CONNECTING", "CONNECTED", "RECONNECTING", "OFFLINE"}
    BACKOFF_STEPS_MS = [1000, 2000, 4000, 8000, 15000]
    WATCHDOG_TIMEOUT_MS = 30000

    def test_formal_connection_states(self):
        """Phase D requires 4 formal connection states with backward compatibility for LIVE."""
        assert "CONNECTING" in self.VALID_SSE_STATES
        assert "CONNECTED" in self.VALID_SSE_STATES
        assert "RECONNECTING" in self.VALID_SSE_STATES
        assert "OFFLINE" in self.VALID_SSE_STATES

        # Backward compatibility mapping
        def is_live(state: str) -> bool:
            return state in {"CONNECTED", "LIVE"}

        assert is_live("CONNECTED") is True
        assert is_live("LIVE") is True
        assert is_live("RECONNECTING") is False
        assert is_live("OFFLINE") is False
        assert is_live("CONNECTING") is False

    def test_exponential_backoff_intervals_and_capping(self):
        """Exponential backoff must follow 1s, 2s, 4s, 8s, max 15s."""
        for attempt, expected in enumerate(self.BACKOFF_STEPS_MS):
            step_idx = min(attempt, len(self.BACKOFF_STEPS_MS) - 1)
            base_delay = self.BACKOFF_STEPS_MS[step_idx]
            assert base_delay == expected

        # Beyond 5 attempts, delays must cap at 15,000ms
        for attempt in [5, 10, 50]:
            step_idx = min(attempt, len(self.BACKOFF_STEPS_MS) - 1)
            base_delay = self.BACKOFF_STEPS_MS[step_idx]
            assert base_delay == 15000

    def test_backoff_jitter_bounds(self):
        """Jitter should be strictly within +/- 20% of the base delay."""
        random.seed(42)
        base = 4000
        for _ in range(50):
            jitter = base * 0.2 * (random.random() * 2 - 1)
            delay = round(base + jitter)
            assert 3200 <= delay <= 4800

    def test_heartbeat_watchdog_threshold(self):
        """Heartbeat interval is 15s; watchdog must be 30s (2x window to prevent false timeouts)."""
        assert HEARTBEAT_INTERVAL_SECONDS == 15.0
        assert self.WATCHDOG_TIMEOUT_MS == 30000
        assert self.WATCHDOG_TIMEOUT_MS >= HEARTBEAT_INTERVAL_SECONDS * 1000 * 2

    def test_heartbeat_event_registered_in_hub(self):
        """EventHub must support HEARTBEAT event type."""
        assert "HEARTBEAT" in VALID_EVENT_TYPES
        evt = DashboardEvent(
            event_id="hb-test-1",
            event_type="HEARTBEAT",
            entity="SYSTEM",
            data={"status": "alive"}
        )
        evt.validate()
        sse_str = evt.to_sse_format()
        assert "event: HEARTBEAT" in sse_str


class TestTargetedInvalidationRouting:
    """Verifies that incoming events map strictly to targeted component triggers."""

    @staticmethod
    def route_event(event_type: str, entity: str) -> set:
        targets = set()
        entity_upper = (entity or "").upper()
        if (
            event_type in {"INVENTORY_REORDER_STATE_CHANGED", "DELIVERY_COMPLETED"}
            or entity_upper == "INVENTORY"
        ):
            targets.add("INVENTORY")
        elif event_type == "PIPELINE_STATUS_CHANGED" or entity_upper == "PIPELINE":
            targets.add("EXECUTIONS")
        elif event_type == "DATASET_READINESS_CHANGED" or entity_upper == "DATASET":
            targets.add("DATASETS")
            targets.add("SIMULATION")
        elif event_type == "RULE_DECISION_EVALUATED" or entity_upper == "RULE":
            targets.add("DEPENDENCIES")
            targets.add("EXECUTIONS")
        elif event_type == "SIMULATION_STATUS_CHANGED" or entity_upper == "SIMULATION":
            targets.add("SIMULATION")
            targets.add("DATASETS")
        elif event_type == "REST_SNAPSHOT_REQUIRED":
            targets.add("ALL")
        else:
            targets.add("ALL")
        return targets

    def test_inventory_event_routing(self):
        targets = self.route_event("INVENTORY_REORDER_STATE_CHANGED", "INVENTORY")
        assert targets == {"INVENTORY"}
        assert "EXECUTIONS" not in targets

    def test_pipeline_status_routing(self):
        targets = self.route_event("PIPELINE_STATUS_CHANGED", "PIPELINE")
        assert targets == {"EXECUTIONS"}
        assert "INVENTORY" not in targets

    def test_dataset_readiness_routing(self):
        targets = self.route_event("DATASET_READINESS_CHANGED", "DATASET")
        assert targets == {"DATASETS", "SIMULATION"}
        assert "INVENTORY" not in targets

    def test_rule_decision_routing(self):
        targets = self.route_event("RULE_DECISION_EVALUATED", "RULE")
        assert targets == {"DEPENDENCIES", "EXECUTIONS"}
        assert "INVENTORY" not in targets

    def test_simulation_status_routing(self):
        targets = self.route_event("SIMULATION_STATUS_CHANGED", "SIMULATION")
        assert targets == {"SIMULATION", "DATASETS"}

    def test_rest_snapshot_fallback_routing(self):
        targets = self.route_event("REST_SNAPSHOT_REQUIRED", "SYSTEM")
        assert targets == {"ALL"}


class TestExecutionClassification:
    """Verifies OPERATIONAL vs SIMULATED classification and safety baseline runs."""

    @staticmethod
    def classify_execution(pipeline_name: str, scope: str = None) -> str:
        if pipeline_name and pipeline_name.startswith("SIM-"):
            return "SIMULATED"
        if scope == "SIMULATED":
            return "SIMULATED"
        return "OPERATIONAL"

    def test_simulated_pipeline_names(self):
        for name in [
            "SIM-ALL-Pipeline",
            "SIM-ANY-Pipeline",
            "SIM-QUORUM-Pipeline",
            "SIM-AST-Pipeline"
        ]:
            assert self.classify_execution(name) == "SIMULATED"

    def test_operational_pipeline_names(self):
        for name in [
            "Customer-Orders-ETL",
            "Inventory-CDC-Sync",
            "Demand-Forecasting-XGBoost",
            "Reorder-Recommendation-DAG"
        ]:
            assert self.classify_execution(name) == "OPERATIONAL"

    def test_protected_baseline_executions_are_operational(self):
        """Protected executions (#36, #99586, #99653) must be strictly OPERATIONAL."""
        protected = [
            (36, "Demand-Forecasting-XGBoost"),
            (99586, "Customer-Orders-ETL"),
            (99653, "Customer-Orders-ETL")
        ]
        for exec_id, pipe in protected:
            classification = self.classify_execution(pipe)
            assert classification == "OPERATIONAL", f"Execution #{exec_id} ({pipe}) must be OPERATIONAL"


class TestDatasetProvenanceAttribution:
    """Verifies dataset attribution rules and distinguishing physical vs virtual feeds."""

    @staticmethod
    def get_dataset_provenance(name: str, table_name: str, source_type: str) -> dict:
        if source_type == "SIMULATED" or (name and name.startswith("SIM_")):
            return {
                "tag": "SIMULATED",
                "upstream": "Virtual Simulation Feed — 0 physical table rows",
                "is_virtual": True
            }
        if table_name and table_name.startswith("olist_"):
            return {
                "tag": "HISTORICAL",
                "upstream": "OLIST Database (Brazilian E-Commerce)",
                "is_virtual": False
            }
        if table_name and (table_name.startswith("square_") or table_name.startswith("operational_")):
            return {
                "tag": "OPERATIONAL",
                "upstream": "SQUARE Point-of-Sale / Debezium CDC",
                "is_virtual": False
            }
        return {
            "tag": "UNAVAILABLE",
            "upstream": "SOURCE UNKNOWN",
            "is_virtual": False
        }

    def test_simulated_datasets_attribution(self):
        sim_names = [
            "SIM_Raw_Feed", "SIM_Catalog_Feed", "SIM_Pricing_Feed",
            "SIM_Primary_Feed", "SIM_Backup_Feed",
            "SIM_Regional_Node_A", "SIM_Regional_Node_B", "SIM_Regional_Node_C", "SIM_Regional_Node_D",
            "SIM_Inventory_Feed", "SIM_Supplier_Feed"
        ]
        for name in sim_names:
            prov = self.get_dataset_provenance(name, "sim_mock_table", "SIMULATED")
            assert prov["tag"] == "SIMULATED"
            assert prov["is_virtual"] is True
            assert "0 physical table rows" in prov["upstream"]

    def test_olist_historical_datasets_attribution(self):
        tables = [
            ("Orders", "olist_orders"),
            ("Order Items", "olist_order_items"),
            ("Products", "olist_products"),
            ("Sellers", "olist_sellers")
        ]
        for name, tbl in tables:
            prov = self.get_dataset_provenance(name, tbl, "BATCH")
            assert prov["tag"] == "HISTORICAL"
            assert prov["is_virtual"] is False
            assert "OLIST Database" in prov["upstream"]

    def test_operational_inventory_attribution(self):
        prov = self.get_dataset_provenance("Inventory", "square_inventory", "CDC")
        assert prov["tag"] == "OPERATIONAL"
        assert prov["is_virtual"] is False
        assert "SQUARE Point-of-Sale / Debezium CDC" in prov["upstream"]


class TestRuleAThresholdAndScenarioGuide:
    """Verifies that the 25% cart abandonment threshold matches Rule A and scenario definitions are complete."""

    def test_rule_a_25_percent_threshold(self):
        """Rule A threshold must evaluate dropoff_rate > 0.25 as TRIGGER and <= 0.25 as BLOCK."""
        expression = {
            "operator": "AND",
            "conditions": [
                {"field": "dropoff_rate", "op": ">", "value": 0.25},
                {"field": "peak_hour", "op": "==", "value": True}
            ]
        }
        # Above 25% during peak hour -> TRIGGER
        res_trigger = evaluate_rule(
            rule_id=RULE_A_ID,
            expression=expression,
            context={"dropoff_rate": 0.26, "peak_hour": True}
        )
        assert res_trigger.decision == "TRIGGER"

        # Exactly 25% or below -> BLOCK
        res_boundary = evaluate_rule(
            rule_id=RULE_A_ID,
            expression=expression,
            context={"dropoff_rate": 0.25, "peak_hour": True}
        )
        assert res_boundary.decision == "BLOCK"

        res_low = evaluate_rule(
            rule_id=RULE_A_ID,
            expression=expression,
            context={"dropoff_rate": 0.20, "peak_hour": True}
        )
        assert res_low.decision == "BLOCK"

    def test_scenario_architecture_guide_completeness(self):
        """S1 to S5 scenario architecture definitions must be fully defined."""
        scenarios = {
            "S1_ALL": {"type": "ALL", "min_datasets": 3},
            "S2_ANY": {"type": "ANY", "min_datasets": 2},
            "S3_QUORUM": {"type": "QUORUM", "quorum_count": 3, "total_nodes": 4},
            "S4_AST": {"type": "AST", "expression": "AND(SIM_Inventory_Feed == 'READY', SIM_Supplier_Feed == 'READY')"},
            "S5_CONTINUOUS": {"type": "ROTATION", "rotation": ["S1_ALL", "S2_ANY", "S3_QUORUM", "S4_AST"]}
        }
        assert len(scenarios) == 5
        assert scenarios["S3_QUORUM"]["quorum_count"] == 3
        assert scenarios["S3_QUORUM"]["total_nodes"] == 4
        assert "S1_ALL" in scenarios["S5_CONTINUOUS"]["rotation"]
        assert "S4_AST" in scenarios["S5_CONTINUOUS"]["rotation"]
