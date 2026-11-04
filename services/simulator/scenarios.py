"""
SupplySense AI - Simulation Scenario Definitions (Phase 2)

Defines the canonical set of supply-chain simulation scenarios that exercise
the existing ALL, ANY, QUORUM and AST dependency evaluation strategies.

Each scenario defines:
  - scenario_id: unique string identifier.
  - pipeline_name: the SIM-* pipeline name registered in the pipeline registry.
  - condition_type: ALL, ANY, QUORUM, or AST.
  - datasets: ordered list of dataset names for this scenario.
  - event_sequence: ordered list of SimEvent descriptors that drive the test.
  - ast_expression: optional AST expression string (required for AST scenarios).
  - description: human-readable summary.

Safety Invariants:
  - All datasets named here are SIMULATED source_type (dataset_id >= 265).
  - No operational datasets (IDs 1-5) are referenced or mutated.
  - No Airflow DAGs, ML training, or replenishment orders are triggered.
  - No historical Olist records are accessed.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any

# ---------------------------------------------------------------------------
# SimEvent descriptor: a single Kafka event within a scenario sequence
# ---------------------------------------------------------------------------

@dataclass
class SimEvent:
    """
    Describes a single dataset-readiness event emitted during a scenario run.

    Fields:
        dataset_name: Target simulated dataset to mark as READY.
        expected_decision: 'BLOCK' or 'TRIGGER' expected after this event.
        step_description: Human-readable label for this step.
        new_status: Status to set on the dataset (default: 'READY').
    """
    dataset_name: str
    expected_decision: str          # 'BLOCK' | 'TRIGGER'
    step_description: str
    new_status: str = "READY"


# ---------------------------------------------------------------------------
# SimScenario descriptor
# ---------------------------------------------------------------------------

@dataclass
class SimScenario:
    """
    Complete specification for a simulation scenario.

    Fields:
        scenario_id: Unique identifier (e.g. 'S1_ALL').
        pipeline_name: Registered SIM-* pipeline name.
        condition_type: 'ALL' | 'ANY' | 'QUORUM' | 'AST'.
        description: Human-readable summary.
        datasets: All dataset names associated with this pipeline.
        event_sequence: Ordered list of SimEvent steps.
        ast_expression: AST rule string (required for AST scenarios, else None).
        required_count: QUORUM threshold (required for QUORUM scenarios, else None).
        tags: Optional metadata tags.
    """
    scenario_id: str
    pipeline_name: str
    condition_type: str
    description: str
    datasets: List[str]
    event_sequence: List[SimEvent]
    ast_expression: Optional[str] = None
    required_count: Optional[int] = None
    tags: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# S1 — ALL Strategy
# All three datasets must reach READY before the pipeline triggers.
# Expected: BLOCK, BLOCK, TRIGGER
# ---------------------------------------------------------------------------

S1_ALL = SimScenario(
    scenario_id="S1_ALL",
    pipeline_name="SIM-ALL-Pipeline",
    condition_type="ALL",
    description=(
        "ALL scenario: SIM-ALL-Pipeline triggers only when ALL three datasets "
        "(SIM_Raw_Orders, SIM_Catalog_Feed, SIM_Pricing_Index) are READY. "
        "Steps 1 and 2 produce BLOCK; Step 3 produces TRIGGER."
    ),
    datasets=["SIM_Raw_Orders", "SIM_Catalog_Feed", "SIM_Pricing_Index"],
    event_sequence=[
        SimEvent(
            dataset_name="SIM_Raw_Orders",
            expected_decision="BLOCK",
            step_description="Step 1/3 — SIM_Raw_Orders READY (1 of 3 ready). Expected: BLOCK.",
        ),
        SimEvent(
            dataset_name="SIM_Catalog_Feed",
            expected_decision="BLOCK",
            step_description="Step 2/3 — SIM_Catalog_Feed READY (2 of 3 ready). Expected: BLOCK.",
        ),
        SimEvent(
            dataset_name="SIM_Pricing_Index",
            expected_decision="TRIGGER",
            step_description="Step 3/3 — SIM_Pricing_Index READY (3 of 3 ready). Expected: TRIGGER.",
        ),
    ],
    ast_expression=None,
    required_count=None,
    tags=["all", "three-dataset", "sequential"],
)


# ---------------------------------------------------------------------------
# S2 — ANY Strategy
# The pipeline triggers as soon as the FIRST dataset reaches READY.
# Expected: TRIGGER on Step 1.
# ---------------------------------------------------------------------------

S2_ANY = SimScenario(
    scenario_id="S2_ANY",
    pipeline_name="SIM-ANY-Pipeline",
    condition_type="ANY",
    description=(
        "ANY scenario: SIM-ANY-Pipeline triggers as soon as any one of "
        "(SIM_Primary_Stream, SIM_Backup_Stream) becomes READY. "
        "Step 1 produces TRIGGER immediately."
    ),
    datasets=["SIM_Primary_Stream", "SIM_Backup_Stream"],
    event_sequence=[
        SimEvent(
            dataset_name="SIM_Primary_Stream",
            expected_decision="TRIGGER",
            step_description="Step 1/2 — SIM_Primary_Stream READY (any-one satisfied). Expected: TRIGGER.",
        ),
        # Step 2 is only reached in continuous mode (after reset); first READY already triggers.
        SimEvent(
            dataset_name="SIM_Backup_Stream",
            expected_decision="TRIGGER",
            step_description="Step 2/2 — SIM_Backup_Stream READY (any-one satisfied). Expected: TRIGGER.",
        ),
    ],
    ast_expression=None,
    required_count=None,
    tags=["any", "two-dataset", "fast-trigger"],
)


# ---------------------------------------------------------------------------
# S3 — QUORUM Strategy (3 of 4 regional nodes)
# Expected: BLOCK, BLOCK, TRIGGER (after three nodes READY).
# ---------------------------------------------------------------------------

S3_QUORUM = SimScenario(
    scenario_id="S3_QUORUM",
    pipeline_name="SIM-QUORUM-Pipeline",
    condition_type="QUORUM",
    description=(
        "QUORUM scenario: SIM-QUORUM-Pipeline triggers when 3 of 4 regional nodes "
        "(SIM_Regional_Node_A/B/C/D) are READY. "
        "Steps 1-2 produce BLOCK; Step 3 produces TRIGGER (quorum met)."
    ),
    datasets=[
        "SIM_Regional_Node_A",
        "SIM_Regional_Node_B",
        "SIM_Regional_Node_C",
        "SIM_Regional_Node_D",
    ],
    event_sequence=[
        SimEvent(
            dataset_name="SIM_Regional_Node_A",
            expected_decision="BLOCK",
            step_description="Step 1/4 — Node A READY (1 of 3 required). Expected: BLOCK.",
        ),
        SimEvent(
            dataset_name="SIM_Regional_Node_B",
            expected_decision="BLOCK",
            step_description="Step 2/4 — Node B READY (2 of 3 required). Expected: BLOCK.",
        ),
        SimEvent(
            dataset_name="SIM_Regional_Node_C",
            expected_decision="TRIGGER",
            step_description="Step 3/4 — Node C READY (3 of 3 required = quorum met). Expected: TRIGGER.",
        ),
        # Step 4 only reached after cycle reset in continuous mode.
        SimEvent(
            dataset_name="SIM_Regional_Node_D",
            expected_decision="BLOCK",
            step_description="Step 4/4 — Node D READY (1 of 3 required after cycle reset). Expected: BLOCK.",
        ),
    ],
    ast_expression=None,
    required_count=3,
    tags=["quorum", "four-dataset", "threshold-3"],
)


# ---------------------------------------------------------------------------
# S4 — AST Strategy
# Expression: AND(SIM_Inventory_Feed == 'READY', SIM_Supplier_Feed == 'READY')
# Expected: BLOCK when only one condition met; TRIGGER when both are met.
# ---------------------------------------------------------------------------

S4_AST = SimScenario(
    scenario_id="S4_AST",
    pipeline_name="SIM-AST-Pipeline",
    condition_type="AST",
    description=(
        "AST scenario: SIM-AST-Pipeline triggers when the AST expression "
        "AND(SIM_Inventory_Feed == 'READY', SIM_Supplier_Feed == 'READY') evaluates to True. "
        "Step 1 produces BLOCK (only one condition met); Step 2 produces TRIGGER."
    ),
    datasets=["SIM_Inventory_Feed", "SIM_Supplier_Feed"],
    event_sequence=[
        SimEvent(
            dataset_name="SIM_Inventory_Feed",
            expected_decision="BLOCK",
            step_description=(
                "Step 1/2 — SIM_Inventory_Feed READY. "
                "AST: AND requires both; SIM_Supplier_Feed still WAITING. Expected: BLOCK."
            ),
        ),
        SimEvent(
            dataset_name="SIM_Supplier_Feed",
            expected_decision="TRIGGER",
            step_description=(
                "Step 2/2 — SIM_Supplier_Feed READY. "
                "AST: AND(SIM_Inventory_Feed=='READY', SIM_Supplier_Feed=='READY') = True. Expected: TRIGGER."
            ),
        ),
    ],
    ast_expression="AND(SIM_Inventory_Feed == 'READY', SIM_Supplier_Feed == 'READY')",
    required_count=None,
    tags=["ast", "two-dataset", "logical-and"],
)


# ---------------------------------------------------------------------------
# S5 — Continuous Rotation (S1 → S2 → S3 → S4 → repeat)
# Not a scenario with its own datasets; it is a scheduling directive that
# the SimulationEngine uses to rotate through S1–S4 indefinitely.
# ---------------------------------------------------------------------------

SCENARIO_ROTATION_ORDER = [
    S1_ALL,
    S2_ANY,
    S3_QUORUM,
    S4_AST,
]

# ---------------------------------------------------------------------------
# Public catalogue
# ---------------------------------------------------------------------------

# Registry for all named scenarios (excluding the meta-rotation entry)
SCENARIO_REGISTRY: Dict[str, SimScenario] = {
    s.scenario_id: s for s in SCENARIO_ROTATION_ORDER
}


def get_scenario(scenario_id: str) -> Optional[SimScenario]:
    """Returns the SimScenario for the given ID, or None if not found."""
    return SCENARIO_REGISTRY.get(scenario_id)


def list_scenarios() -> List[Dict[str, Any]]:
    """Returns a JSON-serializable summary of all registered scenarios."""
    result = []
    for s in SCENARIO_ROTATION_ORDER:
        result.append({
            "scenario_id": s.scenario_id,
            "pipeline_name": s.pipeline_name,
            "condition_type": s.condition_type,
            "description": s.description,
            "datasets": s.datasets,
            "steps": len(s.event_sequence),
            "ast_expression": s.ast_expression,
            "required_count": s.required_count,
            "tags": s.tags,
            "event_sequence": [
                {
                    "dataset_name": e.dataset_name,
                    "expected_decision": e.expected_decision,
                    "step_description": e.step_description,
                    "new_status": e.new_status,
                }
                for e in s.event_sequence
            ],
        })
    return result
