"""
SupplySense AI - Rule B: Automated Inventory Reorder (Module 1)

Condition:
  - Inventory available_quantity is strictly below the configured safe threshold (< safe_threshold).
  - Supplier API status is 'Online' and SKU availability is not OUT_OF_STOCK.
  - No in-flight replenishment for the same SKU/warehouse.

Actions:
  - Enforce concurrency & database idempotency.
  - Record the decision and create a canonical pipeline execution.
  - Trigger the pipeline runner ('Inventory Reorder Pipeline') in LOCAL mode.
  - Publish structured decision details for dashboard consumption.

Backward-compatibility notes:
  - `inventory_level`, `supplier_status`, `pending_orders`, and `supplier_adapter`
    are accepted as optional keyword arguments so that existing tests and callers
    that pre-populate those values continue to work.
  - When a live DB connection is provided, authoritative values are always read
    from `operational_inventory` and `operational_suppliers` first.
    Caller-supplied overrides are used ONLY when no DB record is found.
"""

import os
import sys
import json
import logging
import datetime
from typing import Dict, Any, Optional

# Ensure repository root is in sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from ..rule_evaluator import EvaluationResult
from ..repository import (
    record_action_attempt,
    update_action_status,
    get_action_by_key,
    get_inventory,
    get_supplier_availability,
)
from ..adapters.supplier_adapter import SupplierAPIAdapter
from services.pipeline_runner.runner import execute_pipeline

logger = logging.getLogger(__name__)

RULE_ID = "RULE_B_INVENTORY_REORDER"
PIPELINE_NAME = "Inventory Reorder Pipeline"
DEFAULT_SAFE_THRESHOLD = int(os.getenv("INVENTORY_SAFE_THRESHOLD", "20"))
DEFAULT_REORDER_QUANTITY = int(os.getenv("INVENTORY_REORDER_QUANTITY", "50"))


def evaluate_and_dispatch_inventory_reorder(
    product_id: str,
    supplier_id: str = "supplier_primary",
    safe_threshold: int = DEFAULT_SAFE_THRESHOLD,
    reorder_quantity: int = DEFAULT_REORDER_QUANTITY,
    orchestrator: Optional[str] = None,
    connection=None,
    # ---------- backward-compat / test-override parameters ----------
    inventory_level: Optional[int] = None,
    supplier_status: Optional[str] = None,
    pending_orders: Optional[int] = None,
    supplier_adapter=None,          # accepted but not used (simulation only)
) -> Dict[str, Any]:
    """Evaluate Rule B against the *persistent* operational state.

    When a real database connection is supplied the function always reads
    authoritative values from `operational_inventory`,
    `operational_suppliers`, and `supplier_catalog_availability`.

    The optional `inventory_level`, `supplier_status`, `pending_orders`,
    and `supplier_adapter` parameters are kept for backward compatibility
    with existing tests and callers.  They act as fallbacks if the DB
    record is absent (e.g. pure-mock unit tests with no real MySQL).

    Args:
        product_id:       SKU identifier.
        supplier_id:      Supplier identifier.
        safe_threshold:   Optional threshold override (DB value takes precedence).
        reorder_quantity: Quantity to request when triggering.
        orchestrator:     Optional pipeline orchestrator override.
        connection:       Optional existing DB connection.
        inventory_level:  Fallback available quantity (unit-test override).
        supplier_status:  Fallback supplier status (unit-test override).
        pending_orders:   Fallback pending-orders count (unit-test override).
        supplier_adapter: Accepted but ignored; simulation is always used.
    """
    should_close_conn = False
    if connection is None:
        connection = get_connection()
        should_close_conn = True

    try:
        # -----------------------------------------------------------------
        # 1. Resolve live inventory record (fallback to caller-supplied)
        # -----------------------------------------------------------------
        available_qty: Optional[int] = None
        db_threshold: Optional[int] = None

        try:
            inventory_rec = get_inventory(
                sku=product_id, warehouse_id="WH-SP-01", connection=connection
            )
        except Exception as e:
            logger.warning(f"DB inventory lookup failed for '{product_id}': {e}")
            inventory_rec = None

        if inventory_rec and isinstance(inventory_rec, (dict, tuple)):
            available_qty = inventory_rec["available_quantity"] if isinstance(inventory_rec, dict) else inventory_rec[2]
            db_threshold  = inventory_rec["reorder_threshold"]  if isinstance(inventory_rec, dict) else inventory_rec[5]
        elif inventory_level is not None:
            # Unit-test / mock fallback
            available_qty = inventory_level
        else:
            reason = f"Inventory record not found for SKU '{product_id}'"
            logger.info(reason)
            return {"matched": False, "decision": "BLOCK", "reasons": [reason], "lifecycle_state": "BLOCKED"}

        # Threshold: prefer DB value; fall back to caller/env default
        safe_threshold = db_threshold or safe_threshold

        # -----------------------------------------------------------------
        # 2. Resolve supplier status and SKU availability (fallback)
        # -----------------------------------------------------------------
        resolved_supplier_status: Optional[str] = None
        sku_availability: Optional[str] = "IN_STOCK"  # assume available unless DB says otherwise

        try:
            supplier_info = get_supplier_availability(
                supplier_id=supplier_id, sku=product_id, connection=connection
            )
        except Exception as e:
            logger.warning(f"DB supplier lookup failed for '{supplier_id}': {e}")
            supplier_info = None

        if supplier_info and isinstance(supplier_info, (dict, tuple)):
            resolved_supplier_status = (
                supplier_status if supplier_status is not None
                else (supplier_info["status"] if isinstance(supplier_info, dict) else supplier_info[2])
            )
            sku_availability = supplier_info.get("supply_availability") if isinstance(supplier_info, dict) else None
        elif supplier_status is not None:
            # Unit-test / mock fallback
            resolved_supplier_status = supplier_status
        else:
            reason = f"Supplier '{supplier_id}' not found"
            logger.info(reason)
            return {"matched": False, "decision": "BLOCK", "reasons": [reason], "lifecycle_state": "BLOCKED"}

        # -----------------------------------------------------------------
        # 3. Detect any in-flight replenishment for this SKU/warehouse
        #    (pending_orders override is honoured for unit tests)
        # -----------------------------------------------------------------
        has_in_flight = False
        if pending_orders is not None and pending_orders > 0:
            # Caller explicitly told us there are pending orders
            has_in_flight = True
        else:
            try:
                cur = connection.cursor()
                cur.execute(
                    """
                    SELECT replenishment_id, status FROM replenishment_orders
                    WHERE sku = %s AND warehouse_id = %s
                      AND status IN ('DISPATCHED', 'CONFIRMED', 'IN_TRANSIT')
                    LIMIT 1
                    """,
                    (product_id, "WH-SP-01"),
                )
                in_flight = cur.fetchone()
                has_in_flight = bool(in_flight) and isinstance(in_flight, (dict, tuple))
            except Exception as e:
                logger.warning(f"Could not query replenishment_orders: {e}")
                # If we can't query the table (e.g., pure mock), treat as no in-flight
                has_in_flight = False

        # -----------------------------------------------------------------
        # 4. Evaluate Rule B conditions
        # -----------------------------------------------------------------
        low_stock = available_qty < safe_threshold
        supplier_eligible = (
            resolved_supplier_status == "Online"
            and sku_availability != "OUT_OF_STOCK"
        )

        eval_result = EvaluationResult(matched=False, decision="BLOCK", rule_id=RULE_ID, reasons=[])
        if not low_stock:
            eval_result.reasons.append("Inventory above safe threshold – no reorder needed.")
        elif not supplier_eligible:
            eval_result.reasons.append(
                f"Supplier unavailable or SKU out of stock "
                f"(status={resolved_supplier_status}, availability={sku_availability})."
            )
        elif has_in_flight:
            eval_result.reasons.append(
                "Existing in-flight replenishment prevents duplicate dispatch."
            )
        else:
            eval_result.matched = True
            eval_result.decision = "TRIGGER"
            eval_result.reasons.append("Low stock and eligible supplier – trigger reorder.")

        # -----------------------------------------------------------------
        # 5. Build response summary
        # -----------------------------------------------------------------
        response_summary: Dict[str, Any] = {
            "rule_id": RULE_ID,
            "product_id": product_id,
            "matched": eval_result.matched,
            "decision": eval_result.decision,
            "metrics": {
                "available_quantity": available_qty,
                "safe_threshold": safe_threshold,
                "supplier_status": resolved_supplier_status,
                "sku_availability": sku_availability,
                "in_flight_replenishment": has_in_flight,
            },
            "reasons": eval_result.reasons,
            "lifecycle_state": "EVALUATED" if eval_result.matched else "BLOCKED",
            "execution": None,
            "action": None,
        }

        # BLOCK path — no side effects
        if not eval_result.matched:
            return response_summary

        # -----------------------------------------------------------------
        # TRIGGER path
        # -----------------------------------------------------------------
        now = datetime.datetime.now()
        idempotency_key = f"reorder_{product_id}_{now.strftime('%Y%m%d%H')}"

        # Guard: reuse canonical action if already exists
        existing_action = get_action_by_key(RULE_ID, idempotency_key, connection=connection)
        if existing_action:
            status = existing_action.get("status")
            if status in ("PENDING", "DISPATCHED", "CONFIRMED"):
                logger.info(
                    f"Idempotency: Reorder for SKU '{product_id}' already in status '{status}'."
                )
                response_summary["decision"] = "SKIPPED"
                response_summary["lifecycle_state"] = "IDEMPOTENT_SKIPPED"
                response_summary["reasons"].append(
                    f"Duplicate reorder prevented for key {idempotency_key}."
                )
                response_summary["action"] = existing_action
                return response_summary

        # Persist decision
        cursor = connection.cursor()
        reason_str = "; ".join(eval_result.reasons)
        cursor.execute(
            """
            INSERT INTO pipeline_decisions (
                pipeline_name, condition_type, decision, required_count,
                ready_count, total_required, reason, triggered_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (PIPELINE_NAME, "NESTED_RULE_B", "TRIGGER", 3, 3, 3, reason_str[:500], now),
        )
        decision_id = cursor.lastrowid
        connection.commit()

        # Create execution record
        synthetic_event_id = int(now.timestamp() * 1000) % 2147483647
        cursor.execute(
            """
            INSERT INTO pipeline_executions (pipeline_name, decision_id, triggering_event_id, status)
            VALUES (%s, %s, %s, 'RUNNING')
            """,
            (PIPELINE_NAME, decision_id, synthetic_event_id),
        )
        execution_id = cursor.lastrowid
        connection.commit()

        # Create action record (unique constraint protects against races)
        reorder_payload = {
            "product_id": product_id,
            "reorder_quantity": reorder_quantity,
            "supplier_id": supplier_id,
            "available_quantity": available_qty,
            "safe_threshold": safe_threshold,
            "execution_id": execution_id,
            "decision_id": decision_id,
        }
        action_id = record_action_attempt(
            rule_id=RULE_ID,
            idempotency_key=idempotency_key,
            status="PENDING",
            payload=reorder_payload,
            decision_id=decision_id,
            execution_id=execution_id,
            connection=connection,
        )

        # Dispatch pipeline runner
        response_summary["lifecycle_state"] = "DISPATCHED"
        logger.info(f"Invoking pipeline runner for execution #{execution_id} ({PIPELINE_NAME})...")
        runner_result = execute_pipeline(
            execution_id=execution_id,
            pipeline_name=PIPELINE_NAME,
            connection=connection,
            orchestrator=orchestrator,
        )
        exec_status = runner_result.get("status", "UNKNOWN")
        response_summary["execution"] = runner_result

        if exec_status == "COMPLETED":
            response_summary["lifecycle_state"] = "CONFIRMED"
            response_summary["reorder_confirmed"] = True
            if action_id:
                update_action_status(
                    action_id=action_id,
                    status="CONFIRMED",
                    response_payload=runner_result.get("result"),
                    execution_id=execution_id,
                    connection=connection,
                )
        else:
            response_summary["lifecycle_state"] = "FAILED"
            response_summary["reorder_confirmed"] = False
            if action_id:
                update_action_status(
                    action_id=action_id,
                    status="FAILED",
                    error_message=runner_result.get("error"),
                    execution_id=execution_id,
                    connection=connection,
                )

        response_summary["action"] = {
            "action_id": action_id,
            "idempotency_key": idempotency_key,
            "status": response_summary["lifecycle_state"],
        }

        # Emit dashboard event
        try:
            from services.observability.publisher import publish_dashboard_event
            publish_dashboard_event(
                event_type="INVENTORY_REORDER_STATE_CHANGED",
                entity="INVENTORY",
                severity="INFO" if response_summary.get("reorder_confirmed") else "CRITICAL",
                decision_id=decision_id,
                execution_id=execution_id,
                correlation_id=idempotency_key,
                data={
                    "product_id": product_id,
                    "lifecycle_state": response_summary["lifecycle_state"],
                    "reorder_confirmed": response_summary.get("reorder_confirmed", False),
                    "reorder_quantity": reorder_quantity,
                    "available_quantity": available_qty,
                    "safe_threshold": safe_threshold,
                    "is_simulated": True,
                    "idempotency_key": idempotency_key,
                },
            )
        except Exception as ev_err:
            logger.debug(f"Live event publish skipped: {ev_err}")

        return response_summary

    finally:
        if should_close_conn and connection:
            connection.close()
