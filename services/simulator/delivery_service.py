"""
SupplySense AI - Deterministic Simulated Delivery Service (Phase 2B)

Provides a single explicit operation to complete a specific in-flight
replenishment order. No background timers, loops, or external supplier
calls are used.

Transaction guarantee
---------------------
All mutations — replenishment status transition to DELIVERED, delivered_at
timestamp, inventory on_hand_quantity / version increment, AND the
DELIVERY_COMPLETED event in the MySQL `dashboard_events` persistent journal
— are executed within a SINGLE PyMySQL transaction using the same connection.
If any step fails or an invariant is violated, all changes (including the
event record) roll back atomically, preventing phantom events and partial state.
Post-commit, the event is broadcast to in-process EventHub SSE subscribers.

Idempotency & Retry Safety
--------------------------
If the replenishment is already DELIVERED, inventory is never incremented again.
Subsequent delivery calls or publisher retries use deterministic event IDs
(`DELIVERY_<replenishment_id>`) with INSERT IGNORE, ensuring zero duplicate
database rows and zero double-stocking.
"""

import os
import sys
import logging
import datetime
from typing import Dict, Any, Optional

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from services.dependency_engine.repository import apply_inventory_delta, InventoryNotFoundError
from services.observability.publisher import publish_dashboard_event, broadcast_dashboard_event

logger = logging.getLogger(__name__)

class OperationalDeliveryForbiddenError(PermissionError):
    """Raised when simulated delivery is attempted against operational inventory or replenishment orders."""
    pass


# Replenishment statuses that are eligible for delivery
DELIVERABLE_STATUSES = {"DISPATCHED", "CONFIRMED", "IN_TRANSIT"}


def deliver_replenishment(
    replenishment_id: str,
    connection=None,
) -> Dict[str, Any]:
    """Complete a specific in-flight replenishment deterministically.

    Steps (all in one transaction):
      1. SELECT … FOR UPDATE on the replenishment row.
      2. If status == DELIVERED → return idempotent no-op (no inventory mutation).
      3. If status not in DELIVERABLE_STATUSES → raise ValueError.
      4. UPDATE replenishment: status = DELIVERED, delivered_at = NOW().
      5. apply_inventory_delta: on_hand_quantity += quantity (also increments version).
      6. Insert DELIVERY_COMPLETED event into dashboard_events (same connection, uncommitted).
      7. COMMIT all three mutations atomically.
      8. Broadcast DELIVERY_COMPLETED event to in-process EventHub subscribers post-commit.

    Args:
        replenishment_id: Primary key of the replenishment_orders row.
        connection:       Optional existing PyMySQL connection. When supplied
                          the caller owns the connection lifecycle; when None
                          this function opens and closes its own connection.

    Returns:
        Dict with keys: replenishment_id, sku, warehouse_id, quantity,
        status, delivered_at, idempotent (bool), event_id.

    Raises:
        ValueError:              Row not found, or invalid status transition.
        InventoryNotFoundError:  Inventory row missing for the SKU/warehouse.
        RuntimeError:            Any other database error (transaction rolled back).
    """
    should_close_conn = False
    if connection is None:
        connection = get_connection()
        should_close_conn = True

    cursor = connection.cursor()
    try:
        # ------------------------------------------------------------------
        # 1. Lock the row
        # ------------------------------------------------------------------
        cursor.execute(
            """
            SELECT replenishment_id, sku, warehouse_id, supplier_id,
                   quantity, status, action_id, decision_id, execution_id,
                   supplier_order_reference, dispatched_at, estimated_delivery_at,
                   delivered_at
            FROM replenishment_orders
            WHERE replenishment_id = %s
            FOR UPDATE
            """,
            (replenishment_id,),
        )
        row = cursor.fetchone()

        if not row:
            raise ValueError(f"Replenishment '{replenishment_id}' not found.")

        if isinstance(row, dict):
            current_status = row["status"]
            sku            = row["sku"]
            warehouse_id   = row["warehouse_id"]
            quantity       = row["quantity"]
            action_id      = row.get("action_id")
            decision_id    = row.get("decision_id")
            execution_id   = row.get("execution_id")
            delivered_at_existing = row.get("delivered_at")
        else:
            current_status = row[5]
            sku            = row[1]
            warehouse_id   = row[2]
            quantity       = row[4]
            action_id      = row[6]
            decision_id    = row[7]
            execution_id   = row[8]
            delivered_at_existing = row[12]

        # ------------------------------------------------------------------
        # 1b. Safety Guard: Validate order, warehouse, SKU, and data_origin
        # Strictly reject delivery requests targeting operational records (HTTP 403)
        # ------------------------------------------------------------------
        pipeline_name = None
        if decision_id:
            cursor.execute("SELECT pipeline_name FROM pipeline_decisions WHERE decision_id = %s", (decision_id,))
            dec_row = cursor.fetchone()
            if dec_row:
                pipeline_name = dec_row.get("pipeline_name") if isinstance(dec_row, dict) else (dec_row[0] if len(dec_row) > 0 else None)
        elif execution_id:
            cursor.execute("SELECT pipeline_name FROM pipeline_executions WHERE execution_id = %s", (execution_id,))
            exec_row = cursor.fetchone()
            if exec_row:
                pipeline_name = exec_row.get("pipeline_name") if isinstance(exec_row, dict) else (exec_row[0] if len(exec_row) > 0 else None)

        cursor.execute(
            """
            SELECT sku, warehouse_id, data_origin, on_hand_quantity
            FROM operational_inventory
            WHERE sku = %s AND warehouse_id = %s
            FOR UPDATE
            """,
            (sku, warehouse_id),
        )
        inv_row = cursor.fetchone()
        if not inv_row:
            connection.rollback()
            raise InventoryNotFoundError(
                f"Inventory record not found for SKU '{sku}' in warehouse '{warehouse_id}'."
            )
        data_origin = inv_row.get("data_origin") if isinstance(inv_row, dict) else (inv_row[2] if len(inv_row) > 2 else None)

        # Verify simulated provenance: operational facilities, pipelines, and origins are strictly forbidden
        is_operational = (
            warehouse_id == "WH-SP-01" or
            pipeline_name in ("Inventory Reorder Pipeline", "Demand Forecast Pipeline") or
            data_origin != "SIMULATED" or
            (not warehouse_id.startswith(("WH-SIM-", "WH-TEST-")))
        )
        if is_operational:
            connection.rollback()
            raise OperationalDeliveryForbiddenError(
                f"Simulated delivery rejected: Order '{replenishment_id}' targets operational warehouse '{warehouse_id}' "
                f"(pipeline '{pipeline_name or 'NONE'}', data_origin '{data_origin}'). Simulated delivery is strictly "
                f"prohibited for operational inventory and replenishment orders."
            )

        # ------------------------------------------------------------------
        # 2. Idempotent guard
        # ------------------------------------------------------------------
        if current_status == "DELIVERED":
            logger.info(
                f"Replenishment '{replenishment_id}' already DELIVERED — idempotent no-op."
            )
            # Safe retry check: ensure event is present in journal (INSERT IGNORE) & broadcast
            deterministic_event_id = f"DELIVERY_{replenishment_id}"
            try:
                del_at_str = (
                    delivered_at_existing.isoformat()
                    if hasattr(delivered_at_existing, "isoformat")
                    else (str(delivered_at_existing) if delivered_at_existing else datetime.datetime.now().isoformat())
                )
                publish_dashboard_event(
                    event_type="DELIVERY_COMPLETED",
                    entity="INVENTORY",
                    severity="INFO",
                    event_id=deterministic_event_id,
                    correlation_id=replenishment_id,
                    decision_id=decision_id,
                    execution_id=execution_id,
                    data={
                        "replenishment_id": replenishment_id,
                        "sku": sku,
                        "warehouse_id": warehouse_id,
                        "quantity": quantity,
                        "delivered_at": del_at_str,
                        "is_simulated": True,
                    },
                    connection=connection,
                    commit=True,
                    broadcast=True,
                )
            except Exception as ev_err:
                logger.debug(f"Event republish on idempotent retry skipped: {ev_err}")

            connection.rollback()
            return {
                "replenishment_id": replenishment_id,
                "sku": sku,
                "warehouse_id": warehouse_id,
                "quantity": quantity,
                "status": "DELIVERED",
                "idempotent": True,
                "event_id": deterministic_event_id,
            }

        # ------------------------------------------------------------------
        # 3. Validate state transition
        # ------------------------------------------------------------------
        if current_status not in DELIVERABLE_STATUSES:
            connection.rollback()
            raise ValueError(
                f"Cannot deliver replenishment '{replenishment_id}' in status '{current_status}'. "
                f"Allowed: {sorted(DELIVERABLE_STATUSES)}."
            )

        # ------------------------------------------------------------------
        # 4. Update replenishment status
        # ------------------------------------------------------------------
        delivered_at = datetime.datetime.now()
        cursor.execute(
            """
            UPDATE replenishment_orders
            SET status = 'DELIVERED', delivered_at = %s
            WHERE replenishment_id = %s
            """,
            (delivered_at, replenishment_id),
        )

        # ------------------------------------------------------------------
        # 5. Increment inventory — uses the same connection / transaction
        # ------------------------------------------------------------------
        updated_inventory = apply_inventory_delta(
            sku=sku,
            warehouse_id=warehouse_id,
            delta_on_hand=quantity,
            delta_reserved=0,
            connection=connection,
        )

        # ------------------------------------------------------------------
        # 6. Persist DELIVERY_COMPLETED event into dashboard_events in SAME TX
        # ------------------------------------------------------------------
        deterministic_event_id = f"DELIVERY_{replenishment_id}"
        delivery_event = publish_dashboard_event(
            event_type="DELIVERY_COMPLETED",
            entity="INVENTORY",
            severity="INFO",
            event_id=deterministic_event_id,
            correlation_id=replenishment_id,
            decision_id=decision_id,
            execution_id=execution_id,
            data={
                "replenishment_id": replenishment_id,
                "sku": sku,
                "warehouse_id": warehouse_id,
                "quantity": quantity,
                "delivered_at": delivered_at.isoformat(),
                "is_simulated": True,
            },
            connection=connection,
            commit=False,
            broadcast=False,
        )

        # ------------------------------------------------------------------
        # 7. Commit all mutations atomically (status, inventory, event)
        # ------------------------------------------------------------------
        connection.commit()
        logger.info(
            f"Replenishment '{replenishment_id}' DELIVERED: "
            f"+{quantity} units for SKU {sku}/{warehouse_id}. "
            f"New on_hand={updated_inventory.get('on_hand_quantity') if isinstance(updated_inventory, dict) else '?'}, "
            f"version={updated_inventory.get('version') if isinstance(updated_inventory, dict) else '?'}. "
            f"Event '{deterministic_event_id}' persisted atomically."
        )

        result = {
            "replenishment_id": replenishment_id,
            "sku": sku,
            "warehouse_id": warehouse_id,
            "quantity": quantity,
            "status": "DELIVERED",
            "delivered_at": delivered_at.isoformat(),
            "idempotent": False,
            "inventory": updated_inventory,
            "event_id": deterministic_event_id,
        }

        # ------------------------------------------------------------------
        # 8. Post-commit broadcast to SSE subscribers (best-effort)
        # ------------------------------------------------------------------
        if delivery_event:
            try:
                broadcast_dashboard_event(delivery_event)
            except Exception as ev_err:
                logger.debug(f"DELIVERY_COMPLETED post-commit broadcast skipped: {ev_err}")

        return result

    except (ValueError, InventoryNotFoundError, OperationalDeliveryForbiddenError):
        connection.rollback()
        raise
    except Exception as exc:
        connection.rollback()
        raise RuntimeError(
            f"Delivery transaction failed for '{replenishment_id}': {exc}"
        ) from exc
    finally:
        if should_close_conn:
            connection.close()


def retry_publish_delivery_event(
    replenishment_id: str,
    connection=None,
) -> Optional[Any]:
    """Retries event publishing for an already-delivered replenishment.

    Guarantees:
      - Does NOT modify operational inventory or increment stock.
      - Idempotent: uses deterministic event_id, preventing duplicate database records.
      - Re-broadcasts to EventHub subscribers if previous broadcast failed.
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    try:
        cur = connection.cursor()
        cur.execute(
            """
            SELECT replenishment_id, sku, warehouse_id, quantity, status,
                   decision_id, execution_id, delivered_at
            FROM replenishment_orders
            WHERE replenishment_id = %s
            """,
            (replenishment_id,),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Replenishment '{replenishment_id}' not found.")

        current_status = row["status"] if isinstance(row, dict) else row[4]
        if current_status != "DELIVERED":
            raise ValueError(
                f"Cannot retry event publishing for replenishment '{replenishment_id}' "
                f"in status '{current_status}'. Status must be 'DELIVERED'."
            )

        sku          = row["sku"]          if isinstance(row, dict) else row[1]
        warehouse_id = row["warehouse_id"] if isinstance(row, dict) else row[2]
        quantity     = row["quantity"]     if isinstance(row, dict) else row[3]
        decision_id  = row.get("decision_id")  if isinstance(row, dict) else row[5]
        execution_id = row.get("execution_id") if isinstance(row, dict) else row[6]
        del_at       = row.get("delivered_at") if isinstance(row, dict) else row[7]

        del_at_str = (
            del_at.isoformat()
            if hasattr(del_at, "isoformat")
            else (str(del_at) if del_at else datetime.datetime.now().isoformat())
        )

        event = publish_dashboard_event(
            event_type="DELIVERY_COMPLETED",
            entity="INVENTORY",
            severity="INFO",
            event_id=f"DELIVERY_{replenishment_id}",
            correlation_id=replenishment_id,
            decision_id=decision_id,
            execution_id=execution_id,
            data={
                "replenishment_id": replenishment_id,
                "sku": sku,
                "warehouse_id": warehouse_id,
                "quantity": quantity,
                "delivered_at": del_at_str,
                "is_simulated": True,
                "is_retry": True,
            },
            connection=connection,
            commit=True,
            broadcast=True,
        )
        return event
    finally:
        if should_close:
            connection.close()
