"""
SupplySense AI - Business Rule Action Persistence Repository (Module 1)

Provides database-backed idempotency, cooldown verification, atomic action logging,
and alert lifecycle state management using the business_rule_actions table.
"""

import os
import sys
import json
import logging
import datetime
from typing import Dict, Any, Optional, Tuple, List
import pymysql

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection

logger = logging.getLogger(__name__)


def record_action_attempt(
    rule_id: str,
    idempotency_key: str,
    status: str,
    payload: Optional[Dict[str, Any]] = None,
    decision_id: Optional[int] = None,
    execution_id: Optional[int] = None,
    connection=None
) -> Optional[int]:
    """
    Atomically attempts to insert a new action record with unique (rule_id, idempotency_key).
    Returns the action_id if inserted, or None if already recorded (idempotency clash).
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    cursor = connection.cursor()
    payload_json = json.dumps(payload) if payload is not None else None

    try:
        cursor.execute("""
            INSERT INTO business_rule_actions (
                rule_id, idempotency_key, status, decision_id, execution_id, payload
            )
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (rule_id, idempotency_key, status, decision_id, execution_id, payload_json))
        action_id = cursor.lastrowid
        connection.commit()
        return action_id
    except pymysql.err.IntegrityError:
        # Idempotency collision: record already exists
        logger.info(f"IDEMPOTENCY: Action for rule '{rule_id}' with key '{idempotency_key}' already exists.")
        connection.rollback()
        return None
    finally:
        if should_close:
            connection.close()


def get_action_by_key(
    rule_id: str,
    idempotency_key: str,
    connection=None
) -> Optional[Dict[str, Any]]:
    """Retrieves an action record by rule_id and idempotency_key."""
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT
                action_id, rule_id, idempotency_key, status, decision_id,
                execution_id, payload, response_payload, error_message,
                retry_count, created_at, updated_at, resolved_at
            FROM business_rule_actions
            WHERE rule_id = %s AND idempotency_key = %s
            LIMIT 1
        """, (rule_id, idempotency_key))
        row = cursor.fetchone()
        return row
    finally:
        if should_close:
            connection.close()


def update_action_status(
    action_id: int,
    status: str,
    response_payload: Optional[Dict[str, Any]] = None,
    error_message: Optional[str] = None,
    execution_id: Optional[int] = None,
    resolved: bool = False,
    connection=None
) -> bool:
    """Updates the status and response/error of an existing action."""
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    cursor = connection.cursor()
    resp_json = json.dumps(response_payload) if response_payload is not None else None
    resolved_at = datetime.datetime.now() if resolved else None

    try:
        cursor.execute("""
            UPDATE business_rule_actions
            SET status = %s,
                response_payload = COALESCE(%s, response_payload),
                error_message = COALESCE(%s, error_message),
                execution_id = COALESCE(%s, execution_id),
                resolved_at = COALESCE(%s, resolved_at)
            WHERE action_id = %s
        """, (status, resp_json, error_message, execution_id, resolved_at, action_id))
        connection.commit()
        return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"Failed to update action #{action_id}: {e}")
        connection.rollback()
        return False
    finally:
        if should_close:
            connection.close()


def check_cooldown_active(
    rule_id: str,
    cooldown_seconds: int,
    connection=None
) -> Tuple[bool, Optional[datetime.datetime]]:
    """
    Checks if a successful or pending action for rule_id was created within cooldown_seconds.
    Returns (is_active, last_created_at).
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT created_at
            FROM business_rule_actions
            WHERE rule_id = %s
              AND status IN ('DISPATCHED', 'CONFIRMED', 'PENDING')
            ORDER BY created_at DESC
            LIMIT 1
        """, (rule_id,))
        row = cursor.fetchone()
        if not row:
            return False, None

        last_time = row["created_at"] if isinstance(row, dict) else row[0]
        if not last_time:
            return False, None

        now = datetime.datetime.now()
        elapsed = (now - last_time).total_seconds()
        is_active = elapsed < cooldown_seconds
        return is_active, last_time
    finally:
        if should_close:
            connection.close()


def get_active_alert(
    rule_id: str,
    connection=None
) -> Optional[Dict[str, Any]]:
    """Returns the latest unresolved active alert record for the rule, if any."""
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT
                action_id, rule_id, idempotency_key, status, payload,
                response_payload, created_at, updated_at
            FROM business_rule_actions
            WHERE rule_id = %s AND status = 'ACTIVE' AND resolved_at IS NULL
            ORDER BY created_at DESC
            LIMIT 1
        """, (rule_id,))
        row = cursor.fetchone()
        return row
    finally:
        if should_close:
            connection.close()


def resolve_active_alert(
    action_id: int,
    resolution_payload: Optional[Dict[str, Any]] = None,
    connection=None
) -> bool:
    """Marks an active alert as RESOLVED with timestamp and resolution metadata."""
    return update_action_status(
        action_id=action_id,
        status="RESOLVED",
        response_payload=resolution_payload,
        resolved=True,
        connection=connection
    )


# =============================================================================
# Operational Inventory Data-Access Layer (Phase 2A)
# =============================================================================

class InventoryNotFoundError(Exception):
    """Raised when an inventory record for a given (sku, warehouse_id) does not exist."""
    pass


class InventoryInvariantError(ValueError):
    """Raised when an operation would violate stock nonnegativity or reservation constraints."""
    pass


def get_inventory(
    sku: str,
    warehouse_id: str = "WH-SP-01",
    connection=None
) -> Optional[Dict[str, Any]]:
    """Retrieves operational inventory balance for a specific (sku, warehouse_id)."""
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT 
                sku, warehouse_id, on_hand_quantity, reserved_quantity,
                available_quantity, reorder_threshold, reorder_target_quantity,
                safety_stock_buffer, data_origin, version, last_updated_at
            FROM operational_inventory
            WHERE sku = %s AND warehouse_id = %s
            LIMIT 1
        """, (sku, warehouse_id))
        return cursor.fetchone()
    finally:
        if should_close:
            connection.close()


def list_inventory(
    limit: int = 50,
    offset: int = 0,
    warehouse_id: Optional[str] = None,
    connection=None
) -> List[Dict[str, Any]]:
    """Returns a bounded list of operational inventory records with pagination."""
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    safe_limit = max(1, min(limit, 100))
    safe_offset = max(0, offset)
    cursor = connection.cursor()
    try:
        if warehouse_id:
            cursor.execute("""
                SELECT 
                    sku, warehouse_id, on_hand_quantity, reserved_quantity,
                    available_quantity, reorder_threshold, reorder_target_quantity,
                    safety_stock_buffer, data_origin, version, last_updated_at
                FROM operational_inventory
                WHERE warehouse_id = %s
                ORDER BY sku ASC
                LIMIT %s OFFSET %s
            """, (warehouse_id, safe_limit, safe_offset))
        else:
            cursor.execute("""
                SELECT 
                    sku, warehouse_id, on_hand_quantity, reserved_quantity,
                    available_quantity, reorder_threshold, reorder_target_quantity,
                    safety_stock_buffer, data_origin, version, last_updated_at
                FROM operational_inventory
                ORDER BY warehouse_id ASC, sku ASC
                LIMIT %s OFFSET %s
            """, (safe_limit, safe_offset))
        rows = cursor.fetchall()
        return rows if rows else []
    finally:
        if should_close:
            connection.close()


def get_supplier_availability(
    supplier_id: str,
    sku: Optional[str] = None,
    connection=None
) -> Optional[Dict[str, Any]]:
    """
    Returns supplier status, reliability score, and optionally SKU-level catalog availability.
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    cursor = connection.cursor()
    try:
        if sku:
            cursor.execute("""
                SELECT 
                    s.supplier_id, s.supplier_name, s.status, s.reliability_score,
                    s.average_lead_time_days, s.last_health_check_at,
                    c.sku, c.supply_availability, c.unit_reorder_cost,
                    c.min_reorder_quantity, c.lead_time_hours
                FROM operational_suppliers s
                LEFT JOIN supplier_catalog_availability c 
                    ON s.supplier_id = c.supplier_id AND c.sku = %s
                WHERE s.supplier_id = %s
                LIMIT 1
            """, (sku, supplier_id))
        else:
            cursor.execute("""
                SELECT 
                    supplier_id, supplier_name, status, reliability_score,
                    average_lead_time_days, last_health_check_at
                FROM operational_suppliers
                WHERE supplier_id = %s
                LIMIT 1
            """, (supplier_id,))
        return cursor.fetchone()
    finally:
        if should_close:
            connection.close()


def apply_inventory_delta(
    sku: str,
    warehouse_id: str = "WH-SP-01",
    delta_on_hand: int = 0,
    delta_reserved: int = 0,
    connection=None
) -> Dict[str, Any]:
    """
    Atomically applies validated delta adjustments to an inventory record under a transaction.
    Locks row using FOR UPDATE, validates invariants, increments version by 1, and commits.
    
    Invariants enforced:
      - on_hand_quantity >= 0
      - reserved_quantity >= 0
      - reserved_quantity <= on_hand_quantity (which guarantees available_quantity >= 0)
    
    Returns the updated inventory dictionary.
    Raises InventoryNotFoundError if record does not exist.
    Raises InventoryInvariantError if proposed change violates constraints.
    Rolls back on error.
    """
    should_close = False
    if connection is None:
        connection = get_connection()
        should_close = True

    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT 
                sku, warehouse_id, on_hand_quantity, reserved_quantity,
                available_quantity, reorder_threshold, version
            FROM operational_inventory
            WHERE sku = %s AND warehouse_id = %s
            FOR UPDATE
        """, (sku, warehouse_id))
        row = cursor.fetchone()

        if not row:
            raise InventoryNotFoundError(
                f"Inventory record not found for SKU '{sku}' in warehouse '{warehouse_id}'"
            )

        curr_on_hand = row["on_hand_quantity"]
        curr_reserved = row["reserved_quantity"]
        new_on_hand = curr_on_hand + delta_on_hand
        new_reserved = curr_reserved + delta_reserved
        new_version = row["version"] + 1

        # Enforce inventory invariants strictly
        if new_on_hand < 0:
            raise InventoryInvariantError(
                f"Cannot reduce on-hand stock below 0: current={curr_on_hand}, delta={delta_on_hand}, proposed={new_on_hand}"
            )
        if new_reserved < 0:
            raise InventoryInvariantError(
                f"Cannot reduce reserved stock below 0: current={curr_reserved}, delta={delta_reserved}, proposed={new_reserved}"
            )
        if new_reserved > new_on_hand:
            raise InventoryInvariantError(
                f"Reserved stock ({new_reserved}) cannot exceed on-hand stock ({new_on_hand}); "
                f"would result in negative available quantity ({new_on_hand - new_reserved})"
            )

        cursor.execute("""
            UPDATE operational_inventory
            SET on_hand_quantity = %s,
                reserved_quantity = %s,
                version = %s
            WHERE sku = %s AND warehouse_id = %s
        """, (new_on_hand, new_reserved, new_version, sku, warehouse_id))

        connection.commit()

        cursor.execute("""
            SELECT 
                sku, warehouse_id, on_hand_quantity, reserved_quantity,
                available_quantity, reorder_threshold, version, last_updated_at
            FROM operational_inventory
            WHERE sku = %s AND warehouse_id = %s
        """, (sku, warehouse_id))
        updated_row = cursor.fetchone()
        return updated_row

    except Exception:
        connection.rollback()
        raise
    finally:
        if should_close:
            connection.close()
