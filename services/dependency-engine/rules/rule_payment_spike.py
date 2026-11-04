"""
SupplySense AI - Rule C: Payment Failure Spike Detection (Module 1)

Condition:
  - Evaluates the most recent 50 eligible, deduplicated payment transactions.
  - Triggers an alert when the rolling success rate is strictly below 92% (< 0.92).

Requirements:
  - Terminal eligible outcomes: 'SUCCESS' / 'APPROVED' vs 'FAILED' / 'DECLINED' / 'ERROR'.
  - Pending transactions do NOT count as successes.
  - Duplicate IDs deduplicated; out-of-order arrivals sorted by timestamp.
  - Insufficient history (< 50 transactions) safely evaluates to BLOCK.
  - Deduplicate active alerts: does not spawn duplicate alerts while an alert is active.
  - Automatic recovery: when success rate recovers to >= 92%, resolves the active alert.
  - Does NOT trigger downstream pipeline executions.
"""

import os
import sys
import logging
import datetime
from typing import Dict, Any, List, Optional, Tuple

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from ..rule_evaluator import evaluate_rule, EvaluationResult
from ..repository import (
    record_action_attempt,
    update_action_status,
    get_active_alert,
    resolve_active_alert
)

logger = logging.getLogger(__name__)

RULE_ID = "RULE_C_PAYMENT_FAILURE_SPIKE"
WINDOW_SIZE = 50
SUCCESS_THRESHOLD = 0.92

SUCCESS_OUTCOMES = {"SUCCESS", "APPROVED", "COMPLETED", "PAID"}
FAILURE_OUTCOMES = {"FAILED", "DECLINED", "ERROR", "REJECTED", "CANCELLED"}
PENDING_OUTCOMES = {"PENDING", "PROCESSING", "INITIATED", "AUTHORIZED"}


def process_transactions_window(
    transactions: List[Dict[str, Any]],
    window_size: int = WINDOW_SIZE
) -> Tuple[List[Dict[str, Any]], int, int, float]:
    """
    Sorts, deduplicates, and filters for the most recent eligible transactions.
    Pending transactions do NOT count as successes.

    Returns:
      (window_transactions, success_count, total_count, success_rate)
    """
    if not transactions:
        return [], 0, 0, 1.0

    # 1. Deduplicate by transaction_id (latest timestamp or latest entry wins)
    dedup_map: Dict[str, Dict[str, Any]] = {}
    for tx in transactions:
        tx_id = tx.get("transaction_id")
        if not tx_id:
            continue
        dedup_map[tx_id] = tx

    # 2. Sort chronologically by timestamp
    sorted_txs = list(dedup_map.values())

    def _get_ts(item):
        ts = item.get("timestamp") or item.get("created_at") or datetime.datetime.min
        if isinstance(ts, str):
            try:
                return datetime.datetime.fromisoformat(ts)
            except Exception:
                return datetime.datetime.min
        return ts

    sorted_txs.sort(key=_get_ts)

    # 3. Filter for terminal eligible outcomes (pending transactions do not count as successes)
    # We take the most recent eligible transactions up to window_size
    eligible_txs = []
    for tx in reversed(sorted_txs):
        status = str(tx.get("status", "")).upper()
        if status in SUCCESS_OUTCOMES or status in FAILURE_OUTCOMES or status in PENDING_OUTCOMES:
            eligible_txs.append(tx)
            if len(eligible_txs) == window_size:
                break

    eligible_txs.reverse()  # Restore chronological order

    total_count = len(eligible_txs)
    if total_count == 0:
        return [], 0, 0, 1.0

    success_count = sum(
        1 for tx in eligible_txs
        if str(tx.get("status", "")).upper() in SUCCESS_OUTCOMES
    )

    success_rate = round(success_count / total_count, 4)
    return eligible_txs, success_count, total_count, success_rate


def evaluate_and_manage_payment_alerts(
    transactions: List[Dict[str, Any]],
    current_time: Optional[datetime.datetime] = None,
    threshold: float = SUCCESS_THRESHOLD,
    window_size: int = WINDOW_SIZE,
    connection=None
) -> Dict[str, Any]:
    """
    Evaluates Rule C over the most recent 50 eligible transactions.
    Manages active alert lifecycle and recovery detection.
    """
    if current_time is None:
        current_time = datetime.datetime.now(datetime.timezone.utc)

    window_txs, success_count, total_count, success_rate = process_transactions_window(
        transactions=transactions,
        window_size=window_size
    )

    # Context dictionary
    eval_context = {
        "success_rate": success_rate,
        "total_count": total_count,
        "window_size": window_size,
        "has_sufficient_history": total_count >= window_size
    }

    # Nested rule expression:
    # AND(
    #     has_sufficient_history == True,
    #     success_rate < 0.92
    # )
    expression = {
        "operator": "AND",
        "conditions": [
            {"field": "has_sufficient_history", "op": "==", "value": True},
            {"field": "success_rate", "op": "<", "value": threshold}
        ]
    }

    eval_result: EvaluationResult = evaluate_rule(
        rule_id=RULE_ID,
        expression=expression,
        context=eval_context,
        rule_name="Payment Gateway Failure Spike"
    )

    # Check database for currently active alert
    active_alert = get_active_alert(rule_id=RULE_ID, connection=connection)

    response_summary: Dict[str, Any] = {
        "rule_id": RULE_ID,
        "matched": eval_result.matched,
        "decision": eval_result.decision,
        "metrics": {
            "window_size": window_size,
            "total_transactions_in_window": total_count,
            "success_count": success_count,
            "failed_count": total_count - success_count,
            "success_rate": success_rate,
            "threshold": threshold,
            "has_sufficient_history": total_count >= window_size
        },
        "reasons": eval_result.reasons,
        "alert_status": "NONE",
        "alert_event": None
    }

    # Case 1: Spike Detected (< 92% with 50 transactions)
    if eval_result.matched:
        if active_alert:
            # Active alert already exists: deduplicate!
            action_id = active_alert["action_id"] if isinstance(active_alert, dict) else active_alert[0]
            reason_msg = f"Payment failure spike persisting: active alert #{action_id} already open. Deduplicating alert."
            response_summary["alert_status"] = "ACTIVE_PERSISTING"
            response_summary["reasons"].append(reason_msg)
            response_summary["alert_event"] = {
                "event_type": "PAYMENT_GATEWAY_ALERT_PERSISTING",
                "action_id": action_id,
                "severity": "CRITICAL",
                "success_rate": success_rate,
                "message": f"Payment success rate is {success_rate*100:.1f}%, below {threshold*100}% threshold."
            }
            return response_summary

        # No active alert: publish new alert
        bucket_ts = int(current_time.timestamp())
        idempotency_key = f"payment_spike_alert_{bucket_ts}"
        alert_payload = {
            "rule_id": RULE_ID,
            "severity": "CRITICAL",
            "success_rate": success_rate,
            "success_count": success_count,
            "total_count": total_count,
            "threshold": threshold,
            "detected_at": current_time.isoformat(),
            "message": f"CRITICAL: Payment gateway success rate dropped to {success_rate*100:.1f}% across last {total_count} transactions."
        }

        action_id = record_action_attempt(
            rule_id=RULE_ID,
            idempotency_key=idempotency_key,
            status="ACTIVE",
            payload=alert_payload,
            connection=connection
        )

        response_summary["alert_status"] = "NEW_ALERT_PUBLISHED"
        response_summary["alert_event"] = {
            "event_type": "PAYMENT_GATEWAY_ALERT_TRIGGERED",
            "action_id": action_id,
            "severity": "CRITICAL",
            "success_rate": success_rate,
            "message": alert_payload["message"],
            "idempotency_key": idempotency_key
        }

        # Emit Live Dashboard Event
        try:
            from services.observability.publisher import publish_dashboard_event
            publish_dashboard_event(
                event_type="PAYMENT_GATEWAY_ALERT_TRIGGERED",
                entity="PAYMENT_GATEWAY",
                severity="CRITICAL",
                correlation_id=idempotency_key,
                data={
                    "success_rate": success_rate,
                    "threshold": threshold,
                    "total_count": total_count,
                    "success_count": success_count,
                    "alert_status": "NEW_ALERT_PUBLISHED",
                    "action_id": action_id,
                    "message": alert_payload["message"]
                }
            )
        except Exception as ev_err:
            logger.debug(f"Live event publish skipped: {ev_err}")

        return response_summary

    # Case 2: Healthy or Recovered (>= 92%)
    else:
        if active_alert:
            # System was degraded, now recovered!
            action_id = active_alert["action_id"] if isinstance(active_alert, dict) else active_alert[0]
            resolution_payload = {
                "resolved_at": current_time.isoformat(),
                "recovered_success_rate": success_rate,
                "recovery_transaction_count": total_count,
                "message": f"RECOVERED: Payment gateway success rate returned to healthy level ({success_rate*100:.1f}%)."
            }
            resolve_active_alert(
                action_id=action_id,
                resolution_payload=resolution_payload,
                connection=connection
            )

            response_summary["alert_status"] = "ALERT_RECOVERED"
            recovery_reason = f"Payment health restored: success rate returned to {success_rate*100:.1f}%. Alert #{action_id} resolved."
            response_summary["reasons"].append(recovery_reason)
            response_summary["alert_event"] = {
                "event_type": "PAYMENT_GATEWAY_ALERT_RECOVERED",
                "action_id": action_id,
                "severity": "INFO",
                "success_rate": success_rate,
                "message": recovery_reason
            }

            # Emit Live Recovery Event
            try:
                from services.observability.publisher import publish_dashboard_event
                publish_dashboard_event(
                    event_type="PAYMENT_GATEWAY_ALERT_RECOVERED",
                    entity="PAYMENT_GATEWAY",
                    severity="INFO",
                    data={
                        "success_rate": success_rate,
                        "action_id": action_id,
                        "recovery_transaction_count": total_count,
                        "message": recovery_reason
                    }
                )
            except Exception as ev_err:
                logger.debug(f"Live event publish skipped: {ev_err}")

            return response_summary

        # System healthy and no alert active
        response_summary["alert_status"] = "HEALTHY"
        return response_summary

