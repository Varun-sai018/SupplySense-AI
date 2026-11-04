"""
SupplySense AI - Rule A: High-Velocity Cart Abandonment (Module 1)

Condition:
  - Live session drop-off rate strictly exceeds 25% (> 0.25) in a rolling 5-minute window.
  - Current time falls within configured peak hours (timezone-aware).

Action:
  - Dispatch marketing funnel webhook with idempotency, timeout protection, bounded retries,
    and a configurable cooldown policy to prevent spamming notifications for an active incident.
"""

import os
import sys
import logging
import datetime
from zoneinfo import ZoneInfo
from typing import Dict, Any, List, Optional, Tuple

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from ..rule_evaluator import evaluate_rule, EvaluationResult
from ..repository import record_action_attempt, update_action_status, check_cooldown_active
from ..adapters.marketing_adapter import MarketingWebhookAdapter

logger = logging.getLogger(__name__)

RULE_ID = "RULE_A_CART_ABANDONMENT"

# Configuration defaults
DEFAULT_PEAK_START_HOUR = int(os.getenv("CART_PEAK_START_HOUR", "18"))  # 18:00 (6 PM)
DEFAULT_PEAK_END_HOUR = int(os.getenv("CART_PEAK_END_HOUR", "22"))      # 22:00 (10 PM)
DEFAULT_TIMEZONE = os.getenv("CART_TIMEZONE", "UTC")
DEFAULT_COOLDOWN_SECONDS = int(os.getenv("CART_COOLDOWN_SECONDS", "900"))  # 15 minutes


def is_peak_hour(
    current_time: Optional[datetime.datetime] = None,
    tz_name: str = DEFAULT_TIMEZONE,
    start_hour: int = DEFAULT_PEAK_START_HOUR,
    end_hour: int = DEFAULT_PEAK_END_HOUR
) -> bool:
    """
    Checks if current local time in tz_name falls within [start_hour, end_hour).
    """
    if current_time is None:
        current_time = datetime.datetime.now(datetime.timezone.utc)

    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = datetime.timezone.utc

    if current_time.tzinfo is None:
        local_time = current_time.replace(tzinfo=datetime.timezone.utc).astimezone(tz)
    else:
        local_time = current_time.astimezone(tz)

    hour = local_time.hour
    return start_hour <= hour < end_hour


def calculate_rolling_dropoff(
    sessions: List[Dict[str, Any]],
    current_time: datetime.datetime,
    window_seconds: int = 300
) -> Tuple[float, int, int]:
    """
    Computes unique session drop-off rate in rolling window:
      window = [current_time - window_seconds, current_time]

    Denominator: count of unique eligible sessions started in window.
    Numerator: count of unique sessions that ended in drop-off/abandonment.

    Returns:
      (dropoff_rate, abandoned_count, total_sessions)
    """
    cutoff = current_time - datetime.timedelta(seconds=window_seconds)

    # Deduplicate sessions by session_id, keeping the latest status
    unique_sessions: Dict[str, Dict[str, Any]] = {}
    for s in sessions:
        sid = s.get("session_id")
        if not sid:
            continue

        st_time = s.get("started_at")
        if isinstance(st_time, str):
            try:
                st_time = datetime.datetime.fromisoformat(st_time)
            except Exception:
                continue

        # Handle timezone
        if st_time and st_time.tzinfo is None and current_time.tzinfo is not None:
            st_time = st_time.replace(tzinfo=current_time.tzinfo)

        # Skip stale events outside window
        if st_time and (st_time < cutoff or st_time > current_time):
            continue

        unique_sessions[sid] = s

    total_sessions = len(unique_sessions)
    if total_sessions == 0:
        return 0.0, 0, 0

    abandoned_count = sum(
        1 for s in unique_sessions.values()
        if str(s.get("status", "")).lower() in ("abandoned", "drop_off", "dropped_off")
    )

    dropoff_rate = round(abandoned_count / total_sessions, 4)
    return dropoff_rate, abandoned_count, total_sessions


def evaluate_and_dispatch_cart_abandonment(
    sessions: List[Dict[str, Any]],
    current_time: Optional[datetime.datetime] = None,
    tz_name: str = DEFAULT_TIMEZONE,
    peak_start: int = DEFAULT_PEAK_START_HOUR,
    peak_end: int = DEFAULT_PEAK_END_HOUR,
    cooldown_seconds: int = DEFAULT_COOLDOWN_SECONDS,
    webhook_adapter: Optional[MarketingWebhookAdapter] = None,
    connection=None
) -> Dict[str, Any]:
    """
    Full lifecycle for Rule A:
      1. Calculate metrics over rolling 5-minute window.
      2. Check peak hour in configured timezone.
      3. Evaluate nested rule AST.
      4. If triggered, enforce cooldown and cross-process idempotency.
      5. Dispatch marketing webhook and record status.
    """
    if current_time is None:
        current_time = datetime.datetime.now(datetime.timezone.utc)

    dropoff_rate, abandoned_count, total_sessions = calculate_rolling_dropoff(
        sessions=sessions,
        current_time=current_time,
        window_seconds=300
    )
    peak = is_peak_hour(current_time=current_time, tz_name=tz_name, start_hour=peak_start, end_hour=peak_end)

    # Context dictionary for evaluation
    eval_context = {
        "dropoff_rate": dropoff_rate,
        "peak_hour": peak,
        "total_sessions": total_sessions,
        "abandoned_count": abandoned_count
    }

    # Nested AST rule expression: AND(dropoff_rate > 0.25, peak_hour == True)
    expression = {
        "operator": "AND",
        "conditions": [
            {"field": "dropoff_rate", "op": ">", "value": 0.25},
            {"field": "peak_hour", "op": "==", "value": True}
        ]
    }

    eval_result: EvaluationResult = evaluate_rule(
        rule_id=RULE_ID,
        expression=expression,
        context=eval_context,
        rule_name="High-Velocity Cart Abandonment"
    )

    response_summary: Dict[str, Any] = {
        "rule_id": RULE_ID,
        "matched": eval_result.matched,
        "decision": eval_result.decision,
        "metrics": {
            "dropoff_rate": dropoff_rate,
            "abandoned_sessions": abandoned_count,
            "total_sessions": total_sessions,
            "peak_hour": peak,
            "timezone": tz_name
        },
        "reasons": eval_result.reasons,
        "action": None
    }

    # If blocked, return cleanly without side-effects
    if not eval_result.matched:
        return response_summary

    # Cooldown Check
    is_cooldown, last_action_time = check_cooldown_active(
        rule_id=RULE_ID,
        cooldown_seconds=cooldown_seconds,
        connection=connection
    )
    if is_cooldown:
        reason_msg = f"Cooldown active: previous alert dispatched at {last_action_time} (within {cooldown_seconds}s window)."
        response_summary["decision"] = "BLOCK"
        response_summary["action"] = {
            "status": "COOLDOWN",
            "reason": reason_msg
        }
        response_summary["reasons"].append(reason_msg)
        return response_summary

    # Stable 5-minute bucket idempotency key
    bucket_ts = int(current_time.timestamp() // 300) * 300
    idempotency_key = f"cart_abandonment_{bucket_ts}"

    # Atomic insert into database
    payload = {
        "incident": "High-Velocity Cart Abandonment",
        "dropoff_rate": dropoff_rate,
        "abandoned_sessions": abandoned_count,
        "total_sessions": total_sessions,
        "evaluated_at": current_time.isoformat(),
        "idempotency_key": idempotency_key
    }

    action_id = record_action_attempt(
        rule_id=RULE_ID,
        idempotency_key=idempotency_key,
        status="PENDING",
        payload=payload,
        connection=connection
    )

    if action_id is None:
        reason_msg = f"Idempotency collision: incident for bucket {bucket_ts} already recorded."
        response_summary["decision"] = "SKIPPED"
        response_summary["action"] = {"status": "SKIPPED", "reason": reason_msg}
        response_summary["reasons"].append(reason_msg)
        return response_summary

    # Dispatch Webhook
    adapter = webhook_adapter or MarketingWebhookAdapter()
    dispatch_res = adapter.dispatch(payload=payload, idempotency_key=idempotency_key)

    action_status = dispatch_res.get("status", "FAILED")
    update_action_status(
        action_id=action_id,
        status=action_status,
        response_payload=dispatch_res.get("response"),
        error_message=dispatch_res.get("error"),
        connection=connection
    )

    response_summary["action"] = {
        "action_id": action_id,
        "status": action_status,
        "idempotency_key": idempotency_key,
        "dispatch_details": dispatch_res
    }

    # Emit Live Dashboard Event
    try:
        from services.observability.publisher import publish_dashboard_event
        publish_dashboard_event(
            event_type="CART_ABANDONMENT_TRIGGERED",
            entity="MARKETING_FUNNEL",
            severity="WARNING" if action_status == "CONFIRMED" else "CRITICAL",
            data={
                "dropoff_rate": dropoff_rate,
                "abandoned_sessions": abandoned_count,
                "total_sessions": total_sessions,
                "action_status": action_status,
                "is_simulated": dispatch_res.get("mode") == "SIMULATION",
                "idempotency_key": idempotency_key
            },
            correlation_id=idempotency_key
        )
    except Exception as ev_err:
        logger.debug(f"Live event publish skipped: {ev_err}")

    return response_summary

