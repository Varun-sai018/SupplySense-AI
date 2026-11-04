"""
SupplySense AI - Dataset Dependency Evaluator

Evaluates dataset readiness conditions (ALL, ANY, QUORUM) and nested AST
expressions to determine if a pipeline should trigger or block.
"""

from typing import Tuple, Optional, Dict, Any, Union
import logging

logger = logging.getLogger(__name__)


def evaluate_condition(
    condition_type: str,
    ready_count: int,
    total_required: int,
    required_count: Optional[int] = None,
    expression: Union[Dict[str, Any], str, None] = None,
    context: Optional[Dict[str, Any]] = None
) -> Tuple[str, str]:
    """
    Evaluates dataset dependencies to determine if a pipeline should trigger.

    Args:
        condition_type: 'ALL', 'ANY', 'QUORUM', 'EXPRESSION', 'AST', or rule string.
        ready_count: Number of required datasets currently in 'READY' status.
        total_required: Total number of datasets required by the pipeline.
        required_count: Specific threshold for 'QUORUM' type.
        expression: Optional AST dictionary or rule string for expression evaluation.
        context: Optional dictionary mapping dataset names or fields to their values.

    Returns:
        tuple[str, str]: (decision, reason) where decision is 'TRIGGER' or 'BLOCK'.
    """
    decision = "BLOCK"
    reason = ""

    if not condition_type:
        return "BLOCK", "Condition type cannot be empty."

    cond_raw = condition_type.strip()
    cond_upper = cond_raw.upper()

    # Normalize counts
    ready_count = max(0, ready_count)
    total_required = max(0, total_required)

    if cond_upper == "ALL":
        if total_required <= 0:
            return "BLOCK", "No required datasets configured."
        if ready_count == total_required:
            decision = "TRIGGER"
            reason = "All required datasets are READY."
        else:
            decision = "BLOCK"
            reason = f"{ready_count} of {total_required} required datasets are READY."

    elif cond_upper == "ANY":
        if total_required <= 0:
            return "BLOCK", "No required datasets configured."
        if ready_count >= 1:
            decision = "TRIGGER"
            reason = "At least one required dataset is READY."
        else:
            decision = "BLOCK"
            reason = "No required dataset is READY."

    elif cond_upper == "QUORUM":
        if required_count is None:
            decision = "BLOCK"
            reason = "QUORUM condition requires a required_count threshold."
        elif required_count <= 0:
            decision = "BLOCK"
            reason = "QUORUM threshold must be greater than 0."
        elif ready_count >= required_count:
            decision = "TRIGGER"
            reason = f"Required quorum of {required_count} datasets satisfied."
        else:
            decision = "BLOCK"
            reason = f"Only {ready_count} of {required_count} required datasets are READY."

    elif cond_upper in ("EXPRESSION", "AST") or cond_upper.startswith(("AND(", "OR(", "NOT(")):
        # Nested AST expression evaluation
        try:
            from .rule_evaluator import evaluate_rule, parse_rule_string, RuleValidationError
        except ImportError:
            try:
                from services.dependency_engine.rule_evaluator import evaluate_rule, parse_rule_string, RuleValidationError
            except ImportError:
                return "BLOCK", "AST rule_evaluator module unavailable."

        eval_ctx = context or {}
        ast_expr = expression

        try:
            if cond_upper.startswith(("AND(", "OR(", "NOT(")):
                ast_expr = parse_rule_string(cond_raw)
            elif isinstance(expression, str):
                ast_expr = parse_rule_string(expression)
            elif isinstance(expression, dict):
                ast_expr = expression
            else:
                return "BLOCK", "AST condition requires a valid expression string or AST dictionary."

            result = evaluate_rule(
                rule_id="pipeline_ast_dependency",
                expression=ast_expr,
                context=eval_ctx,
                rule_name="Pipeline AST Dependency"
            )
            decision = result.decision
            reason = "; ".join(result.reasons) if result.reasons else f"AST evaluation returned {decision}."
        except RuleValidationError as rve:
            decision = "BLOCK"
            reason = f"AST validation error: {rve}"
        except Exception as exc:
            decision = "BLOCK"
            reason = f"AST evaluation failed: {exc}"

    else:
        decision = "BLOCK"
        reason = f"Unsupported condition type: {condition_type}"

    return decision, reason
