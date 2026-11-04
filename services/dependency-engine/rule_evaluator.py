"""
SupplySense AI - Validated Nested Rule Evaluator (Module 1)

Supports hierarchical logical expressions (AND, OR, NOT) with typed comparison
operators, short-circuit evaluation, strict validation, deterministic reason tracing,
and backwards compatibility with legacy ALL / ANY / QUORUM dependency decisions.
"""

import re
import datetime
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Union


class RuleValidationError(ValueError):
    """Raised when a rule expression schema, operator, or threshold is malformed."""
    pass


SUPPORTED_LOGICAL_OPS = {"AND", "OR", "NOT"}
SUPPORTED_COMPARISON_OPS = {">", ">=", "<", "<=", "==", "!=", "in", "not in"}
NUMERIC_COMPARISON_OPS = {">", ">=", "<", "<="}


@dataclass
class EvaluationResult:
    """Structured evaluation output containing deterministic decision and reasons."""
    matched: bool
    decision: str  # "TRIGGER" or "BLOCK"
    rule_id: str
    reasons: List[str] = field(default_factory=list)
    evaluated_at: str = field(default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).isoformat())
    context_snapshot: Dict[str, Any] = field(default_factory=dict)
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "matched": self.matched,
            "decision": self.decision,
            "rule_id": self.rule_id,
            "reasons": self.reasons,
            "evaluated_at": self.evaluated_at,
            "context_snapshot": self.context_snapshot,
            "details": self.details,
        }


def validate_expression(expr: Any) -> None:
    """
    Validates the structure and types of an expression AST.
    Raises RuleValidationError on invalid syntax, empty conditions, or unsupported ops.
    """
    if not isinstance(expr, dict):
        raise RuleValidationError(f"Expression must be a dictionary, got {type(expr).__name__}")

    if "operator" in expr:
        op = expr["operator"]
        if not isinstance(op, str) or op.upper() not in SUPPORTED_LOGICAL_OPS:
            raise RuleValidationError(f"Unsupported logical operator '{op}'. Must be one of {sorted(SUPPORTED_LOGICAL_OPS)}")
        op = op.upper()

        if "conditions" not in expr:
            raise RuleValidationError(f"Logical expression '{op}' must contain 'conditions' list")

        conditions = expr["conditions"]
        if not isinstance(conditions, list):
            raise RuleValidationError(f"'conditions' in '{op}' must be a list, got {type(conditions).__name__}")

        if op in ("AND", "OR") and len(conditions) == 0:
            raise RuleValidationError(f"Logical expression '{op}' cannot have empty conditions list")

        if op == "NOT":
            if len(conditions) != 1:
                raise RuleValidationError(f"Logical operator 'NOT' requires exactly 1 child condition, got {len(conditions)}")

        for child in conditions:
            validate_expression(child)

    elif "field" in expr:
        field_name = expr["field"]
        if not isinstance(field_name, str) or not field_name.strip():
            raise RuleValidationError("Comparison expression must define a non-empty string 'field'")

        op = expr.get("op")
        if not isinstance(op, str) or op not in SUPPORTED_COMPARISON_OPS:
            raise RuleValidationError(f"Unsupported comparison operator '{op}'. Must be one of {sorted(SUPPORTED_COMPARISON_OPS)}")

        if "value" not in expr:
            raise RuleValidationError("Comparison expression missing required 'value' key")

        expected_val = expr["value"]

        # Validate numeric operator thresholds
        if op in NUMERIC_COMPARISON_OPS:
            if not isinstance(expected_val, (int, float)) or isinstance(expected_val, bool):
                raise RuleValidationError(f"Comparison operator '{op}' requires numeric threshold, got {type(expected_val).__name__}: {expected_val}")

        # Validate collection operators
        if op in ("in", "not in"):
            if not isinstance(expected_val, (list, tuple, set)):
                raise RuleValidationError(f"Operator '{op}' requires list/set threshold, got {type(expected_val).__name__}")

    else:
        raise RuleValidationError(f"Invalid rule node: must contain either 'operator' or 'field'. Got: {expr}")


def _evaluate_leaf(expr: Dict[str, Any], context: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Evaluates a single leaf comparison node against the context dictionary.
    Returns (matched, reason).
    Unknown, missing, or null values safely evaluate to False.
    """
    field_name = expr["field"]
    op = expr["op"]
    target_value = expr["value"]

    if field_name not in context or context[field_name] is None:
        return False, f"Field '{field_name}' is missing or null in evaluation context (evaluated as False)"

    actual_value = context[field_name]

    # Type check for numeric operations
    if op in NUMERIC_COMPARISON_OPS:
        if not isinstance(actual_value, (int, float)) or isinstance(actual_value, bool):
            return False, f"Field '{field_name}' value {actual_value!r} is not numeric; cannot evaluate '{op}'"

    try:
        if op == ">":
            matched = actual_value > target_value
        elif op == ">=":
            matched = actual_value >= target_value
        elif op == "<":
            matched = actual_value < target_value
        elif op == "<=":
            matched = actual_value <= target_value
        elif op == "==":
            matched = actual_value == target_value
        elif op == "!=":
            matched = actual_value != target_value
        elif op == "in":
            matched = actual_value in target_value
        elif op == "not in":
            matched = actual_value not in target_value
        else:
            return False, f"Unknown operator '{op}'"
    except Exception as exc:
        return False, f"Comparison error on '{field_name}' ({actual_value!r} {op} {target_value!r}): {exc}"

    status_str = "satisfied" if matched else "violated"
    reason = f"Condition '{field_name} {op} {target_value}' {status_str} (actual: {actual_value!r})"
    return matched, reason


def _evaluate_node(expr: Dict[str, Any], context: Dict[str, Any], reasons: List[str]) -> bool:
    """Recursively evaluates the AST node with short-circuiting."""
    if "operator" in expr:
        op = expr["operator"].upper()
        children = expr["conditions"]

        if op == "AND":
            for idx, child in enumerate(children):
                child_matched = _evaluate_node(child, context, reasons)
                if not child_matched:
                    # Short circuit
                    reasons.append(f"AND condition branch {idx+1}/{len(children)} failed; short-circuiting to False.")
                    return False
            return True

        elif op == "OR":
            any_matched = False
            branch_reasons: List[str] = []
            for idx, child in enumerate(children):
                child_reasons: List[str] = []
                child_matched = _evaluate_node(child, context, child_reasons)
                if child_matched:
                    reasons.extend(child_reasons)
                    reasons.append(f"OR condition satisfied by branch {idx+1}/{len(children)}.")
                    return True
                branch_reasons.extend(child_reasons)
            reasons.extend(branch_reasons)
            reasons.append(f"None of {len(children)} OR branches satisfied.")
            return False

        elif op == "NOT":
            child = children[0]
            child_reasons: List[str] = []
            child_matched = _evaluate_node(child, context, child_reasons)
            reasons.extend(child_reasons)
            result = not child_matched
            reasons.append(f"NOT condition inverted child result ({child_matched} -> {result}).")
            return result

        else:
            reasons.append(f"Unsupported operator '{op}' encountered.")
            return False

    else:
        # Leaf comparison
        matched, reason = _evaluate_leaf(expr, context)
        reasons.append(reason)
        return matched


def evaluate_rule(
    rule_id: str,
    expression: Dict[str, Any],
    context: Dict[str, Any],
    rule_name: Optional[str] = None
) -> EvaluationResult:
    """
    Evaluates a validated rule expression against the input context.
    Returns a typed EvaluationResult.
    """
    validate_expression(expression)

    reasons: List[str] = []
    matched = _evaluate_node(expression, context, reasons)
    decision = "TRIGGER" if matched else "BLOCK"

    return EvaluationResult(
        matched=matched,
        decision=decision,
        rule_id=rule_id,
        reasons=reasons,
        evaluated_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        context_snapshot={k: v for k, v in context.items() if not k.startswith("_")},
        details={"rule_name": rule_name or rule_id}
    )


# ---------------------------------------------------------------------------
# Simple String Expression Parser for rules like:
# AND(dropoff_rate > 0.25, OR(peak_hour == true, emergency_override == true))
# ---------------------------------------------------------------------------

def parse_rule_string(expr_str: str) -> Dict[str, Any]:
    """
    Parses a human-readable rule expression string into a structured AST dictionary.
    Example:
        AND(
            dropoff_rate > 0.25,
            OR(
                peak_hour == true,
                emergency_override == true
            )
        )
    """
    s = expr_str.strip()
    if not s:
        raise RuleValidationError("Cannot parse empty rule string")

    # Match logical function at start: AND(...), OR(...), NOT(...)
    match_logical = re.match(r"^(AND|OR|NOT)\s*\((.*)\)$", s, re.IGNORECASE | re.DOTALL)
    if match_logical:
        op = match_logical.group(1).upper()
        inner_content = match_logical.group(2).strip()

        # Split inner items by top-level commas (respecting nested parentheses)
        children_strs = _split_top_level(inner_content, ",")
        if not children_strs:
            raise RuleValidationError(f"Empty condition list inside {op}()")

        conditions = [parse_rule_string(c) for c in children_strs]
        ast_node = {"operator": op, "conditions": conditions}
        validate_expression(ast_node)
        return ast_node

    # Otherwise, it must be a comparison: field op value
    # Operators: ==, !=, >=, <=, >, <, in, not in
    comp_regex = re.match(
        r"^\s*([A-Za-z0-9_]+)\s*(==|!=|>=|<=|>|<|\bnot in\b|\bin\b)\s*(.+?)\s*$",
        s,
        re.IGNORECASE
    )
    if not comp_regex:
        raise RuleValidationError(f"Malformed comparison expression: '{s}'")

    field_name = comp_regex.group(1).strip()
    op = comp_regex.group(2).strip()
    raw_val = comp_regex.group(3).strip()

    # Parse value
    parsed_val = _parse_literal_value(raw_val)

    ast_node = {
        "field": field_name,
        "op": op,
        "value": parsed_val
    }
    validate_expression(ast_node)
    return ast_node


def _split_top_level(s: str, delimiter: str = ",") -> List[str]:
    """Splits string by delimiter, only when outside parentheses and quotes."""
    results = []
    current: List[str] = []
    paren_depth = 0
    in_quote: Optional[str] = None

    for char in s:
        if in_quote:
            current.append(char)
            if char == in_quote:
                in_quote = None
        else:
            if char in ('"', "'"):
                in_quote = char
                current.append(char)
            elif char == '(':
                paren_depth += 1
                current.append(char)
            elif char == ')':
                paren_depth -= 1
                current.append(char)
            elif char == delimiter and paren_depth == 0:
                token = "".join(current).strip()
                if token:
                    results.append(token)
                current = []
            else:
                current.append(char)

    if paren_depth != 0:
        raise RuleValidationError("Unbalanced parentheses in rule expression")

    token = "".join(current).strip()
    if token:
        results.append(token)

    return results


def _parse_literal_value(val_str: str) -> Any:
    """Parses boolean, number, string, or list literals."""
    lowered = val_str.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered == "null" or lowered == "none":
        return None

    # Number
    try:
        if "." in val_str:
            return float(val_str)
        return int(val_str)
    except ValueError:
        pass

    # String with quotes
    if (val_str.startswith('"') and val_str.endswith('"')) or (val_str.startswith("'") and val_str.endswith("'")):
        return val_str[1:-1]

    # List literal: [a, b, c]
    if val_str.startswith("[") and val_str.endswith("]"):
        inner = val_str[1:-1].strip()
        if not inner:
            return []
        items = _split_top_level(inner, ",")
        return [_parse_literal_value(it) for it in items]

    # Raw unquoted string identifier
    return val_str


# ---------------------------------------------------------------------------
# Backwards Compatibility with Legacy Dataset Readiness Evaluator
# ---------------------------------------------------------------------------

def evaluate_legacy_condition(
    condition_type: str,
    ready_count: int,
    total_required: int,
    required_count: Optional[int] = None
) -> Tuple[str, str]:
    """
    Preserves exact backwards compatibility with services/dependency-engine/evaluator.py.
    """
    decision = "BLOCK"
    reason = ""

    if condition_type == "ALL":
        if ready_count == total_required:
            decision = "TRIGGER"
            reason = "All required datasets are READY."
        else:
            decision = "BLOCK"
            reason = f"{ready_count} of {total_required} required datasets are READY."

    elif condition_type == "ANY":
        if ready_count >= 1:
            decision = "TRIGGER"
            reason = "At least one required dataset is READY."
        else:
            decision = "BLOCK"
            reason = "No required dataset is READY."

    elif condition_type == "QUORUM":
        if required_count is None:
            decision = "BLOCK"
            reason = "QUORUM condition requires a required_count threshold."
        elif ready_count >= required_count:
            decision = "TRIGGER"
            reason = f"Required quorum of {required_count} datasets satisfied."
        else:
            decision = "BLOCK"
            reason = f"Only {ready_count} of {required_count} required datasets are READY."
    else:
        decision = "BLOCK"
        reason = f"Unsupported condition type: {condition_type}"

    return decision, reason
