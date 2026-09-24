def evaluate_condition(condition_type: str, ready_count: int, total_required: int, required_count: int = None) -> tuple[str, str]:
    """
    Evaluates dataset dependencies to determine if a pipeline should trigger.

    Args:
        condition_type: 'ALL', 'ANY', or 'QUORUM'.
        ready_count: Number of required datasets currently in 'READY' status.
        total_required: Total number of datasets required by the pipeline.
        required_count: Specific threshold for 'QUORUM' type.

    Returns:
        tuple[str, str]: (decision, reason) where decision is 'TRIGGER' or 'BLOCK'.
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
