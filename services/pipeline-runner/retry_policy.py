"""
SupplySense AI - Pipeline Execution Retry Policy & Error Classification

Defines classification rules for distinguishing transient (retryable) errors from
deterministic (non-retryable) errors, along with exponential backoff calculations.
"""

import os
import sys
import logging
from typing import Optional

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from config.settings import (
    PIPELINE_RETRY_BASE_DELAY_SECONDS,
    PIPELINE_RETRY_MAX_DELAY_SECONDS,
    PIPELINE_MAX_RETRIES
)

logger = logging.getLogger(__name__)

# Explicit Non-Retryable Exception Types
NON_RETRYABLE_EXCEPTIONS = (
    FileNotFoundError,
    ValueError,
    KeyError,
    TypeError,
    AttributeError,
    SyntaxError,
    ZeroDivisionError,
    ImportError,
    ModuleNotFoundError,
    NameError,
    NotImplementedError,
)

# Explicit Non-Retryable Message Substrings (case-insensitive)
NON_RETRYABLE_PATTERNS = [
    "missing required prepared dataset",
    "unknown pipeline",
    "handler not found",
    "duplicate entry",
    "cannot be null",
    "invalid literal",
    "invalid format",
    "invalid argument",
    "foreign key constraint fails",
    "syntax error",
]

# Explicit Retryable Message Substrings (case-insensitive)
RETRYABLE_PATTERNS = [
    "timed out",
    "timeout",
    "connection lost",
    "connection timed out",
    "connection refused",
    "connection reset",
    "deadlock",
    "lock wait timeout",
    "reaper",
    "temporarily unavailable",
    "transient",
    "network error",
    "broker not available",
    "service unavailable",
    "500 internal server error",
    "502 bad gateway",
    "503 service unavailable",
    "504 gateway timeout",
    "operationalerror",
    "interfaceerror",
]


def classify_error_type(
    error_message: Optional[str] = None,
    exception: Optional[Exception] = None
) -> str:
    """
    Classifies a failure into a structured error category for metrics and debugging.

    Returns:
        One of: 'TRANSIENT_TIMEOUT', 'TRANSIENT_DB', 'TRANSIENT_NETWORK', 'TRANSIENT_REAPER',
                'TRANSIENT_GENERAL', 'DETERMINISTIC_INPUT', 'DETERMINISTIC_CONFIG',
                'DETERMINISTIC_INTEGRITY', 'DETERMINISTIC_APPLICATION'
    """
    err_str = (error_message or (str(exception) if exception else "")).lower()

    if exception:
        if isinstance(exception, FileNotFoundError) or "missing required prepared dataset" in err_str:
            return "DETERMINISTIC_INPUT"
        if isinstance(exception, (ValueError, KeyError, TypeError, AttributeError)):
            return "DETERMINISTIC_APPLICATION"
        if isinstance(exception, (ImportError, ModuleNotFoundError)):
            return "DETERMINISTIC_CONFIG"

    if "reaper" in err_str:
        return "TRANSIENT_REAPER"
    if any(p in err_str for p in ["deadlock", "lock wait", "db connection", "mysql connection", "operationalerror", "interfaceerror"]):
        return "TRANSIENT_DB"
    if any(p in err_str for p in ["kafka", "network", "connection refused", "broker"]):
        return "TRANSIENT_NETWORK"
    if "timeout" in err_str or "timed out" in err_str:
        return "TRANSIENT_TIMEOUT"
    if any(p in err_str for p in ["duplicate entry", "foreign key", "cannot be null"]):
        return "DETERMINISTIC_INTEGRITY"
    if "unknown pipeline" in err_str:
        return "DETERMINISTIC_CONFIG"
    if any(p in err_str for p in NON_RETRYABLE_PATTERNS):
        return "DETERMINISTIC_APPLICATION"
    if any(p in err_str for p in RETRYABLE_PATTERNS):
        return "TRANSIENT_GENERAL"

    return "DETERMINISTIC_APPLICATION"


def is_retryable_error(
    error_message: Optional[str] = None,
    exception: Optional[Exception] = None
) -> bool:
    """
    Determines whether an execution failure is transient and eligible for automated retry.

    Args:
        error_message: Optional error message string recorded in database.
        exception: Optional Python Exception instance caught during execution.

    Returns:
        True if the error is transient/recoverable, False if deterministic/permanent.
    """
    if exception is not None:
        if isinstance(exception, NON_RETRYABLE_EXCEPTIONS):
            return False

    err_str = (error_message or (str(exception) if exception else "")).lower()

    # Check explicit non-retryable substrings
    for non_ret in NON_RETRYABLE_PATTERNS:
        if non_ret in err_str:
            return False

    # Check explicit retryable substrings
    for ret in RETRYABLE_PATTERNS:
        if ret in err_str:
            return True

    # If exception is a PyMySQL or connection-related error
    if exception is not None:
        exc_type = type(exception).__name__
        if "OperationalError" in exc_type or "InterfaceError" in exc_type or "Timeout" in exc_type:
            return True

    return False


def calculate_backoff_delay(
    retry_count: int,
    base_delay: Optional[int] = None,
    max_delay: Optional[int] = None
) -> int:
    """
    Calculates bounded exponential backoff delay in seconds:
        delay = min(base_delay * (2 ** retry_count), max_delay)

    Args:
        retry_count: Number of previous retries attempted (0-indexed).
        base_delay: Initial retry delay in seconds (default: PIPELINE_RETRY_BASE_DELAY_SECONDS).
        max_delay: Maximum delay ceiling in seconds (default: PIPELINE_RETRY_MAX_DELAY_SECONDS).

    Returns:
        Backoff delay in seconds as an integer.
    """
    if base_delay is None:
        base_delay = PIPELINE_RETRY_BASE_DELAY_SECONDS
    if max_delay is None:
        max_delay = PIPELINE_RETRY_MAX_DELAY_SECONDS

    # Avoid negative retry counts
    count = max(0, retry_count)
    computed = base_delay * (2 ** count)
    return int(min(computed, max_delay))
