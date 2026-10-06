"""
SupplySense AI - Observability & Monitoring Service.

Provides a unified read-only monitoring and operational intelligence layer
over pipeline executions, dataset states, dependency evaluations, and ML forecasts.
"""

from .service import (
    get_health_status,
    get_observability_summary,
    get_recent_executions,
    get_execution_detail,
    get_dataset_status,
    get_dependency_status,
    get_forecast_status
)

__all__ = [
    "get_health_status",
    "get_observability_summary",
    "get_recent_executions",
    "get_execution_detail",
    "get_dataset_status",
    "get_dependency_status",
    "get_forecast_status"
]
