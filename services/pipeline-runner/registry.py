"""
Pipeline Registry for SupplySense AI.

Provides an extensible registry mapping pipeline names to execution handler callables.
"""

import logging
from typing import Dict, Callable, Any, Optional, List
from .handlers import demand_forecast_pipeline_handler

logger = logging.getLogger(__name__)

# Type definition for pipeline handler callable
# handler(execution_id: int, pipeline_name: str) -> Dict[str, Any]
PipelineHandler = Callable[[int, str], Dict[str, Any]]

_PIPELINE_REGISTRY: Dict[str, PipelineHandler] = {}


def register_pipeline(pipeline_name: str, handler_func: PipelineHandler) -> None:
    """Registers a pipeline name with its corresponding execution handler."""
    _PIPELINE_REGISTRY[pipeline_name] = handler_func
    logger.debug(f"Registered pipeline handler for '{pipeline_name}'")


def get_pipeline_handler(pipeline_name: str) -> Optional[PipelineHandler]:
    """Retrieves the execution handler for a given pipeline name, or None if unknown."""
    return _PIPELINE_REGISTRY.get(pipeline_name)


def list_registered_pipelines() -> List[str]:
    """Returns a list of all registered pipeline names."""
    return list(_PIPELINE_REGISTRY.keys())


# Default registration for Phase 4
register_pipeline("Demand Forecast Pipeline", demand_forecast_pipeline_handler)
