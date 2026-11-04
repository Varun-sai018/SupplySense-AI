"""
Pipeline Registry for SupplySense AI.

Provides an extensible registry mapping pipeline names to execution handler callables.
"""

import logging
from typing import Dict, Callable, Any, Optional, List
from .handlers import (
    demand_forecast_pipeline_handler,
    inventory_reorder_pipeline_handler,
    simulated_pipeline_handler,
    sim_demand_forecast_handler,
    sim_inventory_reorder_handler,
    operational_feature_pipeline_handler,
    operational_demand_forecast_handler,
)

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


# Default registration for Phase 4 & Phase F5
register_pipeline("Demand Forecast Pipeline", demand_forecast_pipeline_handler)
register_pipeline("Inventory Reorder Pipeline", inventory_reorder_pipeline_handler)

# Phase 1: Explicit registrations for isolated simulated pipelines
register_pipeline("SIM-ALL-Pipeline", simulated_pipeline_handler)
register_pipeline("SIM-ANY-Pipeline", simulated_pipeline_handler)
register_pipeline("SIM-QUORUM-Pipeline", simulated_pipeline_handler)
register_pipeline("SIM-AST-Pipeline", simulated_pipeline_handler)

# End-to-End Simulation Pipelines
register_pipeline("SIM-FORECAST-Pipeline", sim_demand_forecast_handler)
register_pipeline("SIM-REORDER-Pipeline", sim_inventory_reorder_handler)

# Phase 3: Operational Demand Feature Pipeline
register_pipeline("Operational Demand Feature Pipeline", operational_feature_pipeline_handler)
register_pipeline("Operational Feature Pipeline", operational_feature_pipeline_handler)

# Phase 4: Operational Demand Forecast Pipeline
register_pipeline("Operational Demand Forecast Pipeline", operational_demand_forecast_handler)
register_pipeline("Operational Forecast Pipeline", operational_demand_forecast_handler)



# Authoritative Real Olist Pipelines
try:
    from .handlers import (
        olist_core_forecast_handler,
        olist_regional_dispatch_handler,
        olist_payment_settlement_handler,
    )
    register_pipeline("Olist Core Forecast Pipeline", olist_core_forecast_handler)
    register_pipeline("Olist Regional Dispatch Pipeline", olist_regional_dispatch_handler)
    register_pipeline("Olist Payment Settlement Pipeline", olist_payment_settlement_handler)
except ImportError as err:
    logger.warning(f"Could not register Olist handlers: {err}")
