"""
FastAPI Application for SupplySense AI Observability & Monitoring.

Exposes read-only REST API endpoints for operational intelligence, pipeline execution
monitoring, dataset synchronization tracking, and forecast auditability.
"""

import os
import sys
import logging
from typing import Optional
from fastapi import FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from . import service

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(
    title="SupplySense AI - Observability Service",
    description="Read-only operational monitoring and telemetry API for pipeline executions, datasets, dependencies, and forecasts.",
    version="1.0.0"
)

# Enable CORS for local dashboards or development tooling
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/health", status_code=status.HTTP_200_OK)
def health_check():
    """Health check verifying service liveness and database connectivity."""
    health_info = service.get_health_status()
    if health_info["database"] != "connected":
        return health_info
    return health_info


@app.get("/api/observability/summary")
def get_summary():
    """
    Returns high-level operational metrics:
      - Execution counts (total, running, completed, failed, retrying)
      - Success / Failure rates
      - Average execution duration
      - Latest execution summary
      - Dataset status counts
      - Latest forecast summary
    """
    try:
        return service.get_observability_summary()
    except Exception as e:
        logger.error(f"Error fetching observability summary: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve observability summary"
        )


@app.get("/api/observability/executions")
def get_executions(
    status: Optional[str] = Query(None, description="Filter by status (RUNNING, COMPLETED, FAILED)"),
    pipeline_name: Optional[str] = Query(None, description="Filter by pipeline name"),
    limit: int = Query(20, ge=1, le=100, description="Max records to return (default 20, max 100)")
):
    """
    Returns recent pipeline executions ordered newest first, with duration and retry information.
    """
    try:
        return service.get_recent_executions(status=status, pipeline_name=pipeline_name, limit=limit)
    except Exception as e:
        logger.error(f"Error fetching executions: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve pipeline executions"
        )


@app.get("/api/observability/executions/{execution_id}")
def get_execution_detail(execution_id: int):
    """
    Returns detailed telemetry for a single execution including decision context,
    forecast summary, and artifact verification.
    """
    try:
        detail = service.get_execution_detail(execution_id)
        if not detail:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Execution #{execution_id} not found"
            )
        return detail
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching execution #{execution_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve execution #{execution_id}"
        )


@app.get("/api/observability/datasets")
def get_datasets():
    """
    Returns synchronization and event tracking status for all datasets.
    """
    try:
        return service.get_dataset_status()
    except Exception as e:
        logger.error(f"Error fetching dataset statuses: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve dataset statuses"
        )


@app.get("/api/observability/dependencies")
def get_dependencies():
    """
    Returns configured pipeline dependencies and their latest evaluated decision states.
    """
    try:
        return service.get_dependency_status()
    except Exception as e:
        logger.error(f"Error fetching dependency statuses: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve dependency statuses"
        )


@app.get("/api/observability/forecasts")
def get_forecasts(
    limit: int = Query(20, ge=1, le=100, description="Max forecast execution summaries to return")
):
    """
    Returns aggregated forecast summaries grouped by execution_id.
    """
    try:
        return service.get_forecast_status(limit=limit)
    except Exception as e:
        logger.error(f"Error fetching forecast summaries: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve forecast summaries"
        )
