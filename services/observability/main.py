"""
FastAPI Application for SupplySense AI Observability & Monitoring.

Exposes read-only REST API endpoints for operational intelligence, pipeline execution
monitoring, dataset synchronization tracking, and forecast auditability.
"""

import os
import sys
import asyncio
import logging
from typing import Optional, AsyncGenerator
from pydantic import BaseModel, Field
from fastapi import FastAPI, HTTPException, Query, Header, Request, status
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from . import service
from . import approval_service
from .event_hub import hub, DashboardEvent
from . import analyst_service
from . import event_journal
from .publisher import publish_dashboard_event

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(
    title="SupplySense AI - Observability Service",
    description="Read-only operational monitoring and telemetry API for pipeline executions, datasets, dependencies, and forecasts.",
    version="1.0.0"
)

from concurrent.futures import ThreadPoolExecutor

_worker_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="obs_bg_worker")
_shutdown_event = asyncio.Event()
_background_tasks = []


async def cross_process_event_poller():
    """
    Background worker that continuously polls the MySQL dashboard_events journal
    for events emitted by external processes (rehearsal runners, rule workers)
    and forwards them into the active FastAPI EventHub for live SSE streaming.
    Offloaded to a thread executor so synchronous DB calls do not block the event loop.
    """
    loop = asyncio.get_running_loop()
    try:
        last_seen_id = await loop.run_in_executor(_worker_executor, event_journal.get_latest_event_row_id)
    except Exception:
        last_seen_id = 0
    logger.info(f"Cross-process event poller initialized starting at event row #{last_seen_id}.")
    poll_count = 0

    while not _shutdown_event.is_set():
        try:
            await asyncio.sleep(0.5)
            if _shutdown_event.is_set():
                break
            new_records = await loop.run_in_executor(
                _worker_executor, event_journal.fetch_events_after_id, last_seen_id, 50
            )
            if new_records:
                for row_id, event in new_records:
                    hub.publish(event)
                    last_seen_id = max(last_seen_id, row_id)

            poll_count += 1
            if poll_count >= 600:
                poll_count = 0
                await loop.run_in_executor(_worker_executor, event_journal.prune_old_events, 1000)

        except (asyncio.CancelledError, GeneratorExit):
            break
        except Exception as poll_err:
            if _shutdown_event.is_set():
                break
            logger.debug(f"Error in cross-process event poller: {poll_err}")
            await asyncio.sleep(1.0)


async def background_dependency_engine_poller():
    """
    Automatic end-to-end event execution worker:
    Continuously polls MySQL dataset_events for eligible NEW events and runs
    the Dependency Engine + Pipeline Runner in a separate thread executor.
    Multi-worker safe via SELECT ... FOR UPDATE SKIP LOCKED.
    """
    logger.info("Automatic dependency engine background poller started.")
    loop = asyncio.get_running_loop()
    from services.dependency_engine.mysql_consumer import poll_and_process_batch

    while not _shutdown_event.is_set():
        try:
            await asyncio.sleep(1.5)
            if _shutdown_event.is_set():
                break
            result = await loop.run_in_executor(_worker_executor, poll_and_process_batch, 10, 300)
            if result.get("claimed", 0) > 0:
                logger.info(
                    f"Auto-Engine pass completed: claimed={result['claimed']}, "
                    f"processed={result['processed']}, failed={result['failed']}"
                )
        except (asyncio.CancelledError, GeneratorExit):
            break
        except Exception as exc:
            if _shutdown_event.is_set():
                break
            logger.debug(f"Auto-engine background poller error: {exc}")
            await asyncio.sleep(2.0)


async def background_automatic_simulation_worker():
    """
    Automatically simulates incoming real Olist data events in the background,
    which feeds into dataset_events -> Dependency Engine poller -> downstream pipelines.
    """
    logger.info("Automatic Olist stream simulation background worker started.")
    await asyncio.sleep(3.0)
    loop = asyncio.get_running_loop()
    from services.simulator.auto_simulator import auto_olist_simulator

    interval = float(os.getenv("AUTO_SIMULATION_INTERVAL_SECONDS", "5.0"))
    while not _shutdown_event.is_set():
        try:
            await asyncio.sleep(interval)
            if _shutdown_event.is_set():
                break
            await loop.run_in_executor(_worker_executor, auto_olist_simulator.step)
        except (asyncio.CancelledError, GeneratorExit):
            break
        except Exception as sim_err:
            if _shutdown_event.is_set():
                break
            logger.debug(f"Auto-simulation step error: {sim_err}")
            await asyncio.sleep(2.0)


@app.on_event("startup")
async def startup_event():
    """Registers the active asyncio event loop with EventHub and starts background workers."""
    _shutdown_event.clear()
    hub.set_event_loop(asyncio.get_running_loop())
    logger.info("Observability EventHub initialized with active asyncio event loop.")
    _background_tasks.clear()
    _background_tasks.append(asyncio.create_task(cross_process_event_poller()))
    _background_tasks.append(asyncio.create_task(background_dependency_engine_poller()))
    _background_tasks.append(asyncio.create_task(background_automatic_simulation_worker()))


@app.on_event("shutdown")
async def shutdown_event():
    """Stops all background workers and terminates the thread executor cleanly."""
    logger.info("Observability service shutting down. Terminating background workers...")
    _shutdown_event.set()
    for t in _background_tasks:
        if not t.done():
            t.cancel()
    
    if _background_tasks:
        await asyncio.gather(*_background_tasks, return_exceptions=True)
    _worker_executor.shutdown(wait=False, cancel_futures=True)
    logger.info("Observability background workers stopped cleanly.")


# Enable CORS for local dashboards or development tooling
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
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
def get_summary(
    scope: str = Query("OPERATIONAL", description="Scope filter: 'OPERATIONAL' (excludes simulated) or 'ALL'")
):
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
        exclude_simulated = (scope.upper() != "ALL")
        return service.get_observability_summary(exclude_simulated=exclude_simulated)
    except Exception as e:
        logger.error(f"Error fetching observability summary: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve observability summary"
        )


VALID_EXECUTION_SCOPES = {"OPERATIONAL", "SIMULATED", "ALL"}
VALID_DATASET_SCOPES = {"OPERATIONAL", "SIMULATED", "ALL"}
VALID_DECISION_SCOPES = {"OPERATIONAL", "SIMULATED", "ALL"}


@app.get("/api/observability/executions")
def get_executions(
    status: Optional[str] = Query(None, description="Filter by status (RUNNING, COMPLETED, FAILED)"),
    pipeline_name: Optional[str] = Query(None, description="Filter by pipeline name"),
    scope: str = Query("OPERATIONAL", description="Filter scope: OPERATIONAL, SIMULATED, or ALL (default OPERATIONAL)"),
    limit: int = Query(20, ge=1, le=100, description="Max records to return (default 20, max 100)"),
    offset: int = Query(0, ge=0, description="Pagination offset (default 0)"),
    exclude_simulated: Optional[bool] = Query(None, description="Deprecated: use scope=OPERATIONAL instead")
):
    """
    Returns recent pipeline executions ordered newest first, with duration, action linkage, and sanitized retry/error information.
    Paginated response format: { items, total_count, limit, offset }.
    """
    scope_upper = scope.upper() if scope else "OPERATIONAL"
    if scope_upper not in VALID_EXECUTION_SCOPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid scope '{scope}'. Allowed values are: {', '.join(sorted(VALID_EXECUTION_SCOPES))}"
        )

    # Backward compatibility with exclude_simulated parameter
    if exclude_simulated is False and scope == "OPERATIONAL":
        scope_upper = "ALL"
    elif exclude_simulated is True:
        scope_upper = "OPERATIONAL"

    try:
        return service.get_recent_executions(
            status=status,
            pipeline_name=pipeline_name,
            limit=limit,
            offset=offset,
            exclude_simulated=(scope_upper == "OPERATIONAL"),
            scope=scope_upper
        )
    except Exception as e:
        logger.error(f"Error fetching executions: {e}")
        raise HTTPException(
            status_code=500,
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
                status_code=404,
                detail=f"Execution #{execution_id} not found"
            )
        return detail
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching execution #{execution_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve execution #{execution_id}"
        )


@app.get("/api/observability/datasets")
def get_datasets(
    scope: str = Query("ALL", description="Filter scope: ALL, OPERATIONAL, or SIMULATED (default ALL)"),
    source_type: Optional[str] = Query(None, description="Filter by source_type (e.g. SIMULATED, OPERATIONAL)"),
    exclude_simulated: Optional[bool] = Query(None, description="Exclude SIMULATED datasets"),
    limit: int = Query(20, ge=1, le=100, description="Max records to return (default 20, max 100)"),
    offset: int = Query(0, ge=0, description="Pagination offset (default 0)")
):
    """
    Returns synchronization and event tracking status for tracked datasets with SQL pagination and scope filtering.
    Paginated response format: { items, total_count, limit, offset }.
    """
    scope_upper = scope.upper() if scope else "ALL"
    if scope_upper not in VALID_DATASET_SCOPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid scope '{scope}'. Allowed values are: {', '.join(sorted(VALID_DATASET_SCOPES))}"
        )

    effective_exclude = exclude_simulated if exclude_simulated is not None else False
    if scope_upper == "OPERATIONAL":
        effective_exclude = True

    try:
        return service.get_dataset_status_paginated(
            source_type=source_type,
            exclude_simulated=effective_exclude,
            scope=scope_upper,
            limit=limit,
            offset=offset
        )
    except Exception as e:
        logger.error(f"Error fetching dataset statuses: {e}")
        raise HTTPException(
            status_code=500,
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


@app.get("/api/observability/dependencies/decisions")
def get_dependency_decisions(
    scope: str = Query("OPERATIONAL", description="Filter scope: OPERATIONAL, SIMULATED, or ALL (default OPERATIONAL)"),
    decision: Optional[str] = Query(None, description="Filter by decision: TRIGGER or BLOCK"),
    pipeline_name: Optional[str] = Query(None, description="Filter by target pipeline name"),
    limit: int = Query(50, ge=1, le=200, description="Max recent decisions to return (default 50, max 200)"),
    offset: int = Query(0, ge=0, description="Pagination offset (default 0)")
):
    """
    Returns recent evaluated dependency decisions audit log with scope isolation and pagination.
    """
    # Defensive unwrapping if called directly in Python without FastAPI DI
    scope_val = scope.default if hasattr(scope, "default") else scope
    decision_val = decision.default if hasattr(decision, "default") else decision
    pipeline_name_val = pipeline_name.default if hasattr(pipeline_name, "default") else pipeline_name
    limit_val = limit.default if hasattr(limit, "default") else limit
    offset_val = offset.default if hasattr(offset, "default") else offset

    scope_upper = scope_val.upper() if scope_val else "OPERATIONAL"
    if scope_upper not in VALID_DECISION_SCOPES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid scope '{scope_val}'. Allowed values are: {', '.join(sorted(VALID_DECISION_SCOPES))}"
        )

    try:
        return service.get_dependency_decisions(
            limit=limit_val,
            offset=offset_val,
            scope=scope_upper,
            decision=decision_val,
            pipeline_name=pipeline_name_val
        )
    except Exception as e:
        logger.error(f"Error fetching dependency decisions: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve dependency decisions"
        )


@app.get("/api/observability/dependencies/events")
def get_dependency_events(
    dataset_name: Optional[str] = Query(None, description="Optional dataset name filter"),
    event_status: Optional[str] = Query(None, description="Optional status filter: NEW, PROCESSING, COMPLETED, etc."),
    limit: int = Query(25, ge=1, le=100, description="Max incoming events to return (default 25, max 100)"),
    offset: int = Query(0, ge=0, description="Pagination offset (default 0)")
):
    """
    Returns live incoming dataset events stream arriving at the Dependency Engine with
    enriched realistic Olist e-commerce business context, target pipelines, and evaluated decisions.
    """
    try:
        return service.get_dependency_events(
            limit=limit,
            offset=offset,
            dataset_name=dataset_name,
            event_status=event_status
        )
    except Exception as e:
        logger.error(f"Error fetching dependency events: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve dependency incoming events"
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


@app.get("/api/observability/forecasts/{execution_id}/category-trends")
def get_category_trends(
    execution_id: int,
    categories: Optional[str] = Query(None, description="Comma-separated category names to filter (default: top N)"),
    limit: int = Query(5, ge=1, le=20, description="Max categories to return if no filter specified")
):
    """
    Returns weekly predicted demand trends grouped by product category across the 14-week horizon.
    """
    try:
        cat_list = [c.strip() for c in categories.split(",") if c.strip()] if categories else None
        trends = service.get_category_forecast_trends(
            execution_id=execution_id,
            categories=cat_list,
            limit=limit
        )
        if trends is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Forecast trends not found or unavailable for execution #{execution_id}"
            )
        return trends
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching category forecast trends for #{execution_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve category forecast trends for execution #{execution_id}"
        )


@app.get("/api/observability/executions/{execution_id}/model-comparison")
def get_model_comparison(execution_id: int):
    """
    Returns validated model comparison metrics (Naive Baseline vs XGBoost)
    from the execution's isolated artifact directory.
    """
    try:
        metrics = service.get_model_comparison_metrics(execution_id)
        if metrics is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Model comparison metrics not found or unavailable for execution #{execution_id}"
            )
        return metrics
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching model comparison metrics for #{execution_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve model comparison metrics for execution #{execution_id}"
        )


@app.get("/api/observability/forecasts/{execution_id}/products")
def get_forecast_products_endpoint(
    execution_id: int,
    category: Optional[str] = Query(None, description="Category filter"),
    search: Optional[str] = Query(None, description="Search term for SKU or category"),
    sort_by: str = Query("demand", description="Sort field: demand, stock, risk, name"),
    order: str = Query("desc", description="Sort order: asc or desc"),
    limit: int = Query(25, ge=1, le=100, description="Items per page"),
    offset: int = Query(0, ge=0, description="Pagination offset")
):
    """
    Returns paginated product-level demand, category forecast share, and stockout risk metrics.
    """
    try:
        return service.get_forecast_products(
            execution_id=execution_id,
            category=category,
            search=search,
            sort_by=sort_by,
            order=order,
            limit=limit,
            offset=offset
        )
    except Exception as e:
        logger.error(f"Error fetching forecast products for #{execution_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve forecast products for execution #{execution_id}"
        )


@app.get("/api/observability/products/{product_id}/details")
def get_product_detail_endpoint(
    product_id: str,
    execution_id: Optional[int] = Query(None, description="Optional forecast execution ID for benchmark context")
):
    """
    Returns complete product details, dimensions, inventory levels, supplier lead times,
    and actual historical weekly sales timeseries for chart rendering.
    """
    try:
        data = service.get_product_detail(product_id=product_id, execution_id=execution_id)
        if not data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Product '{product_id}' not found"
            )
        return data
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching details for product {product_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve details for product {product_id}"
        )



@app.get("/api/observability/events/stream")
async def stream_events(
    request: Request,
    last_event_id: Optional[str] = Header(None, alias="Last-Event-ID")
):
    """
    Server-Sent Events (SSE) live telemetry stream.
    Supports:
      - Continuous streaming with Keep-Alive / Heartbeat comments.
      - Reconnection via Last-Event-ID header from in-memory ring buffer.
      - REST snapshot fallback notification if replay window has expired.
      - Clean subscriber unregistration on client disconnect or cancellation.
    """
    queue = hub.subscribe(maxsize=100)

    async def event_generator() -> AsyncGenerator[str, None]:
        try:
            # 1. Handle Reconnection / Replay
            if last_event_id:
                found, replay_events = hub.get_replay_events(last_event_id)
                if not found:
                    # Attempt database-backed replay when not in memory ring buffer
                    found, replay_events = event_journal.fetch_replay_events_by_event_id(last_event_id, limit=50)

                if found:
                    for ev in replay_events:
                        yield ev.to_sse_format()
                else:
                    # Last-Event-ID was not in memory or DB (expired or invalid)
                    yield hub.format_snapshot_required()

            # 2. Continuous Event Streaming Loop
            while True:
                # Check for client disconnect
                if await request.is_disconnected():
                    break

                try:
                    # Wait for next event or heartbeat timeout (15s)
                    event: DashboardEvent = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield event.to_sse_format()
                except asyncio.TimeoutError:
                    # Yield heartbeat keep-alive comment
                    yield hub.format_heartbeat()
                except asyncio.CancelledError:
                    break

        except Exception as e:
            logger.debug(f"SSE client stream error: {e}")
        finally:
            hub.unsubscribe(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
    )


@app.get("/api/observability/events/status")
def get_stream_status():
    """Returns telemetry on current SSE active subscribers and ring buffer occupancy."""
    return {
        "status": "active",
        "active_subscribers": hub.subscriber_count(),
        "ring_buffer_size": len(hub._ring_buffer),
        "ring_buffer_capacity": hub.buffer_size
    }


@app.get("/api/observability/analyst/metrics")
def get_analyst_metrics(
    domain: Optional[str] = Query(None, description="Optional domain filter: financial, operational, customer, products, provenance, or all"),
    refresh: bool = Query(False, description="Force cache invalidation")
):
    """
    Returns consolidated or domain-specific analyst intelligence.
    Supports in-memory caching with sub-second response times (< 10ms on cache hit).
    """
    try:
        return analyst_service.get_analyst_metrics_by_domain(domain=domain, force_refresh=refresh)
    except Exception as e:
        logger.error(f"Analyst metrics error: {e}")
        raise HTTPException(status_code=500, detail=f"Analyst metrics unavailable: {str(e)}")


@app.post("/api/observability/events/publish", status_code=status.HTTP_201_CREATED)
async def publish_event_endpoint(payload: Request):
    """
    HTTP endpoint allowing external scripts or processes to publish live events directly
    into the FastAPI EventHub and MySQL journal for immediate SSE delivery.
    """
    try:
        body = await payload.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    event_type = body.get("event_type")
    entity = body.get("entity", "SYSTEM")
    severity = body.get("severity", "INFO")
    data = body.get("data", {})
    decision_id = body.get("decision_id")
    execution_id = body.get("execution_id")
    correlation_id = body.get("correlation_id")

    if not event_type:
        raise HTTPException(status_code=400, detail="Missing required field: 'event_type'")

    event = publish_dashboard_event(
        event_type=event_type,
        entity=entity,
        severity=severity,
        data=data,
        decision_id=decision_id,
        execution_id=execution_id,
        correlation_id=correlation_id
    )

    if not event:
        raise HTTPException(status_code=400, detail="Failed to publish event (validation error)")

    return {
        "status": "published",
        "event_id": event.event_id,
        "event_type": event.event_type,
        "timestamp": event.timestamp
    }




# ---------------------------------------------------------------------------
# Phase 3A: Operational Inventory, Supplier & Replenishment Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/observability/inventory")
def get_inventory_endpoint(
    warehouse_id: Optional[str] = Query(None, description="Optional warehouse filter (e.g. WH-SP-01)"),
    low_stock_only: bool = Query(False, description="Filter to items where available_quantity < reorder_threshold"),
    search: Optional[str] = Query(None, description="Search filter for SKU or warehouse"),
    limit: int = Query(50, ge=1, le=100, description="Max items to return (default 50, max 100)"),
    offset: int = Query(0, ge=0, description="Pagination offset (default 0)")
):
    """
    Returns paginated operational inventory records from operational_inventory.
    Computes is_low_stock using actual Rule B threshold semantics (available_quantity < reorder_threshold).
    """
    try:
        return service.get_operational_inventory(
            warehouse_id=warehouse_id,
            low_stock_only=low_stock_only,
            search=search,
            limit=limit,
            offset=offset
        )
    except Exception as e:
        logger.error(f"Error fetching operational inventory: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve operational inventory: {str(e)}"
        )


@app.get("/api/observability/suppliers")
def get_suppliers_endpoint(
    supplier_id: Optional[str] = Query(None, description="Optional supplier filter"),
    status_filter: Optional[str] = Query(None, alias="status", description="Filter by status (Online, Degraded, Offline)"),
    limit: int = Query(50, ge=1, le=100, description="Max suppliers to return (default 50, max 100)"),
    offset: int = Query(0, ge=0, description="Pagination offset (default 0)")
):
    """
    Returns operational suppliers with catalog constraints and connectivity status.
    Distinguishes supplier connectivity (Online/Degraded/Offline) from SKU catalog availability.
    """
    try:
        return service.get_operational_suppliers(
            supplier_id=supplier_id,
            status=status_filter,
            limit=limit,
            offset=offset
        )
    except Exception as e:
        logger.error(f"Error fetching operational suppliers: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve operational suppliers: {str(e)}"
        )


@app.get("/api/observability/replenishments")
def get_replenishments_endpoint(
    status_filter: Optional[str] = Query(None, alias="status", description="Filter by status (e.g. CONFIRMED, DELIVERED, CANCELLED)"),
    sku: Optional[str] = Query(None, description="Filter by SKU/product identifier"),
    limit: int = Query(50, ge=1, le=100, description="Max replenishment orders to return (default 50, max 100)"),
    offset: int = Query(0, ge=0, description="Pagination offset (default 0)")
):
    """
    Returns persisted replenishment orders from replenishment_orders.
    Includes linked action_id, decision_id, and execution_id.
    """
    try:
        return service.get_replenishment_orders(
            status=status_filter,
            sku=sku,
            limit=limit,
            offset=offset
        )
    except Exception as e:
        logger.error(f"Error fetching replenishment orders: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve replenishment orders: {str(e)}"
        )


@app.post("/api/observability/replenishments/{replenishment_id}/deliver", status_code=status.HTTP_200_OK)
def deliver_replenishment_endpoint(replenishment_id: str):
    """
    Simulates deterministic delivery receipt for an in-flight replenishment order.
    Increments inventory balance and version atomically, updates order to DELIVERED,
    and publishes DELIVERY_COMPLETED event to dashboard journal and SSE stream.
    Idempotent: Subsequent calls for already delivered orders return success without double-stocking.
    """
    from services.dependency_engine.repository import InventoryNotFoundError
    from services.simulator.delivery_service import OperationalDeliveryForbiddenError
    try:
        result = service.deliver_replenishment(replenishment_id=replenishment_id)
        return result
    except OperationalDeliveryForbiddenError as e:
        logger.warning(f"Simulated delivery rejected for operational data '{replenishment_id}': {e}")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Simulated delivery rejected for operational data: {str(e)}"
        )
    except InventoryNotFoundError as e:
        logger.warning(f"Delivery failed: Inventory missing for replenishment '{replenishment_id}': {e}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except ValueError as e:
        err_msg = str(e)
        if "not found" in err_msg.lower():
            logger.warning(f"Replenishment '{replenishment_id}' not found for delivery: {e}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=err_msg
            )
        logger.warning(f"Invalid delivery request for replenishment '{replenishment_id}': {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=err_msg
        )
    except Exception as e:
        logger.error(f"Unexpected error executing delivery for replenishment '{replenishment_id}': {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Delivery failed: {str(e)}"
        )


@app.get("/api/observability/operational-summary")
def get_operational_summary_endpoint():
    """
    Returns database-backed operational summary metrics:
    - total_skus
    - low_stock_skus
    - active_replenishments
    - delivered_replenishments
    - offline_suppliers
    """
    try:
        return service.get_operational_summary_metrics()
    except Exception as e:
        logger.error(f"Error fetching operational summary metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve operational summary metrics: {str(e)}"
        )


@app.get("/api/observability/events/recent")
def get_recent_events_endpoint(
    event_type: Optional[str] = Query(None, description="Optional event type filter (e.g. INVENTORY_REORDER_STATE_CHANGED, DELIVERY_COMPLETED)"),
    limit: int = Query(25, ge=1, le=100, description="Max events to return (default 25, max 100)")
):
    """
    Returns recent operational dashboard events from dashboard_events journal.
    Specifically provides fast initial state for INVENTORY_REORDER_STATE_CHANGED and DELIVERY_COMPLETED.
    """
    try:
        return service.get_recent_operational_events(limit=limit, event_type=event_type)
    except Exception as e:
        logger.error(f"Error fetching recent dashboard events: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve recent dashboard events: {str(e)}"
        )



@app.get("/api/observability/alerts")
def get_operational_alerts_endpoint(
    severity: Optional[str] = Query(None, description="Filter by severity: CRITICAL, WARNING, INFO"),
    category: Optional[str] = Query(None, description="Filter by category: SUPPLIER, CATALOG, REPLENISHMENT, PIPELINE"),
    stuck_threshold_hours: int = Query(24, ge=1, le=168, description="Hours threshold to consider in-flight replenishment stuck (default 24)"),
    limit: int = Query(50, ge=1, le=100, description="Max alerts to return (default 50, max 100)"),
    exclude_simulated: bool = Query(True, description="Exclude SIM-* pipeline failures (default True)")
):
    """
    Returns prioritized operational alerts and failure diagnostics across:
    - Offline & degraded suppliers
    - SKU catalog availability failures (OUT_OF_STOCK)
    - Failed or cancelled replenishment orders
    - Overdue deliveries (estimated_delivery_at in the past)
    - Replenishments stuck in intermediate states
    - Failed pipeline executions
    """
    try:
        return service.get_operational_alerts(
            severity=severity,
            category=category,
            stuck_threshold_hours=stuck_threshold_hours,
            limit=limit,
            exclude_simulated=exclude_simulated
        )
    except Exception as e:
        logger.error(f"Error fetching operational alerts: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve operational alerts: {str(e)}"
        )


@app.get("/api/observability/event-health")
def get_event_health_endpoint():
    """
    Returns event processing diagnostics for EventHub pub/sub and the MySQL event journal:
    - Active SSE subscriber count
    - Memory ring-buffer utilization
    - Total journaled events and 24h event distribution
    - Processing lag and journal bounds
    """
    try:
        return service.get_event_processing_health()
    except Exception as e:
        logger.error(f"Error fetching event processing health: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve event processing health: {str(e)}"
        )



# ---------------------------------------------------------------------------
# Phase 3D: Forecast-Driven Replenishment Recommendation Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/observability/forecast-recommendations")
def get_forecast_recommendations_endpoint(
    execution_id: Optional[int] = Query(None, description="Demand forecast execution ID (defaults to latest completed)"),
    scope_type: Optional[str] = Query(None, description="Filter by scope: SKU, CATEGORY"),
    status: Optional[str] = Query(None, description="Filter by status: REPLENISHMENT_RECOMMENDED, STOCK_SUFFICIENT, NEEDS_REVIEW, INSUFFICIENT_DATA"),
    category: Optional[str] = Query(None, description="Filter by product category name"),
    sku: Optional[str] = Query(None, description="Filter by SKU identifier"),
    warehouse_id: Optional[str] = Query(None, description="Optional warehouse filter (e.g. WH-SP-01)"),
    planning_horizon_weeks: int = Query(4, ge=1, le=12, description="Planning horizon in weeks (1-12, default 4)"),
    limit: int = Query(50, ge=1, le=100, description="Max recommendations to return (default 50, max 100)"),
    offset: int = Query(0, ge=0, description="Pagination offset (default 0)")
):
    """
    Returns forecast-driven replenishment recommendations correlating ML predictions
    with real-time operational inventory, lead times, and safety stock levels.
    Purely read-only advisory endpoint: does NOT mutate inventory or trigger orders.
    """
    try:
        return service.get_forecast_recommendations(
            execution_id=execution_id,
            scope_type=scope_type,
            status=status,
            category=category,
            sku=sku,
            warehouse_id=warehouse_id,
            planning_horizon_weeks=planning_horizon_weeks,
            limit=limit,
            offset=offset
        )
    except Exception as e:
        logger.error(f"Error computing forecast recommendations: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR if hasattr(status, 'HTTP_500_INTERNAL_SERVER_ERROR') else 500,
            detail=f"Failed to generate forecast recommendations: {str(e)}"
        )


@app.get("/api/observability/forecast-recommendations/{recommendation_id}")
def get_forecast_recommendation_detail_endpoint(recommendation_id: str):
    """
    Retrieves detailed breakdown for a single forecast recommendation by its identifier.
    """
    try:
        rec = service.get_forecast_recommendation_by_id(recommendation_id=recommendation_id)
        if not rec:
            raise HTTPException(
                status_code=404,
                detail=f"Forecast recommendation '{recommendation_id}' not found."
            )
        return rec
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching recommendation detail for {recommendation_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve recommendation detail: {str(e)}"
        )


# ---------------------------------------------------------------------------
# Phase 3E: Human-in-the-Loop Replenishment Approval Endpoints & Models
# ---------------------------------------------------------------------------

class ApprovalSubmissionRequest(BaseModel):
    recommendation_id: str = Field(..., description="Target forecast recommendation identifier")
    requested_quantity: Optional[int] = Field(None, ge=1, description="Optional replenishment quantity override (>0)")
    sku: Optional[str] = Field(None, description="Optional target SKU")
    sku_override: Optional[str] = Field(None, description="Optional target SKU for category-level recommendations")
    warehouse: Optional[str] = Field(None, description="Target warehouse ID")
    warehouse_id: Optional[str] = Field(None, description="Target warehouse ID")
    warehouse_id_override: Optional[str] = Field(None, description="Target warehouse ID (default: WH-SP-01)")
    idempotency_key: Optional[str] = Field(None, description="Client-supplied idempotency key")
    operator_notes: Optional[str] = Field(None, description="Optional operator submission notes")


class ApprovalActionRequest(BaseModel):
    notes: Optional[str] = Field(None, description="Optional managerial notes for approval audit log")


class ApprovalRejectionRequest(BaseModel):
    reason: str = Field(..., min_length=1, description="Mandatory reason for rejection")


@app.post("/api/observability/replenishment-approvals", status_code=status.HTTP_201_CREATED)
def submit_recommendation_for_approval_endpoint(
    req: ApprovalSubmissionRequest,
    x_actor_id: str = Header(..., alias="X-Actor-ID", description="Submitting operator identifier"),
    x_actor_role: str = Header("OPERATOR", alias="X-Actor-Role", description="Actor role: OPERATOR, MANAGER, ADMIN")
):
    """
    Submits a forecast-driven replenishment recommendation for managerial review and approval.
    Re-validates stock balances, lead times, and active supplier status from the operational ledger.
    """
    try:
        from . import approval_service
        return approval_service.submit_recommendation_for_approval(
            recommendation_id=req.recommendation_id,
            actor_id=x_actor_id,
            actor_role=x_actor_role,
            requested_quantity=req.requested_quantity,
            sku_override=req.sku_override or req.sku,
            warehouse_id_override=req.warehouse_id_override or req.warehouse_id or req.warehouse,
            idempotency_key=req.idempotency_key
        )
    except approval_service.ApprovalSecurityError as sec_err:
        raise HTTPException(status_code=403, detail=str(sec_err))
    except approval_service.ApprovalConflictError as conf_err:
        raise HTTPException(status_code=409, detail=str(conf_err))
    except approval_service.ApprovalValidationError as val_err:
        raise HTTPException(status_code=400, detail=str(val_err))
    except Exception as e:
        logger.error(f"Error submitting recommendation for approval: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to submit replenishment approval: {str(e)}")


@app.get("/api/observability/replenishment-approvals")
def list_replenishment_approvals_endpoint(
    status: Optional[str] = Query(None, description="Filter by approval status: PENDING_REVIEW, APPROVED, REJECTED, DISPATCHED, FAILED"),
    sku: Optional[str] = Query(None, description="Filter by SKU identifier"),
    warehouse_id: Optional[str] = Query(None, description="Filter by warehouse ID"),
    execution_id: Optional[int] = Query(None, description="Filter by forecast execution ID"),
    limit: int = Query(50, ge=1, le=100, description="Max records to return"),
    offset: int = Query(0, ge=0, description="Pagination offset")
):
    """
    Returns paginated replenishment approvals with summary KPI counts.
    """
    try:
        from . import approval_service
        return approval_service.list_replenishment_approvals(
            status=status,
            sku=sku,
            warehouse_id=warehouse_id,
            execution_id=execution_id,
            limit=limit,
            offset=offset
        )
    except Exception as e:
        logger.error(f"Error listing replenishment approvals: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list replenishment approvals: {str(e)}")


@app.get("/api/observability/replenishment-approvals/{approval_id}")
def get_replenishment_approval_detail_endpoint(approval_id: str):
    """
    Retrieves full approval record including complete audit trail history.
    """
    try:
        from . import approval_service
        res = approval_service.get_replenishment_approval_detail(approval_id=approval_id)
        if not res:
            raise HTTPException(status_code=404, detail=f"Approval request '{approval_id}' not found.")
        return res
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving approval detail for {approval_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve approval detail: {str(e)}")


@app.post("/api/observability/replenishment-approvals/{approval_id}/approve")
def approve_replenishment_endpoint(
    approval_id: str,
    req: ApprovalActionRequest = None,
    x_actor_id: str = Header(..., alias="X-Actor-ID", description="Approving manager identifier"),
    x_actor_role: str = Header(..., alias="X-Actor-Role", description="Managerial role: MANAGER or ADMIN")
):
    """
    Approves a pending replenishment request and immediately executes idempotent supplier dispatch.
    Locks row for update, performs optimistic inventory version checks, and prevents stale execution.
    """
    try:
        from . import approval_service
        notes = req.notes if req else None
        return approval_service.approve_replenishment(
            approval_id=approval_id,
            actor_id=x_actor_id,
            actor_role=x_actor_role,
            notes=notes
        )
    except approval_service.ApprovalSecurityError as sec_err:
        raise HTTPException(status_code=403, detail=str(sec_err))
    except approval_service.ApprovalConflictError as conf_err:
        raise HTTPException(status_code=409, detail=str(conf_err))
    except approval_service.ApprovalValidationError as val_err:
        raise HTTPException(status_code=400, detail=str(val_err))
    except Exception as e:
        logger.error(f"Error approving replenishment {approval_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to approve replenishment: {str(e)}")


@app.post("/api/observability/replenishment-approvals/{approval_id}/reject")
def reject_replenishment_endpoint(
    approval_id: str,
    req: ApprovalRejectionRequest,
    x_actor_id: str = Header(..., alias="X-Actor-ID", description="Rejecting manager identifier"),
    x_actor_role: str = Header(..., alias="X-Actor-Role", description="Managerial role: MANAGER or ADMIN")
):
    """
    Rejects a pending replenishment request with a mandatory documented reason.
    Prevents rejected requests from being approved or dispatched.
    """
    try:
        from . import approval_service
        return approval_service.reject_replenishment(
            approval_id=approval_id,
            actor_id=x_actor_id,
            actor_role=x_actor_role,
            reason=req.reason
        )
    except approval_service.ApprovalSecurityError as sec_err:
        raise HTTPException(status_code=403, detail=str(sec_err))
    except approval_service.ApprovalValidationError as val_err:
        raise HTTPException(status_code=400, detail=str(val_err))
    except Exception as e:
        logger.error(f"Error rejecting replenishment {approval_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to reject replenishment: {str(e)}")


# =============================================================================
# --- Phase 2: Simulator API Endpoints ---
# =============================================================================
#
# All endpoints interact with the module-level SimulationEngine singleton.
# No operational data is ever mutated by these endpoints.
# =============================================================================

from services.simulator.simulation_engine import (
    get_engine,
    SimEngineState,
    MODE_CONTINUOUS,
    MIN_INTERVAL_SECONDS,
    MAX_INTERVAL_SECONDS,
)
from services.simulator.scenarios import list_scenarios, SCENARIO_REGISTRY


class SimulatorStartRequest(BaseModel):
    """Request body for POST /api/simulator/start."""
    mode: str = Field(
        default=MODE_CONTINUOUS,
        description=(
            "Simulation mode: 'CONTINUOUS' to rotate through all scenarios, "
            "or a specific scenario_id (e.g. 'S1_ALL', 'S2_ANY', 'S3_QUORUM', 'S4_AST')."
        ),
    )
    event_interval_seconds: float = Field(
        default=7.0,
        ge=MIN_INTERVAL_SECONDS,
        le=MAX_INTERVAL_SECONDS,
        description=f"Event generation interval in seconds ({MIN_INTERVAL_SECONDS}–{MAX_INTERVAL_SECONDS}).",
    )


@app.post("/api/simulator/start", status_code=status.HTTP_200_OK)
def simulator_start(req: SimulatorStartRequest):
    """
    Starts the simulation engine.

    Modes:
      - 'CONTINUOUS': rotates through S1_ALL → S2_ANY → S3_QUORUM → S4_AST indefinitely.
      - Specific scenario_id (e.g. 'S1_ALL'): runs one full pass of that scenario, then stops.

    Returns 409 if the engine is already running or paused.
    Returns 400 for invalid mode or interval.

    Safety: Never triggers Airflow, ML training, operational inventory changes,
    replenishment orders, or modifications to historical Olist data.
    """
    engine = get_engine()
    try:
        result = engine.start(
            mode=req.mode,
            event_interval_seconds=req.event_interval_seconds,
        )
        try:
            from services.observability.publisher import publish_dashboard_event
            publish_dashboard_event(
                event_type="SIMULATION_STATUS_CHANGED",
                entity="SIMULATOR",
                severity="INFO",
                data={
                    "action": "STARTED",
                    "simulation_id": result.get("simulation_id"),
                    "mode": result.get("mode"),
                    "event_interval_seconds": result.get("event_interval_seconds"),
                },
            )
        except Exception:
            pass
        return result
    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))
    except RuntimeError as re:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(re))
    except Exception as exc:
        logger.error(f"Simulator start error: {exc}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start simulator: {str(exc)}",
        )


@app.post("/api/simulator/pause", status_code=status.HTTP_200_OK)
def simulator_pause():
    """
    Pauses the simulation engine.
    No more Kafka events will be emitted until resume() is called.
    Returns 409 if the engine is not currently RUNNING.
    """
    engine = get_engine()
    try:
        result = engine.pause()
        try:
            from services.observability.publisher import publish_dashboard_event
            publish_dashboard_event(
                event_type="SIMULATION_STATUS_CHANGED",
                entity="SIMULATOR",
                severity="INFO",
                data={"action": "PAUSED", "simulation_id": result.get("simulation_id")},
            )
        except Exception:
            pass
        return result
    except RuntimeError as re:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(re))
    except Exception as exc:
        logger.error(f"Simulator pause error: {exc}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to pause simulator: {str(exc)}",
        )


@app.post("/api/simulator/resume", status_code=status.HTTP_200_OK)
def simulator_resume():
    """
    Resumes the simulation engine after a pause.
    Returns 409 if the engine is not currently PAUSED.
    """
    engine = get_engine()
    try:
        result = engine.resume()
        try:
            from services.observability.publisher import publish_dashboard_event
            publish_dashboard_event(
                event_type="SIMULATION_STATUS_CHANGED",
                entity="SIMULATOR",
                severity="INFO",
                data={"action": "RESUMED", "simulation_id": result.get("simulation_id")},
            )
        except Exception:
            pass
        return result
    except RuntimeError as re:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(re))
    except Exception as exc:
        logger.error(f"Simulator resume error: {exc}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to resume simulator: {str(exc)}",
        )


@app.post("/api/simulator/stop", status_code=status.HTTP_200_OK)
def simulator_stop():
    """
    Gracefully stops the simulation engine and resets state to IDLE.
    Idempotent: safe to call when engine is already IDLE.
    Also resets all simulated dataset statuses back to 'WAITING'.
    """
    engine = get_engine()
    try:
        result = engine.stop()

        # Reset simulated dataset statuses back to WAITING after stop
        try:
            from common.database import get_connection
            conn = get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE dataset_metadata SET status = 'WAITING' "
                    "WHERE source_type = 'SIMULATED'"
                )
                rows_reset = cur.rowcount
            conn.commit()
            conn.close()
            logger.info(f"Simulator stop: reset {rows_reset} simulated dataset(s) to WAITING.")
            result["datasets_reset"] = rows_reset
        except Exception as db_err:
            logger.warning(f"Simulator stop: could not reset simulated dataset statuses: {db_err}")

        try:
            from services.observability.publisher import publish_dashboard_event
            publish_dashboard_event(
                event_type="SIMULATION_STATUS_CHANGED",
                entity="SIMULATOR",
                severity="INFO",
                data={"action": "STOPPED", "simulation_id": result.get("simulation_id")},
            )
        except Exception:
            pass
        return result
    except Exception as exc:
        logger.error(f"Simulator stop error: {exc}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to stop simulator: {str(exc)}",
        )


@app.get("/api/simulator/status", status_code=status.HTTP_200_OK)
def simulator_status():
    """
    Returns the current simulation engine state snapshot:
      - state: IDLE | RUNNING | PAUSED | STOPPING
      - simulation_id: UUID of the active simulation run (or null).
      - mode: active mode ('CONTINUOUS' or scenario_id).
      - active_scenario_id: currently executing scenario.
      - emitted_event_count: total successfully published events.
      - last_event_timestamp: ISO-8601 timestamp of the last successful event.
      - started_at: ISO-8601 timestamp when the engine was started.
      - event_interval_seconds: configured inter-event delay.
      - error_message: last worker crash message (if any).
    """
    return get_engine().get_status()


@app.get("/api/simulator/scenarios", status_code=status.HTTP_200_OK)
def simulator_scenarios():
    """
    Returns the full catalogue of available simulation scenarios:
      - S1_ALL:   ALL strategy — SIM-ALL-Pipeline (3 datasets, expects BLOCK, BLOCK, TRIGGER).
      - S2_ANY:   ANY strategy — SIM-ANY-Pipeline (2 datasets, expects TRIGGER on step 1).
      - S3_QUORUM: QUORUM strategy — SIM-QUORUM-Pipeline (4 datasets, threshold 3, BLOCK, BLOCK, TRIGGER).
      - S4_AST:   AST strategy — SIM-AST-Pipeline (2 datasets, BLOCK then TRIGGER).
    """
    return {
        "scenarios": list_scenarios(),
        "continuous_rotation_order": [s.scenario_id for s in
            __import__('services.simulator.scenarios', fromlist=['SCENARIO_ROTATION_ORDER']).SCENARIO_ROTATION_ORDER
        ],
    }


# =============================================================================
# --- Phase F3: True End-to-End Simulation Endpoints ---
# =============================================================================

from services.observability import sim_repository


class E2ESimulatorRunRequest(BaseModel):
    batch_size: int = Field(default=200, ge=50, le=1000, description="Synthetic orders count (50-1000)")


@app.post("/api/simulator/run-e2e", status_code=status.HTTP_200_OK)
def simulator_run_e2e(req: E2ESimulatorRunRequest):
    """
    Executes a complete, realistic, end-to-end data simulation workflow:
      1. Generates 50-1000 synthetic orders in isolated sim_orders table.
      2. Generates inventory snapshots with low-stock alerts in sim_inventory_snapshots.
      3. Sets SIM_Orders_Feed & SIM_Inventory_Snapshot status to READY in dataset_metadata.
      4. Evaluates dependencies and triggers SIM-FORECAST-Pipeline & SIM-REORDER-Pipeline.
      5. Executes SIM-FORECAST-Pipeline (XGBoost inference) -> writes to sim_forecast_results.
      6. Executes SIM-REORDER-Pipeline -> writes to sim_replenishment_orders.
      7. Emits real-time SSE dashboard events for all lifecycle transitions.
      8. Prunes older simulation runs (retaining latest 10 runs).
    """
    try:
        return sim_repository.execute_e2e_simulation(batch_size=req.batch_size)
    except Exception as exc:
        logger.error(f"E2E simulation error: {exc}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to execute E2E simulation: {str(exc)}"
        )


@app.get("/api/simulator/runs", status_code=status.HTTP_200_OK)
def get_simulation_runs(limit: int = Query(20, ge=1, le=100)):
    """Returns history of simulation runs."""
    try:
        return sim_repository.get_sim_runs(limit=limit)
    except Exception as exc:
        logger.error(f"Error fetching simulation runs: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/simulator/runs/{run_id}", status_code=status.HTTP_200_OK)
def get_simulation_run_detail(run_id: str):
    """Returns details for a specific simulation run."""
    try:
        run = sim_repository.get_sim_run_detail(simulation_run_id=run_id)
        if not run:
            raise HTTPException(status_code=404, detail=f"Simulation run '{run_id}' not found")
        return run
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Error fetching simulation run '{run_id}': {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/observability/sim-forecasts", status_code=status.HTTP_200_OK)
def get_sim_forecasts(
    run_id: Optional[str] = Query(None, description="Filter by simulation_run_id"),
    limit: int = Query(100, ge=1, le=500)
):
    """Returns simulated forecast predictions."""
    try:
        return sim_repository.get_sim_forecasts(simulation_run_id=run_id, limit=limit)
    except Exception as exc:
        logger.error(f"Error fetching sim forecasts: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/observability/sim-forecasts/{execution_id}/category-trends", status_code=status.HTTP_200_OK)
def get_sim_category_trends(
    execution_id: int,
    categories: Optional[str] = Query(None, description="Comma-separated categories"),
    limit: int = Query(6, ge=1, le=20)
):
    """Returns category forecast trends formatted for the Recharts frontend chart."""
    try:
        cat_list = [c.strip() for c in categories.split(",") if c.strip()] if categories else None
        trends = sim_repository.get_sim_forecast_category_trends(
            execution_id=execution_id,
            categories=cat_list,
            limit=limit
        )
        if not trends:
            raise HTTPException(status_code=404, detail="Simulated forecast trends not found")
        return trends
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Error fetching sim category trends: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/observability/sim-replenishments", status_code=status.HTTP_200_OK)
def get_sim_replenishments(
    run_id: Optional[str] = Query(None, description="Filter by simulation_run_id"),
    limit: int = Query(50, ge=1, le=200)
):
    """Returns simulated replenishment orders."""
    try:
        return sim_repository.get_sim_replenishments(simulation_run_id=run_id, limit=limit)
    except Exception as exc:
        logger.error(f"Error fetching sim replenishments: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/observability/sim-inventory", status_code=status.HTTP_200_OK)
def get_sim_inventory(
    run_id: Optional[str] = Query(None, description="Filter by simulation_run_id"),
    limit: int = Query(50, ge=1, le=200)
):
    """Returns simulated inventory snapshots."""
    try:
        return sim_repository.get_sim_inventory(simulation_run_id=run_id, limit=limit)
    except Exception as exc:
        logger.error(f"Error fetching sim inventory: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


# ==============================================================================
# Operational Order Ingestion & Observability (Phase 1)
# ==============================================================================

from fastapi import Response


class OperationalOrderItem(BaseModel):
    order_id: Optional[str] = Field(None, description="Unique client or order ID")
    sku: str = Field(..., description="Product SKU or product_id")
    warehouse_id: Optional[str] = Field(None, description="Warehouse ID (default: WH-SP-01)")
    quantity: int = Field(..., gt=0, description="Order item quantity (must be positive integer)")
    unit_price: Optional[float] = Field(0.00, ge=0.0, description="Unit price")
    event_time: Optional[str] = Field(None, description="ISO timestamp of order event")
    idempotency_key: Optional[str] = Field(None, description="Optional item-level idempotency key")


class OperationalOrderBatchRequest(BaseModel):
    idempotency_key: Optional[str] = Field(None, description="Unique batch idempotency key")
    source: Optional[str] = Field("OPERATIONAL_API", description="Order source: OPERATIONAL_API, SIMULATED, SQUARE_SANDBOX, MANUAL_IMPORT")
    warehouse_id: Optional[str] = Field("WH-SP-01", description="Default warehouse for the batch")
    orders: Optional[list[OperationalOrderItem]] = Field(None, description="List of order items")
    order_id: Optional[str] = Field(None, description="Single order: client or order ID")
    sku: Optional[str] = Field(None, description="Single order: product SKU")
    quantity: Optional[int] = Field(None, description="Single order: item quantity")
    unit_price: Optional[float] = Field(None, description="Single order: unit price")
    event_time: Optional[str] = Field(None, description="Single order: ISO timestamp")


@app.post("/api/observability/orders")
def ingest_operational_orders_endpoint(req: OperationalOrderBatchRequest, response: Response):
    """
    Ingests operational orders into MySQL with batch idempotency and source provenance.
    Emits dataset event for CDC/dependency engine tracking and dashboard SSE feeds.
    Returns 201 Created for new batches, or 200 OK for idempotent replays.
    """
    order_items = []
    if req.orders and len(req.orders) > 0:
        for it in req.orders:
            order_items.append({
                "order_id": it.order_id,
                "sku": it.sku,
                "warehouse_id": it.warehouse_id or req.warehouse_id,
                "quantity": it.quantity,
                "unit_price": it.unit_price or 0.0,
                "event_time": it.event_time,
                "idempotency_key": it.idempotency_key
            })
    elif req.sku and req.quantity is not None:
        order_items.append({
            "order_id": req.order_id,
            "sku": req.sku,
            "warehouse_id": req.warehouse_id,
            "quantity": req.quantity,
            "unit_price": req.unit_price or 0.0,
            "event_time": req.event_time,
            "idempotency_key": req.idempotency_key
        })
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Request must provide either non-empty 'orders' list or 'sku' and 'quantity'."
        )

    try:
        result = service.ingest_operational_orders(
            orders=order_items,
            idempotency_key=req.idempotency_key,
            source=req.source or "OPERATIONAL_API",
            warehouse_id=req.warehouse_id or "WH-SP-01"
        )
        if result.get("idempotent_replay"):
            response.status_code = status.HTTP_200_OK
        else:
            response.status_code = status.HTTP_201_CREATED
        return result
    except ValueError as val_err:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(val_err))
    except Exception as exc:
        logger.error(f"Error ingesting operational orders: {exc}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to ingest orders: {str(exc)}")


@app.get("/api/observability/orders", status_code=status.HTTP_200_OK)
def list_operational_orders(
    source: Optional[str] = Query(None, description="Filter by source (OPERATIONAL_API, SIMULATED, etc.)"),
    order_status: Optional[str] = Query(None, description="Filter by order_status (PENDING, ALLOCATED, etc.)"),
    limit: int = Query(50, ge=1, le=200, description="Page limit"),
    offset: int = Query(0, ge=0, description="Page offset")
):
    """Retrieves paginated operational orders."""
    try:
        return service.get_operational_orders(
            limit=limit,
            offset=offset,
            source=source,
            order_status=order_status
        )
    except Exception as exc:
        logger.error(f"Error listing operational orders: {exc}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@app.get("/api/observability/orders/{order_id}", status_code=status.HTTP_200_OK)
def get_operational_order(order_id: str):
    """Retrieves single operational order by order_id."""
    try:
        order = service.get_operational_order_by_id(order_id=order_id)
        if not order:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Order '{order_id}' not found")
        return order
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Error retrieving operational order {order_id}: {exc}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@app.get("/api/observability/operational-forecasts", status_code=status.HTTP_200_OK)
def list_operational_forecasts(
    execution_id: Optional[int] = Query(None, description="Filter by execution_id"),
    category: Optional[str] = Query(None, description="Filter by product_category"),
    horizon_days: Optional[int] = Query(None, description="Filter by horizon (7 or 14)"),
    limit: int = Query(50, ge=1, le=200, description="Page limit"),
    offset: int = Query(0, ge=0, description="Page offset")
):
    """
    Returns forward demand forecasts from operational_forecast_results with
    prediction eligibility, cold-start fallback indicators, and catalog translation.
    """
    try:
        return service.get_operational_forecasts(
            execution_id=execution_id,
            category=category,
            horizon_days=horizon_days,
            limit=limit,
            offset=offset
        )
    except Exception as exc:
        logger.error(f"Error fetching operational forecasts: {exc}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


# =============================================================================
# --- Olist Historical Order Replay Simulator Endpoints ---
# =============================================================================

from services.simulator.olist_replay import replay_simulator


class OlistReplayStartRequest(BaseModel):
    """Request model for starting Olist historical order replay simulation."""
    batch_size: int = Field(default=25, ge=1, le=500, description="Orders per batch (default 25)")
    interval_seconds: float = Field(default=5.0, ge=0.5, le=300.0, description="Interval between batches in seconds (default 5.0)")
    window_start: str = Field(default="2018-W36", description="Window start ISO week (default 2018-W36)")
    window_end: str = Field(default="2018-W42", description="Window end ISO week (default 2018-W42)")
    auto_fallback: bool = Field(default=True, description="Fallback to active weeks if target window has fewer than batch_size orders")
    max_batches: Optional[int] = Field(default=None, ge=1, description="Maximum batches to run (optional)")


@app.post("/api/simulator/replay/start", status_code=status.HTTP_200_OK)
def olist_replay_start(req: OlistReplayStartRequest = OlistReplayStartRequest()):
    """Starts the historical Olist order replay simulation."""
    try:
        return replay_simulator.start(
            batch_size=req.batch_size,
            interval_seconds=req.interval_seconds,
            window_start=req.window_start,
            window_end=req.window_end,
            auto_fallback=req.auto_fallback,
            max_batches=req.max_batches
        )
    except Exception as exc:
        logger.error(f"Error starting Olist replay simulator: {exc}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


@app.post("/api/simulator/replay/pause", status_code=status.HTTP_200_OK)
def olist_replay_pause():
    """Pauses the active Olist order replay simulation."""
    try:
        return replay_simulator.pause()
    except Exception as exc:
        logger.error(f"Error pausing Olist replay simulator: {exc}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@app.post("/api/simulator/replay/resume", status_code=status.HTTP_200_OK)
def olist_replay_resume():
    """Resumes the paused Olist order replay simulation."""
    try:
        return replay_simulator.resume()
    except Exception as exc:
        logger.error(f"Error resuming Olist replay simulator: {exc}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@app.post("/api/simulator/replay/stop", status_code=status.HTTP_200_OK)
def olist_replay_stop():
    """Stops the active Olist order replay simulation."""
    try:
        return replay_simulator.stop()
    except Exception as exc:
        logger.error(f"Error stopping Olist replay simulator: {exc}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@app.get("/api/simulator/replay/status", status_code=status.HTTP_200_OK)
def olist_replay_status():
    """Returns current telemetry, batch progress, and order counts for Olist replay."""
    try:
        return replay_simulator.get_status()
    except Exception as exc:
        logger.error(f"Error getting Olist replay status: {exc}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@app.get("/api/simulator/replay/preview", status_code=status.HTTP_200_OK)
def olist_replay_preview(
    window_start: str = Query("2018-W36", description="Window start ISO week"),
    window_end: str = Query("2018-W42", description="Window end ISO week"),
    auto_fallback: bool = Query(True, description="Expand to active weeks if target has fewer than batch_size orders"),
    batch_size: int = Query(25, ge=1, le=500, description="Orders per batch")
):
    """Previews Olist dataset filtering and batch counts without starting execution."""
    try:
        return replay_simulator.preview(
            window_start=window_start,
            window_end=window_end,
            auto_fallback=auto_fallback,
            batch_size=batch_size
        )
    except Exception as exc:
        logger.error(f"Error previewing Olist replay: {exc}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
