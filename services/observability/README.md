# Observability & Monitoring Service

## Overview

The **Observability & Monitoring Service** (`services/observability`) is a lightweight, read-only operational intelligence layer for SupplySense AI.

It aggregates telemetry across the end-to-end event-driven architecture:
- **Pipeline Execution Tracking**: Success/failure rates, average durations, active runs, retry backoff, and failure classifications.
- **Dataset Synchronization**: CDC and webhook version tracking, row counts, and latest batch identifiers.
- **Dependency Evaluations**: Condition rules (`ALL`, `ANY`, `QUORUM`), ready counts, and trigger/block decision history.
- **Machine Learning Forecasts**: Prediction counts, category coverage, forecast horizons, and model metadata.
- **Execution Artifacts**: Scoped model comparison reports and artifact audit trails.

---

## Service Architecture

```
services/observability/
├── __init__.py      # Package exports
├── repository.py    # Read-only SQL queries and aggregations
├── service.py       # Duration/rate computations and business logic
├── main.py          # FastAPI application & REST endpoints
└── README.md        # Service documentation
```

---

## API Endpoints

All endpoints are strictly **READ-ONLY**:

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Service liveness & database connectivity check |
| `GET` | `/api/observability/summary` | Aggregate operational summary |
| `GET` | `/api/observability/executions` | Filtered execution list with duration & retry status |
| `GET` | `/api/observability/executions/{id}` | Detailed execution telemetry, decision & artifacts |
| `GET` | `/api/observability/datasets` | Real-time dataset synchronization states |
| `GET` | `/api/observability/dependencies` | Pipeline dependency definitions & latest decisions |
| `GET` | `/api/observability/forecasts` | ML forecast execution summaries |

---

## Running the Service

```bash
uvicorn services.observability.main:app --host 0.0.0.0 --port 8000 --reload
```
