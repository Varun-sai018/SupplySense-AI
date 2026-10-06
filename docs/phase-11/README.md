# Phase 11: Pipeline Execution Observability & Monitoring

## 1. Overview & Problem Statement

SupplySense AI encompasses an event-driven supply chain automation architecture:
- **Phase 1–3**: Feature engineering and XGBoost demand forecasting.
- **Phase 4–6**: Dependency evaluation engine, pipeline runner, and forecast persistence.
- **Phase 7–8**: Square webhooks, Debezium CDC ingestion, stream deduplication, micro-batching, and stale execution reconciliation.
- **Phase 9**: Automated pipeline retries and failure recovery with exponential backoff.
- **Phase 10**: Execution-scoped artifact directories and atomic result isolation.

### The Operational Blindspot
Prior to Phase 11, operational telemetry was scattered across multiple database tables (`pipeline_executions`, `pipeline_decisions`, `dataset_events`, `dataset_metadata`, `forecast_results`) and application logs. Administrators had to manually construct complex SQL queries to answer critical questions:
- What executions are currently running or failing?
- How many retries occurred, and which error classes triggered them?
- What are the average execution latencies?
- Which datasets are waiting vs ready?
- Where are the execution-scoped model comparison artifacts located?

**Phase 11** introduces a unified, **read-only Observability & Monitoring Service** (`services/observability`) exposing structured telemetry endpoints over the entire pipeline lifecycle.

---

## 2. Architecture & Service Design

```
                               ┌────────────────────────────────────────────────┐
                               │   FastAPI Observability Service (Port 8000)    │
                               │          (services/observability)              │
                               └──────────────────────┬─────────────────────────┘
                                                      │
                       ┌──────────────────────────────┼──────────────────────────────┐
                       ▼                              ▼                              ▼
                 [Repository]                    [Service]                    [REST Router]
              (Optimized SQL &               (Duration & Rate               (Read-Only JSON
                Aggregations)                  Computations)                   Endpoints)
                       │                              │                              │
                       └──────────────────────────────┼──────────────────────────────┘
                                                      │
                       ┌──────────────────────────────┴──────────────────────────────┐
                       ▼                              ▼                              ▼
              [MySQL Database]               [Execution Artifacts]            [Health Checks]
            (executions, events,           (ml/results/executions/           (Live DB ping &
             decisions, forecasts)            <execution_id>/)               readiness probe)
```

### Key Architectural Principles
1. **Strictly Read-Only**: The service cannot trigger pipelines, alter dataset status, or manipulate retry queues.
2. **Server-Side SQL Aggregations**: Uses `COUNT`, `SUM(CASE ...)`, `AVG`, `MIN`, `MAX`, and `GROUP BY` to compute summary statistics in single round-trips without pulling unindexed raw tables into memory.
3. **Execution Artifact Scoping**: Audits and reports artifact existence directly in `ml/results/executions/<execution_id>/`.
4. **Zero External Dependencies**: Operates natively on existing Python and MySQL infrastructure without heavy dependencies (e.g., Grafana, Prometheus, Redis, or Celery).

---

## 3. API Endpoints Reference

All endpoints are hosted under `http://localhost:8000`:

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Liveness check and MySQL connectivity validation |
| `GET` | `/api/observability/summary` | Top-level execution counts, rates, durations, and dataset states |
| `GET` | `/api/observability/executions` | Filtered executions list (`status`, `pipeline_name`, `limit`) |
| `GET` | `/api/observability/executions/{id}` | Detailed execution telemetry, decision context, and artifacts |
| `GET` | `/api/observability/datasets` | Real-time dataset synchronization states and latest CDC events |
| `GET` | `/api/observability/dependencies` | Pipeline dependency rules and latest evaluated decisions |
| `GET` | `/api/observability/forecasts` | ML forecast execution summaries grouped by execution |

---

## 4. Response Examples

### 1. Operational Summary (`GET /api/observability/summary`)
```json
{
  "executions": {
    "total": 42,
    "running": 1,
    "completed": 38,
    "failed": 3,
    "retrying": 1,
    "retried_total": 5
  },
  "success_rate": 90.48,
  "failure_rate": 7.14,
  "average_duration_seconds": 14.82,
  "latest_execution": {
    "execution_id": 10028,
    "pipeline_name": "Demand Forecast Pipeline",
    "status": "COMPLETED",
    "started_at": "2026-10-06T10:06:21",
    "completed_at": "2026-10-06T10:06:35",
    "duration_seconds": 14.0,
    "output_location": "ml/results/executions/10028/model_comparison.json"
  },
  "datasets": {
    "total": 4,
    "ready": 3,
    "waiting": 1
  },
  "latest_forecast": {
    "execution_id": 10028,
    "pipeline_name": "Demand Forecast Pipeline",
    "model_name": "XGBoost",
    "model_version": "xgboost-v1",
    "row_count": 770,
    "category_count": 73,
    "min_forecast_week": "2018-02-19",
    "max_forecast_week": "2018-04-30",
    "avg_predicted_demand": 14.285,
    "created_at": "2026-10-06T10:06:35",
    "output_location": "ml/results/executions/10028/model_comparison.json"
  }
}
```

### 2. Execution Detail (`GET /api/observability/executions/{execution_id}`)
```json
{
  "execution_id": 10028,
  "pipeline_name": "Demand Forecast Pipeline",
  "decision_id": 150,
  "triggering_event_id": 999903,
  "status": "COMPLETED",
  "retry_count": 1,
  "max_retries": 3,
  "started_at": "2026-10-06T10:06:21",
  "completed_at": "2026-10-06T10:06:35",
  "duration_seconds": 14.0,
  "next_retry_at": null,
  "retry_error_type": null,
  "output_location": "ml/results/executions/10028/model_comparison.json",
  "error_message": null,
  "decision": {
    "decision_id": 150,
    "condition_type": "ALL",
    "decision": "TRIGGER",
    "ready_count": 3,
    "total_required": 3,
    "reason": "All required datasets are READY.",
    "triggered_at": "2026-10-06T10:06:20"
  },
  "forecast": {
    "row_count": 770,
    "category_count": 73,
    "model_name": "XGBoost",
    "model_version": "xgboost-v1",
    "min_forecast_week": "2018-02-19",
    "max_forecast_week": "2018-04-30",
    "avg_predicted_demand": 14.285
  },
  "artifacts": {
    "output_location": "ml/results/executions/10028/model_comparison.json",
    "exists": true,
    "scoped_files": [
      "baseline_metrics.csv",
      "baseline_metrics.json",
      "baseline_test_predictions.csv",
      "baseline_validation_predictions.csv",
      "model_comparison.json",
      "xgboost_demand_model.json",
      "xgboost_metrics.json",
      "xgboost_test_predictions.csv",
      "xgboost_validation_predictions.csv"
    ]
  }
}
```

---

## 5. Progression Across Architecture Phases

- **Phase 9 (Automated Retry)**: Added self-healing capability (`retry_count`, `max_retries`, `next_retry_at`, exponential backoff).
- **Phase 10 (Artifact Isolation)**: Scoped predictions, models, and comparisons to `ml/results/executions/<execution_id>/`.
- **Phase 11 (Observability)**: Glues the whole system together into a queryable, read-only operational telemetry plane.

---

## 6. Verification & Testing

### Unit Tests
[`tests/unit/test_observability.py`](file:///c:/Users/varun%20sai/PycharmProjects/PythonProject/capstone/tests/unit/test_observability.py):
- Verifies rate calculations, duration computations, dataset formatting, dependency condition rendering, and missing execution handling.

### Integration Tests
[`tests/integration/test_phase11_observability.py`](file:///c:/Users/varun%20sai/PycharmProjects/PythonProject/capstone/tests/integration/test_phase11_observability.py):
- Validates live SQL aggregations, parameter filtering, 404 responses, and strict read-only guarantees against MySQL.

---

## 7. Running the Demo

Execute the interactive verification script:

```bash
python scripts/demo_phase11_observability.py
```
