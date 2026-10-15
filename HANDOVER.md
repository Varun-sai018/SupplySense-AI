# SupplySense AI — Capstone Handover Documentation

> **Project:** SupplySense AI
> **Repository:** `C:\Users\varun sai\PycharmProjects\PythonProject\capstone`
> **Branch:** `main`
> **Release Readiness Checkpoint:** `e2096b6` (Phase D3 Response Hardening)
> **Documentation Version:** Capstone Final Handover (Phase E2)
> **Status:** All Phases Completed and Verified (Phases 1–12, A, B, C, D1–D4, E1–E2)

---

## 1. Executive Summary

**SupplySense AI** is an event-conditioned data pipeline orchestration platform designed for supply-chain demand forecasting and automated pipeline reliability. In dynamic logistics and retail supply chains, batch data often arrives out-of-order, across disparate upstream sources (e.g., e-commerce order databases, point-of-sale systems, warehouse stock updates). Traditional scheduled batch jobs (cron) either execute prematurely on incomplete data or delay unnecessarily.

SupplySense AI solves this through:
1. **Event-Conditioned Dependency Evaluation:** A dedicated Dependency Engine continuously listens to dataset events published to Apache Kafka (ingested via Square webhooks or Debezium CDC) and evaluates satisfaction rules (`ALL`, `ANY`, `QUORUM`) before triggering downstream pipelines.
2. **Dual-Mode Orchestration Architecture:**
   - **`LOCAL` Mode:** In-process Python handler execution for development and lightweight execution.
   - **`AIRFLOW` Mode:** Enterprise-grade production dispatch via the Apache Airflow REST API to a production-grade Directed Acyclic Graph (`supplysense_demand_forecast`), complete with task-level isolation, linear progression, and artifact generation.
3. **Automated Self-Healing & Fault Tolerance:** An autonomous background Reaper process reconciles stale/hung pipeline executions, and an automated Retry Engine resubmits failed executions using exponential backoff with jitter while maintaining strict idempotency.
4. **End-to-End Live Verification:** Verified on live infrastructure with execution #36 dispatched through Apache Airflow, generating 770 persisted forecast predictions in MySQL and 9 execution-scoped filesystem artifacts.
5. **Quality Assurance:** A robust test suite of **220 tests** (170 unit, 50 integration across 14 modules) passing with zero failures.

---

## 2. End-to-End Architecture Overview

```mermaid
flowchart TD
    subgraph Ingestion["1. Ingestion Layer"]
        Square["Square POS / Webhooks
(Real-time API)"]
        Debezium["Debezium CDC Adapter
(MySQL Binlog Connector)"]
        Kafka["Apache Kafka (port 9092)
Topic: dataset-events"]
        Square -->|POST /webhooks/square| Kafka
        Debezium -->|CDC Micro-batching| Kafka
    end

    subgraph Dependency["2. Dependency & Orchestration Engine"]
        Engine["Dependency Engine
(Consumer Group: dependency-engine)"]
        Evaluator["Dependency Evaluator
(ALL / ANY / QUORUM)"]
        Runner["Pipeline Runner"]
        Idempotency["Idempotency Filter
(triggering_event_id)"]

        Kafka --> Engine
        Engine --> Evaluator
        Evaluator -->|Rule Satisfied| Runner
        Runner --> Idempotency
    end

    subgraph Execution["3. Execution Layer (Dual Mode)"]
        Dispatch{"PIPELINE_ORCHESTRATOR"}
        LocalHandler["LOCAL Mode
In-Process ML Handler"]
        AirflowAPI["AIRFLOW Mode
Airflow REST API (port 8080/18080)
Deterministic Run ID"]

        Idempotency --> Dispatch
        Dispatch -->|LOCAL| LocalHandler
        Dispatch -->|AIRFLOW| AirflowAPI
    end

    subgraph DAG["4. Airflow Workflow (supplysense_demand_forecast)"]
        T1["1. validate_input_datasets"]
        T2["2. run_naive_baseline_benchmark"]
        T3["3. engineer_leak_free_features"]
        T4["4. train_xgboost_demand_model"]
        T5["5. persist_forecast_predictions"]
        T6["6. finalize_execution_telemetry"]

        AirflowAPI --> T1 --> T2 --> T3 --> T4 --> T5 --> T6
    end

    subgraph Storage["5. Persistence & Artifacts"]
        MySQL[("MySQL 8.0 (port 3306)
supplysense DB")]
        Artifacts["Filesystem Storage
ml/results/executions/<id>/"]

        T1 & T2 & T3 & T4 --> Artifacts
        T5 -->|770 rows| MySQL
        T6 -->|COMPLETED status| MySQL
        LocalHandler --> MySQL
        LocalHandler --> Artifacts
    end

    subgraph SelfHealing["6. Reliability & Self-Healing"]
        Reaper["Stale Execution Reaper
(Timeout Reconciler)"]
        RetryEngine["Retry Service
(Exponential Backoff + Jitter)"]

        Reaper -->|Checks timeout & Airflow status| MySQL
        RetryEngine -->|Re-dispatches failed runs| Runner
    end

    subgraph Observability["7. Observability & User Interface"]
        ObsAPI["FastAPI Observability API (port 8000)
Swagger /docs"]
        Dashboard["React + Vite Dashboard (port 5173)
Pipeline & Forecast Explorer"]

        MySQL --> ObsAPI
        Artifacts --> ObsAPI
        ObsAPI --> Dashboard
    end
```

---

## 3. Service Inventory & Topology

SupplySense AI operates as a hybrid topology combining Docker Compose containerized middleware with native host services on Windows:

| Service | Component Type | Container Name | Host Port | Internal Port | Healthcheck / Probe URL |
|---|---|---|---|---|---|
| **Observability API** | Python / FastAPI | Host Process (`uvicorn`) | `8000` | `8000` | `http://127.0.0.1:8000/health` |
| **Observability Docs** | OpenAPI / Swagger | Host Process | `8000` | `8000` | `http://127.0.0.1:8000/docs` |
| **React Dashboard** | React / Vite | Host Process (`npm run dev`) | `5173` | `5173` | `http://127.0.0.1:5173/` |
| **Airflow Webserver** | Apache Airflow 2.8.1 | `supplysense-airflow-webserver` | `8080` (or `18080`)* | `8080` | `http://localhost:8080/health` |
| **Airflow Scheduler** | Apache Airflow 2.8.1 | `supplysense-airflow-scheduler` | — | — | Container health check |
| **Airflow Postgres** | PostgreSQL 13 | `supplysense-postgres` | `5432` | `5432` | `pg_isready -U airflow` |
| **MySQL Database** | MySQL 8.0 | `supplysense-mysql` | `3306` | `3306` | Native client / TCP probe |
| **Kafka Broker** | Apache Kafka (KRaft) | `supplysense-kafka` | `9092` | `9092` | Broker TCP connection |
| **Debezium Connect** | Debezium Connect 2.4 | `supplysense-debezium` | `8083` | `8083` | `http://localhost:8083/connectors` |

> [!NOTE]
> **Windows Host Port 8080 Conflict Handling:**
> On Windows systems, host port `8080` is frequently claimed by local application servers (e.g. `java.exe` / Apache Tomcat). To resolve this without container modifications, `docker-compose.yml` can map `18080:8080` for `supplysense-airflow-webserver`, and `.env` should be configured with `AIRFLOW_BASE_URL=http://localhost:18080`.

---

## 4. Environment Configuration Reference

All application configuration is driven by environment variables loaded through [`config/settings.py`](file:///C:/Users/varun%20sai/PycharmProjects/PythonProject/capstone/config/settings.py) and defined in [`.env.example`](file:///C:/Users/varun%20sai/PycharmProjects/PythonProject/capstone/.env.example).

### Database Configuration
| Variable | Type | Default | Description |
|---|---|---|---|
| `DB_HOST` | string | `localhost` | MySQL hostname (use `host.docker.internal` from inside containers) |
| `DB_PORT` | integer | `3306` | MySQL connection port |
| `DB_USER` | string | `root` | MySQL username (development default) |
| `DB_PASSWORD` | string | `root` | MySQL password (development default) |
| `DB_NAME` | string | `supplysense` | Canonical application database name |

### Kafka & Ingestion Configuration
| Variable | Type | Default | Description |
|---|---|---|---|
| `KAFKA_BROKER` | string | `localhost:9092` | Kafka broker bootstrap address |
| `KAFKA_TOPIC_EVENTS` | string | `dataset-events` | Kafka topic for dataset arrival events |
| `KAFKA_CONSUMER_GROUP` | string | `dependency-engine` | Kafka consumer group identifier |
| `OLIST_DATA_DIR` | string | `./data/raw_csv` | Local directory containing raw Olist CSV datasets |

### Square Sandbox Configuration
| Variable | Type | Default | Description |
|---|---|---|---|
| `SQUARE_ENVIRONMENT` | string | `sandbox` | Square environment (`sandbox` or `production`) |
| `SQUARE_BASE_URL` | string | `https://connect.squareupsandbox.com` | Square API endpoint |
| `SQUARE_APPLICATION_ID` | string | `your_square_application_id` | Application ID from Square Developer portal |
| `SQUARE_ACCESS_TOKEN` | string | `YOUR_SQUARE_SANDBOX_ACCESS_TOKEN` | OAuth access token (must be kept secret) |
| `SQUARE_LOCATION_ID` | string | `your_square_location_id` | Square business location identifier |
| `SQUARE_WEBHOOK_SIGNATURE_KEY` | string | `your_square_webhook_signature_key` | Secret key used to verify HMAC-SHA256 signatures |

### Debezium CDC & Micro-Batching
| Variable | Type | Default | Description |
|---|---|---|---|
| `DEBEZIUM_CONNECT_URL` | string | `http://localhost:8083` | Debezium Connect REST API base URL |
| `CDC_BATCH_MAX_EVENTS` | integer | `100` | Maximum batch size before flushing CDC events |
| `CDC_BATCH_MAX_WAIT_MS` | integer | `1000` | Maximum wait time (ms) before flushing partial CDC batches |

### Reaper & Retry Engine Configuration
| Variable | Type | Default | Description |
|---|---|---|---|
| `PIPELINE_EXECUTION_STALE_TIMEOUT_SECONDS` | integer | `1800` | Elapsed seconds before an in-flight execution is flagged as stale (30m) |
| `PIPELINE_MAX_RETRIES` | integer | `3` | Maximum automatic retry attempts per execution |
| `PIPELINE_RETRY_BASE_DELAY_SECONDS` | integer | `5` | Initial exponential backoff delay base |
| `PIPELINE_RETRY_MAX_DELAY_SECONDS` | integer | `60` | Maximum backoff delay ceiling |

### Airflow Orchestration Configuration
| Variable | Type | Default | Description |
|---|---|---|---|
| `PIPELINE_ORCHESTRATOR` | string | `LOCAL` | Orchestration engine: `LOCAL` (in-process) or `AIRFLOW` (Airflow REST API) |
| `AIRFLOW_BASE_URL` | string | `http://localhost:8080` | Airflow webserver endpoint (use `http://localhost:18080` if remapped) |
| `AIRFLOW_API_USER` | string | `admin` | Airflow REST API authentication username |
| `AIRFLOW_API_PASSWORD` | string | `admin` | Airflow REST API authentication password |
| `AIRFLOW_DAG_ID` | string | `supplysense_demand_forecast` | Target Airflow DAG identifier |
| `AIRFLOW_REQUEST_TIMEOUT_SECONDS` | integer | `10` | HTTP request timeout for Airflow REST dispatches |

---

## 5. Database Schema & Migration Catalog

The database schema is managed through ordered SQL migrations in [`database/migrations/`](file:///C:/Users/varun%20sai/PycharmProjects/PythonProject/capstone/database/migrations/).

| Migration | File | Target Table(s) | Key Columns & Indexes | Description |
|---|---|---|---|---|
| **001** | `001_create_dataset_metadata.sql` | `dataset_metadata` | `id`, `dataset_name`, `version`, `row_count`, `checksum` | Tracks metadata and checksums of ingested source datasets. |
| **002** | `002_create_pipeline_definitions.sql` | `pipeline_definitions` | `id`, `pipeline_name`, `orchestrator`, `entry_point`, `is_active` | Catalog of known pipelines (e.g. Demand Forecast Pipeline). |
| **003** | `003_create_pipeline_dependencies.sql` | `pipeline_dependencies` | `id`, `pipeline_id`, `dataset_name`, `rule_type` (`ALL`/`ANY`/`QUORUM`) | Defines dependency conditions required to trigger each pipeline. |
| **004** | `004_create_dataset_events.sql` | `dataset_events` | `id`, `dataset_name`, `event_type`, `event_payload`, `received_at` | Append-only log of dataset events received from Kafka. |
| **005** | `005_create_processed_cdc_events.sql` | `processed_cdc_events` | `id`, `event_id`, `source_table`, `processed_at`, `status` | Deduplication table preventing repeated processing of CDC records. |
| **006** | `006_create_pipeline_executions.sql` | `pipeline_executions` | `id`, `pipeline_id`, `triggering_event_id`, `status`, `started_at`, `completed_at` | Canonical execution records (`PENDING`, `RUNNING`, `COMPLETED`, `FAILED`). |
| **007** | `007_create_forecast_results.sql` | `forecast_results` | `id`, `execution_id`, `product_category`, `forecast_week`, `predicted_demand`, `model_version` | Stores model forecast predictions; foreign keyed to execution. |
| **008** | `008_create_raw_square_payloads.sql` | `raw_square_payloads` | `id`, `event_id`, `payload`, `signature`, `verified` | Audit storage for incoming Square POS webhook payloads. |
| **009** | `009_create_model_metadata.sql` | `model_metadata` | `id`, `model_name`, `version`, `parameters`, `trained_at` | Tracks model configurations and hyperparameters. |
| **010** | `010_create_execution_metrics.sql` | `execution_metrics` | `id`, `execution_id`, `metric_name`, `metric_value` | Stores runtime performance metrics (MAE, RMSE, R²). |
| **011** | `011_add_pipeline_retry_tracking.sql` | `pipeline_executions` (ALTER) | `retry_count`, `next_retry_at`, `error_message`, `failure_stage` | Adds columns supporting automated exponential retry recovery. |
| **012** | `012_add_airflow_orchestration_metadata.sql` | `pipeline_executions` (ALTER) | `airflow_run_id`, `airflow_dag_id`, `orchestrator_mode` | Adds Airflow execution correlation and dispatch tracking columns. |

> [!WARNING]
> **Migration Re-application Notice:**
> These migrations are sequential SQL scripts and are not idempotent. Executing an already-applied migration will result in a syntax error (`table or column already exists`). Apply each migration exactly once.

---

## 6. Demand Forecasting ML Pipeline

The demand forecasting component predicts weekly aggregate demand by product category using the public Brazilian E-Commerce (Olist) dataset.

### Pipeline Data Splits
- **Source:** Brazilian E-Commerce dataset (`orders`, `order_items`, `products`).
- **Granularity:** Weekly aggregation (`order_purchase_timestamp` grouped by ISO week and `product_category_name`).
- **Splits:**
  - **Train Split:** Historical periods up to split cutoff week.
  - **Validation Split:** Intermediate period for validation-based early stopping.
  - **Test Split:** Final evaluation period withheld from training.

### Feature Engineering
Features are constructed without temporal data leakage using continuous calendar grids:
- **Autoregressive Lags:** `lag_1_demand`, `lag_2_demand`, `lag_4_demand` (weekly unit sales).
- **Rolling Aggregations:** `rolling_mean_4w`, `rolling_std_4w`, `rolling_mean_8w`.
- **Trend Ratio:** `demand_trend_ratio` ($	ext{demand} / 	ext{rolling\_mean\_4w}$).
- **Calendar Encodings:** `month_of_year`, `week_of_year`, `quarter`.
- **Categorical Feature:** `product_category_enc` (label-encoded product category).

### Models & Benchmarking
1. **Naive Rolling Baseline:**
   One-step-ahead persistence model predicting demand at week $t$ using the observed demand at week $t-1$:
   $$\hat{y}_t = y_{t-1}$$
2. **XGBoost Regressor:**
   Gradient-boosted decision trees trained with validation-based early stopping to prevent overfitting on seasonal anomalies.

### Evaluation Metrics (from `ml/results/model_comparison.json`)
| Model | Split | MAE | RMSE | R² |
|---|---|---|---|---|
| **Naive Baseline** | Validation | 7.98 | 15.32 | 0.899 |
| **Naive Baseline** | Test | 9.46 | 17.99 | 0.835 |
| **XGBoost Regressor** | Validation | **5.28** | **6.32** | 0.543 |
| **XGBoost Regressor** | Test | **5.52** | **6.76** | 0.570 |

> [!NOTE]
> XGBoost substantially reduces prediction errors (MAE 5.52 vs 9.46, RMSE 6.76 vs 17.99), delivering tighter operational predictions. The naive model displays a higher R² score because it mirrors the heavy autocorrelation variance of weekly retail spikes without reducing the magnitude of forecast error.

### Execution Artifact Directory Structure
Each pipeline run produces an isolated set of 9 execution artifacts stored under:
`ml/results/executions/<execution_id>/`

Verified artifacts produced in live execution #36:
- `baseline_metrics.csv` — Baseline benchmark evaluation metrics in tabular CSV format.
- `baseline_metrics.json` — Serialized baseline metrics (MAE, RMSE, R²).
- `baseline_test_predictions.csv` — Baseline rolling predictions on the withheld test split.
- `baseline_validation_predictions.csv` — Baseline rolling predictions on the validation split.
- `model_comparison.json` — Comparative metrics between the naive baseline model and XGBoost regressor.
- `xgboost_demand_model.json` — Serialized XGBoost booster model and configuration.
- `xgboost_metrics.json` — XGBoost model evaluation metrics (MAE, RMSE, R²).
- `xgboost_test_predictions.csv` — XGBoost forecast predictions on the test split.
- `xgboost_validation_predictions.csv` — XGBoost forecast predictions on the validation split.

Shared intermediate datasets (e.g. `data/processed/forecasting_features.csv`) are written to `data/processed/` across pipeline runs.

---

## 7. Airflow Orchestration & DAG Specification

### DAG Architecture: `supplysense_demand_forecast`
Located in [`dags/supplysense_demand_forecast.py`](file:///C:/Users/varun%20sai/PycharmProjects/PythonProject/capstone/dags/supplysense_demand_forecast.py).
- **Schedule:** `None` (on-demand only, triggered via REST API or UI with configuration).
- **Configuration Contract:** Requires `{"execution_id": <int>, "pipeline_name": "<string>"}` in `dag_run.conf`.

### Linear Task Sequence
```mermaid
flowchart LR
    T1["validate_input_datasets"] --> T2["run_naive_baseline_benchmark"]
    T2 --> T3["engineer_leak_free_features"]
    T3 --> T4["train_xgboost_demand_model"]
    T4 --> T5["persist_forecast_predictions"]
    T5 --> T6["finalize_execution_telemetry"]
```

1. **`validate_input_datasets`:** Verifies raw dataset files (`train.csv`, `validation.csv`, `test.csv`), validates canonical record in `pipeline_executions`, marks status `RUNNING`, sets `started_at`, and provisions the scoped artifact folder.
2. **`run_naive_baseline_benchmark`:** Computes 1-step rolling baseline benchmark; writes `baseline_metrics.json`, `baseline_metrics.csv`, and baseline prediction CSVs.
3. **`engineer_leak_free_features`:** Generates causal lag and rolling features; writes `data/processed/forecasting_features.csv`.
4. **`train_xgboost_demand_model`:** Fits XGBoost regressor with early stopping; writes `xgboost_demand_model.json`, `xgboost_metrics.json`, and XGBoost prediction CSVs.
5. **`persist_forecast_predictions`:** Batch-inserts 770 test predictions into MySQL `forecast_results` table tagged with `model_version='xgboost-v1'`.
6. **`finalize_execution_telemetry`:** Verifies all 9 execution-scoped artifacts exist, records output location, and updates `pipeline_executions.status='COMPLETED'` with strict `airflow_run_id` matching.

### Idempotency & Concurrency Safeguards
- **Deterministic Run IDs:**
  Airflow DAG runs use deterministic IDs: `supplysense_exec_{execution_id}_attempt_{retry_count}`.
- **Race Condition & Late-Callback Protection:**
  `finalize_execution_telemetry` executes an atomic conditional update:
  ```sql
  UPDATE pipeline_executions
  SET status = 'COMPLETED', completed_at = NOW(), output_location = %s
  WHERE execution_id = %s AND airflow_run_id = %s;
  ```
  If an execution timed out and a retry attempt was spawned, a delayed task from the previous attempt cannot overwrite or corrupt the newer attempt.
- **Response Hardening (Phase D3):**
  [`services/pipeline-runner/airflow_client.py`](file:///C:/Users/varun%20sai/PycharmProjects/PythonProject/capstone/services/pipeline-runner/airflow_client.py) accepts both HTTP 200 and HTTP 201 for DAG-run creation, with response schema validation and 409 conflict reconciliation.

---

## 8. Self-Healing, Reaper & Retry Engine

SupplySense AI incorporates automated resilience mechanisms to handle worker crashes, network partitions, and pipeline stalls.

```mermaid
flowchart TD
    subgraph StaleReaper["Stale Execution Reaper"]
        R1["Find executions with status='RUNNING'
and updated_at < NOW() - 1800s"]
        R2{"Orchestrator?"}
        R3["Query Airflow REST API
GET /dags/{dag_id}/dagRuns/{run_id}"]
        R4{"Airflow Run State?"}
        R5["Keep RUNNING
(Wait for worker)"]
        R6["Mark FAILED
(Set failure_stage='STALE_TIMEOUT')"]

        R1 --> R2
        R2 -->|LOCAL| R6
        R2 -->|AIRFLOW| R3
        R3 --> R4
        R4 -->|running| R5
        R4 -->|failed / not found| R6
    end

    subgraph RetryService["Automated Retry Service"]
        RT1["Find executions with status='FAILED'
and retry_count < 3 and next_retry_at <= NOW()"]
        RT2["Calculate backoff with jitter:
delay = min(5 * 2^(retry-1), 60) * (1 ± 0.1)"]
        RT3["Increment retry_count
Reset status='PENDING'"]
        RT4["Dispatch execution
(Deterministic Run ID with attempt_{retry_count})"]

        R6 -.-> RT1
        RT1 --> RT2
        RT2 --> RT3
        RT3 --> RT4
    end
```

### 1. Stale-Execution Reaper (`services/pipeline-runner/reaper.py`)
- **Trigger:** Scheduled cron or manual execution via [`scripts/reap_stale_executions.py`](file:///C:/Users/varun%20sai/PycharmProjects/PythonProject/capstone/scripts/reap_stale_executions.py).
- **Detection Window:** Configured via `PIPELINE_EXECUTION_STALE_TIMEOUT_SECONDS` (default: 1800s / 30m).
- **Airflow-Aware Verification:** For Airflow-dispatched runs, the reaper verifies the live state of the DAG run via `AirflowClient.get_dag_run()`. It will not fail an execution if Airflow confirms the tasks are actively running.

### 2. Automated Retry Service (`services/pipeline-runner/retry_service.py`)
- **Trigger:** Scheduled cron or manual execution via [`scripts/process_pipeline_retries.py`](file:///C:/Users/varun%20sai/PycharmProjects/PythonProject/capstone/scripts/process_pipeline_retries.py).
- **Backoff Formula:**
  $$	ext{delay} = \min\left(	ext{base} 	imes 2^{(	ext{retry\_count}-1)}, 	ext{max\_delay}ight) 	imes (1 + 	ext{Uniform}(-0.1, 0.1))$$
- **Max Retries:** 3 attempts. When exhausted, the execution enters terminal `FAILED` status with an error log.

---

## 9. Observability & Dashboard

The observability stack gives visibility into pipeline runs, dependency conditions, and forecasted demand.

### Observability API Endpoints (FastAPI, Port 8000)
- `GET /health` — Service liveness and database connectivity check.
- `GET /api/observability/summary` — High-level operational metrics: execution counts by status, success/failure rates, average duration, latest execution summary, dataset status counts, and latest forecast summary.
- `GET /api/observability/executions` — Filterable execution history ordered newest first (query params: `status`, `pipeline_name`, `limit`). Includes orchestrator, Airflow DAG ID, and run ID.
- `GET /api/observability/executions/{execution_id}` — Detailed telemetry for a single execution: decision context, forecast summary, artifact verification, and Airflow metadata.
- `GET /api/observability/datasets` — Synchronization and event tracking status for all registered datasets.
- `GET /api/observability/dependencies` — Configured pipeline dependencies and latest evaluated decision states.
- `GET /api/observability/forecasts` — Aggregated forecast summaries grouped by execution.

### React Dashboard (`frontend/src/App.jsx`, Port 5173)
- Built with React, Vite, and Lucide icons.
- Auto-refreshes pipeline status and execution logs.
- Interactive charts displaying weekly historical demand vs. XGBoost predicted demand.
- Filterable execution table displaying execution ID, trigger event, orchestrator (`LOCAL` vs `AIRFLOW`), status pills, and direct artifact inspection links.

---

## 10. Live Verification Evidence (Phase D Run #36)

During Phase D live verification, an end-to-end controlled execution was dispatched through the running Apache Airflow cluster to validate live production integration:

| Metric / Attribute | Live Evidence Recorded |
|---|---|
| **Execution ID** | **#36** |
| **Pipeline Name** | `Demand Forecast Pipeline` |
| **Triggering Event ID** | `16` (Live controlled dataset event in MySQL; distinct from test fixtures) |
| **Orchestrator Mode** | `AIRFLOW` |
| **Airflow DAG ID** | `supplysense_demand_forecast` |
| **Airflow DAG Run ID** | `supplysense_exec_36_attempt_0` |
| **Task Sequence Status** | All 6 tasks completed with state `success`: |
| Task 1: `validate_input_datasets` | `success` |
| Task 2: `run_naive_baseline_benchmark` | `success` |
| Task 3: `engineer_leak_free_features` | `success` |
| Task 4: `train_xgboost_demand_model` | `success` |
| Task 5: `persist_forecast_predictions` | `success` |
| Task 6: `finalize_execution_telemetry` | `success` |
| **Final MySQL Execution Status** | **`COMPLETED`** |
| **Persisted Forecast Rows** | **770 rows** in `forecast_results` table |
| **Model Version Stored** | `xgboost-v1` |
| **Persisted Artifacts** | **9 artifacts** in `ml/results/executions/36/` |
| **Observability API Verification** | Validated via `http://127.0.0.1:8000/api/observability/executions/36` |

---

## 11. Operator Runbook

Use the following step-by-step PowerShell procedures to operate, monitor, and maintain the SupplySense AI platform.

### Step 1 — Environment Preparation
```powershell
# Open terminal at repository root
cd "C:\Users\varun sai\PycharmProjects\PythonProject\capstone"

# Activate Python virtual environment
.\.venv\Scripts\Activate.ps1

# Verify required dependencies
python --version
pip --version
```

### Step 2 — Start Middleware Services (Docker Compose)
```powershell
# Start MySQL, Kafka, Debezium, Postgres, and Airflow stack
docker compose up -d

# Check container health status (wait for healthy webserver)
docker compose ps
```

### Step 3 — Apply Database Migrations (Fresh setup only)
```powershell
# If setting up a fresh database, run migrations in sequential order
Get-ChildItem database\migrations\*.sql | Sort-Object Name | ForEach-Object {
    Write-Host "Applying $($_.Name)..."
    Get-Content $_.FullName | docker exec -i supplysense-mysql mysql -uroot -proot supplysense
}
```

### Step 4 — Verify Airflow Cluster
```powershell
# Confirm Airflow webserver responds (use port 18080 if remapped)
Invoke-WebRequest -Uri http://localhost:8080/health -UseBasicParsing | Select-Object -ExpandProperty Content

# Unpause the forecasting DAG
docker exec -i supplysense-airflow-scheduler airflow dags unpause supplysense_demand_forecast
```

### Step 5 — Start Observability API & React Frontend
```powershell
# Terminal 1: Start Observability API
.\.venv\Scripts\python.exe -m uvicorn services.observability.main:app --host 127.0.0.1 --port 8000 --reload

# Terminal 2: Start React Frontend
cd frontend
npm run dev
# Dashboard available at http://127.0.0.1:5173
```

### Step 6 — Run Background Self-Healing Services
```powershell
# Terminal 3: Run Stale Execution Reaper (can be scheduled via Windows Task Scheduler)
.\.venv\Scripts\python.exe scripts\reap_stale_executions.py

# Terminal 4: Run Retry Processor
.\.venv\Scripts\python.exe scripts\process_pipeline_retries.py
```

### Step 7 — Trigger a Controlled Pipeline Execution
```powershell
# Trigger execution via Pipeline Runner in AIRFLOW mode
$env:PIPELINE_ORCHESTRATOR="AIRFLOW"
.\.venv\Scripts\python.exe -c "from services.pipeline_runner.runner import PipelineRunner; runner = PipelineRunner(); exec_id = runner.trigger_execution(pipeline_id=1, triggering_event_id=105); print(f'Dispatched execution: {exec_id}')"
```

### Step 8 — Safe Service Shutdown
```powershell
# Stop Docker containers (preserves MySQL, Postgres, and Kafka volumes)
docker compose down

# WARNING: Only use -v if you want to completely erase the database:
# docker compose down -v
```

---

## 12. Troubleshooting Guide

### Issue 1: Airflow Webserver Fails to Bind on Host Port 8080
- **Symptom:** `docker compose up` fails with `Bind for 0.0.0.0:8080 failed: port is already allocated`.
- **Root Cause:** A local Windows service (e.g. `java.exe` / Apache Tomcat) is listening on port 8080.
- **Remediation:**
  1. In `docker-compose.yml`, change the webserver port mapping to `"18080:8080"`.
  2. In `.env`, set `AIRFLOW_BASE_URL=http://localhost:18080`.
  3. Restart Airflow: `docker compose up -d airflow-webserver`.

### Issue 2: MySQL Connection Refused from Container (`host.docker.internal`)
- **Symptom:** Airflow DAG tasks fail with `Can't connect to MySQL server on 'host.docker.internal'`.
- **Root Cause:** Docker Desktop DNS resolution or Windows firewall blocking container-to-host traffic.
- **Remediation:**
  1. Confirm MySQL is running on port 3306.
  2. Verify Windows Firewall allows inbound connections on port 3306 for the Docker NAT subnet.
  3. Ensure `host.docker.internal` is mapped under `extra_hosts` in `docker-compose.yml`.

### Issue 3: Stale Execution Status After Machine Sleep / Interrupt
- **Symptom:** Execution remains in `RUNNING` status indefinitely in the UI.
- **Root Cause:** Host process was terminated before calling Airflow or writing MySQL status update.
- **Remediation:**
  Run the reaper script:
  ```powershell
  .\.venv\Scripts\python.exe scripts\reap_stale_executions.py
  ```
  The reaper will reconcile the status against Airflow and mark it `FAILED` if timed out.

### Issue 4: Migrations Error "Table already exists"
- **Symptom:** Applying migrations fails with `ERROR 1050 (42S01): Table already exists`.
- **Root Cause:** Migrations were run against an already-initialized database.
- **Remediation:** Migrations are non-idempotent by design. Do not rerun previously executed scripts. Check `SHOW TABLES IN supplysense;` to verify table presence.

---

## 13. Test Suite & Quality Assurance

SupplySense AI maintains a comprehensive automated test suite.

```powershell
# Run the complete test suite (unit + integration)
.\.venv\Scripts\python.exe -m unittest discover -s tests -p "test_*.py"
```

### Test Suite Summary
- **Total Tests:** **220 tests**
- **Failures / Errors:** **0**
- **Test Categories:**
  - **Unit Tests (170 tests in `tests/unit/`):**
    - `test_evaluator.py`: Dependency evaluation rules (`ALL`, `ANY`, `QUORUM`).
    - `test_cdc_batcher.py`: Micro-batching, flush triggers, and deduplication.
    - `test_square_signature.py`: HMAC-SHA256 webhook signature verification.
    - `test_pipeline_runner.py`: Canonical execution tracking and idempotency enforcement.
    - `test_reaper.py`: Stale timeout detection and Airflow status querying.
    - `test_retry_policy.py`: Exponential backoff calculations, jitter bounds, and retry ceilings.
    - `test_airflow_client.py`: Airflow REST API DAG dispatch, HTTP 200/201 response acceptance, HTTP 409 conflict reconciliation, and run-ID validation.
    - `test_observability_service.py`: Repository querying and summary metrics calculations.
  - **Integration Tests (50 tests across 14 modules in `tests/integration/`):**
    - `test_db_connection.py`: Live MySQL connectivity and connection pool lifecycle.
    - `test_airflow_dag_safety.py`: Race condition immunity, late-callback isolation, and schema migration integrity.
    - `test_airflow_runner_dispatch.py`: Pipeline Runner to Airflow REST API dispatch integration.
    - `test_objective1_flow.py`, `test_phase4_execution_flow.py`, `test_phase6_forecast_persistence.py`: End-to-end database pipeline execution flows.
    - `test_phase7_cdc_flow.py`, `test_phase8_batching.py`, `test_phase8_reaper.py`, `test_phase9_retry_flow.py`: CDC ingestion, batching, reaper reconciliation, and retry engine persistence flows.
    - `test_phase10_artifact_isolation.py`, `test_phase11_observability.py`, `test_phase12_capstone_readiness.py`, `test_square_persistence.py`: Execution artifact isolation, observability queries, capstone readiness, and Square audit log persistence.

---

## 14. Security & Compliance Checklist

- [x] **Square Sandbox Token Sanitization:**
  Hardcoded sandbox credentials in `services/realtime-api/README.md` have been replaced with the secure placeholder `YOUR_SQUARE_SANDBOX_ACCESS_TOKEN`. Tracked documentation contains zero live credentials.
  > [!IMPORTANT]
  > The repository owner should ensure that any previously exposed sandbox token is revoked and refreshed in the [Square Developer Dashboard](https://developer.squareup.com/apps).
- [x] **Development Credentials Notice:**
  Default database passwords (`root`/`root`) and Airflow admin logins (`admin`/`admin`) are strictly for local development environments. Production environments must inject credentials via secure key vaults or environment managers.
- [x] **Environment File Exclusion:**
  The root `.gitignore` protects all `.env` files and virtual environments from version control commits.
- [x] **Observability API CORS Policy:**
  The FastAPI service currently permits `allow_origins=["*"]` for local Vite development. Before deploying to shared servers, restrict CORS origins to trusted hostnames.
- [x] **Artifact Filesystem Safety:**
  Execution-scoped artifacts are written to `ml/results/executions/<id>/` with restricted filesystem permissions.

---

## 14.1 Evidence Boundaries and Operational Scope

To maintain rigorous engineering integrity, this documentation distinguishes between live-verified behavior, mock-tested behavior, and operational scope boundaries:

1. **Live-Verified in Local Environment:**
   - Single controlled live execution (#36) dispatched via Airflow REST API.
   - Successful completion of all 6 tasks in DAG `supplysense_demand_forecast`.
   - Persistence of 770 forecast rows in MySQL table `forecast_results`.
   - Production of 9 execution-scoped artifacts under `ml/results/executions/36/`.
   - Observability API query resolution against live MySQL and filesystem.
2. **Mock-Tested and Simulated in Automated Test Suite:**
   - Multi-worker race conditions and duplicate-dispatch collision reconciliation (HTTP 409).
   - Stale execution reaper timeout queries and worker crash reconciliation.
   - Exponential retry backoff timing and jitter calculations across 3 retry attempts.
   - Square HMAC-SHA256 signature verification edge cases and malformed payloads.
   - Debezium CDC micro-batching flush triggers under high event volume.
3. **Untested Scenarios & Production Caveats:**
   - **Physical Network Partitions:** Behavior during mid-task physical network disconnection between the Airflow worker container and host MySQL was verified via exception handlers, not physical link severance.
   - **Multi-Node Airflow Clusters:** Verification was conducted on a single-node Celery/Local Docker Compose stack; multi-node Celery/Kubernetes executor deployments will require shared NFS/S3 storage for `ml/results/executions/`.
   - **Production Readiness Disclaimer:** This deployment is a validated academic capstone. Production adoption requires TLS on Kafka/Debezium, authenticated database proxies, restricted CORS policies, and enterprise secrets management.

---
## 15. Capstone Project History & Milestone Log

| Phase | Milestone Name | Commit | Summary & Deliverables |
|---|---|---|---|
| **Phases 1–3** | ML Pipeline & Benchmarking | — | Historical dataset cleaning, lag feature engineering, naive rolling baseline model, and XGBoost regression training. |
| **Phases 4–6** | Dependency Engine & Webhooks | — | Kafka-based dataset event listener, multi-dataset dependency rule evaluator, and Square POS webhook receiver. |
| **Phases 7–8** | CDC & Self-Healing Reaper | — | Debezium MySQL CDC adapter, CDC deduplication, micro-batching, and background stale-execution reaper daemon. |
| **Phases 9–10** | Automated Retry & Artifacts | — | Exponential backoff retry engine, execution-scoped artifact filesystem isolation (`ml/results/executions/<id>/`). |
| **Phase 11** | Observability Service | — | FastAPI REST API and React + Vite analytics dashboard for live telemetry and forecast visualization. |
| **Phase 12** | E2E Capstone Integration | `c3d5429` | Full end-to-end integration demo connecting ingestion, dependencies, ML pipeline, and observability. |
| **Phase A** | Airflow Infrastructure Stack | `ebe9107` | Docker Compose orchestration defining Airflow Webserver, Scheduler, PostgreSQL, MySQL, and Kafka. |
| **Phase B** | Demand Forecast DAG | `aba0ae5` | 6-task production DAG `supplysense_demand_forecast` with task isolation, baseline comparison, and MySQL persistence. |
| **Phase C** | Airflow REST API Integration | `33e1d6a` | Pipeline Runner Airflow dispatch, reaper/retry Airflow awareness, late-callback protection, and 214 passing tests. |
| **Phase D1** | Live Preflight Inspection | — | Read-only verification of Docker containers, MySQL connection, and host environment readiness. |
| **Phase D2** | Controlled Airflow Execution | — | Successful live Airflow dispatch: execution #36 completed all 6 tasks, persisting 770 forecasts and 9 artifacts. |
| **Phase D3** | Dispatch Response Hardening | `e2096b6` | Hardened AirflowClient to accept both HTTP 200 and 201 for DAG runs, verified with 220 passing tests. |
| **Phase D4** | Reliability & Recovery Verification | — | Live reliability verification of idempotency, reaper reconciliation, retry logic, and zero regressions. |
| **Phase E1** | Final Repository Audit | — | Comprehensive read-only audit of Git state, credentials, port bindings, and repository hygiene. |
| **Phase E2** | Release Readiness & Handover | *(working tree)* | Square credential redaction, `.env.example` Airflow synchronization, README update, and `HANDOVER.md` creation. |

---

*Handover document authored and verified on October 15, 2026. All systems verified ready for capstone evaluation.*
