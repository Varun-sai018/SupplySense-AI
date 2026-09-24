# SupplySense AI Developer Runbook

## 1. Project Overview

SupplySense AI is an event-driven supply chain intelligence platform designed to orchestrate downstream machine learning pipelines based on upstream dataset availability and freshness. 

In Review-2, the project verifies **Objective-1** (Dependency Management and Orchestration Engine) and the initial **Machine Learning Foundation** (chronological dataset creation, train/validation/test splitting without lookahead leakage, and naive baseline forecasting benchmark):
- **Objective-1 Orchestration:** Evaluates multi-dataset dependencies (`ALL`, `ANY`, `QUORUM`) when dataset update events arrive via Apache Kafka, emits automated execution decisions (`TRIGGER` or `BLOCK`), records audit trails in MySQL, and enforces idempotency to prevent duplicate pipeline executions.
- **ML Foundation:** Aggregates Brazilian e-commerce (Olist) transaction data into weekly product category demand series, applies strict chronological splitting (70% Train, 15% Validation, 15% Test), and evaluates a one-step-ahead rolling persistence baseline against verified regression metrics (MAE, RMSE, R²).

---

## 2. Prerequisites

Before setting up the repository, verify that your host system meets the following prerequisites:

* **Python:** Python 3.10+ (Verified on Python 3.13.5).
* **pip:** Python package installer (`pip` bundled with Python).
* **Docker Desktop:** Required for running containerized MySQL and Apache Kafka (Verified Docker engine 29.3.1).
* **Docker Compose:** Docker Compose v2+ (Docker Compose plugin / v5.1.0 verified).
* **MySQL Client / Local MySQL (Optional):** Optional GUI (e.g., MySQL Workbench, DBeaver) or CLI client (`mysql`) for direct inspection of containerized MySQL on port `3306`.
* **Operating System:** Windows 10/11 (PowerShell), macOS, or Linux.

---

## 3. Project Structure

The cleaned, source-of-truth project directory structure is as follows:

```
capstone/
├── common/                     # Shared cross-service utilities
│   ├── __init__.py
│   └── database.py             # PyMySQL connection factory with dictionary cursors
├── config/                     # Centralized configuration management
│   ├── __init__.py
│   └── settings.py             # Environment variable loader with defaults
├── data/                       # Dataset directories and ingestion scripts
│   ├── generated/              # Placeholder for ad-hoc runtime generated data
│   ├── processed/              # Processed ML datasets & baseline predictions
│   │   ├── forecasting_dataset.csv
│   │   ├── train.csv
│   │   ├── validation.csv
│   │   ├── test.csv
│   │   ├── baseline_validation_predictions.csv
│   │   └── baseline_test_predictions.csv
│   ├── raw_csv/                # Directory for raw historical Olist CSV files
│   └── scripts/                # Data loading and ETL utilities
│       ├── README.md
│       └── upload_olist.py     # Ingests raw Olist CSVs into MySQL
├── database/                   # Database migrations and documentation
│   ├── migrations/             # Lexically ordered SQL migration files (001 - 006)
│   │   ├── 001_create_dataset_metadata.sql
│   │   ├── 002_create_dataset_events.sql
│   │   ├── 003_create_pipeline_dependencies.sql
│   │   ├── 004_create_dependency_datasets.sql
│   │   ├── 005_create_pipeline_decisions.sql
│   │   └── 006_create_pipeline_executions.sql
│   ├── seeds/                  # Seed scripts (if any)
│   └── README.md               # Migration order & schema reference
├── docs/                       # Project documentation
│   ├── RUNBOOK.md              # Comprehensive developer runbook (this document)
│   ├── QUICKSTART.md           # Fast-path command sequence
│   ├── COMMANDS.md             # Complete reference of validated commands
│   ├── architecture.md         # System architecture diagram & design notes
│   └── review-2/               # Review-2 academic and presentation documentation
│       ├── README.md
│       ├── baseline-metrics.md
│       ├── dataset-split.md
│       ├── demo-checklist.md
│       ├── demo-scenario.md
│       ├── live-demo-script.md
│       ├── ml-test-summary.md
│       ├── objective-1.md
│       ├── one-page-summary.md
│       ├── presentation-outline.md
│       ├── test-plan.md
│       └── viva-questions.md
├── ml/                         # Machine learning forecasting pipeline
│   ├── __init__.py
│   ├── README.md               # ML formulation & instructions
│   ├── baseline/               # Baseline forecasting models
│   │   ├── __init__.py
│   │   └── naive_baseline.py   # One-step-ahead persistence baseline & metrics
│   ├── data/                   # ML dataset processing scripts
│   │   ├── __init__.py
│   │   ├── prepare_dataset.py  # Weekly aggregation from MySQL
│   │   └── split_dataset.py    # Strict chronological Train/Val/Test split
│   └── results/                # Output evaluation metrics
│       ├── baseline_metrics.csv
│       └── baseline_metrics.json
├── services/                   # Modular microservices
│   ├── __init__.py
│   ├── dependency-engine/      # Dependency evaluation consumer
│   │   ├── __init__.py
│   │   ├── evaluator.py        # ALL / ANY / QUORUM rule logic
│   │   ├── main.py             # Kafka consumer daemon, decision & execution logger
│   │   └── README.md
│   ├── event-generator/        # Interactive dataset update simulator
│   │   ├── __init__.py
│   │   ├── main.py             # CLI simulation tool
│   │   └── README.md
│   └── kafka-producer/         # Database-to-Kafka publisher
│       ├── __init__.py
│       ├── main.py             # Event publisher & state updater
│       └── README.md
├── tests/                      # Automated test suite (29 tests)
│   ├── __init__.py
│   ├── integration/            # Multi-component integration tests
│   │   ├── __init__.py
│   │   ├── test_db_connection.py
│   │   └── test_objective1_flow.py
│   └── unit/                   # Isolated unit tests
│       ├── __init__.py
│       ├── test_config.py
│       ├── test_dataset_split.py
│       ├── test_dependency_evaluator.py
│       └── test_naive_baseline.py
├── .env                        # Local environment variables (untracked)
├── .env.example                # Example host configuration template
├── .env.docker.example         # Example Docker container network template
├── .gitignore                  # Git exclusion rules
├── docker-compose.yml          # Container configuration for MySQL and Kafka
├── README.md                   # Repository overview
└── requirements.txt            # Python package dependencies
```

---

## 4. Environment Configuration

SupplySense AI uses environment variables loaded from a `.env` file via `python-dotenv`.

Templates provided:
- `.env.example`: Configured for executing Python scripts directly on the host machine connecting to containers mapped to `localhost`.
- `.env.docker.example`: Configured for services communicating within a Docker container bridge network.

### Difference Between Host Execution and Container Execution:

1. **Running Python Directly on Host (Current Development Workflow):**
   When your Python scripts run in your terminal / virtual environment, access MySQL and Kafka through published localhost ports:
   ```dotenv
   DB_HOST=localhost
   DB_PORT=3306
   DB_USER=root
   DB_PASSWORD=root
   DB_NAME=supplysense

   KAFKA_BROKER=localhost:9092
   KAFKA_TOPIC_EVENTS=dataset-events
   KAFKA_CONSUMER_GROUP=supplysense-group
   ```

2. **Running Services Inside Docker (Future Containerized Workflow):**
   When services run inside Docker containers on the same Docker Compose network, use container service names for DNS resolution:
   ```dotenv
   DB_HOST=mysql
   DB_PORT=3306
   DB_USER=root
   DB_PASSWORD=root
   DB_NAME=supplysense

   KAFKA_BROKER=kafka:9092
   KAFKA_TOPIC_EVENTS=dataset-events
   KAFKA_CONSUMER_GROUP=supplysense-group
   ```

> [!CAUTION]
> Never commit actual passwords, secrets, or `.env` files to source control. The `.gitignore` file excludes `.env`.

To initialize your environment configuration:
```powershell
Copy-Item .env.example .env
```
*(On Linux/macOS: `cp .env.example .env`)*

---

## 5. Install Python Dependencies

Set up an isolated virtual environment and install the required dependencies.

### Windows PowerShell:
```powershell
# 1. Create virtual environment
python -m venv .venv

# 2. Activate virtual environment
.\.venv\Scripts\Activate.ps1

# 3. Upgrade pip and install requirements
pip install -r requirements.txt
```

### Linux / macOS:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Verified Packages (`requirements.txt`):
- `pymysql` (Database driver)
- `kafka-python` (Kafka producer and consumer)
- `pandas` (Data aggregation and table handling)
- `python-dotenv` (Environment variable loading)
- `scikit-learn` (Metrics computation: MAE, RMSE, R²)

---

## 6. Start Infrastructure

SupplySense AI relies on Docker Compose to launch containerized MySQL 8.0 and Apache Kafka 4.3.1 (KRaft mode).

### 1. Validate Docker Compose Syntax:
```powershell
docker compose config
```

### 2. Start Services in Background:
```powershell
docker compose up -d
```

### 3. Check Container Status:
```powershell
docker compose ps
```
Both `supplysense-mysql` and `supplysense-kafka` should show healthy or running states.

### 4. Inspect Container Logs:
```powershell
docker compose logs -f
# Or inspect individually:
docker compose logs -f mysql
docker compose logs -f kafka
```

### 5. Stop Infrastructure:
```powershell
docker compose down
```

### Database Volume Reset (Destructive):
If you need to wipe the MySQL database, migrations, and event state completely:
```powershell
docker compose down -v
docker compose up -d
```
> [!WARNING]
> Running `docker compose down -v` permanently removes the `mysql_data` and `kafka_data` volumes. Only use when explicitly resetting your database.

---

## 7. Verify Tests

The repository maintains an automated test suite with 33 passing unit and integration tests.

### Run All Tests:
```powershell
python -m unittest discover -s tests
```

### Expected Output:
```
Ran 33 tests in ~0.35s
OK
```

### Test Coverage Breakdown:
1. **`tests/unit/test_config.py` (3 tests):**
   - Validates configuration defaults, environment variable loading, and integer parsing.
2. **`tests/unit/test_dependency_evaluator.py` (8 tests):**
   - Tests `ALL` condition logic (all datasets must be `READY`, missing dataset blocks).
   - Tests `ANY` condition logic (at least 1 dataset must be `READY`, zero ready blocks).
   - Tests `QUORUM` condition logic (threshold met vs not met, above threshold).
   - Tests unknown condition handling.
3. **`tests/unit/test_dataset_split.py` (7 tests):**
   - Verifies 70% / 15% / 15% chronological split ratio calculations.
   - Asserts non-overlapping contiguous calendar dates (no lookahead / temporal leakage).
   - Validates category preservation and empty dataset handling.
4. **`tests/unit/test_naive_baseline.py` (12 tests):**
   - Tests one-step-ahead persistence forecasting logic ($\hat{y}_t = y_{t-1}$).
   - Validates zero-fill imputation for unobserved preceding weeks.
   - Validates MAE, RMSE, and $R^2$ mathematical calculations and edge cases.
5. **`tests/integration/test_db_connection.py` (1 test):**
   - Tests database connection retrieval with dictionary cursors.
6. **`tests/integration/test_objective1_flow.py` (5 tests):**
   - End-to-end simulated flow: Orders READY, Products READY, Sellers WAITING (`BLOCK`), Sellers becoming READY (`TRIGGER`).
   - `ALL` lifecycle: 2/3 -> `BLOCK`, 3/3 -> `TRIGGER`, reset to `WAITING`, new event required for next cycle.
   - `ANY` lifecycle: 0/3 -> `BLOCK`, 1/3 -> `TRIGGER`, reset to `WAITING`.
   - `QUORUM` lifecycle: 1/3 -> `BLOCK`, 2/3 -> `TRIGGER` (quorum 2), reset to `WAITING`.
   - Idempotency: duplicate triggering event prevents duplicate execution records.

---

## 8. Run Event Generator

The Event Generator simulates dataset update events (such as table updates or ingestion completions).

### Execute Service:
```powershell
python services/event-generator/main.py
```

### Interactive Usage:
The command presents interactive prompts:
```
Enter dataset name: Orders
Enter number of rows changed: 1500
```
It updates the dataset metadata in `dataset_metadata` and inserts a `NEW` event in `dataset_events`.

> [!NOTE]
> The current Event Generator simulates dataset updates for testing dependency rules. It does **NOT** mean Olist itself is a real-time data source. Olist is a historical static dataset.

---

## 9. Run Kafka Producer

The Kafka Producer queries the MySQL database for unhandled dataset events and publishes them to Kafka.

### Execute Service:
```powershell
python services/kafka-producer/main.py
```

### Behavior:
1. Queries `dataset_events` for rows where `event_status = 'NEW'`.
2. Serializes each event as JSON.
3. Emits messages to the Kafka topic `dataset-events`.
4. Updates MySQL event records to `event_status = 'PUBLISHED'` and records `processed_at = NOW()`.
5. Exits cleanly after the current batch.

---

## 10. Run Dependency Engine

The Dependency Engine is the core Objective-1 consumer. It listens to Kafka, evaluates pipeline dependency rules against MySQL metadata, and decides whether to trigger downstream pipelines.

### Execute Service:
```powershell
python services/dependency-engine/main.py
```

### Behavior:
1. Subscribes to `dataset-events` on Kafka broker.
2. For each incoming event, queries configured pipelines from `pipeline_dependencies`.
3. Checks the readiness of required datasets in `dataset_metadata`.
4. Evaluates condition (`ALL`, `ANY`, `QUORUM`) via `services/dependency-engine/evaluator.py`.
5. Writes the audit record to `pipeline_decisions` (`decision = 'TRIGGER'` or `'BLOCK'`).
6. If `TRIGGER`, checks `pipeline_executions` for existing execution on `(pipeline_name, triggering_event_id)`:
   - If not found: creates record with status `RUNNING`.
   - If already present: logs `IDEMPOTENCY: Pipeline was already triggered. Skipping execution.`
7. After creating the execution record, resets all relevant dependency datasets in `dataset_metadata` back to `WAITING` for the next pipeline cycle.

---

## 11. Review-2 Demo Flow

To demonstrate the full deterministic Objective-1 flow:

```mermaid
sequenceDiagram
    autonumber
    actor Dev as Developer / Evaluator
    participant EG as Event Generator
    participant DB as MySQL Database
    participant KP as Kafka Producer
    participant K as Kafka (dataset-events)
    participant DE as Dependency Engine

    Note over DB: Initial State: Orders=READY, Products=READY, Sellers=WAITING
    Dev->>EG: Generate event for Orders (Version N)
    EG->>DB: Insert event (Status=NEW) into dataset_events
    Dev->>KP: Run Kafka Producer
    KP->>DB: Query NEW events
    KP->>K: Publish Orders event
    KP->>DB: Mark event PUBLISHED
    K->>DE: Receive Orders event
    DE->>DB: Query required datasets for Demand Forecast Pipeline
    Note over DE: 2 of 3 datasets READY (Orders, Products). Condition is ALL.
    DE->>DB: Record Decision: BLOCK
    
    Dev->>EG: Generate event for Sellers (Version M)
    EG->>DB: Update Sellers status to READY; insert event
    Dev->>KP: Run Kafka Producer
    KP->>K: Publish Sellers event
    K->>DE: Receive Sellers event
    DE->>DB: Query required datasets
    Note over DE: 3 of 3 datasets READY! Condition ALL satisfied.
    DE->>DB: Record Decision: TRIGGER
    DE->>DB: Create execution in pipeline_executions (Status=RUNNING)
    DE->>DB: Reset cycle datasets to WAITING (Orders, Products, Sellers)
    
    Dev->>KP: Re-publish duplicate Sellers event
    K->>DE: Receive duplicate Sellers event
    DE->>DB: Detect existing execution for (pipeline, event_id)
    Note over DE: IDEMPOTENCY check blocks duplicate run
```

---

## 12. ML Dataset Preparation

Extracts delivered orders, items, and products from MySQL tables and aggregates them into a weekly category-level demand series:

```powershell
python ml/data/prepare_dataset.py
```

### Requirements & Outputs:
- Requires MySQL container running with Olist tables populated.
- Aggregates by `product_category_name` and ISO calendar week.
- Saves output to `data/processed/forecasting_dataset.csv`.

---

## 13. Dataset Split

Splits the aggregated forecasting dataset chronologically without lookahead bias:

```powershell
python ml/data/split_dataset.py
```

### Partitioning Rules:
- **70% Train:** Earliest contiguous weeks (`2016-09-12` to `2018-02-12`, 2,795 rows)
- **15% Validation:** Middle contiguous weeks (`2018-02-19` to `2018-05-21`, 807 rows)
- **15% Test:** Final out-of-time contiguous weeks (`2018-05-28` to `2018-08-27`, 770 rows)
- Outputs: `data/processed/train.csv`, `data/processed/validation.csv`, `data/processed/test.csv`.

---

## 14. Baseline Metrics

Evaluates the one-step-ahead rolling persistence baseline ($\hat{y}_t = y_{t-1}$) across Validation and Test splits:

```powershell
python ml/baseline/naive_baseline.py
```

### Verified Benchmark Results:
| Metric | Validation Set | Test Set | Formula / Description |
|---|---|---|---|
| **MAE** | **7.9802** | **9.4636** | Mean Absolute Error |
| **RMSE** | **15.3196** | **17.9912** | Root Mean Squared Error |
| **$R^2$** | **0.8992** | **0.8351** | Coefficient of Determination |

Generated files:
- `data/processed/baseline_validation_predictions.csv`
- `data/processed/baseline_test_predictions.csv`
- `ml/results/baseline_metrics.csv`
- `ml/results/baseline_metrics.json`

---

## 15. Important Database Tables

| Table Name | Primary Role |
|---|---|
| `dataset_metadata` | Master catalog of registered datasets, tables, versions, row counts, and synchronization status (`WAITING`, `READY`). |
| `dataset_events` | Change-data-capture log of dataset update events. Tracks lifecycle from `NEW` to `PUBLISHED`. |
| `pipeline_dependencies` | Rules governing pipeline triggers: `pipeline_name`, `condition_type` (`ALL`, `ANY`, `QUORUM`), and `required_count`. |
| `dependency_datasets` | Junction table mapping each pipeline to its set of required dependent datasets. |
| `pipeline_decisions` | Audit history recording every evaluation outcome (`TRIGGER` or `BLOCK`), timestamp, required count, ready count, and reason. |
| `pipeline_executions` | Tracks pipeline execution lifecycle (`RUNNING`, `COMPLETED`, `FAILED`). Guarantees idempotency via unique `(pipeline_name, triggering_event_id)`. |

---

## 16. Troubleshooting

### 1. MySQL Connection Failure (`OperationalError: 2003 / Can't connect to MySQL server`):
- Ensure Docker container is running: `docker compose ps`.
- Check MySQL logs: `docker compose logs mysql`.
- Verify host and port in `.env`: `DB_HOST=localhost`, `DB_PORT=3306`.
- Verify container port binding: `docker port supplysense-mysql`.

### 2. Kafka Unavailable (`NoBrokersAvailable`):
- Ensure Kafka container is running: `docker compose ps`.
- Check Kafka logs: `docker compose logs kafka`.
- Wait 15–20 seconds for KRaft controller election to complete after container startup.
- Verify `KAFKA_BROKER=localhost:9092` in `.env`.

### 3. Docker Desktop Not Running:
- If `docker compose up -d` outputs `error during connect: ... daemon is not running`:
  - Open Docker Desktop application and wait until Docker Engine reports running.
  - On Windows, verify WSL2 backend is initialized.

### 4. Port 3306 or 9092 Already in Use:
- Another instance of MySQL or Kafka may be running locally on your host.
- In PowerShell, identify conflicting processes:
  ```powershell
  Get-NetTCPConnection -LocalPort 3306, 9092 | Select-Object LocalPort, OwningProcess
  ```
- If a local MySQL service is running, stop it or change the host port mapping in `docker-compose.yml` (e.g., `"3307:3306"`) and update `DB_PORT` in `.env`.

### 5. Missing Python Package:
- Ensure your virtual environment is active (prompt should show `(.venv)`).
- Reinstall dependencies:
  ```powershell
  pip install -r requirements.txt
  ```

### 6. `.env` File Missing:
- If `settings.py` logs warnings or uses defaults:
  ```powershell
  Copy-Item .env.example .env
  ```

---

## 17. Current Limitations

To maintain academic and professional integrity, the following limitations of Review-2 are explicitly noted:

- **Historical Data:** The Olist Brazilian E-Commerce dataset is a static historical dataset spanning 2016 to 2018. It is not an actual real-time streaming production source.
- **Simulated Updates:** The Event Generator simulates table update signals to exercise the event-driven dependency graph.
- **Naive Baseline:** Current ML forecasts rely on a one-step-ahead persistence model. This serves as the minimum performance floor.
- **Future Components Not Yet Implemented:**
  - Machine learning models like XGBoost, LightGBM, or Prophet are not yet implemented.
  - Production workflow orchestrators like Apache Airflow are not yet integrated.
  - Distributed computing frameworks like Apache Spark are not yet added.
  - Front-end dashboards (React / Spring Boot) are not yet implemented.
  - Cloud deployment and automated CI/CD pipelines are planned for subsequent reviews.

---

## 18. Future Development

The planned architectural phases for future reviews include:

1. **Review-3: Advanced ML Forecasting & Feature Store:**
   - Implement lag features, rolling statistics, calendar seasonality, and price elasticity features.
   - Train and tune XGBoost / LightGBM models against the naive baseline benchmark.
   - Evaluate against the validated validation/test splits.
2. **Review-4: Automated Pipeline Orchestration:**
   - Integrate triggering mechanism with Apache Airflow / Celery workers.
   - Implement automated model retraining upon threshold-exceeding dataset version drift.
3. **Review-5: End-to-End User Interface & Monitoring:**
   - Develop operator dashboard for pipeline monitoring, decision inspection, and alert management.
   - Deploy full containerized stack via Docker Compose / Kubernetes.
