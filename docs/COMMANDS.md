# SupplySense AI Command Reference

This reference documents all verified CLI and SQL commands used throughout the SupplySense AI project.

---

## 1. Environment

### Create Virtual Environment
```powershell
# Windows
python -m venv .venv
```
```bash
# Linux / macOS
python3 -m venv .venv
```

### Activate Virtual Environment
```powershell
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
```
```cmd
# Windows CMD
.\.venv\Scripts\activate.bat
```
```bash
# Linux / macOS
source .venv/bin/activate
```

### Install Dependencies
```bash
pip install -r requirements.txt
```

### Configure Environment File
```powershell
# Windows PowerShell
Copy-Item .env.example .env
```
```bash
# Linux / macOS
cp .env.example .env
```

---

## 2. Docker

### Validate Compose Configuration
```bash
docker compose config
```

### Start Containers in Background
```bash
docker compose up -d
```

### Inspect Container Status
```bash
docker compose ps
```

### View Service Logs
```bash
# Stream all logs
docker compose logs -f

# Stream MySQL logs only
docker compose logs -f mysql

# Stream Kafka logs only
docker compose logs -f kafka
```

### Stop Containers
```bash
docker compose down
```

### Reset Database Volume (Destructive)
```bash
docker compose down -v
docker compose up -d
```

---

## 3. Testing

### Run Entire Test Suite (29 Tests)
```bash
python -m unittest discover -s tests
```

### Run Unit Tests Only
```bash
python -m unittest discover -s tests/unit
```

### Run Integration Tests Only
```bash
python -m unittest discover -s tests/integration
```

### Run Specific Test Modules
```bash
# Config test
python -m unittest tests/unit/test_config.py

# Dependency evaluator unit tests
python -m unittest tests/unit/test_dependency_evaluator.py

# Dataset split unit tests
python -m unittest tests/unit/test_dataset_split.py

# Naive baseline forecasting unit tests
python -m unittest tests/unit/test_naive_baseline.py

# Database connection test
python -m unittest tests/integration/test_db_connection.py

# Objective-1 end-to-end integration test
python -m unittest tests/integration/test_objective1_flow.py
```

---

## 4. Event Generator

Simulates an update event for a registered dataset:
```bash
python services/event-generator/main.py
```
**Interactive prompts:**
- Dataset name: `Orders` | `Products` | `Sellers`
- Rows changed: Integer value (e.g. `100`)

---

## 5. Kafka Producer

Scans `dataset_events` for rows where `event_status = 'NEW'`, publishes them to Kafka topic `dataset-events`, and marks them `PUBLISHED`:
```bash
python services/kafka-producer/main.py
```

---

## 6. Dependency Engine

Listens to Kafka topic `dataset-events`, queries required dataset statuses, evaluates condition rules (`ALL`, `ANY`, `QUORUM`), logs decisions to `pipeline_decisions`, and records execution instances to `pipeline_executions`:
```bash
python services/dependency-engine/main.py
```

---

## 7. Ingestion of Historical Olist Data

Bulk-loads raw CSV files into MySQL:
```bash
python data/scripts/upload_olist.py
```

---

## 8. ML Dataset Preparation

Aggregates delivered orders, items, and products into weekly demand time series:
```bash
python ml/data/prepare_dataset.py
```
**Output:** `data/processed/forecasting_dataset.csv`

---

## 9. Dataset Split

Splits the aggregated dataset chronologically (70% Train, 15% Validation, 15% Test):
```bash
python ml/data/split_dataset.py
```
**Outputs:**
- `data/processed/train.csv`
- `data/processed/validation.csv`
- `data/processed/test.csv`

---

## 10. Baseline Metrics

Evaluates the one-step-ahead persistence baseline model:
```bash
python ml/baseline/naive_baseline.py
```
**Outputs:**
- `data/processed/baseline_validation_predictions.csv`
- `data/processed/baseline_test_predictions.csv`
- `ml/results/baseline_metrics.csv`
- `ml/results/baseline_metrics.json`

---

## 11. Database Verification

Run these queries in MySQL client (`mysql -h localhost -u root -p supplysense` or database IDE):

### Check Dataset Metadata
```sql
SELECT dataset_id, dataset_name, current_version, status, row_count, last_updated_at 
FROM dataset_metadata;
```

### Check Generated Events
```sql
SELECT event_id, dataset_name, dataset_version, rows_changed, event_status, event_time, processed_at 
FROM dataset_events 
ORDER BY event_id DESC 
LIMIT 10;
```

### Check Configured Pipelines & Dependency Topologies
```sql
SELECT p.pipeline_name, p.condition_type, p.required_count, d.dataset_name, d.status
FROM pipeline_dependencies p
JOIN dependency_datasets dd ON p.dependency_id = dd.dependency_id
JOIN dataset_metadata d ON dd.dataset_id = d.dataset_id;
```

### Check Evaluated Decisions
```sql
SELECT decision_id, pipeline_name, condition_type, decision, ready_count, total_required, reason, triggered_at 
FROM pipeline_decisions 
ORDER BY decision_id DESC 
LIMIT 10;
```

### Check Pipeline Executions & Idempotency Logs
```sql
SELECT execution_id, pipeline_name, decision_id, triggering_event_id, status, started_at, completed_at 
FROM pipeline_executions 
ORDER BY execution_id DESC 
LIMIT 10;
```

### Check Olist Ingestion Row Counts
```sql
SELECT 'olist_orders' AS tbl, COUNT(*) AS cnt FROM olist_orders
UNION ALL
SELECT 'olist_order_items' AS tbl, COUNT(*) AS cnt FROM olist_order_items
UNION ALL
SELECT 'olist_products' AS tbl, COUNT(*) AS cnt FROM olist_products
UNION ALL
SELECT 'olist_sellers' AS tbl, COUNT(*) AS cnt FROM olist_sellers;
```
