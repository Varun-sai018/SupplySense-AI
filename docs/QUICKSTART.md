# SupplySense AI - Quick Start Guide

This guide provides the minimal sequential commands to initialize the infrastructure, execute the event-driven dependency flow, and run the ML baseline pipeline.

---

### 1. Create and Activate Virtual Environment

**Windows PowerShell:**
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

**Linux / macOS:**
```bash
python3 -m venv .venv
source .venv/bin/activate
```

---

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

---

### 3. Configure Environment Variables

```powershell
Copy-Item .env.example .env
```
*(On Linux/macOS: `cp .env.example .env`)*

---

### 4. Start Infrastructure (Docker)

```bash
docker compose up -d
```
Verify containers are running:
```bash
docker compose ps
```

---

### 5. Ingest Olist Data (First-Time Setup)

```bash
python data/scripts/upload_olist.py
```

---

### 6. Run Test Suite

```bash
python -m unittest discover -s tests
```
*(Expected: 29 tests passing)*

---

### 7. Run Event Generator (Terminal 1)

Simulate a dataset update event:
```bash
python services/event-generator/main.py
```
*(Enter dataset name, e.g., `Orders`, and number of rows changed, e.g., `100`)*

---

### 8. Run Kafka Producer (Terminal 2)

Publish `NEW` events from MySQL to Kafka:
```bash
python services/kafka-producer/main.py
```

---

### 9. Run Dependency Engine (Terminal 3)

Consume events, evaluate dependencies, and record decisions:
```bash
python services/dependency-engine/main.py
```

---

### 10. Check MySQL Decisions & Executions

Open MySQL CLI or Workbench and inspect:
```sql
SELECT * FROM pipeline_decisions ORDER BY decision_id DESC LIMIT 5;
SELECT * FROM pipeline_executions ORDER BY execution_id DESC LIMIT 5;
```

---

### 11. Run Machine Learning Pipeline

```bash
# Step A: Aggregate weekly forecasting dataset
python ml/data/prepare_dataset.py

# Step B: Split chronologically (70% Train, 15% Val, 15% Test)
python ml/data/split_dataset.py

# Step C: Evaluate naive baseline
python ml/baseline/naive_baseline.py
```

---

### Stopping the Services

```bash
docker compose down
```
*(To completely reset the database volumes: `docker compose down -v`)*
