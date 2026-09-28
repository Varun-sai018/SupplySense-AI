# SupplySense AI - Phase 4 Demo & Verification Guide

This guide provides step-by-step instructions to demonstrate and verify the **Phase 4 Pipeline Execution** functionality on Windows PowerShell.

---

## 1. Quick Automated Demo

Run the end-to-end automated demo script to test both BLOCK and TRIGGER scenarios, SQL recording, output verification, and idempotency in one command:

```powershell
.\.venv\Scripts\python.exe scripts\demo_phase4.py
```

---

## 2. Step-by-Step Manual Demo

### Step 1: Initialize Database Dependency State
Open MySQL CLI or run via Python to configure `Demand Forecast Pipeline` for `ALL` condition with datasets reset to `WAITING`:

```sql
UPDATE dataset_metadata
SET status = 'WAITING'
WHERE dataset_name IN ('Orders', 'Products', 'Sellers');

UPDATE pipeline_dependencies
SET condition_type = 'ALL', required_count = NULL
WHERE pipeline_name = 'Demand Forecast Pipeline';
```

---

### Step 2: SCENARIO A — BLOCK Demonstration
In this scenario, only `Orders` receives an update (1/3 datasets READY). Under `ALL` condition, this must produce a `BLOCK` decision with **no** downstream pipeline execution.

#### A1. Generate Event for Orders
```powershell
.\.venv\Scripts\python.exe services\event-generator\main.py
# Input: Orders
# Rows: 25
```

#### A2. Process the Event
```powershell
.\.venv\Scripts\python.exe -c "
import importlib
from common.database import get_connection

dep_engine = importlib.import_module('services.dependency-engine.main')
conn = get_connection()
cur = conn.cursor()
cur.execute(\"SELECT * FROM dataset_events WHERE dataset_name = 'Orders' ORDER BY event_id DESC LIMIT 1\")
event = cur.fetchone()
decision, reason = dep_engine.process_event(event, conn)
print(f'Decision: {decision}, Reason: {reason}')
conn.close()
"
```

#### A3. Expected Output (BLOCK)
```text
===================================
Received Kafka Event
===================================
Event ID       : <EVENT_ID>
Dataset        : Orders
Version        : 16
Rows Changed   : 25

Dependency Status:
Orders -> READY (Version 16)
Products -> WAITING (Version 7)
Sellers -> WAITING (Version 5)

===================================
DEPENDENCY DECISION
===================================
Pipeline       : Demand Forecast Pipeline
Condition      : ALL
Ready          : 1
Required       : 3
Decision       : BLOCK
Reason         : 1 of 3 required datasets are READY.
Decision processing completed.
```

#### A4. Verify No Execution Created
```sql
SELECT COUNT(*) FROM pipeline_executions WHERE triggering_event_id = <EVENT_ID>;
-- Expected: 0
```

---

### Step 3: SCENARIO B — TRIGGER Demonstration
In this scenario, `Products` and `Sellers` receive updates, bringing all 3 datasets to `READY`. The `Sellers` event triggers the `Demand Forecast Pipeline`.

#### B1. Generate Event for Products (2/3 READY -> BLOCK)
```powershell
.\.venv\Scripts\python.exe services\event-generator\main.py
# Input: Products
# Rows: 10
```

#### B2. Generate Event for Sellers (3/3 READY -> TRIGGER)
```powershell
.\.venv\Scripts\python.exe services\event-generator\main.py
# Input: Sellers
# Rows: 5
```

#### B3. Process the Sellers Event
```powershell
.\.venv\Scripts\python.exe -c "
import importlib
from common.database import get_connection

dep_engine = importlib.import_module('services.dependency-engine.main')
conn = get_connection()
cur = conn.cursor()
cur.execute(\"SELECT * FROM dataset_events WHERE dataset_name = 'Sellers' ORDER BY event_id DESC LIMIT 1\")
event = cur.fetchone()
decision, reason = dep_engine.process_event(event, conn)
print(f'Decision: {decision}, Reason: {reason}')
conn.close()
"
```

#### B4. Expected Output (TRIGGER & COMPLETED)
```text
===================================
Received Kafka Event
===================================
Event ID       : <EVENT_ID>
Dataset        : Sellers
Version        : 6
Rows Changed   : 5

Dependency Status:
Orders -> READY (Version 16)
Products -> READY (Version 8)
Sellers -> READY (Version 6)

===================================
DEPENDENCY DECISION
===================================
Pipeline       : Demand Forecast Pipeline
Condition      : ALL
Ready          : 3
Required       : 3
Decision       : TRIGGER
Reason         : All required datasets are READY.

Pipeline 'Demand Forecast Pipeline' EXECUTION RECORD CREATED (Status: RUNNING, ID: <EXEC_ID>).

Dependency Cycle Reset:
Orders -> WAITING
Products -> WAITING
Sellers -> WAITING

Pipeline 'Demand Forecast Pipeline' EXECUTION RESULT: COMPLETED
Output Location: ml/results/baseline_metrics.json
Decision processing completed.
```

---

## 3. SQL Verification Queries

Run these queries in MySQL to verify execution audit records:

### 1. View Pipeline Decisions Audit
```sql
SELECT decision_id, pipeline_name, condition_type, decision, ready_count, total_required, reason, triggered_at, created_at
FROM pipeline_decisions
ORDER BY decision_id DESC
LIMIT 5;
```

### 2. View Pipeline Executions and Status
```sql
SELECT execution_id, pipeline_name, decision_id, triggering_event_id, status, started_at, completed_at, output_location, error_message
FROM pipeline_executions
ORDER BY execution_id DESC
LIMIT 5;
```

### 3. Verify Dependency Cycle Reset
```sql
SELECT dataset_id, dataset_name, status, current_version, last_updated_at
FROM dataset_metadata
WHERE dataset_name IN ('Orders', 'Products', 'Sellers', 'Inventory');
```

---

## 4. Automated Test Suite

Run the full automated test suite containing all 59 unit and integration tests:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests
```

Expected Result:
```text
Ran 59 tests in ~1.5s
OK
```
