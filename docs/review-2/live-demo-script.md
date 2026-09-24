# SupplySense AI — Live Demonstration Script (Review-2)

**Target Duration:** 5–8 minutes  
**Goal:** Prove end-to-end functionality of Objective-1 (Event generation, Kafka messaging, dependency evaluation, execution tracking, idempotency) and Machine Learning foundation (dataset preparation, chronological split, baseline metrics).

---

## Pre-Demo Setup Verification (30 Seconds Before Demo)
Open a terminal in the project root: `c:\Users\varun sai\PycharmProjects\PythonProject\capstone`.
Run quick automated suite to confirm green status:
```powershell
python -m unittest discover -s tests
```
*Expected Output:* `Ran 29 tests ... OK`

---

## PART 1 — SHOW DATABASE (1 Minute)

### Speaker Cue:
> *"To begin, let's inspect the SupplySense orchestration schema in MySQL. We maintain metadata on upstream datasets, audit logs of update events, dependency configurations, decision records, and pipeline executions."*

### Command:
```powershell
python -c "
import sys, os
sys.path.insert(0, os.path.abspath('.'))
from common.database import get_connection

conn = get_connection()
cursor = conn.cursor()

tables = ['dataset_metadata', 'dataset_events', 'pipeline_dependencies', 'dependency_datasets', 'pipeline_decisions', 'pipeline_executions']
for t in tables:
    cursor.execute(f'SELECT COUNT(*) as count FROM {t}')
    print(f'Table {t:<25} : {cursor.fetchone()[\"count\"]} rows')
conn.close()
"
```

### What Evaluators See:
Row counts for all 6 orchestration tables showing active, persistent storage.

---

## PART 2 — SHOW DEPENDENCY CONFIGURATION (45 Seconds)

### Speaker Cue:
> *"Here is the dependency mapping for our primary pipeline: the 'Demand Forecast Pipeline'. It uses an 'ALL' condition requiring three distinct datasets to be in READY state before execution is permitted: Orders, Products, and Sellers."*

### Command:
```powershell
python -c "
import sys, os
sys.path.insert(0, os.path.abspath('.'))
from common.database import get_connection

conn = get_connection()
cursor = conn.cursor()
cursor.execute('''
    SELECT pd.pipeline_name, pd.condition_type, dm.dataset_name, dm.status
    FROM pipeline_dependencies pd
    JOIN dependency_datasets dd ON pd.dependency_id = dd.dependency_id
    JOIN dataset_metadata dm ON dd.dataset_id = dm.dataset_id
    WHERE pd.pipeline_name = 'Demand Forecast Pipeline'
''')
print(f'{\"Pipeline\":<25} | {\"Condition\":<10} | {\"Required Dataset\":<18} | {\"Current Status\"}')
print('-' * 70)
for row in cursor.fetchall():
    print(f\"{row['pipeline_name']:<25} | {row['condition_type']:<10} | {row['dataset_name']:<18} | {row['status']}\")
conn.close()
"
```

---

## PART 3 — BLOCK SCENARIO (1 Minute)

### Speaker Cue:
> *"Now we demonstrate the BLOCK scenario. Orders and Products are in READY state, but Sellers is WAITING. When an update event occurs on Orders, the Dependency Engine evaluates the graph, recognizes that Sellers is missing, and issues a BLOCK decision."*

### Command:
```powershell
python -c "
import sys, os
sys.path.insert(0, os.path.abspath('.'))
from common.database import get_connection
import importlib
dep_engine = importlib.import_module('services.dependency-engine.main')

conn = get_connection()
cursor = conn.cursor()
# Set up condition: Orders=READY, Products=READY, Sellers=WAITING
cursor.execute(\"UPDATE dataset_metadata SET status = 'READY' WHERE dataset_name IN ('Orders', 'Products')\")
cursor.execute(\"UPDATE dataset_metadata SET status = 'WAITING' WHERE dataset_name = 'Sellers'\")
conn.commit()

# Simulate receiving an event on Orders
event = {'event_id': 1001, 'dataset_name': 'Orders', 'dataset_version': 2, 'rows_changed': 50}
dep_engine.process_event(event, conn)
conn.close()
"
```

### Verbal Explanation:
> *"Notice the terminal output: Ready count is 2 out of 3. Condition is ALL. Decision is BLOCK. The pipeline is not executed because one required upstream dataset is still unavailable."*

---

## PART 4 — TRIGGER SCENARIO (1.5 Minutes)

### Speaker Cue:
> *"Next, the Sellers dataset finishes its upstream batch and transitions to READY. A Sellers update event is published and consumed. The Dependency Engine re-evaluates all dependencies: now 3 of 3 datasets are READY, producing a TRIGGER decision and spawning a downstream execution record."*

### Command:
```powershell
python -c "
import sys, os
sys.path.insert(0, os.path.abspath('.'))
from common.database import get_connection
import importlib
dep_engine = importlib.import_module('services.dependency-engine.main')

conn = get_connection()
cursor = conn.cursor()
# Set Sellers to READY
cursor.execute(\"UPDATE dataset_metadata SET status = 'READY' WHERE dataset_name = 'Sellers'\")
conn.commit()

# Sellers update event arrives
event = {'event_id': 1002, 'dataset_name': 'Sellers', 'dataset_version': 2, 'rows_changed': 20}
dep_engine.process_event(event, conn)

# Inspect the pipeline_executions table
cursor.execute(\"SELECT execution_id, pipeline_name, status, triggering_event_id, started_at FROM pipeline_executions ORDER BY execution_id DESC LIMIT 1\")
print('\n=== LATEST PIPELINE EXECUTION RECORD ===')
print(cursor.fetchone())
conn.close()
"
```

### Verbal Explanation:
> *"All required datasets are READY. The decision changes to TRIGGER. An execution record is created in `pipeline_executions` with status 'RUNNING' linked to Event ID 1002. This proves data-conditioned triggering."*

---

## PART 5 — IDEMPOTENCY PROTECTION (1 Minute)

### Speaker Cue:
> *"In distributed streaming architectures, network retries can cause duplicate delivery of the exact same event. Without idempotency, duplicate pipelines would launch. Let's send Event ID 1002 a second time."*

### Command:
```powershell
python -c "
import sys, os
sys.path.insert(0, os.path.abspath('.'))
from common.database import get_connection
import importlib
dep_engine = importlib.import_module('services.dependency-engine.main')

conn = get_connection()
# Resend duplicate event 1002
event = {'event_id': 1002, 'dataset_name': 'Sellers', 'dataset_version': 2, 'rows_changed': 20}
dep_engine.process_event(event, conn)

# Count executions for this event
cursor = conn.cursor()
cursor.execute('SELECT COUNT(*) as cnt FROM pipeline_executions WHERE triggering_event_id = 1002')
print(f'\nTotal execution records for Event 1002: {cursor.fetchone()[\"cnt\"]}')
conn.close()
"
```

### Verbal Explanation:
> *"The engine logs: 'IDEMPOTENCY: Pipeline was already triggered for Event ID 1002. Skipping execution.' The total execution count remains exactly 1. Both application logic and the database unique key prevent duplicate runs."*

---

## PART 6 — ML FORECASTING DATASET (45 Seconds)

### Speaker Cue:
> *"Now turning to our Machine Learning preparation: We aggregated 96,478 historical delivered orders from the Olist dataset into a weekly demand series across 73 product categories."*

### Command:
```powershell
python -c "
import pandas as pd
df = pd.read_csv('data/processed/forecasting_dataset.csv')
print(f'Total rows: {len(df):,}')
print(f'Categories: {df[\"product_category\"].nunique()}')
print(f'Distinct weeks: {df[\"week_start_date\"].nunique()}')
print('\nSample Records:')
print(df.head(5))
"
```

---

## PART 7 — CHRONOLOGICAL DATASET SPLIT (45 Seconds)

### Speaker Cue:
> *"To avoid data leakage, we perform a strict chronological split (70% Train, 15% Validation, 15% Test) based on calendar weeks. Future data is never mixed into training."*

### Command:
```powershell
python -c "
import pandas as pd
for name in ['train', 'validation', 'test']:
    df = pd.read_csv(f'data/processed/{name}.csv')
    print(f'{name.capitalize():<12} | Rows: {len(df):<6} | Range: {df[\"week_start_date\"].min()} to {df[\"week_start_date\"].max()}')
"
```

### Verbal Explanation:
> *"Notice: Train ends on 2018-02-12. Validation begins on 2018-02-19. Validation ends on 2018-05-21. Test begins on 2018-05-28. There are zero overlapping calendar dates."*

---

## PART 8 — BASELINE MODEL & METRICS (1 Minute)

### Speaker Cue:
> *"Finally, we establish our empirical benchmark using a one-step-ahead previous-period Naive forecast. This provides the baseline hurdle for future XGBoost models."*

### Command:
```powershell
python -c "
import pandas as pd
df = pd.read_csv('ml/results/baseline_metrics.csv')
print(df.to_string(index=False))
"
```

### Verbal Explanation:
> *"On unseen test data across 770 category-week observations, the baseline achieves a Mean Absolute Error of 9.46 items and an R² of 0.8351. In the next phase, our XGBoost regressor will use multi-week lag features to beat this baseline."*

---

## Demo Conclusion (15 Seconds)
> *"In summary, Review-2 successfully delivers: a tested event-driven dependency orchestrator, execution tracking, idempotency, a clean chronological forecasting dataset, an empirical benchmark, and 29 passing automated tests. Thank you."*
