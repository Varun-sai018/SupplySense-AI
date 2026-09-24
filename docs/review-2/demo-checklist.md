# SupplySense AI — Live Demo Readiness Checklist & Failover Plan

Use this checklist 15 minutes prior to the Review-2 presentation to ensure all systems, services, and artifacts are online.

---

## 1. Pre-Demo Verification Checklist

Run each check in PowerShell from the project root (`c:\Users\varun sai\PycharmProjects\PythonProject\capstone`):

- [ ] **1. Python Environment Active**
  ```powershell
  python --version
  ```
  *Check:* Python 3.10+ (Current: 3.13) is active.

- [ ] **2. MySQL Running & Accessible**
  ```powershell
  python -c "from common.database import get_connection; conn=get_connection(); print('MySQL Online!'); conn.close()"
  ```
  *Check:* Prints `MySQL Online!`.

- [ ] **3. Required Database Tables Exist**
  ```powershell
  python -c "
  from common.database import get_connection
  conn=get_connection()
  c=conn.cursor()
  c.execute('SHOW TABLES')
  tables = [r[list(r.keys())[0]] for r in c.fetchall()]
  req = ['dataset_metadata', 'dataset_events', 'pipeline_dependencies', 'dependency_datasets', 'pipeline_decisions', 'pipeline_executions', 'olist_orders', 'olist_order_items', 'olist_products']
  missing = set(req) - set(tables)
  print('Missing tables:', missing if missing else 'None! All tables present.')
  conn.close()
  "
  ```
  *Check:* Prints `None! All tables present.`.

- [ ] **4. Initial Dependency Metadata State Clean**
  ```powershell
  python -c "
  from common.database import get_connection
  conn=get_connection()
  c=conn.cursor()
  c.execute(\"UPDATE dataset_metadata SET status='READY' WHERE dataset_name IN ('Orders', 'Products')\")
  c.execute(\"UPDATE dataset_metadata SET status='WAITING' WHERE dataset_name='Sellers'\")
  conn.commit()
  print('State reset: Orders=READY, Products=READY, Sellers=WAITING')
  conn.close()
  "
  ```

- [ ] **5. Processed ML Datasets Present**
  ```powershell
  python -c "
  import os
  files = ['forecasting_dataset.csv', 'train.csv', 'validation.csv', 'test.csv', 'baseline_validation_predictions.csv', 'baseline_test_predictions.csv']
  missing = [f for f in files if not os.path.exists(os.path.join('data/processed', f))]
  print('Missing ML data files:', missing if missing else 'All 6 CSV files present.')
  "
  ```
  *Check:* All 6 files exist in `data/processed/`.

- [ ] **6. Baseline Metric Artifacts Present**
  ```powershell
  python -c "
  import os
  files = ['baseline_metrics.csv', 'baseline_metrics.json']
  missing = [f for f in files if not os.path.exists(os.path.join('ml/results', f))]
  print('Missing metrics files:', missing if missing else 'All metric result files present.')
  "
  ```
  *Check:* Both `baseline_metrics.csv` and `baseline_metrics.json` exist in `ml/results/`.

- [ ] **7. Full Automated Test Suite Passing**
  ```powershell
  python -m unittest discover -s tests
  ```
  *Check:* Outputs `Ran 29 tests ... OK`.

---

## 2. Live Demo Failover & Recovery Plan

### Scenario A: Docker Desktop or Kafka Broker is Offline
- **Symptom:** Kafka producer/consumer raises `KafkaTimeoutError: Unable to bootstrap from localhost:9092`.
- **Root Cause:** Docker daemon stopped or Kafka container restarting.
- **Failover Strategy:**
  1. Explain calmly:
     > *"In our architecture, the Dependency Engine is decoupled from transport. Notice how the core engine exposes a clean `process_event(event, connection)` handler that can be invoked via Kafka or tested via direct event ingestion."*
  2. Execute the deterministic integration script:
     ```powershell
     python tests/integration/test_objective1_flow.py
     ```
  3. This executes the exact same end-to-end event flow (BLOCK $\rightarrow$ TRIGGER $\rightarrow$ RUNNING $\rightarrow$ Idempotency) against the live MySQL database, bypassing external Kafka broker downtime while validating 100% of the orchestration logic.

### Scenario B: Database State Has Leftover Test Records
- **Symptom:** Idempotency test skips immediately or Sellers is already in `READY` status.
- **Quick 5-Second Reset Command:**
  ```powershell
  python -c "
  from common.database import get_connection
  conn=get_connection()
  c=conn.cursor()
  c.execute('DELETE FROM pipeline_executions WHERE triggering_event_id IN (1001, 1002, 9991, 9992, 9993)')
  c.execute(\"UPDATE dataset_metadata SET status='READY' WHERE dataset_name IN ('Orders', 'Products')\")
  c.execute(\"UPDATE dataset_metadata SET status='WAITING' WHERE dataset_name='Sellers'\")
  conn.commit()
  print('Demo state cleanly reset in 1 second!')
  conn.close()
  "
  ```

### Scenario C: Evaluator Asks to Re-run ML Baseline Live
- **Quick 10-Second Regeneration Command:**
  ```powershell
  python ml/baseline/naive_baseline.py
  ```
  *Result:* Re-computes validation and test MAE, RMSE, and $R^2$ live in ~3 seconds.
