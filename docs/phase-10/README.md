# Phase 10: Execution-Scoped Artifacts & Result Isolation

## 1. Overview & Problem Statement

Prior to Phase 10, the Demand Forecast Pipeline wrote machine learning model predictions, metrics, and comparisons to static, shared locations (such as `ml/results/model_comparison.json` and `data/processed/xgboost_test_predictions.csv`).

### The Collision & Contamination Problem
In production scenarios with concurrent, rapid, or replayed pipeline executions:
- **Artifact Overwriting**: Execution B running right after Execution A would overwrite Execution A's `model_comparison.json`, destroying historical audit trails.
- **Race Conditions**: Parallel executions could read or write overlapping prediction CSVs simultaneously.
- **Traceability Loss**: Database records in `pipeline_executions.output_location` pointed to shared files rather than the immutable outputs generated specifically by that execution run.

**Phase 10** resolves this by introducing **Execution-Scoped Artifact Directories and Atomic Isolation** across the entire ML forecasting pipeline.

---

## 2. Execution Directory Structure

Every pipeline execution now generates all its metrics, reports, forecasts, and model snapshots inside a dedicated, isolated subfolder under `ml/results/executions/<execution_id>/`:

```
ml/results/executions/<execution_id>/
├── baseline_metrics.csv
├── baseline_metrics.json
├── baseline_test_predictions.csv
├── baseline_validation_predictions.csv
├── model_comparison.json              <-- Primary output_location in MySQL
├── xgboost_demand_model.json          <-- Scoped copy of trained model
├── xgboost_metrics.json
├── xgboost_test_predictions.csv
└── xgboost_validation_predictions.csv
```

---

## 3. Key Design Principles & Guarantees

### 1. Zero Collisions Between Executions
- Each execution is identified by its unique `execution_id` from `pipeline_executions`.
- ML routines (`run_baseline()` and `train_and_evaluate_xgboost()`) receive `output_dir` and `execution_id` parameters, ensuring all file I/O is contained within `ml/results/executions/<execution_id>/`.
- The canonical production model path (`ml/models/xgboost_demand_model.json`) remains updated for current inference, while an exact replica is archived within the execution folder for auditing.

### 2. Full Metadata Traceability
- All generated JSON reports (`model_comparison.json`, `xgboost_metrics.json`, `baseline_metrics.json`) include an explicit `"execution_id"` field in their top-level `metadata` block.
- `pipeline_executions.output_location` stores the relative path to the scoped primary comparison report (e.g. `ml/results/executions/104/model_comparison.json`).

### 3. Atomic File Writes
- All JSON and CSV artifacts are written to temporary staging files (e.g. `.tmp`) and committed via atomic rename (`os.replace`).
- If an execution crashes mid-way, incomplete files are never left in place to corrupt subsequent reads or retries.

### 4. Safe Retry & Recovery Integration
- When an execution is retried under Phase 9, it reuses the same execution directory `ml/results/executions/<execution_id>/`.
- Atomic writes guarantee that stale or partial artifacts from the failed attempt are cleanly replaced.

---

## 4. Verification & Testing

Phase 10 is verified with extensive unit and integration test coverage:

- [`tests/unit/test_execution_artifacts.py`](file:///c:/Users/varun%20sai/PycharmProjects/PythonProject/capstone/tests/unit/test_execution_artifacts.py):
  - Validates isolated directory generation for baseline and XGBoost models.
  - Verifies execution metadata injection into JSON files.
  - Tests atomic file replacement behavior.
  - Verifies handler routing and path prefixing.

- [`tests/integration/test_phase10_artifact_isolation.py`](file:///c:/Users/varun%20sai/PycharmProjects/PythonProject/capstone/tests/integration/test_phase10_artifact_isolation.py):
  - Simulates two successive/concurrent executions (Execution A and Execution B) against MySQL.
  - Asserts that both executions produce completely independent directories with zero cross-contamination.
  - Verifies `pipeline_executions.output_location` links to the correct execution-scoped artifact.
  - Confirms `forecast_results` entries link directly to the corresponding `execution_id`.
  - Verifies retry execution clean replacement of corrupted artifacts.

---

## 5. Running the Phase 10 Demo

Run the interactive demonstration:

```bash
python scripts/demo_phase10_artifacts.py
```
