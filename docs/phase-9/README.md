# Phase 9: Automated Pipeline Retry & Failure Recovery

## 1. Overview & Problem Statement

In Phase 8, SupplySense AI introduced the **Pipeline Execution Reaper** to detect orphaned pipeline executions stuck in `RUNNING` status due to process crashes or network timeouts, safely reconciling them to `FAILED`.

However, marking an execution `FAILED` alone was insufficient for production resilience:
- **No Self-Healing**: Transient errors (database connection loss, deadlocks, temporary Kafka broker unreachability, network blips, or reaper-reaped worker crashes) left downstream demand forecasts uncomputed until manual intervention.
- **Unbounded / Duplicate Triggers Risk**: Without structured retry tracking, naive retry mechanisms could violate `(pipeline_name, triggering_event_id)` uniqueness constraints or spam downstream forecast models.

**Phase 9** introduces bounded, idempotent **Automated Pipeline Retry & Failure Recovery** on top of the existing Pipeline Runner and Reaper architecture.

---

## 2. Architecture & Retry Lifecycle

```
[Triggering Dataset Event]
        │
        ▼
[Dependency Engine] ──(TRIGGER)──► [pipeline_executions Insert] (Status: RUNNING, retry_count=0)
                                            │
                                            ▼
                                   [Handler Execution]
                                            ├── Success ──► [Status: COMPLETED] ──► [forecast_results Persisted]
                                            │
                                            └── Transient Failure ──► [Status: FAILED]
                                                                           │
                                                                           ▼
                                                                 [Retry Policy Check]
                                                    ┌──────────────────────┴──────────────────────┐
                                                    ▼                                             ▼
                                          [Retryable & Count < Max]                    [Non-Retryable / Max Reached]
                                                    │                                             │
                                                    ▼                                             ▼
                                         [Backoff Delay Computed]                      [Permanent FAILED]
                                         (next_retry_at = NOW + delay)                 (next_retry_at = NULL)
                                                    │
                                                    ▼
                                          [Atomic Claim by Worker]
                                      (UPDATE ... WHERE status='FAILED')
                                                    │
                                                    ▼
                                            [Status: RUNNING]
                                          (retry_count = count + 1)
                                                    │
                                                    ▼
                                            [Handler Re-attempt]
```

### Distinction Between Phase 8 & Phase 9
- **Phase 8 (Reaper)**: **Detection & Reconciliation** — Identifies stale executions that crashed while `RUNNING` without updating their status and safely transitions them to `FAILED`.
- **Phase 9 (Retry & Recovery)**: **Self-Healing & Re-execution** — Evaluates `FAILED` executions (whether from immediate handler errors or reaper timeouts), computes exponential backoff, claims them atomically, and drives them to `COMPLETED`.

---

## 3. Database Schema Changes (Migration 011)

Migration [`011_add_pipeline_retry_tracking.sql`](file:///c:/Users/varun%20sai/PycharmProjects/PythonProject/capstone/database/migrations/011_add_pipeline_retry_tracking.sql) adds dedicated retry tracking columns to `pipeline_executions`:

| Column | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `retry_count` | `INT NOT NULL` | `0` | Number of retries already attempted for this execution. |
| `max_retries` | `INT NOT NULL` | `3` | Maximum allowed retries before permanent failure. |
| `next_retry_at` | `DATETIME NULL` | `NULL` | Timestamp when this execution becomes eligible for its next retry attempt. |
| `retry_error_type` | `VARCHAR(100) NULL` | `NULL` | Categorized error type (e.g., `TRANSIENT_DB`, `TRANSIENT_REAPER`, `DETERMINISTIC_INPUT`). |

---

## 4. Configuration Reference

Retry parameters are configured via environment variables in `config/settings.py` and `.env.example`:

| Parameter | Environment Variable | Default | Description |
| :--- | :--- | :--- | :--- |
| Maximum Retries | `PIPELINE_MAX_RETRIES` | `3` | Maximum retry attempts per failed execution (initial attempt + up to 3 retries). |
| Base Delay | `PIPELINE_RETRY_BASE_DELAY_SECONDS` | `5` | Initial backoff base delay in seconds. |
| Max Delay | `PIPELINE_RETRY_MAX_DELAY_SECONDS` | `60` | Upper ceiling for exponential backoff delay in seconds. |

---

## 5. Retry Policy & Error Classification

Failures are categorized into transient (retryable) vs deterministic (non-retryable) by [`services/pipeline-runner/retry_policy.py`](file:///c:/Users/varun%20sai/PycharmProjects/PythonProject/capstone/services/pipeline-runner/retry_policy.py):

### A. Transient (Retryable) Errors
- **Database Connectivity / Deadlocks (`TRANSIENT_DB`)**: `OperationalError (2006/2013)`, MySQL server gone away, lock wait timeouts, deadlocks.
- **Timeouts (`TRANSIENT_TIMEOUT`)**: Socket timeouts, HTTP 504 Gateway Timeouts.
- **Reaper Recoveries (`TRANSIENT_REAPER`)**: Stale executions recovered by Phase 8 reaper after process crashes.
- **Network / Broker Blips (`TRANSIENT_NETWORK`)**: Temporary Kafka broker unreachability, connection refused.

### B. Deterministic (Non-Retryable) Errors
- **Input Data Missing (`DETERMINISTIC_INPUT`)**: `FileNotFoundError`, missing prepared CSV datasets (`train.csv`, `test.csv`).
- **Application Logic (`DETERMINISTIC_APPLICATION`)**: `ValueError`, `KeyError`, `TypeError`, syntax errors.
- **Configuration / Pipeline Unknown (`DETERMINISTIC_CONFIG`)**: Unregistered pipeline handler names.
- **Integrity Violations (`DETERMINISTIC_INTEGRITY`)**: Duplicate primary keys or foreign key constraint failures.

---

## 6. Backoff Strategy & Bounded Progression

Backoff delays follow bounded exponential growth:
$$\text{delay} = \min\left(\text{base\_delay} \times 2^{\text{retry\_count}}, \text{max\_delay}\right)$$

| Attempt | `retry_count` | Formula ($\text{base}=5\text{s}, \text{max}=60\text{s}$) | Effective Delay |
| :--- | :---: | :--- | :---: |
| 1st Retry | `0` | $\min(5 \times 2^0, 60)$ | **5s** |
| 2nd Retry | `1` | $\min(5 \times 2^1, 60)$ | **10s** |
| 3rd Retry | `2` | $\min(5 \times 2^2, 60)$ | **20s** |
| 4th Retry (Capped) | `3` | $\min(5 \times 2^3, 60)$ | **40s** |

---

## 7. Idempotency & Concurrency Safety

1. **Triggering-Event Idempotency**:
   - Retries operate directly on the **existing `pipeline_executions` record** for that trigger.
   - The unique database constraint `idx_unique_pipeline_event (pipeline_name, triggering_event_id)` is never violated.
   - Replayed Kafka events for the same triggering event are idempotently skipped.
2. **Atomic Claiming (No Double-Execution)**:
   - Multiple concurrent retry workers safely coordinate via conditional atomic updates:
     ```sql
     UPDATE pipeline_executions
     SET status = 'RUNNING',
         retry_count = retry_count + 1,
         started_at = %s,
         completed_at = NULL,
         next_retry_at = NULL,
         error_message = NULL
     WHERE execution_id = %s
       AND status = 'FAILED'
       AND retry_count < max_retries;
     ```
   - Only the single worker obtaining `cursor.rowcount == 1` proceeds to execute the handler.

---

## 8. Forecast Persistence Safety

- Successful retries execute [`demand_forecast_pipeline_handler`](file:///c:/Users/varun%20sai/PycharmProjects/PythonProject/capstone/services/pipeline-runner/handlers.py) which saves predictions to MySQL `forecast_results` linked directly to `execution_id`.
- The database constraint `UNIQUE KEY idx_unique_execution_forecast (execution_id, product_category, forecast_week)` guarantees no duplicate forecast rows are created across repeated attempts.

---

## 9. Verification & Testing

### Running Unit & Integration Tests
```powershell
# Run Phase 9 Unit Tests
python -m unittest tests.unit.test_pipeline_retry -v

# Run Phase 9 Integration Tests
python -m unittest tests.integration.test_phase9_retry_flow -v

# Run Full Test Suite
python -m unittest discover -s tests -p "test_*.py"
```

### Running the Phase 9 Demo
```powershell
python scripts/demo_phase9_retry.py
```

### Running the CLI Retry Worker Sweep
```powershell
python scripts/process_pipeline_retries.py --batch-size 10 --verbose
```

---

## 10. Limitations

- **In-Process Single Node Execution**: The retry service executes handlers within the local Python runtime environment. Distributed multi-cluster worker queues (e.g., Celery/Kubernetes Jobs) are out of scope for Phase 9.
- **Stateless Handlers**: Handlers must remain idempotent and self-contained; intermediate in-memory variables from a crashed attempt are not carried over across process restarts.
