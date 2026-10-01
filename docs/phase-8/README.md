# Phase 8: Resilient CDC Stream Processing, Source Deduplication & Event Batching

## 1. Overview & Problem Statement

In Phase 7, SupplySense AI introduced real-time Change Data Capture (CDC) via Debezium and MySQL binary logging. While this enabled instantaneous detection of OLTP database updates, row-level change capture introduces two architectural challenges:

1. **Event Amplification & Downstream Saturation**:
   - A single bulk database transaction affecting 500 rows produces 500 distinct Debezium binlog messages.
   - Without batching, this triggers 500 individual `dataset_events` writes, 500 Kafka message publications, 500 Dependency Engine evaluations, and redundant pipeline executions.
2. **Replay Vulnerability**:
   - Connector restarts, Kafka offset rewinds, or network retries re-deliver historical binlog messages.
   - Without source coordinate tracking, replayed messages generate new synthetic `event_id` records and advance dataset versions incorrectly.

Phase 8 solves these vulnerabilities through **Source-Level Deduplication (Task 8.2 & 8.3)** and **CDC Micro-Batching (Task 8.4)**.

---

## 2. Architecture & Micro-Batching Flow

```
[MySQL OLTP Tables]
       │ (500 Row Inserts/Updates)
       ▼
[MySQL Binary Log] ──► [Debezium MySQL Connector]
                              │ (500 CDC JSON Messages)
                              ▼
                       [Kafka CDC Topics] (supplysense_cdc.supplysense.*)
                              │
                              ▼
                   [CDC Adapter & Micro-Batcher]
                              ├── Step 1: Deterministic source_change_id deduplication
                              ├── Step 2: In-memory per-dataset buffer (CDCBatcher)
                              ├── Step 3: Threshold flush (max_events=100) or Timeout (max_wait_ms=1000)
                              ├── Step 4: Consolidated state update in MySQL dataset_metadata
                              └── Step 5: Single dataset_events insert (batch_id, rows_changed=500)
                                     │
                                     ▼
                          [Kafka: dataset-events] (Exactly 1 Consolidated Event)
                                     │
                                     ▼
                           [Dependency Engine] (1 Evaluation: ALL/ANY/QUORUM)
                                     │
                                     ▼
                            [Pipeline Runner] (At most 1 Execution per Batch)
```

---

## 3. Key Components & Implementation Details

### A. Source-Level Deduplication (`source_change_id`)
- Every incoming Debezium record is deterministically identified using binlog coordinates:
  $$\text{source\_change\_id} = \begin{cases} \text{gtid:}\{\text{gtid}\}:\{\text{row}\} & \text{if GTID available} \\ \{\text{file}\}:\{\text{pos}\}:\{\text{row}\} & \text{if file/pos available} \\ \{\text{table}\}:\{\text{ts\_ms}\}:\{\text{server\_id}\} & \text{fallback} \end{cases}$$
- Replayed messages are rejected before entering the batch buffer, preventing duplicate row counts or version increments.
- Enforced atomically at the database tier via `UNIQUE KEY idx_unique_source_change (source_change_id)`.

### B. Micro-Batch Aggregation (`CDCBatcher`)
- **Per-Dataset Buffering**: `Orders`, `Products`, and `Sellers` maintain isolated buffers to prevent cross-dataset contamination.
- **Configurable Flush Triggers**:
  - **Count Threshold (`CDC_BATCH_MAX_EVENTS`)**: Flushes immediately when buffer reaches $N$ events (default: `100`).
  - **Time Window (`CDC_BATCH_MAX_WAIT_MS`)**: Flushes if events have been waiting longer than $T$ ms (default: `1000` ms).
- **Consolidated Metadata**:
  - `batch_id`: Traceable identifier formatted as `batch_<dataset>_<uuid>` stored in `dataset_events.batch_id`.
  - `rows_changed`: Sum of all CDC operations in the batch (e.g. `500`).
  - `dataset_version`: Incremented **once** per batch (e.g. `v10 -> v11`, not `v10 -> v510`).

---

## 4. Configuration Reference

The micro-batching parameters are configured via environment variables or `config/settings.py`:

| Parameter | Environment Variable | Default | Description |
| :--- | :--- | :--- | :--- |
| Max Events | `CDC_BATCH_MAX_EVENTS` | `100` | Maximum buffered CDC events per dataset before flushing a consolidated batch. |
| Max Wait Time | `CDC_BATCH_MAX_WAIT_MS` | `1000` | Maximum time in milliseconds before partially filled batches are flushed. |

---

## 5. End-to-End Example Flow

```
1. 500 order rows updated in MySQL
2. 500 Debezium CDC messages emitted to 'supplysense_cdc.supplysense.olist_orders'
3. CDC Adapter normalizes events and derives source_change_id for each row
4. CDCBatcher buffers the 500 events under dataset 'Orders'
5. Max event threshold (or flush) triggers consolidated batch emission:
   - batch_id: batch_orders_8400511f390f
   - rows_changed: 500
   - dataset_version: incremented by +1
6. Exactly 1 dataset event inserted into MySQL dataset_events
7. Exactly 1 message published to Kafka topic 'dataset-events'
8. Dependency Engine evaluates dependency condition exactly ONCE
9. Pipeline Runner triggers downstream XGBoost Demand Forecast execution once
```

---

## 6. Failure & Concurrency Handling

- **Database Rollback on Failure**: If batch persistence fails, the transaction rolls back cleanly, and Kafka offsets are not advanced past unpersisted batches.
- **Deduplication Resilience**: If an ungraceful shutdown occurs mid-batch and Kafka replays messages upon restart, the `source_change_id` check filters out already-committed items.
- **Thread Safety**: The batcher is synchronous with Kafka consumer polling loops, avoiding multi-threaded lock contention and double-flush race conditions.
