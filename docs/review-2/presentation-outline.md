# SupplySense AI — Review-2 Presentation Outline

---

## SLIDE 1: Title
### SupplySense AI
**Event-Conditioned Data-Pipeline Trigger and Dependency Orchestrator for AI-Driven Supply Chain Management**

- **Project Stage:** Review-2 Gateway Presentation
- **Focus Areas:**
  1. Objective-1: Event-Conditioned Dependency Orchestration Mechanism
  2. Machine Learning Foundation: Dataset Preparation, Chronological Splitting, and Baseline Evaluation

---

## SLIDE 2: Problem Statement
### The Vulnerability of Schedule-Driven Supply Chain Pipelines

Traditional enterprise data architectures trigger downstream analytics and machine learning pipelines using static, time-based schedules (e.g., cron jobs executing daily at midnight).

**Key Failure Modes:**
1. **Premature Execution:** Pipelines fire before upstream tables (e.g., Orders, Inventory, Supplier Catalogs) complete their ETL loads.
2. **Incomplete & Biased Data:** Models train or forecast on partial datasets, producing distorted inventory replenishment recommendations.
3. **Unnecessary Compute Waste:** Pipelines re-run blindly when no new business transactions or catalog updates have arrived.
4. **Stale Results & Silent Failures:** Downstream teams assume models reflect current supply chain states when in reality inputs failed to update.

---

## SLIDE 3: Project Objective
### Review-2 Milestone: Objective-1

> *"To develop an event-conditioned dependency orchestration mechanism that detects dataset updates, evaluates dependency conditions, and automatically triggers or blocks downstream data pipelines based on data availability."*

**Scope of Review-2:**
- Validate the event-driven trigger and dependency evaluation engine.
- Verify pipeline execution tracking and duplicate event idempotency.
- Prepare and validate a leak-free chronological forecasting dataset from historical e-commerce records.
- Establish an empirical regression baseline benchmark for future ML model comparisons.

---

## SLIDE 4: Proposed Architecture
### Decoupled Event Orchestration & ML Forecasting Tracks

```
[ ORCHESTRATION TRACK (Objective-1) ]

Dataset Update (Simulated Source)
            │
            ▼
    Event Generator
            │
            ▼
     dataset_events (MySQL)
            │
            ▼
     Kafka Producer
            │
            ▼
  Kafka Topic: dataset-events
            │
            ▼
   Dependency Engine
     ├── Evaluates: ALL / ANY / QUORUM
     └── Inspects: pipeline_dependencies & dependency_datasets
            │
            ▼
    pipeline_decisions (TRIGGER / BLOCK)
            │
            ▼ (If TRIGGER)
    pipeline_executions (Status: RUNNING)
      [Guarded by Idempotency Constraint]


[ MACHINE LEARNING EVALUATION TRACK ]

Historical Olist Database (MySQL)
  (olist_orders, olist_order_items, olist_products)
            │
            ▼
  ml/data/prepare_dataset.py (Delivered orders, Weekly Category Demand)
            │
            ▼
  data/processed/forecasting_dataset.csv (4,372 rows)
            │
            ▼
  ml/data/split_dataset.py (Chronological Partitioning)
     ├── Train:      2,795 rows (70% wks)
     ├── Validation:   807 rows (15% wks)
     └── Test:         770 rows (15% wks)
            │
            ▼
  ml/baseline/naive_baseline.py (One-Step-Ahead Rolling Naive)
            │
            ▼
  Regression Benchmarks: MAE / RMSE / R²
```

---

## SLIDE 5: Technology Stack
### Implemented Technologies vs. Future Roadmap

| Layer | Implemented & Verified in Review-2 | Planned for Post-Review-2 |
| :--- | :--- | :--- |
| **Language & Runtime** | Python 3.13, Virtual Environment | Python Container Images |
| **Event Streaming** | Apache Kafka 4.3.1 (KRaft Mode, `dataset-events` topic) | Multi-broker cluster, Kafka Connect CDC |
| **Relational Storage** | MySQL 8.0 (Orchestration & Historical Olist Data) | Production DB with Migration Tooling |
| **Data Processing** | Pandas, PyMySQL, Standard Library (math, csv, json) | Apache Spark (Distributed ETL) |
| **Orchestrator** | SupplySense Dependency Engine (Python) | Apache Airflow (DAG Task Execution) |
| **Machine Learning** | Scikit-learn (Metric calculations), Naive Baseline | XGBoost Regressor, Feature Store |
| **Testing** | Python `unittest` (29 automated test cases) | Automated CI/CD Regression Suite |
| **User Interface** | CLI & Terminal Logging | React Dashboard, Spring Boot REST API |

---

## SLIDE 6: Objective-1 Workflow
### Complete State Transition Walkthrough

**Scenario: Demand Forecast Pipeline requiring `ALL(Orders, Products, Sellers)`**

1. **Initial State (Partial Readiness):**
   - `Orders` = READY, `Products` = READY, `Sellers` = WAITING
   - Evaluator evaluates: $2 \text{ of } 3 \text{ ready}$.
   - Decision: **BLOCK**.
   - Result: Logged in `pipeline_decisions`. Zero rows created in `pipeline_executions`.

2. **Upstream Update Event:**
   - Simulated event updates `Sellers` dataset; row inserted in `dataset_events`.
   - `Sellers` status transitions to `READY`.
   - Event published to Kafka topic `dataset-events`.

3. **Re-Evaluation & Downstream Trigger:**
   - Dependency Engine consumes the `Sellers` event.
   - Evaluator checks: $3 \text{ of } 3 \text{ ready}$ ($100\%$).
   - Decision: **TRIGGER**.
   - Action: Record created in `pipeline_executions` with `status = 'RUNNING'`.

---

## SLIDE 7: Dependency Conditions
### Supported Mathematical Evaluation Logic

1. **`ALL` (Conjunctive Dependency):**
   - *Rule:* $\text{ready\_count} == \text{total\_required}$
   - *Use Case:* Core training pipelines requiring all core entity tables (e.g., Orders, Items, Products, Sellers) to be refreshed before feature generation.
   - *Example:* 3 of 3 ready $\rightarrow$ **TRIGGER**; 2 of 3 ready $\rightarrow$ **BLOCK**.

2. **`ANY` (Disjunctive Dependency):**
   - *Rule:* $\text{ready\_count} \ge 1$
   - *Use Case:* High-frequency alert monitors or partial price-update ingestion pipelines that can trigger on any upstream data availability.
   - *Example:* 1 of 5 ready $\rightarrow$ **TRIGGER**; 0 of 5 ready $\rightarrow$ **BLOCK**.

3. **`QUORUM` (Threshold Dependency):**
   - *Rule:* $\text{ready\_count} \ge \text{required\_count}$
   - *Use Case:* Distributed telemetry or regional sensor feeds where a minimum critical mass of partitions is sufficient to proceed.
   - *Example:* 3 of 5 regional feeds ready (quorum = 3) $\rightarrow$ **TRIGGER**; 2 of 5 ready $\rightarrow$ **BLOCK**.

---

## SLIDE 8: Idempotency Protection
### Eliminating Duplicate Pipeline Execution

**The Problem:**
Distributed message brokers such as Kafka operate with at-least-once delivery guarantees. Network retries or consumer restarts can cause the same dataset update event to be delivered multiple times.

**The Failure Mode Without Idempotency:**
The downstream pipeline is triggered multiple times for the same logical update, wasting compute and corrupting stateful downstream tables.

**SupplySense AI Solution:**
1. **Application Pre-Check:** Before triggering, the Dependency Engine queries `pipeline_executions` for existing `(pipeline_name, triggering_event_id)`.
2. **Database Constraint Enforcement:** The `pipeline_executions` table enforces a strict unique key constraint:
   ```sql
   UNIQUE KEY idx_unique_pipeline_event (pipeline_name, triggering_event_id)
   ```
3. **Empirical Behavior:** First event triggers `RUNNING` execution. Duplicate event delivery is caught, logged, and skipped without side effects.

---

## SLIDE 9: Testing & Verification
### 29 / 29 Tests Passing Across All Subsystems

```
Ran 29 tests in 0.100s — ALL PASSED (100% Green)
```

- **Objective-1 Dependency Evaluator Unit Tests (`tests/unit/test_dependency_evaluator.py`):** 8/8 PASS
  - All variants of ALL, ANY, QUORUM boundaries and invalid condition types.
- **Chronological Split Unit Tests (`tests/unit/test_dataset_split.py`):** 9/9 PASS
  - Temporal ordering, 70/15/15 ratio, zero overlap, CSV round-tripping.
- **Naive Baseline Unit Tests (`tests/unit/test_naive_baseline.py`):** 10/10 PASS
  - Previous-period lookup, causality preservation, boundary transitions, MAE/RMSE/R² calculations.
- **Configuration Unit Test (`tests/unit/test_config.py`):** 1/1 PASS
  - Dynamic environment loading, isolated test harness reload.
- **Objective-1 End-to-End Integration Test (`tests/integration/test_objective1_flow.py`):** 1/1 PASS
  - Live MySQL database test verifying BLOCK $\rightarrow$ TRIGGER $\rightarrow$ Execution $\rightarrow$ Idempotency rejection.

---

## SLIDE 10: Forecasting Dataset Preparation
### Transforming Historical E-Commerce Records

**Dataset Source:**
- Olist Brazilian E-Commerce Public Dataset (100,000+ orders, 2016–2018).
- *Clarification:* Olist is historical source data, not a real-time streaming feed.

**Business Filtering Rule:**
- Filter exclusively to `order_status = 'delivered'` (96,478 orders).
- Excluded unfulfilled/cancelled records: 2,453 items.
- Excluded uncategorized products: 1,537 items.
- Realized item demand extracted: **108,660 items**.

**Target Definition:**
- **Entity:** Product Category (73 unique categories).
- **Time Granularity:** Weekly (ISO calendar week starting Monday).
- **Target Column:** `demand` (Count of delivered items ordered per category per week).
- **Total Dataset Size:** **4,372 rows** across **91 calendar weeks**.

---

## SLIDE 11: Chronological Dataset Split
### Strict Temporal Ordering with Zero Leakage

**Why Random Shuffling is Prohibited in Forecasting:**
Time-series forecasting models must predict the future using only the past. Randomly shuffling rows leaks future trends and seasonal information into training features, resulting in deceptively high training accuracy that fails in production.

**70 / 15 / 15 Chronological Partitioning:**

```
2016-09-12 ───────────────────► 2018-02-12 ──► 2018-02-19 ────────► 2018-05-21 ──► 2018-05-28 ────────► 2018-08-27
[             TRAIN: 63 Weeks            ]     [       VAL: 14 Weeks        ]     [      TEST: 14 Weeks        ]
[         2,795 rows (63.9%)             ]     [      807 rows (18.5%)      ]     [      770 rows (17.6%)      ]
```

**Leakage Verification Results:**
- $\text{Train End (2018-02-12)} < \text{Val Start (2018-02-19)}$: **PASSED**
- $\text{Val End (2018-05-21)} < \text{Test Start (2018-05-28)}$: **PASSED**
- Date intersection across all splits: $\mathbf{0}$ shared calendar dates (**PASSED**).

---

## SLIDE 12: Baseline Forecasting Model
### Establishing a Defensible Benchmark

**Method:** One-Step-Ahead Rolling Previous-Period Naive Forecast
$$\hat{y}_{c, t} = y_{c, t-1}$$
- Predicts that demand for category $c$ in week $t$ equals observed demand from the immediately preceding calendar week ($t - 7\text{ days}$).
- **Missing Period Handling:** If category $c$ had no orders in week $t-1$, historical demand is strictly **0** (no unobserved items).
- **Boundary Handling:**
  - First validation week ($t = \text{2018-02-19}$) uses the final training week ($t-1 = \text{2018-02-12}$).
  - First test week ($t = \text{2018-05-28}$) uses the final validation week ($t-1 = \text{2018-05-21}$).

> **Important:** This is a deterministic mathematical heuristic, **not an AI model**. Its sole purpose is to serve as the reference standard that future ML models (e.g., XGBoost) must outperform.

---

## SLIDE 13: Baseline Evaluation Results
### Quantitative Regression Metrics

**Evaluation Metrics:**
- **MAE (Mean Absolute Error):** Average unit error per category per week.
- **RMSE (Root Mean Squared Error):** Error penalizing large forecasting deviations.
- **$R^2$ (Coefficient of Determination):** Proportion of variance explained relative to mean predictor.
*(Note: These are standard regression metrics, not classification accuracy percentages).*

**Empirical Results:**

| Model | Partition | Evaluated Rows | MAE | RMSE | $R^2$ |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Naive Baseline ($T-1$)** | **Validation** | 807 | **7.9802** | **15.3196** | **0.8992** |
| **Naive Baseline ($T-1$)** | **Test** | 770 | **9.4636** | **17.9912** | **0.8351** |

**Interpretation:**
- On unseen Test data, the naive model misses weekly category demand by an average of ~9.46 items.
- $R^2 = 0.8351$ confirms that weekly category demand exhibits strong week-over-week autocorrelation, providing a strong hurdle for ML models to beat.

---

## SLIDE 14: Project Contribution & Positioning
### What SupplySense AI Uniquely Solves

1. **Intelligent Event-Conditioned Orchestration:**
   Replaces fragile, static time schedules with dynamic, dependency-aware trigger logic that evaluates data readiness before triggering heavy analytical pipelines.
2. **Deterministic State Guardrails:**
   Combines multi-table dependency evaluation (ALL, ANY, QUORUM) with database-enforced idempotency to prevent duplicate downstream execution.
3. **Rigorous ML Engineering Discipline:**
   Establishes clean chronological data segregation and a verified mathematical baseline benchmark before attempting complex ML model training.

---

## SLIDE 15: Honest Limitations
### Current Review-2 Boundaries

1. **Data Source Nature:** Olist is a historical public dataset, not a real-time production stream.
2. **Event Generation:** Upstream dataset update events are generated via a simulation module for testing rather than production Change Data Capture (CDC).
3. **Baseline Model:** The current forecasting model is a pure naive heuristic, not yet an optimized ML model.
4. **Downstream Execution:** A `TRIGGER` currently generates an audited execution record (`RUNNING`) rather than dispatching a physical distributed Airflow DAG or Spark job.

---

## SLIDE 16: Future Roadmap
### Post-Review-2 Implementation Plan

- **Phase 1: Feature Engineering & XGBoost Regressor**
  - Create rolling lag features (lag-1, lag-2, rolling 4-week mean, category momentum).
  - Train and tune an XGBoost model on Train/Validation; evaluate performance against the 9.46 MAE / 0.835 $R^2$ baseline on Test.
- **Phase 2: Airflow Pipeline Integration**
  - Connect the `TRIGGER` decision to execute live Apache Airflow DAGs.
- **Phase 3: Production CDC Integration**
  - Replace manual/simulated event generator with Debezium/Kafka Connect for real-time MySQL binlog change capture.
- **Phase 4: Full Stack & Deployment**
  - Containerize all services with Docker Compose; build React operator dashboard and Spring Boot API.

---

## SLIDE 17: Conclusion & Summary
### Review-2 Verification Summary

1. ✅ **Objective-1 Implemented:** Event generation, Kafka transport, and Dependency Engine successfully deployed.
2. ✅ **Multi-Condition Support:** ALL, ANY, and QUORUM verified.
3. ✅ **Reliable Execution & Idempotency:** Guaranteed execution tracking without duplicate runs.
4. ✅ **ML Data Foundation:** 4,372 category-week demand records prepared from 96,478 delivered orders.
5. ✅ **Leak-Free Chronological Split:** 70% Train, 15% Validation, 15% Test with zero temporal overlap.
6. ✅ **Empirical Baseline:** Naive $T-1$ established (Test MAE: 9.46, $R^2$: 0.835).
7. ✅ **100% Green Test Suite:** 29 of 29 automated tests passing.
