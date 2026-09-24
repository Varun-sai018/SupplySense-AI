# SupplySense AI — Review-2 Executive One-Page Summary

## 1. Problem
Traditional schedule-driven data pipelines (cron jobs) fire blind to upstream data readiness. If upstream supplier or transaction data is delayed, pipelines run on incomplete datasets, generating inaccurate supply chain forecasts, wasting compute, or failing silently.

## 2. Objective-1 (Review-2 Scope)
> *"To develop an event-conditioned dependency orchestration mechanism that detects dataset updates, evaluates dependency conditions, and automatically triggers or blocks downstream data pipelines based on data availability."*

## 3. Architecture
- **Event Track:** Simulated Update $\rightarrow$ `dataset_events` (MySQL) $\rightarrow$ Apache Kafka (`dataset-events`) $\rightarrow$ Dependency Engine (ALL / ANY / QUORUM) $\rightarrow$ `pipeline_decisions` (TRIGGER / BLOCK) $\rightarrow$ `pipeline_executions` (RUNNING) guarded by DB unique-key idempotency.
- **ML Track:** Olist Historical Database $\rightarrow$ Data Preparation $\rightarrow$ Chronological Split (70/15/15) $\rightarrow$ Rolling Naive Baseline Benchmark.

## 4. Objective-1 Verified Results
- **Conditions Supported:** ALL, ANY, QUORUM.
- **BLOCK State:** Orders=READY, Products=READY, Sellers=WAITING $\rightarrow$ Decision: BLOCK (2/3 ready). Zero pipeline executions created.
- **TRIGGER State:** Sellers becomes READY $\rightarrow$ Event emitted to Kafka $\rightarrow$ Re-evaluation: TRIGGER (3/3 ready) $\rightarrow$ `pipeline_executions` created (`status = 'RUNNING'`).
- **Idempotency:** Resending duplicate event logs `IDEMPOTENCY: ... Skipping execution`. Execution count stays strictly 1.

## 5. ML Dataset Preparation
- **Source:** Olist Brazilian E-Commerce public dataset (historical, non-real-time).
- **Rule:** Filtered strictly to `order_status = 'delivered'` (108,660 valid item units across 96,478 orders).
- **Target Formulation:** `product_category` $\times$ `week_start_date` $\rightarrow$ `demand` (Count of delivered items).
- **Final Dataset:** 4,372 rows across 73 categories and 91 continuous calendar weeks.

## 6. Chronological Dataset Split (Zero Leakage)
| Partition | Distinct Weeks | Row Count | Date Horizon | Leakage Check |
| :--- | :--- | :--- | :--- | :--- |
| **Train (70%)** | 63 weeks | 2,795 rows | 2016-09-12 $\rightarrow$ 2018-02-12 | $\text{Train End} < \text{Val Start}$ (**PASS**) |
| **Validation (15%)** | 14 weeks | 807 rows | 2018-02-19 $\rightarrow$ 2018-05-21 | $\text{Val End} < \text{Test Start}$ (**PASS**) |
| **Test (15%)** | 14 weeks | 770 rows | 2018-05-28 $\rightarrow$ 2018-08-27 | $\mathbf{0}$ shared dates across splits (**PASS**) |

## 7. Baseline Model & Empirical Metrics
- **Method:** One-step-ahead rolling previous-period naive forecast ($\hat{y}_{c, t} = y_{c, t-1}$). Deterministic benchmark, **not an AI model**.
- **Missing Periods:** Unobserved category-week demand = 0.

| Model | Partition | Prediction Count | MAE | RMSE | $R^2$ |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Naive Baseline ($T-1$)** | **Validation** | 807 | **7.9802** | **15.3196** | **0.8992** |
| **Naive Baseline ($T-1$)** | **Test** | 770 | **9.4636** | **17.9912** | **0.8351** |

## 8. Testing Summary (100% Green)
- **Total Tests:** **29 / 29 PASS** in `0.100s` via `python -m unittest discover -s tests`.
  - Configuration: 1/1 PASS
  - Objective-1 Unit: 8/8 PASS
  - Chronological Split Unit: 9/9 PASS
  - Naive Baseline Unit: 10/10 PASS
  - Objective-1 Integration: 1/1 PASS

## 9. Current Limitations & Future Roadmap
- **Limitations:** Olist is historical source data; events are generated via a simulation script; the baseline is a heuristic; downstream execution records state without a full Airflow DAG.
- **Future Roadmap:** Feature engineering & XGBoost model; Airflow DAG triggering; Apache Spark ETL; Debezium CDC real-time binlog capture; React operator dashboard & Spring Boot API.
