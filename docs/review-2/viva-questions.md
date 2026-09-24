# SupplySense AI — Review-2 Evaluator Viva Questions & Answers

This guide prepares the project team for likely technical questions from academic and industry evaluators during Review-2. All answers strictly reflect the verified implementation and data in the repository.

---

### 1. Why did you choose an event-driven architecture instead of a traditional scheduled pipeline?
**Answer:**
Traditional scheduled pipelines (e.g., cron jobs) execute at fixed clock times regardless of whether upstream data has been extracted, transformed, or arrived. If an upstream supplier or order batch is delayed, a scheduled pipeline either runs on incomplete data or fails silently. An event-driven architecture ensures pipelines execute **reactively and strictly when prerequisite data changes are verified**.

### 2. Why use Apache Kafka rather than a simple message queue like RabbitMQ or Celery?
**Answer:**
Kafka provides an append-only, distributed commit log with persistent offset management and high throughput. It decouples event producers (data ingestion jobs) from multiple downstream consumers (dependency orchestrators, audit loggers, real-time monitors). In future phases, Kafka enables replaying event streams from specific offsets to recalculate historical pipeline triggers without data loss.

### 3. What is the role of the Dependency Engine in your architecture?
**Answer:**
The Dependency Engine acts as the intelligent orchestration gatekeeper. While Kafka simply transports update messages, the Dependency Engine maintains the directed dependency graph between pipelines and datasets. When an event arrives, it queries current dataset states in MySQL, evaluates multi-dataset dependency conditions (ALL, ANY, QUORUM), logs decisions, and triggers downstream executions only when conditions are met.

### 4. Why support ALL, ANY, and QUORUM condition types?
**Answer:**
Different data pipelines exhibit different data requirements:
- **`ALL`:** Required for holistic model training (e.g., Demand Forecasting needs Orders, Products, and Sellers).
- **`ANY`:** Used for high-frequency or alert pipelines where an update to any single dataset (e.g., an abrupt price change) warrants immediate downstream execution.
- **`QUORUM`:** Useful for distributed, multi-region architectures where a pipeline can safely execute as soon as a majority threshold (e.g., 3 out of 5 regional feeds) is available.

### 5. What happens when a pipeline dependency is missing or not ready?
**Answer:**
The Dependency Engine outputs a **`BLOCK`** decision and records the exact reason in `pipeline_decisions` (e.g., *"2 of 3 required datasets are READY"*). No record is created in `pipeline_executions`, and downstream workloads are prevented from executing on partial data. When the missing dataset subsequently updates, a new event triggers re-evaluation, transitioning the decision to `TRIGGER`.

### 6. What is idempotency, and how is it implemented in SupplySense AI?
**Answer:**
Idempotency ensures that processing the exact same event multiple times produces the exact same outcome without unintended side effects. In distributed messaging, network retries can deliver duplicate events. We implement idempotency at two layers:
1. **Application layer:** The engine checks whether a `pipeline_executions` record already exists for `(pipeline_name, triggering_event_id)` before firing.
2. **Database layer:** The `pipeline_executions` table enforces a `UNIQUE KEY (pipeline_name, triggering_event_id)`. Duplicates are caught, logged, and skipped.

### 7. Why use MySQL for orchestration metadata instead of a NoSQL database?
**Answer:**
Orchestration metadata requires ACID compliance, foreign key relational integrity (e.g., cascading dependencies between pipelines and datasets), and strict uniqueness constraints for idempotency. Relational SQL provides transaction safety and relational joins between `pipeline_dependencies`, `dependency_datasets`, and `dataset_metadata`.

### 8. Why was the Olist Brazilian E-Commerce dataset chosen?
**Answer:**
Olist provides authentic commercial retail transaction data consisting of ~100,000 orders, 112,000+ itemized purchases, 32,000+ products, and multiple operational entities (orders, items, products, sellers, payments, reviews) spanning from late 2016 through mid-2018. It presents realistic supply chain complexities including variable demand, missing values, and category-level variances.

### 9. Is the Olist dataset a real-time data source?
**Answer:**
**No.** Olist is strictly a historical, static dataset. We do not claim that Olist is a real-time streaming feed. In our architecture, Olist represents the historical source repository from which analytical and ML datasets are extracted. Real-time behavior is simulated via an Event Generator that synthesizes update events over these historical entities.

### 10. How are dataset update events generated in the current system?
**Answer:**
Currently, events are created through our `services/event-generator/main.py` module. When invoked (either interactively or programmatically), it increments the dataset version in `dataset_metadata`, updates row counts, timestamps, and appends a `DATASET_UPDATED` record into `dataset_events`. The Kafka Producer then reads this record and streams it to Kafka. In future production work, this module will be replaced by Change Data Capture (CDC).

### 11. What is the target variable for demand forecasting?
**Answer:**
The target is **`demand`**, defined as the total quantity of product units in delivered orders for a specified product category during a given ISO calendar week.

### 12. Why forecast at the product category level instead of individual product IDs?
**Answer:**
The Olist dataset contains 32,951 unique product IDs across 91 weeks. At the individual product level, the matrix is over 99% sparse—most products appear only 1 or 2 times across the entire 2-year horizon. Aggregating at the `product_category_name` level yields 73 categories with continuous, dense weekly observations (64 categories appear in $\ge 20$ weeks), enabling meaningful time-series forecasting.

### 13. Why use weekly time granularity instead of daily or hourly?
**Answer:**
Daily demand in regional e-commerce fluctuates heavily due to weekend purchasing dips, weekday spikes, and carrier dispatch cutoffs. Weekly aggregation (Monday-to-Sunday ISO weeks) smooths out intra-week noise while providing actionable, mid-term planning horizons suitable for supplier purchase orders and warehouse replenishment.

### 14. Why is a chronological split necessary instead of a standard random train/test split?
**Answer:**
In time-series forecasting, observations have an inherent temporal dependency. If data is shuffled randomly, observations from 2018 would be present in the training set while predicting 2017 in the test set. This constitutes severe temporal data leakage, artificially inflating accuracy by allowing the model to "cheat" using future knowledge. A chronological split ensures the model is trained strictly on the past and tested on the future.

### 15. What is data leakage, and how did you verify it was avoided?
**Answer:**
Data leakage occurs when information from the target or test period inadvertently leaks into the training features or model training process. We verified zero leakage by:
1. Ensuring strict temporal boundaries: $\text{Train End (2018-02-12)} < \text{Val Start (2018-02-19)} < \text{Test Start (2018-05-28)}$.
2. Confirming zero overlapping calendar dates across all three partitions ($\emptyset$ shared dates).
3. Ensuring our baseline features for week $t$ strictly reference observations $\le t-1$.

### 16. Why did you choose a 70% Train / 15% Validation / 15% Test split?
**Answer:**
Out of 91 continuous calendar weeks:
- 70% (63 weeks) provides approximately 1.2 years of continuous training history, capturing multiple quarters and holiday periods (e.g., Black Friday 2017).
- 15% (14 weeks / ~1 full calendar quarter) provides a robust validation horizon for hyperparameter tuning.
- 15% (14 weeks / ~1 full calendar quarter) provides an independent holdout test set for final benchmark reporting.

### 17. What is a Naive Baseline model?
**Answer:**
The Naive Baseline is a previous-period persistence heuristic:
$$\hat{y}_{c, t} = y_{c, t-1}$$
It predicts that next week's category demand will be identical to this week's observed demand. It requires no parameter optimization and serves as the simplest defensible standard for time-series forecasting.

### 18. Why use MAE (Mean Absolute Error) as an evaluation metric?
**Answer:**
MAE measures the average absolute difference between predicted and actual demand in raw units (number of items). For supply chain managers, MAE has direct operational interpretability: an MAE of 9.46 means our forecast misses actual inventory demand by approximately 9.5 units per category per week.

### 19. Why use RMSE (Root Mean Squared Error)?
**Answer:**
RMSE squares the errors before averaging and taking the square root. As a result, it heavily penalizes large forecasting errors (outliers). In inventory management, a massive under-prediction causes catastrophic stockouts, while small errors are absorbed by safety stock. RMSE reflects this operational asymmetry.

### 20. What does an R² (Coefficient of Determination) of 0.8351 on the test set mean?
**Answer:**
$R^2 = 0.8351$ means that 83.51% of the total variance in weekly category demand across the test set is explained by the simple persistence heuristic. This demonstrates strong week-over-week autocorrelation in category-level e-commerce purchasing, establishing a high benchmark that any future ML model must surpass.

### 21. Why do you emphasize that the baseline is "not an AI model"?
**Answer:**
Scientific and engineering rigor requires intellectual honesty. Calling a previous-period persistence rule ($\hat{y}_t = y_{t-1}$) "AI" or "machine learning" is technically inaccurate. It is a deterministic heuristic benchmark designed to quantify whether subsequent machine learning models (like XGBoost) deliver measurable added value.

### 22. What components are fully implemented and verified in Review-2?
**Answer:**
1. Event generation and persistence (`dataset_events`).
2. Apache Kafka event streaming (`dataset-events` topic).
3. Dependency Engine with ALL, ANY, and QUORUM evaluation.
4. Downstream pipeline execution logging (`pipeline_executions`).
5. Application and database-level idempotency protection.
6. Olist data extraction and weekly demand preparation (`forecasting_dataset.csv`, 4,372 rows).
7. Leak-free chronological 70/15/15 dataset splitting.
8. One-step-ahead rolling naive baseline model.
9. Formal regression metric calculations (MAE, RMSE, $R^2$).
10. Complete 29/29 passing test suite.

### 23. What components are planned for future phases after Review-2?
**Answer:**
1. Feature engineering (rolling lags, momentum, seasonality encodings) and **XGBoost Regressor** training.
2. Replacing simulated pipeline execution with an **Apache Airflow** DAG trigger.
3. Integrating **Apache Spark** for high-volume distributed data transformations.
4. Implementing real-time **Change Data Capture (CDC)** via Debezium/Kafka Connect.
5. Building a React front-end monitoring dashboard and Spring Boot API.

### 24. How do you expect XGBoost to outperform the Naive Baseline?
**Answer:**
The Naive Baseline only looks at $t-1$, which means it always lags behind turning points, sudden spikes, or seasonal shifts. An XGBoost model trained on multi-week rolling statistics (e.g., 4-week moving average, 12-week trend, holiday calendar indicators) can anticipate shifts, capture non-linear relationships, and reduce both MAE ($< 9.46$) and RMSE ($< 17.99$).

### 25. How will production Change Data Capture (CDC) replace the simulated Event Generator?
**Answer:**
In production, database transactions on MySQL write to the binary log (`binlog`). A tool like Debezium continuously tails the binlog and automatically publishes row-level change events to Kafka without modifying application source code. The Dependency Engine will consume these real-time CDC events using the exact same Kafka consumer interface already built and verified in Objective-1.
