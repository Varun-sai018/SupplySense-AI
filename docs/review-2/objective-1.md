# Objective-1 Implementation Details

## 1. Objective-1 Statement
"To develop an event-conditioned dependency orchestration mechanism that detects dataset updates, evaluates dependency conditions, and automatically triggers or blocks downstream data pipelines based on data availability."

## 2. Architecture
The architecture is structured as follows:
Dataset Update -> Event Generator -> dataset_events -> Kafka Producer -> Kafka topic (dataset-events) -> Dependency Engine -> Dependency Evaluation -> TRIGGER / BLOCK -> pipeline_decisions -> pipeline_executions

## 3. Dependency Logic
The orchestration logic currently supports three distinct rulesets:
- **ALL**: Triggers only when the `ready_count` precisely matches the `total_required` datasets configured for that pipeline.
- **ANY**: Triggers when `ready_count >= 1`.
- **QUORUM**: Triggers when `ready_count >= required_count` (a dynamically configured subset).

## 4. What is Implemented
- Dataset update event generation (via simulation)
- Event persistence (`dataset_events`)
- Kafka publishing and consumption logic
- Dependency lookup (`pipeline_dependencies`, `dependency_datasets`)
- Evaluation functions (ALL, ANY, QUORUM)
- Decision persistence (`pipeline_decisions`)
- Pipeline execution tracking (`pipeline_executions`)
- Strict Idempotency (Unique constraint blocking duplicate pipeline executions for identical event triggers)

## 5. Known Limitations
- The Event Generator is a manual simulation of dataset updates over historical CSVs; it is not yet detecting real-time upstream CDC logs.
- Kafka is locally hosted and reliant on manual invocation of producers due to Docker constraints on the local developer machine.
- A "TRIGGER" decision merely creates a `RUNNING` record in `pipeline_executions`. The physical downstream Machine Learning pipeline (XGBoost/Spark) does not yet physically run.

## 6. What Remains Planned After Review-2
- Airflow/Spark integration (replacing the simulated pipeline execution with an actual DAG payload).
- Machine Learning models (Dataset split, XGBoost optimization).
- Front-end integration (React Dashboard) and API interface (Spring Boot).
- Cloud deployment and CI/CD operations.
