# Technical Dossier: Apache Airflow Data-Aware Scheduling and Dataset Dependencies

- **Organization:** Apache Software Foundation
- **Reference Documentation:** Apache Airflow Documentation (v2.4+ to v2.9.1)
- **URL:** [https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/datasets.html](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/datasets.html)

---

## 1. Overview
In version 2.4, Apache Airflow introduced Dataset-aware scheduling (`Dataset(uri)`), enabling DAGs to trigger when upstream producer tasks emit dataset update events.

## 2. Architectural Comparison with SupplySense AI
- **Airflow Datasets:** Restricted to basic conjunction (all declared datasets updated by upstream DAGs) or basic disjunction. Does not support fractional quorum semantics (e.g. 3 of 4 datasets ready) without custom logic.
- **Worker Slot Consumption:** Polling sensor DAGs hold open worker thread slots, causing queue starvation during high latency.
- **SupplySense AI Advantage:** Operates as a reactive state machine with an AST parser supporting `ALL`, `QUORUM(m, n)`, and `ANY`, executing in < 12ms without holding worker threads.
