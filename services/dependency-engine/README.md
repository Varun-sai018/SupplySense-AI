# Dependency Engine Service

## Purpose
Acts as a Kafka consumer that listens to the `dataset-events` topic. For each event, it evaluates whether the pipeline dependencies (e.g., ALL, ANY, QUORUM) are met by checking the state of all required datasets in MySQL. Records its decision (`TRIGGER` or `BLOCK`) in the `pipeline_decisions` table.

## Inputs
- JSON events from the Kafka topic.
- Dependency configurations and dataset states from MySQL.

## Outputs
- Logs the dependency status, formatted decision (including detailed QUORUM breakdown), and cycle reset to stdout.
- Inserts a decision record into the `pipeline_decisions` table in MySQL.
- Creates an execution record in `pipeline_executions` (Status: RUNNING) on TRIGGER.
- Resets relevant dependency datasets in `dataset_metadata` back to `WAITING` upon execution creation for the next cycle.

## Verified Implemented Business Rules (Module 1)
The dependency engine hosts three verified operational and event-driven business rules:

1. **Rule A: High-Velocity Cart Abandonment Engine (`RULE_A_CART_ABANDONMENT`)**
   - **Evaluator:** `services/dependency-engine/rules/rule_cart_abandonment.py`
   - **Condition:** Rolling 5-minute session `dropoff_rate > 0.25` during peak hours (18:00–22:00 BRT / UTC-3).
   - **Action:** Dispatches personalized marketing re-engagement webhook with idempotency token and 15-minute cooldown.

2. **Rule B: Automated Inventory Reorder Pipeline (`RULE_B_INVENTORY_REORDER`)**
   - **Evaluator:** `services/dependency-engine/rules/rule_inventory_reorder.py`
   - **Condition:** SKU `available_quantity <= safe_threshold` (default 20 units) AND primary supplier status is `'Online'`.
   - **Action:** Dispatches supplier reorder API and triggers the `Inventory Reorder Pipeline`.

3. **Rule C: Payment Failure Spike Detection (`RULE_C_PAYMENT_FAILURE_SPIKE`)**
   - **Evaluator:** `services/dependency-engine/rules/rule_payment_spike.py`
   - **Condition:** Rolling 50-transaction window with terminal `success_rate < 0.92`.
   - **Action:** Triggers critical platform payment security/incident alert; auto-resolves when success rate recovers to `>= 92%`.

> **Rule Taxonomy Reconciliation Notice:**
> The preliminary terms *"Forecast Freshness"* and *"Safety-Stock Violation"* were legacy concept drafts from pre-implementation design discussions. They were superseded during implementation by Rule A (Cart Abandonment) and Rule C (Payment Failure Spike) and are **not** present in the codebase or database schemas.

## Dependencies
- MySQL (`supplysense` database)
- Kafka Broker
- `common/database.py`
- `config/settings.py`

## How to Run
From the repository root, run:
```bash
python services/dependency-engine/main.py
```

## Environment Variables Used
- Database: `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`
- Kafka: `KAFKA_BROKER`, `KAFKA_TOPIC_EVENTS`, `KAFKA_CONSUMER_GROUP`

## Database Tables Used
- `pipeline_dependencies` (Read)
- `dependency_datasets` (Read)
- `dataset_metadata` (Read / Update status to WAITING)
- `pipeline_decisions` (Insert)
- `pipeline_executions` (Read for Idempotency / Insert on TRIGGER)
- `business_rule_actions` (Insert / Update for Rule A, B, and C actions)
- `operational_inventory` (Read stock / threshold)
