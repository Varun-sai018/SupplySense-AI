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
