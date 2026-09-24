# Kafka Producer Service

## Purpose
Scans the database for `NEW` events, publishes them to the Kafka topic, and updates their status to `PUBLISHED`. Currently runs once and exits, processing the backlog of events.

## Inputs
- Reads from the `dataset_events` table in MySQL where `event_status = 'NEW'`.

## Outputs
- Publishes JSON messages to the Kafka broker.
- Updates the `dataset_events` table in MySQL, setting `event_status = 'PUBLISHED'` and `processed_at = NOW()`.

## Dependencies
- MySQL (`supplysense` database)
- Kafka Broker
- `common/database.py`
- `config/settings.py`

## How to Run
From the repository root, run:
```bash
python services/kafka-producer/main.py
```

## Environment Variables Used
- Database: `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`
- Kafka: `KAFKA_BROKER`, `KAFKA_TOPIC_EVENTS`

## Database Tables Used
- `dataset_events` (Read/Update)
