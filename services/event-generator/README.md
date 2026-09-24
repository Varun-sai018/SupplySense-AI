# Event Generator Service

## Purpose
Simulates dataset updates by manually generating events. It increments the dataset version, updates the dataset metadata, and inserts a `NEW` event into the `dataset_events` table.

## Inputs
- Interactive CLI prompt for dataset name and number of rows changed.

## Outputs
- Updates `dataset_metadata` table in MySQL.
- Inserts a record into the `dataset_events` table in MySQL.

## Dependencies
- MySQL (`supplysense` database)
- `common/database.py`
- `config/settings.py`

## How to Run
From the repository root, run:
```bash
python services/event-generator/main.py
```

## Environment Variables Used
- `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`

## Database Tables Used
- `dataset_metadata` (Read/Update)
- `dataset_events` (Insert)
