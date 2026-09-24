# Olist Data Ingestion Script

## Purpose
Reads the Olist historical dataset (CSV files) and bulk-inserts them into the MySQL `supplysense` database. Dynamically maps pandas datatypes to MySQL datatypes and properly handles dataset timestamps.

## Inputs
- Olist CSV files located in `OLIST_DATA_DIR` (`data/raw_csv/`).

## Outputs
- Auto-generates MySQL tables if they do not exist (or drops and recreates them).
- Populates the tables with data in chunks (2000 rows at a time).

## Dependencies
- MySQL (`supplysense` database)
- `pandas`
- `common/database.py`
- `config/settings.py`

## How to Run
From the repository root, run:
```bash
python data/scripts/upload_olist.py
```

## Environment Variables Used
- Database: `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`
- Data: `OLIST_DATA_DIR`

## Database Tables Used
- Various `olist_*` tables (Creates and inserts).
