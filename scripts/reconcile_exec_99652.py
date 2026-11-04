"""
One-time reconciliation: mark execution #99652 as FAILED.

Context: The Airflow task `validate_input_datasets` in DAG `supplysense_demand_forecast`
failed with ModuleNotFoundError: No module named 'pymysql'.  Because the failure callback
also imports pymysql via common.database, it too crashed before it could set the status.
The execution is therefore stuck in RUNNING.

This script performs a narrowly-scoped, idempotent UPDATE — it touches *only* the single
row where execution_id=99652 AND status='RUNNING'.  If the row is already FAILED/COMPLETED
the UPDATE affects 0 rows and the script exits cleanly.

Safe to run multiple times.  Does NOT delete any row or change any other execution.
"""
import sys
import datetime

# Add project root to path so common.database resolves correctly
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from common.database import get_connection
except ImportError as e:
    print(f"ERROR: Cannot import common.database — {e}")
    sys.exit(1)

TARGET_EXECUTION_ID = 99652
TARGET_AIRFLOW_RUN_ID = "supplysense_exec_99652_attempt_0"
ERROR_MESSAGE = (
    "ModuleNotFoundError: No module named 'pymysql' — Airflow task failed before "
    "reporting status; reconciled manually after dependency fix (pymysql==1.1.1 added "
    "to _PIP_ADDITIONAL_REQUIREMENTS)."
)


def reconcile():
    conn = get_connection()
    try:
        cur = conn.cursor()

        # 1. Read current state first (non-destructive)
        cur.execute(
            "SELECT execution_id, status, airflow_run_id, started_at, completed_at "
            "FROM pipeline_executions WHERE execution_id = %s",
            (TARGET_EXECUTION_ID,),
        )
        row = cur.fetchone()
        if row is None:
            print(f"[SKIP] Execution #{TARGET_EXECUTION_ID} not found in pipeline_executions.")
            return

        print(f"[BEFORE] execution_id={row[0]}  status={row[1]}  "
              f"airflow_run_id={row[2]}  started_at={row[3]}  completed_at={row[4]}")

        if row[1] != "RUNNING":
            print(f"[SKIP] Status is already '{row[1]}' — no update needed.")
            return

        # 2. Perform the narrowly-scoped UPDATE
        now = datetime.datetime.now()
        cur.execute(
            """
            UPDATE pipeline_executions
            SET status              = 'FAILED',
                completed_at        = %s,
                error_message       = %s,
                retry_error_type    = 'DETERMINISTIC_APPLICATION'
            WHERE execution_id   = %s
              AND status         = 'RUNNING'
              AND airflow_run_id = %s
            """,
            (now, ERROR_MESSAGE, TARGET_EXECUTION_ID, TARGET_AIRFLOW_RUN_ID),
        )
        rows_affected = cur.rowcount
        conn.commit()

        if rows_affected == 1:
            print(f"[OK] Updated {rows_affected} row — execution #{TARGET_EXECUTION_ID} "
                  f"is now FAILED (completed_at={now}).")
        else:
            print(f"[WARN] UPDATE affected {rows_affected} rows — check manually.")

        # 3. Verify
        cur.execute(
            "SELECT execution_id, status, completed_at, error_message "
            "FROM pipeline_executions WHERE execution_id = %s",
            (TARGET_EXECUTION_ID,),
        )
        after = cur.fetchone()
        print(f"[AFTER]  execution_id={after[0]}  status={after[1]}  completed_at={after[2]}")
        print(f"         error_message={after[3][:80]}…")

    finally:
        conn.close()


if __name__ == "__main__":
    reconcile()
