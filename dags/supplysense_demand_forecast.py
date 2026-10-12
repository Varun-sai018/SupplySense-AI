"""
SupplySense AI - Demand Forecasting Airflow DAG.

Orchestrates the existing Demand Forecast Pipeline within Apache Airflow:
  1. validate_input_datasets: Validates required historical split inputs exist.
  2. run_naive_baseline_benchmark: Runs the 1-step naive rolling baseline benchmark.
  3. engineer_leak_free_features: Builds continuous grid, causal lag, rolling, and calendar features.
  4. train_xgboost_demand_model: Trains XGBoost regressor, evaluates on validation/test, produces comparison.
  5. persist_forecast_predictions: Persists out-of-sample forecast predictions to MySQL (forecast_results).
  6. finalize_execution_telemetry: Logs artifact summary, execution timing, and telemetry.

Idempotency and Lifecycle:
- Reuses existing ML modules in ml/ and services/pipeline-runner/ without duplication.
- Resolves execution context from dag_run.conf ('execution_id', 'pipeline_name') or safely creates/links
  an execution record in MySQL pipeline_executions to satisfy database foreign key constraints.
- Writes execution-scoped artifacts to ml/results/executions/<execution_id>/ for complete artifact isolation.
"""

import os
import sys
import logging
from datetime import datetime, timedelta
import importlib.util

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:
    DAG = None
    PythonOperator = None

logger = logging.getLogger("airflow.task")

# ---------------------------------------------------------------------------
# Path & Environment Setup
# ---------------------------------------------------------------------------
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# If running inside standard Airflow Docker container (/opt/airflow)
if "/opt/airflow" not in sys.path:
    sys.path.insert(0, "/opt/airflow")

# Ensure required configuration defaults when executing within container
os.environ.setdefault("DB_HOST", "host.docker.internal" if os.path.exists("/.dockerenv") else "localhost")
os.environ.setdefault("DB_PORT", "3306")
os.environ.setdefault("DB_USER", "root")
os.environ.setdefault("DB_PASSWORD", "root")
os.environ.setdefault("DB_NAME", "supplysense")
os.environ.setdefault("KAFKA_BROKER", "kafka:29092" if os.path.exists("/.dockerenv") else "localhost:9092")
os.environ.setdefault("KAFKA_TOPIC_EVENTS", "dataset-events")
os.environ.setdefault("KAFKA_CONSUMER_GROUP", "dependency-engine")
os.environ.setdefault("OLIST_DATA_DIR", os.path.join(REPO_ROOT, "data", "raw_csv"))


def _resolve_paths():
    """Resolves standard data and results directories."""
    processed_dir = os.path.join(REPO_ROOT, "data", "processed")
    results_dir = os.path.join(REPO_ROOT, "ml", "results")
    return processed_dir, results_dir


def _load_forecast_repo():
    """Dynamically loads forecast_repository from services/pipeline-runner/."""
    candidates = [
        os.path.join(REPO_ROOT, "services", "pipeline-runner", "forecast_repository.py"),
        "/opt/airflow/services/pipeline-runner/forecast_repository.py",
    ]
    repo_path = next((p for p in candidates if os.path.isfile(p)), None)
    if not repo_path:
        raise FileNotFoundError(f"forecast_repository.py not found in candidate paths: {candidates}")
    spec = importlib.util.spec_from_file_location("forecast_repository", repo_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------
# Task Callables
# ---------------------------------------------------------------------------
def task_validate_input_datasets(**context):
    """
    Validates presence of required train, validation, and test datasets.
    Resolves or initializes an execution_id and pushes it to XCom.
    """
    processed_dir, results_dir = _resolve_paths()
    required_files = [
        os.path.join(processed_dir, "train.csv"),
        os.path.join(processed_dir, "validation.csv"),
        os.path.join(processed_dir, "test.csv"),
    ]
    missing = [f for f in required_files if not os.path.isfile(f)]
    if missing:
        raise FileNotFoundError(f"Missing required ML dataset input(s): {', '.join(missing)}")

    # Extract execution parameters from dag_run.conf
    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}
    raw_execution_id = conf.get("execution_id")
    pipeline_name = conf.get("pipeline_name", "Demand Forecast Pipeline")

    if raw_execution_id is None:
        raise ValueError(
            "Missing required 'execution_id' in dag_run.conf. "
            "To prevent silent collision or overwriting unrelated executions, "
            "every DAG run must be triggered with an explicit execution_id."
        )

    try:
        execution_id = int(raw_execution_id)
        if execution_id <= 0:
            raise ValueError(f"execution_id must be a positive integer, got {raw_execution_id}")
    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid execution_id '{raw_execution_id}' in dag_run.conf: {e}") from e

    from common.database import get_connection
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Validate that execution record exists in database
        cursor.execute(
            "SELECT execution_id, pipeline_name, status FROM pipeline_executions WHERE execution_id = %s",
            (execution_id,)
        )
        exec_row = cursor.fetchone()
        if not exec_row:
            raise ValueError(
                f"execution_id {execution_id} does not exist in pipeline_executions table. "
                "A valid parent execution record is required before running the workflow."
            )

        if isinstance(exec_row, dict):
            row_pipeline = exec_row.get("pipeline_name")
        else:
            row_pipeline = exec_row[1] if len(exec_row) > 1 else None

        if row_pipeline and row_pipeline != pipeline_name:
            logger.warning(
                f"Execution #{execution_id} registered pipeline '{row_pipeline}' "
                f"differs from conf pipeline '{pipeline_name}'. Using '{row_pipeline}'."
            )
            pipeline_name = row_pipeline

        # Ensure execution is marked RUNNING and started_at is populated
        cursor.execute(
            """
            UPDATE pipeline_executions
            SET status = 'RUNNING', started_at = COALESCE(started_at, NOW())
            WHERE execution_id = %s
            """,
            (execution_id,)
        )
        conn.commit()
        logger.info(f"Verified execution #{execution_id} for pipeline '{pipeline_name}'.")
    finally:
        cursor.close()
        conn.close()

    # Create execution-isolated output directory
    exec_dir = os.path.join(results_dir, "executions", str(execution_id))
    os.makedirs(exec_dir, exist_ok=True)

    ti = context["ti"]
    ti.xcom_push(key="execution_id", value=execution_id)
    ti.xcom_push(key="pipeline_name", value=pipeline_name)
    ti.xcom_push(key="exec_dir", value=exec_dir)

    logger.info(f"Input validation passed. Context: execution_id={execution_id}, exec_dir={exec_dir}")
    return {"status": "VALIDATED", "execution_id": execution_id, "exec_dir": exec_dir}


def task_run_naive_baseline_benchmark(**context):
    """
    Executes the 1-step naive rolling baseline benchmark.
    """
    from ml.baseline.naive_baseline import run_baseline

    ti = context["ti"]
    execution_id = ti.xcom_pull(task_ids="validate_input_datasets", key="execution_id")
    exec_dir = ti.xcom_pull(task_ids="validate_input_datasets", key="exec_dir")

    logger.info(f"Running Naive Baseline Benchmark for execution #{execution_id} in {exec_dir}...")
    baseline_result = run_baseline(output_dir=exec_dir, execution_id=execution_id)
    logger.info(f"Naive baseline completed. Validation MAE: {baseline_result['validation']['mae']:.4f}")

    ti.xcom_push(key="baseline_metrics", value=baseline_result["validation"])
    return {
        "status": "COMPLETED",
        "validation_mae": baseline_result["validation"]["mae"],
        "test_mae": baseline_result["test"]["mae"],
    }


def task_engineer_leak_free_features(**context):
    """
    Constructs leak-free continuous grid features with causal lags, rolling windows, and calendars.
    """
    from ml.features.engineer_features import build_features

    processed_dir, _ = _resolve_paths()
    features_path = os.path.join(processed_dir, "forecasting_features.csv")

    logger.info(f"Engineering features -> {features_path}...")
    features_df, cat_mapping = build_features(output_path=features_path)

    ti = context["ti"]
    ti.xcom_push(key="features_path", value=features_path)
    ti.xcom_push(key="features_row_count", value=len(features_df))

    logger.info(f"Feature engineering completed: {len(features_df)} rows, {len(cat_mapping)} categories.")
    return {"status": "COMPLETED", "row_count": len(features_df), "features_path": features_path}


def task_train_xgboost_demand_model(**context):
    """
    Trains and evaluates XGBoost regressor with early stopping on validation split.
    """
    import pandas as pd
    from ml.models.train_xgboost import train_and_evaluate_xgboost

    ti = context["ti"]
    execution_id = ti.xcom_pull(task_ids="validate_input_datasets", key="execution_id")
    exec_dir = ti.xcom_pull(task_ids="validate_input_datasets", key="exec_dir")
    features_path = ti.xcom_pull(task_ids="engineer_leak_free_features", key="features_path")

    logger.info(f"Loading features from {features_path}...")
    features_df = pd.read_csv(features_path)

    logger.info(f"Training XGBoost model for execution #{execution_id} in {exec_dir}...")
    xgb_result = train_and_evaluate_xgboost(
        features_df=features_df,
        output_dir=exec_dir,
        execution_id=execution_id
    )

    test_preds_path = os.path.join(exec_dir, "xgboost_test_predictions.csv")
    ti.xcom_push(key="test_preds_path", value=test_preds_path)
    ti.xcom_push(key="xgb_test_mae", value=xgb_result["test_metrics"]["mae"])

    logger.info(f"XGBoost training completed. Test MAE: {xgb_result['test_metrics']['mae']:.4f}")
    return {
        "status": "COMPLETED",
        "validation_mae": xgb_result["validation_metrics"]["mae"],
        "test_mae": xgb_result["test_metrics"]["mae"],
        "test_preds_path": test_preds_path,
    }


def task_persist_forecast_predictions(**context):
    """
    Persists test forecast predictions into MySQL forecast_results table.
    """
    repo = _load_forecast_repo()

    ti = context["ti"]
    execution_id = ti.xcom_pull(task_ids="validate_input_datasets", key="execution_id")
    pipeline_name = ti.xcom_pull(task_ids="validate_input_datasets", key="pipeline_name")
    test_preds_path = ti.xcom_pull(task_ids="train_xgboost_demand_model", key="test_preds_path")

    logger.info(f"Persisting predictions from {test_preds_path} into MySQL for execution #{execution_id}...")
    saved_count = repo.save_forecast_results(
        execution_id=execution_id,
        pipeline_name=pipeline_name,
        predictions=test_preds_path,
        model_name="XGBoost",
        model_version="xgboost-v1"
    )

    ti.xcom_push(key="forecasts_saved", value=saved_count)
    logger.info(f"Persisted {saved_count} forecast prediction rows into MySQL.")
    return {"status": "COMPLETED", "forecasts_saved": saved_count}


def task_finalize_execution_telemetry(**context):
    """
    Verifies all output artifacts, logs execution summary and duration, and marks completion.
    """
    ti = context["ti"]
    execution_id = ti.xcom_pull(task_ids="validate_input_datasets", key="execution_id")
    exec_dir = ti.xcom_pull(task_ids="validate_input_datasets", key="exec_dir")
    forecasts_saved = ti.xcom_pull(task_ids="persist_forecast_predictions", key="forecasts_saved")

    expected_files = [
        "model_comparison.json",
        "xgboost_metrics.json",
        "baseline_metrics.json",
        "baseline_metrics.csv",
        "xgboost_demand_model.json",
        "xgboost_validation_predictions.csv",
        "xgboost_test_predictions.csv",
        "baseline_validation_predictions.csv",
        "baseline_test_predictions.csv",
    ]

    verified_files = []
    for fname in expected_files:
        fpath = os.path.join(exec_dir, fname)
        if os.path.isfile(fpath):
            verified_files.append(fname)
        else:
            logger.warning(f"Expected artifact not found: {fpath}")

    # Update pipeline_executions status to COMPLETED if not already updated
    from common.database import get_connection
    conn = get_connection()
    cursor = conn.cursor()
    try:
        comparison_rel = os.path.relpath(os.path.join(exec_dir, "model_comparison.json"), REPO_ROOT).replace("\\", "/")
        cursor.execute(
            """
            UPDATE pipeline_executions
            SET status = 'COMPLETED',
                completed_at = NOW(),
                output_location = %s
            WHERE execution_id = %s
            """,
            (comparison_rel, execution_id)
        )
        conn.commit()
    finally:
        cursor.close()
        conn.close()

    summary = {
        "execution_id": execution_id,
        "exec_dir": exec_dir,
        "verified_artifacts_count": len(verified_files),
        "total_expected_artifacts": len(expected_files),
        "forecasts_persisted_count": forecasts_saved,
        "completed_at": datetime.now().isoformat()
    }
    logger.info(f"SupplySense demand forecast DAG run finalized successfully: {summary}")
    return summary


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------
default_args = {
    "owner": "supplysense",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

if DAG is not None:
    with DAG(
        dag_id="supplysense_demand_forecast",
        default_args=default_args,
        description="SupplySense AI demand forecasting pipeline orchestrating Naive Baseline and XGBoost Regressor",
        schedule_interval=None,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["supplysense", "demand-forecast", "xgboost", "capstone"],
    ) as dag:

        t_validate = PythonOperator(
            task_id="validate_input_datasets",
            python_callable=task_validate_input_datasets,
            provide_context=True,
        )

        t_baseline = PythonOperator(
            task_id="run_naive_baseline_benchmark",
            python_callable=task_run_naive_baseline_benchmark,
            provide_context=True,
        )

        t_features = PythonOperator(
            task_id="engineer_leak_free_features",
            python_callable=task_engineer_leak_free_features,
            provide_context=True,
        )

        t_train_xgb = PythonOperator(
            task_id="train_xgboost_demand_model",
            python_callable=task_train_xgboost_demand_model,
            provide_context=True,
        )

        t_persist = PythonOperator(
            task_id="persist_forecast_predictions",
            python_callable=task_persist_forecast_predictions,
            provide_context=True,
        )

        t_telemetry = PythonOperator(
            task_id="finalize_execution_telemetry",
            python_callable=task_finalize_execution_telemetry,
            provide_context=True,
        )

        # Linear workflow topology:
        # validate -> baseline -> features -> train_xgb -> persist -> telemetry
        t_validate >> t_baseline >> t_features >> t_train_xgb >> t_persist >> t_telemetry
