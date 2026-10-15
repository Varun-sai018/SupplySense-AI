-- Migration 012: Add Airflow orchestration metadata columns to pipeline_executions table
ALTER TABLE `pipeline_executions`
ADD COLUMN `orchestrator` VARCHAR(50) NOT NULL DEFAULT 'LOCAL' AFTER `status`,
ADD COLUMN `airflow_dag_id` VARCHAR(150) NULL DEFAULT NULL AFTER `orchestrator`,
ADD COLUMN `airflow_run_id` VARCHAR(250) NULL DEFAULT NULL AFTER `airflow_dag_id`,
ADD KEY `idx_execution_airflow_run` (`airflow_dag_id`, `airflow_run_id`);
