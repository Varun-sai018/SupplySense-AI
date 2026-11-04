-- ============================================================================
-- SupplySense AI - Migration 020: Operational Forward Forecast Results
-- Phase 4: Operational XGBoost Forward Forecasting
-- ============================================================================

-- 1. Operational Forecast Results Table
CREATE TABLE IF NOT EXISTS operational_forecast_results (
    operational_forecast_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    execution_id BIGINT NULL,
    product_category VARCHAR(150) NOT NULL,
    forecast_week DATE NOT NULL,
    year_week VARCHAR(10) NOT NULL,
    predicted_demand DECIMAL(10, 4) NOT NULL,
    model_name VARCHAR(100) NOT NULL DEFAULT 'XGBoost',
    model_version VARCHAR(100) NOT NULL DEFAULT 'xgboost-v1',
    source_feature_execution_id BIGINT NULL,
    data_provenance VARCHAR(50) NOT NULL DEFAULT 'OPERATIONAL',
    forecast_horizon ENUM('7_DAYS', '14_DAYS') NOT NULL DEFAULT '7_DAYS',
    step_ahead INT NOT NULL DEFAULT 1,
    inference_status ENUM('SUCCESS', 'INSUFFICIENT_HISTORY', 'COLD_START_FALLBACK', 'UNSEEN_CATEGORY', 'FAILED') NOT NULL DEFAULT 'SUCCESS',
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    UNIQUE KEY uk_op_forecast_cat_week_horizon (product_category, forecast_week, forecast_horizon, execution_id),
    INDEX idx_op_forecast_cat (product_category),
    INDEX idx_op_forecast_week (forecast_week),
    INDEX idx_op_forecast_exec (execution_id),
    INDEX idx_op_forecast_status (inference_status),
    CONSTRAINT fk_op_forecast_execution FOREIGN KEY (execution_id) REFERENCES pipeline_executions(execution_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 2. Register Operational Forecasts in dataset_metadata
INSERT INTO dataset_metadata (dataset_name, source_type, table_name, current_version, row_count, status)
SELECT 'Operational Forecasts', 'OPERATIONAL', 'operational_forecast_results', 0, 0, 'READY'
WHERE NOT EXISTS (
    SELECT 1 FROM dataset_metadata WHERE dataset_name = 'Operational Forecasts'
);

-- 3. Register 'Operational Demand Forecast Pipeline' in pipeline_dependencies
INSERT INTO pipeline_dependencies (pipeline_name, condition_type, required_count)
SELECT 'Operational Demand Forecast Pipeline', 'ALL', NULL
WHERE NOT EXISTS (
    SELECT 1 FROM pipeline_dependencies WHERE pipeline_name = 'Operational Demand Forecast Pipeline'
);

-- 4. Link 'Operational Demand Forecast Pipeline' to 'Operational Demand Features' dataset (dataset_id = 279)
INSERT INTO dependency_datasets (dependency_id, dataset_id)
SELECT pd.dependency_id, dm.dataset_id
FROM pipeline_dependencies pd
JOIN dataset_metadata dm ON dm.dataset_name = 'Operational Demand Features'
WHERE pd.pipeline_name = 'Operational Demand Forecast Pipeline'
  AND NOT EXISTS (
      SELECT 1 FROM dependency_datasets dd
      WHERE dd.dependency_id = pd.dependency_id AND dd.dataset_id = dm.dataset_id
  );
