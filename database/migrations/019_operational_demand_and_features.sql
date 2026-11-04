-- ============================================================================
-- SupplySense AI - Migration 019: Operational Demand Aggregation & Feature Store
-- Phase 3: Operational Demand Aggregation and Feature Engineering
-- ============================================================================

-- 1. Weekly Operational Demand Aggregation Table
CREATE TABLE IF NOT EXISTS operational_demand_weekly (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    execution_id BIGINT NULL,
    product_category VARCHAR(100) NOT NULL,
    year_week VARCHAR(10) NOT NULL,
    week_start_date DATE NOT NULL,
    demand_units INT NOT NULL DEFAULT 0,
    order_count INT NOT NULL DEFAULT 0,
    cancelled_orders_excluded INT NOT NULL DEFAULT 0,
    source_provenance VARCHAR(50) NOT NULL DEFAULT 'ALL_OPERATIONAL',
    min_order_event_time TIMESTAMP(3) NULL,
    max_order_event_time TIMESTAMP(3) NULL,
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    UNIQUE KEY uk_op_demand_cat_week_prov (product_category, year_week, source_provenance),
    INDEX idx_op_demand_week (week_start_date),
    INDEX idx_op_demand_exec (execution_id),
    CONSTRAINT fk_op_demand_execution FOREIGN KEY (execution_id) REFERENCES pipeline_executions(execution_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 2. Operational 11-Feature Store Table
CREATE TABLE IF NOT EXISTS operational_feature_store (
    feature_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    execution_id BIGINT NULL,
    product_category VARCHAR(100) NOT NULL,
    year_week VARCHAR(10) NOT NULL,
    week_start_date DATE NOT NULL,
    lag_1_demand DOUBLE NOT NULL DEFAULT 0.0,
    lag_2_demand DOUBLE NOT NULL DEFAULT 0.0,
    lag_4_demand DOUBLE NOT NULL DEFAULT 0.0,
    rolling_mean_4w DOUBLE NOT NULL DEFAULT 0.0,
    rolling_std_4w DOUBLE NOT NULL DEFAULT 0.0,
    rolling_mean_8w DOUBLE NOT NULL DEFAULT 0.0,
    demand_trend_ratio DOUBLE NOT NULL DEFAULT 1.0,
    product_category_enc INT NOT NULL DEFAULT -1,
    month_of_year INT NOT NULL,
    week_of_year INT NOT NULL,
    quarter INT NOT NULL,
    history_weeks_count INT NOT NULL DEFAULT 0,
    feature_status ENUM('SUFFICIENT_HISTORY', 'INSUFFICIENT_HISTORY', 'UNSEEN_CATEGORY') NOT NULL DEFAULT 'SUFFICIENT_HISTORY',
    source_provenance VARCHAR(50) NOT NULL DEFAULT 'ALL_OPERATIONAL',
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    UNIQUE KEY uk_op_feat_cat_week_prov (product_category, year_week, source_provenance),
    INDEX idx_op_feat_week (week_start_date),
    INDEX idx_op_feat_status (feature_status),
    INDEX idx_op_feat_exec (execution_id),
    CONSTRAINT fk_op_feat_execution FOREIGN KEY (execution_id) REFERENCES pipeline_executions(execution_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 3. Register Operational Demand Features in dataset_metadata
INSERT INTO dataset_metadata (dataset_name, source_type, table_name, current_version, row_count, status)
SELECT 'Operational Demand Features', 'OPERATIONAL', 'operational_feature_store', 0, 0, 'READY'
WHERE NOT EXISTS (
    SELECT 1 FROM dataset_metadata WHERE dataset_name = 'Operational Demand Features'
);

-- 4. Register 'Operational Demand Feature Pipeline' in pipeline_dependencies
INSERT INTO pipeline_dependencies (pipeline_name, condition_type, required_count)
SELECT 'Operational Demand Feature Pipeline', 'ALL', NULL
WHERE NOT EXISTS (
    SELECT 1 FROM pipeline_dependencies WHERE pipeline_name = 'Operational Demand Feature Pipeline'
);

-- 5. Link 'Operational Demand Feature Pipeline' to 'Operational Orders' dataset (dataset_id = 278)
INSERT INTO dependency_datasets (dependency_id, dataset_id)
SELECT pd.dependency_id, dm.dataset_id
FROM pipeline_dependencies pd
JOIN dataset_metadata dm ON dm.dataset_name = 'Operational Orders'
WHERE pd.pipeline_name = 'Operational Demand Feature Pipeline'
  AND NOT EXISTS (
      SELECT 1 FROM dependency_datasets dd
      WHERE dd.dependency_id = pd.dependency_id AND dd.dataset_id = dm.dataset_id
  );
