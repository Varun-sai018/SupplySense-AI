-- Migration 009: Create forecast_results table to persist downstream model forecasts
CREATE TABLE IF NOT EXISTS `forecast_results` (
  `forecast_id` BIGINT NOT NULL AUTO_INCREMENT,
  `execution_id` BIGINT NOT NULL,
  `pipeline_name` VARCHAR(100) NOT NULL,
  `product_category` VARCHAR(150) NOT NULL,
  `forecast_week` DATE NOT NULL,
  `predicted_demand` DECIMAL(10, 4) NOT NULL,
  `model_name` VARCHAR(100) NOT NULL DEFAULT 'XGBoost',
  `model_version` VARCHAR(100) NOT NULL DEFAULT 'xgboost-v1',
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`forecast_id`),
  KEY `idx_forecast_execution_id` (`execution_id`),
  KEY `idx_forecast_week` (`forecast_week`),
  KEY `idx_forecast_category` (`product_category`),
  UNIQUE KEY `idx_unique_execution_forecast` (`execution_id`, `product_category`, `forecast_week`),
  CONSTRAINT `fk_forecast_execution` FOREIGN KEY (`execution_id`) REFERENCES `pipeline_executions` (`execution_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
