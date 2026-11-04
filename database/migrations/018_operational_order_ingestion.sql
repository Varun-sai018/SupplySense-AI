-- Migration 018: Operational Order Ingestion Support
-- Updates operational_orders source ENUM and registers 'Operational Orders' in dataset_metadata.

-- 1. Expand source ENUM in operational_orders to support OPERATIONAL_API and MANUAL_IMPORT
ALTER TABLE `operational_orders`
  MODIFY COLUMN `source` ENUM('SIMULATED', 'SQUARE_SANDBOX', 'OPERATIONAL_API', 'MANUAL_IMPORT') NOT NULL DEFAULT 'OPERATIONAL_API';

-- 2. Register 'Operational Orders' dataset in dataset_metadata if not already present
INSERT INTO `dataset_metadata` (`dataset_name`, `source_type`, `table_name`, `current_version`, `row_count`, `status`)
VALUES ('Operational Orders', 'OPERATIONAL', 'operational_orders', 0, 0, 'WAITING')
ON DUPLICATE KEY UPDATE `table_name` = 'operational_orders', `source_type` = 'OPERATIONAL';
