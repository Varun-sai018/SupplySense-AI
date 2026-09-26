-- Migration 007: Register Inventory Dataset in dataset_metadata
INSERT INTO `dataset_metadata` (`dataset_name`, `source_type`, `table_name`, `current_version`, `row_count`, `status`)
VALUES ('Inventory', 'SQUARE', 'square_inventory', 0, 0, 'WAITING')
ON DUPLICATE KEY UPDATE `source_type` = 'SQUARE';
