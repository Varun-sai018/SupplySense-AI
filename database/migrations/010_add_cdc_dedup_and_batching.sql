-- Migration 010: Add CDC deduplication coordinates and batch tracking to dataset_events
ALTER TABLE `dataset_events`
  ADD COLUMN `source_file` VARCHAR(255) DEFAULT NULL AFTER `rows_changed`,
  ADD COLUMN `source_pos` BIGINT DEFAULT NULL AFTER `source_file`,
  ADD COLUMN `source_change_id` VARCHAR(255) DEFAULT NULL AFTER `source_pos`,
  ADD COLUMN `batch_id` VARCHAR(100) DEFAULT NULL AFTER `source_change_id`,
  ADD UNIQUE KEY `idx_unique_source_change` (`source_change_id`),
  ADD KEY `idx_dataset_events_batch_id` (`batch_id`),
  ADD KEY `idx_dataset_events_source_coords` (`source_file`, `source_pos`);
