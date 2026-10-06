-- Migration 011: Add retry tracking columns to pipeline_executions table
ALTER TABLE `pipeline_executions`
ADD COLUMN `retry_count` INT NOT NULL DEFAULT 0 AFTER `status`,
ADD COLUMN `max_retries` INT NOT NULL DEFAULT 3 AFTER `retry_count`,
ADD COLUMN `next_retry_at` DATETIME NULL DEFAULT NULL AFTER `completed_at`,
ADD COLUMN `retry_error_type` VARCHAR(100) NULL DEFAULT NULL AFTER `next_retry_at`,
ADD KEY `idx_retry_status` (`status`, `next_retry_at`, `retry_count`);
