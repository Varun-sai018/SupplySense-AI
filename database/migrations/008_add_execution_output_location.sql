-- Migration 008: Add output_location column to pipeline_executions table
ALTER TABLE `pipeline_executions`
ADD COLUMN `output_location` VARCHAR(500) DEFAULT NULL AFTER `error_message`;
