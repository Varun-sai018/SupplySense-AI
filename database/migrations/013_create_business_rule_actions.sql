-- Migration 013: Create business_rule_actions table for Rule A (Cart Abandonment),
-- Rule B (Inventory Reorder), and Rule C (Payment Alerts) cross-process idempotency and tracking.

CREATE TABLE IF NOT EXISTS `business_rule_actions` (
  `action_id` BIGINT NOT NULL AUTO_INCREMENT,
  `rule_id` VARCHAR(50) NOT NULL,
  `idempotency_key` VARCHAR(191) NOT NULL,
  `status` VARCHAR(30) NOT NULL,
  `decision_id` BIGINT DEFAULT NULL,
  `execution_id` BIGINT DEFAULT NULL,
  `payload` JSON DEFAULT NULL,
  `response_payload` JSON DEFAULT NULL,
  `error_message` TEXT DEFAULT NULL,
  `retry_count` INT NOT NULL DEFAULT 0,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `resolved_at` DATETIME DEFAULT NULL,
  PRIMARY KEY (`action_id`),
  UNIQUE KEY `idx_unique_rule_idempotency` (`rule_id`, `idempotency_key`),
  KEY `idx_rule_status` (`rule_id`, `status`),
  KEY `idx_action_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
