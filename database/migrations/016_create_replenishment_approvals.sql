-- Migration 016: Create Replenishment Approvals and Audit Trail Tables (Phase 3E)
-- Establishes a durable, human-in-the-loop review and approval workflow for
-- forecast-driven replenishment recommendations prior to supplier order dispatch.

CREATE TABLE IF NOT EXISTS `replenishment_approvals` (
  `approval_id` VARCHAR(64) NOT NULL,
  `recommendation_id` VARCHAR(128) NOT NULL,
  `scope_type` ENUM('SKU', 'CATEGORY') NOT NULL,
  `scope_id` VARCHAR(128) NOT NULL,
  `sku` VARCHAR(64) NOT NULL,
  `category` VARCHAR(150) DEFAULT NULL,
  `warehouse_id` VARCHAR(32) NOT NULL DEFAULT 'WH-SP-01',
  `requested_quantity` INT NOT NULL,
  `status` ENUM('PENDING_REVIEW', 'APPROVED', 'REJECTED', 'DISPATCHED', 'FAILED') NOT NULL DEFAULT 'PENDING_REVIEW',
  `submitted_by` VARCHAR(64) NOT NULL,
  `submitted_at` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `reviewed_by` VARCHAR(64) DEFAULT NULL,
  `reviewed_at` TIMESTAMP(3) NULL DEFAULT NULL,
  `rejection_reason` TEXT DEFAULT NULL,
  `idempotency_key` VARCHAR(128) NOT NULL,
  `execution_id` BIGINT NOT NULL,
  `snapshot_version` BIGINT DEFAULT 0,
  `snapshot_available_quantity` INT NOT NULL,
  `supplier_id` VARCHAR(64) NOT NULL,
  `action_id` BIGINT DEFAULT NULL,
  `replenishment_id` VARCHAR(64) DEFAULT NULL,
  `error_message` TEXT DEFAULT NULL,
  `created_at` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`approval_id`),
  UNIQUE KEY `idx_approval_idempotency` (`idempotency_key`),
  INDEX `idx_approval_status` (`status`),
  INDEX `idx_approval_recommendation` (`recommendation_id`),
  INDEX `idx_approval_sku` (`sku`),
  INDEX `idx_approval_created` (`created_at`),
  CONSTRAINT `chk_approval_qty_positive` CHECK (`requested_quantity` > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `replenishment_approval_audit` (
  `audit_id` BIGINT NOT NULL AUTO_INCREMENT,
  `approval_id` VARCHAR(64) NOT NULL,
  `from_status` VARCHAR(32) DEFAULT NULL,
  `to_status` VARCHAR(32) NOT NULL,
  `actor_id` VARCHAR(64) NOT NULL,
  `actor_role` VARCHAR(64) NOT NULL,
  `notes` TEXT DEFAULT NULL,
  `created_at` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`audit_id`),
  INDEX `idx_audit_approval_id` (`approval_id`),
  INDEX `idx_audit_created_at` (`created_at`),
  CONSTRAINT `fk_audit_approval` FOREIGN KEY (`approval_id`) REFERENCES `replenishment_approvals` (`approval_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
