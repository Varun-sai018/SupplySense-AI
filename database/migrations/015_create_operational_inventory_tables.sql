-- Migration 015: Create Operational Inventory, Orders, Suppliers, and Replenishment Tables
-- Establishes the operational data model for event-driven inventory monitoring and replenishment.

-- 1. Operational Inventory: Physical stock, reservations, and reorder levels
CREATE TABLE IF NOT EXISTS `operational_inventory` (
  `sku` VARCHAR(64) NOT NULL,
  `warehouse_id` VARCHAR(32) NOT NULL DEFAULT 'WH-SP-01',
  `on_hand_quantity` INT NOT NULL DEFAULT 0,
  `reserved_quantity` INT NOT NULL DEFAULT 0,
  `available_quantity` INT GENERATED ALWAYS AS (`on_hand_quantity` - `reserved_quantity`) STORED,
  `reorder_threshold` INT NOT NULL DEFAULT 20,
  `reorder_target_quantity` INT NOT NULL DEFAULT 100,
  `safety_stock_buffer` INT NOT NULL DEFAULT 10,
  `data_origin` ENUM('HISTORICAL_SEED', 'SIMULATED', 'SANDBOX') NOT NULL DEFAULT 'SIMULATED',
  `version` BIGINT NOT NULL DEFAULT 0,
  `last_updated_at` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`sku`, `warehouse_id`),
  INDEX `idx_inventory_reorder_eval` (`available_quantity`, `reorder_threshold`),
  INDEX `idx_inventory_data_origin` (`data_origin`),
  CONSTRAINT `chk_on_hand_qty_non_negative` CHECK (`on_hand_quantity` >= 0),
  CONSTRAINT `chk_reserved_qty_non_negative` CHECK (`reserved_quantity` >= 0),
  CONSTRAINT `chk_reserved_lte_on_hand` CHECK (`reserved_quantity` <= `on_hand_quantity`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 2. Operational Orders: Simulated orders and sandbox checkouts (isolated from historical Olist orders)
CREATE TABLE IF NOT EXISTS `operational_orders` (
  `order_id` VARCHAR(64) NOT NULL,
  `sku` VARCHAR(64) NOT NULL,
  `warehouse_id` VARCHAR(32) NOT NULL DEFAULT 'WH-SP-01',
  `quantity` INT NOT NULL,
  `unit_price` DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
  `order_status` ENUM('PENDING', 'ALLOCATED', 'BACKORDERED', 'COMPLETED', 'CANCELLED') NOT NULL DEFAULT 'PENDING',
  `source` ENUM('SIMULATED', 'SQUARE_SANDBOX') NOT NULL DEFAULT 'SIMULATED',
  `idempotency_key` VARCHAR(128) NOT NULL,
  `event_time` TIMESTAMP(3) NOT NULL,
  `created_at` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`order_id`),
  UNIQUE KEY `idx_unique_order_idempotency` (`idempotency_key`),
  INDEX `idx_operational_orders_sku` (`sku`),
  INDEX `idx_operational_orders_created` (`created_at`),
  INDEX `idx_operational_orders_status` (`order_status`),
  CONSTRAINT `chk_order_qty_positive` CHECK (`quantity` > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 3. Operational Suppliers: Active supplier health, reliability scores, and status
CREATE TABLE IF NOT EXISTS `operational_suppliers` (
  `supplier_id` VARCHAR(64) NOT NULL,
  `supplier_name` VARCHAR(128) NOT NULL,
  `status` ENUM('Online', 'Degraded', 'Offline') NOT NULL DEFAULT 'Online',
  `api_endpoint` VARCHAR(255) DEFAULT NULL,
  `reliability_score` DECIMAL(4, 3) NOT NULL DEFAULT 0.980,
  `average_lead_time_days` INT NOT NULL DEFAULT 2,
  `last_health_check_at` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`supplier_id`),
  INDEX `idx_supplier_status` (`status`),
  CONSTRAINT `chk_supplier_lead_time_positive` CHECK (`average_lead_time_days` > 0),
  CONSTRAINT `chk_supplier_reliability_range` CHECK (`reliability_score` >= 0.000 AND `reliability_score` <= 1.000)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 4. Supplier Catalog Availability: SKU-level availability constraints and reorder parameters
CREATE TABLE IF NOT EXISTS `supplier_catalog_availability` (
  `supplier_id` VARCHAR(64) NOT NULL,
  `sku` VARCHAR(64) NOT NULL,
  `supply_availability` ENUM('IN_STOCK', 'LOW_STOCK', 'OUT_OF_STOCK') NOT NULL DEFAULT 'IN_STOCK',
  `unit_reorder_cost` DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
  `min_reorder_quantity` INT NOT NULL DEFAULT 10,
  `lead_time_hours` INT NOT NULL DEFAULT 24,
  `last_updated_at` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`supplier_id`, `sku`),
  INDEX `idx_supplier_sku` (`sku`),
  INDEX `idx_supplier_availability` (`sku`, `supply_availability`),
  CONSTRAINT `fk_supp_cat_supplier` FOREIGN KEY (`supplier_id`) REFERENCES `operational_suppliers` (`supplier_id`) ON DELETE CASCADE,
  CONSTRAINT `chk_min_reorder_qty_positive` CHECK (`min_reorder_quantity` > 0),
  CONSTRAINT `chk_unit_cost_non_negative` CHECK (`unit_reorder_cost` >= 0.00),
  CONSTRAINT `chk_lead_time_hours_positive` CHECK (`lead_time_hours` > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 5. Replenishment Orders: Links decision and action ledger to physical shipment lifecycle
CREATE TABLE IF NOT EXISTS `replenishment_orders` (
  `replenishment_id` VARCHAR(64) NOT NULL,
  `action_id` BIGINT NOT NULL,
  `decision_id` BIGINT NOT NULL,
  `execution_id` BIGINT DEFAULT NULL,
  `sku` VARCHAR(64) NOT NULL,
  `warehouse_id` VARCHAR(32) NOT NULL DEFAULT 'WH-SP-01',
  `supplier_id` VARCHAR(64) NOT NULL,
  `quantity` INT NOT NULL,
  `status` ENUM('DISPATCHED', 'CONFIRMED', 'IN_TRANSIT', 'DELIVERED', 'FAILED', 'CANCELLED') NOT NULL DEFAULT 'DISPATCHED',
  `supplier_order_reference` VARCHAR(128) DEFAULT NULL,
  `dispatched_at` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `estimated_delivery_at` TIMESTAMP(3) NOT NULL,
  `delivered_at` TIMESTAMP(3) NULL DEFAULT NULL,
  PRIMARY KEY (`replenishment_id`),
  UNIQUE KEY `idx_replenish_action_id` (`action_id`),
  INDEX `idx_replenish_status` (`status`),
  INDEX `idx_replenish_sku_status` (`sku`, `status`),
  INDEX `idx_replenish_delivery` (`estimated_delivery_at`, `status`),
  CONSTRAINT `fk_replenish_action` FOREIGN KEY (`action_id`) REFERENCES `business_rule_actions` (`action_id`) ON DELETE RESTRICT,
  CONSTRAINT `fk_replenish_decision` FOREIGN KEY (`decision_id`) REFERENCES `pipeline_decisions` (`decision_id`) ON DELETE RESTRICT,
  CONSTRAINT `fk_replenish_execution` FOREIGN KEY (`execution_id`) REFERENCES `pipeline_executions` (`execution_id`) ON DELETE SET NULL,
  CONSTRAINT `fk_replenish_supplier` FOREIGN KEY (`supplier_id`) REFERENCES `operational_suppliers` (`supplier_id`) ON DELETE RESTRICT,
  CONSTRAINT `chk_replenish_qty_positive` CHECK (`quantity` > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
