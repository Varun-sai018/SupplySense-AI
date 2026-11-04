-- SupplySense AI: Migration 017 - End-to-End Simulation Tables
-- Dedicated isolated tables for simulated orders, inventory snapshots,
-- replenishment orders, forecast results, and simulation run tracking.
-- Invariants:
--   - Zero modification to operational or historical tables.
--   - All simulated records are linked via simulation_run_id.

CREATE TABLE IF NOT EXISTS sim_runs (
  simulation_run_id   VARCHAR(64)   PRIMARY KEY,
  scenario_id         VARCHAR(50)   NOT NULL DEFAULT 'E2E_ORDERS_INVENTORY',
  status              VARCHAR(30)   NOT NULL DEFAULT 'PENDING',
  orders_generated    INT           NOT NULL DEFAULT 0,
  sku_count           INT           NOT NULL DEFAULT 0,
  low_stock_count     INT           NOT NULL DEFAULT 0,
  forecast_count      INT           NOT NULL DEFAULT 0,
  replenishment_count INT           NOT NULL DEFAULT 0,
  started_at          DATETIME      NULL,
  completed_at        DATETIME      NULL,
  error_message       TEXT          NULL,
  created_at          DATETIME      DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_sim_run_status (status),
  INDEX idx_sim_run_created (created_at)
);

CREATE TABLE IF NOT EXISTS sim_orders (
  sim_order_id      VARCHAR(64)     PRIMARY KEY,
  simulation_run_id VARCHAR(64)     NOT NULL,
  product_category  VARCHAR(150)    NOT NULL,
  product_id        VARCHAR(64)     NOT NULL,
  quantity          INT             NOT NULL DEFAULT 1,
  unit_price        DECIMAL(10,2)   NOT NULL,
  total_price       DECIMAL(10,2)   NOT NULL,
  order_status      VARCHAR(30)     NOT NULL DEFAULT 'delivered',
  purchase_date     DATE            NOT NULL,
  week_start_date   DATE            NOT NULL,
  year_week         VARCHAR(12)     NOT NULL,
  created_at        DATETIME        DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_sim_ord_run   (simulation_run_id),
  INDEX idx_sim_ord_cat   (product_category),
  INDEX idx_sim_ord_week  (week_start_date)
);

CREATE TABLE IF NOT EXISTS sim_inventory_snapshots (
  snapshot_id         BIGINT        PRIMARY KEY AUTO_INCREMENT,
  simulation_run_id   VARCHAR(64)   NOT NULL,
  sku                 VARCHAR(64)   NOT NULL,
  warehouse_id        VARCHAR(32)   NOT NULL DEFAULT 'SIM-WH-01',
  on_hand_quantity    INT           NOT NULL,
  reserved_quantity   INT           NOT NULL DEFAULT 0,
  reorder_threshold   INT           NOT NULL DEFAULT 15,
  reorder_target_qty  INT           NOT NULL DEFAULT 50,
  is_below_threshold  TINYINT(1)    NOT NULL DEFAULT 0,
  created_at          DATETIME      DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_sim_inv_run (simulation_run_id),
  INDEX idx_sim_inv_sku (sku)
);

CREATE TABLE IF NOT EXISTS sim_replenishment_orders (
  sim_replenishment_id  VARCHAR(64)   PRIMARY KEY,
  simulation_run_id     VARCHAR(64)   NOT NULL,
  execution_id          BIGINT        NULL,
  sku                   VARCHAR(64)   NOT NULL,
  warehouse_id          VARCHAR(32)   NOT NULL DEFAULT 'SIM-WH-01',
  supplier_id           VARCHAR(64)   NOT NULL,
  quantity              INT           NOT NULL,
  status                VARCHAR(30)   NOT NULL DEFAULT 'CONFIRMED',
  dispatched_at         DATETIME      DEFAULT CURRENT_TIMESTAMP,
  estimated_delivery_at DATETIME      NULL,
  created_at            DATETIME      DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_sim_rep_run (simulation_run_id),
  INDEX idx_sim_rep_sku (sku)
);

CREATE TABLE IF NOT EXISTS sim_forecast_results (
  sim_forecast_id     BIGINT        PRIMARY KEY AUTO_INCREMENT,
  simulation_run_id   VARCHAR(64)   NOT NULL,
  execution_id        BIGINT        NULL,
  product_category    VARCHAR(150)  NOT NULL,
  forecast_week       DATE          NOT NULL,
  predicted_demand    DECIMAL(10,4) NOT NULL,
  model_name          VARCHAR(100)  NOT NULL DEFAULT 'XGBoost',
  model_version       VARCHAR(100)  NOT NULL DEFAULT 'xgboost-v1',
  created_at          DATETIME      DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_sim_fc_run  (simulation_run_id),
  INDEX idx_sim_fc_cat  (product_category),
  INDEX idx_sim_fc_week (forecast_week)
);
