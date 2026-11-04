-- Migration 014: Create Cross-Process Dashboard Events Table
-- Provides a bounded, indexed, persistent event journal for cross-process SSE state synchronization.

CREATE TABLE IF NOT EXISTS dashboard_events (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id VARCHAR(64) NOT NULL UNIQUE,
    event_type VARCHAR(64) NOT NULL,
    entity VARCHAR(64) NOT NULL,
    severity VARCHAR(16) NOT NULL DEFAULT 'INFO',
    payload JSON NOT NULL,
    decision_id INT NULL,
    execution_id INT NULL,
    correlation_id VARCHAR(128) NULL,
    created_at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),
    INDEX idx_created_at (created_at),
    INDEX idx_event_type (event_type),
    INDEX idx_correlation_id (correlation_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
