CREATE TABLE IF NOT EXISTS `pipeline_executions` (
  `execution_id` bigint NOT NULL AUTO_INCREMENT,
  `pipeline_name` varchar(100) NOT NULL,
  `decision_id` bigint NOT NULL,
  `triggering_event_id` bigint NOT NULL,
  `status` varchar(30) DEFAULT 'RUNNING',
  `started_at` datetime DEFAULT CURRENT_TIMESTAMP,
  `completed_at` datetime DEFAULT NULL,
  `error_message` text,
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`execution_id`),
  UNIQUE KEY `idx_unique_pipeline_event` (`pipeline_name`, `triggering_event_id`),
  CONSTRAINT `fk_execution_decision` FOREIGN KEY (`decision_id`) REFERENCES `pipeline_decisions` (`decision_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
