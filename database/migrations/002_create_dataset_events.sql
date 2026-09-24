CREATE TABLE IF NOT EXISTS `dataset_events` (
  `event_id` bigint NOT NULL AUTO_INCREMENT,
  `dataset_id` int NOT NULL,
  `dataset_name` varchar(100) NOT NULL,
  `event_type` varchar(50) NOT NULL,
  `event_time` datetime DEFAULT CURRENT_TIMESTAMP,
  `dataset_version` int NOT NULL,
  `rows_changed` bigint DEFAULT '0',
  `event_status` varchar(30) DEFAULT 'NEW',
  `processed_at` datetime DEFAULT NULL,
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`event_id`),
  KEY `fk_dataset_event` (`dataset_id`),
  CONSTRAINT `fk_dataset_event` FOREIGN KEY (`dataset_id`) REFERENCES `dataset_metadata` (`dataset_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
