CREATE TABLE IF NOT EXISTS `dataset_metadata` (
  `dataset_id` int NOT NULL AUTO_INCREMENT,
  `dataset_name` varchar(100) NOT NULL,
  `source_type` varchar(50) DEFAULT NULL,
  `table_name` varchar(100) NOT NULL,
  `last_updated_at` datetime DEFAULT NULL,
  `current_version` int DEFAULT '0',
  `row_count` bigint DEFAULT '0',
  `status` varchar(30) DEFAULT 'WAITING',
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`dataset_id`),
  UNIQUE KEY `dataset_name` (`dataset_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
