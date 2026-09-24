CREATE TABLE IF NOT EXISTS `dependency_datasets` (
  `id` int NOT NULL AUTO_INCREMENT,
  `dependency_id` int NOT NULL,
  `dataset_id` int NOT NULL,
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `dependency_id` (`dependency_id`),
  KEY `dataset_id` (`dataset_id`),
  CONSTRAINT `dependency_datasets_ibfk_1` FOREIGN KEY (`dependency_id`) REFERENCES `pipeline_dependencies` (`dependency_id`),
  CONSTRAINT `dependency_datasets_ibfk_2` FOREIGN KEY (`dataset_id`) REFERENCES `dataset_metadata` (`dataset_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
