CREATE TABLE IF NOT EXISTS `pipeline_decisions` (
  `decision_id` bigint NOT NULL AUTO_INCREMENT,
  `pipeline_name` varchar(100) NOT NULL,
  `condition_type` varchar(30) NOT NULL,
  `decision` varchar(30) NOT NULL,
  `required_count` int DEFAULT NULL,
  `ready_count` int DEFAULT '0',
  `total_required` int DEFAULT '0',
  `reason` varchar(500) DEFAULT NULL,
  `triggered_at` datetime DEFAULT NULL,
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`decision_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
