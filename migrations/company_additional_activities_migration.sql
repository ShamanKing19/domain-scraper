CREATE TABLE IF NOT EXISTS `company_additional_activities`(  
    `id` BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `inn_id` BIGINT UNSIGNED,
    `activity_code` VARCHAR(10),
    FOREIGN KEY (`inn_id`) REFERENCES `company_info` (`id`),
    UNIQUE KEY (`inn_id`, `activity_code`)
);