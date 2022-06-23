CREATE TABLE IF NOT EXISTS `company_additional_activities`(  
    `id` BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `inn` VARCHAR(20),
    `activity_name` VARCHAR(255),
    FOREIGN KEY (`inn`) REFERENCES `company_info` (`inn`),
    UNIQUE KEY (`inn`, `activity_name`)
);