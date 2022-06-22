CREATE TABLE IF NOT EXISTS `company_info`(  
    `id` int NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `domain_id`  BIGINT UNSIGNED,
    `inn` VARCHAR(20),
    `name` VARCHAR(255),
    `type` VARCHAR(255),
    `segment` VARCHAR(255),
    `year` INT(4),
    `income` INT(20),
    `outcome` INT(20),
    FOREIGN KEY (`domain_id`) REFERENCES `domains` (`id`),
    UNIQUE KEY (`inn`, `year`)
);