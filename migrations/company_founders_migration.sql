CREATE TABLE IF NOT EXISTS `company_founders`(  
    `id` BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    `inn` VARCHAR(20),
    `founder_full_name` VARCHAR(255),
    `founder_inn` VARCHAR(20),
    `founder_capital_part_amount` BIGINT,
    `founder_capital_part_percent` INT(3),
    FOREIGN KEY (`inn`) REFERENCES `company_info` (`inn`),
    UNIQUE KEY (`inn`, `founder_full_name`)

);