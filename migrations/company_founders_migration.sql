CREATE TABLE IF NOT EXISTS `company_founders`(  
    `id` BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    `inn_id` BIGINT UNSIGNED,
    `founder_full_name` VARCHAR(255),
    `founder_inn` VARCHAR(20),
    `founder_capital_part_amount` VARCHAR(255),
    `founder_capital_part_percent` INT(3),
    FOREIGN KEY (`inn_id`) REFERENCES `inns` (`id`),
    UNIQUE KEY (`inn_id`, `founder_full_name`)
);