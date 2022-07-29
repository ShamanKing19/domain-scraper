CREATE TABLE IF NOT EXISTS `domain_company_finances`(  
    `id` BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `inn_id` BIGINT UNSIGNED NOT NULL,
    `year` INT(4),
    `income` DOUBLE,
    `outcome` DOUBLE,
    `profit` DOUBLE,
    FOREIGN KEY (`inn_id`) REFERENCES `domain_inns` (`id`),
    UNIQUE KEY (`inn_id`, `year`)
);