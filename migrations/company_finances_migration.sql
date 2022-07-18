CREATE TABLE IF NOT EXISTS `company_finances`(  
    `id` BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `inn_id` BIGINT UNSIGNED NOT NULL,
    `year` INT(4),
    `income` DOUBLE,
    `outcome` DOUBLE,
    `profit` DOUBLE,
    FOREIGN KEY (`inn_id`) REFERENCES `inns` (`id`),
    UNIQUE KEY (`inn_id`, `year`)
);