CREATE TABLE IF NOT EXISTS `company_finances`(  
    `id` BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `inn` VARCHAR(20),
    `year` INT(4),
    `income` INT(20),
    `outcome` INT(20),
    `profit` INT(20),
    FOREIGN KEY (`inn`) REFERENCES `company_info` (`inn`),
    UNIQUE KEY (`inn`, `year`)
);