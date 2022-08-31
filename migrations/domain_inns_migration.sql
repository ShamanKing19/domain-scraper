CREATE TABLE IF NOT EXISTS `domain_inns` (
    `id` INT PRIMARY KEY AUTO_INCREMENT,
    `inn` VARCHAR(20),
    `domain_id` BIGINT UNSIGNED,
    FOREIGN KEY (`domain_id`) REFERENCES `domains` (`id`),
    UNIQUE KEY (`domain_id`)
);