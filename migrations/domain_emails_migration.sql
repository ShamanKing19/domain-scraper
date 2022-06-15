CREATE TABLE IF NOT EXISTS `domain_emails` (
    `id` INT PRIMARY KEY AUTO_INCREMENT,
    `domain_id` BIGINT UNSIGNED,
    `email` VARCHAR(255),
    FOREIGN KEY (`domain_id`) REFERENCES `domains` (`id`),
    UNIQUE KEY (`domain_id`, `email`)
);