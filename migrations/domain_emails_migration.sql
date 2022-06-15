CREATE TABLE IF NOT EXISTS `domain_emails` (
    `id` INT PRIMARY KEY AUTO_INCREMENT,
    `domain_id` BIGINT UNSIGNED,
    `email` TEXT,
    FOREIGN KEY (`domain_id`) REFERENCES `domains` (`Id`)
);