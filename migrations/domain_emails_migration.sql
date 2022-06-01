CREATE TABLE IF NOT EXISTS `domain_emails` (
    `id` INT PRIMARY KEY AUTO_INCREMENT,
    `domain_id` BIGINT UNSIGNED UNIQUE,
    `email` VARCHAR(100),
    FOREIGN KEY (`domain_id`) REFERENCES `domains` (`Id`)
);