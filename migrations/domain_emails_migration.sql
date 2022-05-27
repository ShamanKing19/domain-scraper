CREATE TABLE IF NOT EXISTS `domain_emails` (
    `id` INT PRIMARY KEY,
    `domain_id` INT,
    `email` VARCHAR(100),
    FOREIGN KEY (`domain_id`) REFERENCES `domains` (`Id`)
);