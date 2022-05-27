CREATE TABLE IF NOT EXISTS `domain_phones` (
    `id` INT PRIMARY KEY,
    `domain_id` INT,
    `number` VARCHAR(20),
    FOREIGN KEY (`domain_id`) REFERENCES `domains` (`id`)
);