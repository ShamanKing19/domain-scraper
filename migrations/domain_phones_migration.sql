CREATE TABLE IF NOT EXISTS `domain_phones` (
    `id` INT PRIMARY KEY,
    `domain_id` BIGINT UNSIGNED UNIQUE,
    `number` VARCHAR(20),
    FOREIGN KEY (`domain_id`) REFERENCES `domains` (`id`)
);