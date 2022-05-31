CREATE TABLE IF NOT EXISTS `domain_info` (
    `id` BIGINT UNSIGNED PRIMARY KEY,
    `domain_id` BIGINT UNSIGNED UNIQUE,
    `title` TEXT,
    `description` TEXT,
    `city` VARCHAR(255),
    `inn` VARCHAR(255),
    `cms` VARCHAR(100),
    `tag_id` INT NULL,
    `status` VARCHAR(30) DEFAULT 'Отсутствует',
    `comment` TEXT,
    FOREIGN KEY (`domain_id`) REFERENCES `domains` (`id`),
    FOREIGN KEY (`tag_id`) REFERENCES `tags` (`id`)
);