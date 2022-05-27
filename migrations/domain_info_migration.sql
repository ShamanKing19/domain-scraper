CREATE TABLE IF NOT EXISTS `domain_info` (
    `id` INT PRIMARY KEY,
    `domain_id` INT,
    `title` VARCHAR(255),
    `description` VARCHAR(255),
    `city` VARCHAR(255),
    `inn` VARCHAR(255),
    `cms` VARCHAR(100),
    `tag_id` INT NULL,
    `status` VARCHAR(30) DEFAULT 'Отсутствует',
    `comment` VARCHAR(500),
    FOREIGN KEY (`domain_id`) REFERENCES `domains` (`id`),
    FOREIGN KEY (`tag_id`) REFERENCES `tags` (`id`)
);