CREATE TABLE IF NOT EXISTS `domains` (
    `id` BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    `domain` VARCHAR(255),
    `zone` VARCHAR(10),
    `real_domain` VARCHAR(255),
    `status` INT,
    UNIQUE KEY(`domain`)
);