-- Active: 1655049200921@@127.0.0.1@3306@admin_domains
CREATE TABLE IF NOT EXISTS `inns`(  
    `id` bigint UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `inn` VARCHAR(20) NOT NULL,
    `domain_id` BIGINT UNSIGNED,
    FOREIGN KEY (`domain_id`) REFERENCES `domains` (`id`),
    UNIQUE KEY (`inn`)
);