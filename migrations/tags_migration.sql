CREATE TABLE IF NOT EXISTS `tags` (
  `id` int(11) PRIMARY KEY,
  `tag` TEXT NULL,
  `subcategory_id` int(11) NULL,
  FOREIGN KEY (`subcategory_id`) REFERENCES `subcategory` (`id`)
)