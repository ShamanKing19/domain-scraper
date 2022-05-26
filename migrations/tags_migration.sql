CREATE TABLE IF NOT EXISTS `tags` (
  `id` int(11) PRIMARY KEY,
  `tag` varchar(255) NULL,
  `subcategory_id` int(11) NULL,
  FOREIGN KEY (`subcategory_id`) REFERENCES `subcategory` (`id`)
)