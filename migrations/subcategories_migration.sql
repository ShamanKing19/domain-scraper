CREATE TABLE IF NOT EXISTS `subcategory` (
  `id` int(11) PRIMARY KEY,
  `name` varchar(255) NULL,
  `category_id` int(11) NOT NULL,
  FOREIGN KEY (`category_id`) REFERENCES `category` (`id`)
)