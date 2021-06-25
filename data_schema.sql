

CREATE TABLE IF NOT EXISTS `TweetData` 
(
    `id` INT NOT NULL AUTO_INCREMENT,
    `created_at` datetime NOT NULL,
    `source` VARCHAR(200) NOT NULL,
    `clean_text` TEXT DEFAULT NULL,
    `polarity` FLOAT DEFAULT NULL,
    `subjectivity` FLOAT DEFAULT NULL,
    `language` TEXT DEFAULT NULL,
    `favorite_count` INT DEFAULT NULL,
    `retweet_count` INT DEFAULT NULL,
    `original_author` TEXT DEFAULT NULL,
    `followers_count` INT DEFAULT NULL,
    `friends_count` INT DEFAULT NULL,
    `hashtags` TEXT DEFAULT NULL,
    `user_mentions` TEXT DEFAULT NULL,
    `place` TEXT DEFAULT NULL,
    `place_coordinate` VARCHAR(100) DEFAULT NULL,
    PRIMARY KEY (`id`)
)

