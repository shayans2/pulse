SET NAMES 'utf8';
SET CHARACTER SET utf8;
USE comments;
CREATE TABLE IF NOT EXISTS classified_comments (ID int NOT NULL AUTO_INCREMENT, Comment TEXT, Classified varchar(255), PRIMARY KEY (ID)) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;