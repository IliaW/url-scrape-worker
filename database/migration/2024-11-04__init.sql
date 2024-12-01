CREATE SCHEMA IF NOT EXISTS url_scraper DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;
GRANT ALL PRIVILEGES ON url_scraper.* TO 'admin'@'%';
USE url_scraper;

CREATE TABLE IF NOT EXISTS scrape_metadata
(
    id                    INT AUTO_INCREMENT PRIMARY KEY,
    url                   VARCHAR(255) NOT NULL,
    time_to_scrape        INT          NOT NULL, -- in milliseconds
    timestamp             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status                VARCHAR(64)  NOT NULL,
    status_code           SMALLINT     NOT NULL,
    scrape_mechanism      VARCHAR(30)  NOT NULL,
    scrape_worker_version VARCHAR(30)  NOT NULL,
    e_tag                 VARCHAR(255) NULL,
    INDEX url_etag_index (url, timestamp DESC, e_tag)
) ENGINE = InnoDB
  CHARSET = utf8;
