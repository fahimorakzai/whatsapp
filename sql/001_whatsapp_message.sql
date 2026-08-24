-- whatsapp_message: the durable record of every send, independent of BullMQ's
-- transient job state.
--
-- NOTE: this table already exists on the production database and was never
-- checked in. The definition below is reconstructed from every column
-- src/messageRepository.js reads and writes, so it is faithful enough to run
-- the service locally. Do NOT run it against production -- diff it there first.

CREATE TABLE IF NOT EXISTS `whatsapp_message` (
  `whatsapp_message_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `institute_id`        INT UNSIGNED    NOT NULL,
  `phone`               VARCHAR(32)     NOT NULL,
  `message_type`        VARCHAR(16)     NOT NULL DEFAULT 'text',
  `message`             TEXT            NULL,
  `job_id`              VARCHAR(64)     NULL,
  `shard_id`            SMALLINT        NULL,
  `status`              VARCHAR(16)     NOT NULL DEFAULT 'Queued',
  `attempts`            INT             NOT NULL DEFAULT 0,
  `queued_at`           DATETIME        NULL,
  `processing_at`       DATETIME        NULL,
  `sent_at`             DATETIME        NULL,
  `failed_at`           DATETIME        NULL,
  `last_error`          TEXT            NULL,
  `created_at`          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP
                                        ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`whatsapp_message_id`),
  KEY `idx_whatsapp_message_institute` (`institute_id`, `whatsapp_message_id`),
  KEY `idx_whatsapp_message_status` (`status`),
  KEY `idx_whatsapp_message_phone` (`phone`),
  KEY `idx_whatsapp_message_created` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
