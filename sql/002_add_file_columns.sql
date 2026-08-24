-- Queued file sends (POST /api/messages/send-stored-file) record which
-- attachment went out. Both columns are nullable, so existing text-send inserts
-- are unaffected.
--
-- This migration MUST be applied before deploying the send-stored-file feature:
-- messageRepository.createMessage() now names both columns on every insert,
-- text sends included.

ALTER TABLE `whatsapp_message`
  ADD COLUMN `file_id`   VARCHAR(64)  NULL DEFAULT NULL AFTER `message`,
  ADD COLUMN `file_name` VARCHAR(255) NULL DEFAULT NULL AFTER `file_id`;
