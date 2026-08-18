const { pool } = require('./db');

async function createMessage({
                                 instituteId,
                                 phone,
                                 message,
                                 messageType = 'text',
                                 shardId
                             }) {
    const [result] = await pool.execute(
        `
      INSERT INTO whatsapp_message
      (
        institute_id,
        phone,
        message_type,
        message,
        shard_id,
        status,
        queued_at
      )
      VALUES (?, ?, ?, ?, ?, 'Queued', NOW())
    `,
        [
            instituteId,
            phone,
            messageType,
            message,
            shardId
        ]
    );

    return result.insertId;
}

async function setJobId(messageId, jobId) {
    await pool.execute(
        `
      UPDATE whatsapp_message
      SET job_id = ?
      WHERE whatsapp_message_id = ?
    `,
        [String(jobId), messageId]
    );
}

async function markProcessing(messageId, attempts) {
    await pool.execute(
        `
      UPDATE whatsapp_message
      SET
        status = 'Processing',
        processing_at = NOW(),
        attempts = ?
      WHERE whatsapp_message_id = ?
    `,
        [attempts, messageId]
    );
}

async function markRetrying(messageId, attempts, error) {
    await pool.execute(
        `
      UPDATE whatsapp_message
      SET
        status = 'Retrying',
        attempts = ?,
        last_error = ?
      WHERE whatsapp_message_id = ?
    `,
        [attempts, String(error || '').slice(0, 65000), messageId]
    );
}

async function markSent(messageId, attempts) {
    await pool.execute(
        `
      UPDATE whatsapp_message
      SET
        status = 'Sent',
        attempts = ?,
        sent_at = NOW(),
        failed_at = NULL,
        last_error = NULL
      WHERE whatsapp_message_id = ?
    `,
        [attempts, messageId]
    );
}

async function markFailed(messageId, attempts, error) {
    await pool.execute(
        `
      UPDATE whatsapp_message
      SET
        status = 'Failed',
        attempts = ?,
        failed_at = NOW(),
        last_error = ?
      WHERE whatsapp_message_id = ?
    `,
        [attempts, String(error || '').slice(0, 65000), messageId]
    );
}

async function getMessage(messageId) {
    const [rows] = await pool.execute(
        `
      SELECT *
      FROM whatsapp_message
      WHERE whatsapp_message_id = ?
      LIMIT 1
    `,
        [messageId]
    );

    return rows[0] || null;
}

async function searchMessages(filters = {}) {
    const {
        instituteId,
        status,
        phone,
        messageId,
        dateFrom,
        dateTo,
        limit = 50,
        offset = 0
    } = filters;

    const where = [];
    const params = [];

    if (messageId) {
        where.push('whatsapp_message_id = ?');
        params.push(messageId);
    }

    if (instituteId) {
        where.push('institute_id = ?');
        params.push(instituteId);
    }

    if (status) {
        where.push('status = ?');
        params.push(status);
    }

    if (phone) {
        where.push('phone LIKE ?');
        params.push(`%${phone}%`);
    }

    if (dateFrom) {
        where.push('created_at >= ?');
        params.push(dateFrom);
    }

    if (dateTo) {
        where.push('created_at <= ?');
        params.push(dateTo);
    }

    const safeLimit = Math.min(
        Math.max(Number(limit) || 50, 1),
        500
    );

    const safeOffset = Math.max(
        Number(offset) || 0,
        0
    );

    const whereSql = where.length
        ? `WHERE ${where.join(' AND ')}`
        : '';

    const [rows] = await pool.execute(
        `
      SELECT
        whatsapp_message_id,
        institute_id,
        phone,
        message_type,
        message,
        job_id,
        shard_id,
        status,
        attempts,
        queued_at,
        processing_at,
        sent_at,
        failed_at,
        last_error,
        created_at,
        updated_at
      FROM whatsapp_message
      ${whereSql}
      ORDER BY whatsapp_message_id DESC
      LIMIT ${safeLimit}
      OFFSET ${safeOffset}
    `,
        params
    );

    const [countRows] = await pool.execute(
        `
      SELECT COUNT(*) AS total
      FROM whatsapp_message
      ${whereSql}
    `,
        params
    );

    return {
        rows,
        total: Number(countRows[0]?.total || 0),
        limit: safeLimit,
        offset: safeOffset
    };
}

module.exports = {
    createMessage,
    setJobId,
    markProcessing,
    markRetrying,
    markSent,
    markFailed,
    getMessage,
    searchMessages
};