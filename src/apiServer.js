require('dotenv').config();

const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const os = require('os');
const crypto = require('crypto');
const {
  createMessage,
  setJobId,
  getMessage,
  searchMessages
} = require('./messageRepository');
const ShardRpcClient = require('./shardRpc');
const { FileStore, resolveUploadPath } = require('./fileStore');
const { getShardIdForInstitute, getTotalShards } = require('./shardConfig');

function secureCompare(a, b) {
  const aBuffer = Buffer.from(String(a || ''));
  const bBuffer = Buffer.from(String(b || ''));
  return aBuffer.length === bBuffer.length && crypto.timingSafeEqual(aBuffer, bBuffer);
}

function createUploadMiddleware(uploadPath, maxUploadMb) {
  const storage = multer.diskStorage({
    destination: (_, __, cb) => cb(null, uploadPath),
    filename: (_, file, cb) => {
      const safeName = file.originalname.replace(/[^a-zA-Z0-9._-]/g, '_');
      cb(null, `${Date.now()}-${crypto.randomUUID()}-${safeName}`);
    }
  });

  return multer({ storage, limits: { fileSize: maxUploadMb * 1024 * 1024 } });
}

function buildConfig() {
  const port = Number(process.env.PORT || 3100);
  const apiKey = process.env.API_KEY || '';
  const persistentDataPath = path.resolve(
    process.env.PERSISTENT_DATA_PATH || path.join(os.homedir(), 'pearlnotify-data')
  );
  const uploadPath = resolveUploadPath();
  const maxUploadMb = Number(process.env.MAX_UPLOAD_MB || 15);
  return { apiKey, maxUploadMb, persistentDataPath, port, uploadPath };
}

function sendError(res, error, fallbackStatus = 400) {
  const statusCode = Number(error?.statusCode || error?.status || fallbackStatus);
  res.status(statusCode).json({
    success: false,
    error: error?.message || 'Unknown error',
    details: error?.details || null
  });
}

function startApiServer() {
  const config = buildConfig();
  fs.mkdirSync(config.uploadPath, { recursive: true });

  const totalShards = getTotalShards();
  const rpc = new ShardRpcClient({ totalShards });
  const fileStore = new FileStore({ uploadPath: config.uploadPath });
  fileStore.startPruneTimer();

  const app = express();
  const upload = createUploadMiddleware(config.uploadPath, config.maxUploadMb);

  app.use(helmet());
  app.use(cors());
  app.use(express.json({ limit: '2mb' }));

  app.get('/health', (_, res) => {
    res.json({
      success: true,
      service: 'pearl-whatsapp-api',
      version: '3.0.0',
      role: 'api',
      totalShards,
      pid: process.pid,
      uptimeSeconds: Math.floor(process.uptime())
    });
  });

  app.use('/api', (req, res, next) => {
    if (!config.apiKey) {
      return res.status(500).json({ success: false, error: 'API_KEY is not configured on the WhatsApp service' });
    }
    const supplied = req.get('x-api-key');
    if (!supplied || !secureCompare(supplied, config.apiKey)) {
      return res.status(401).json({ success: false, error: 'Unauthorized' });
    }
    next();
  });

  app.post('/api/sessions/:instituteId/start', async (req, res) => {
    try {
      const data = await rpc.dispatch(req.params.instituteId, 'session-start');
      res.json({ success: true, data });
    } catch (error) {
      sendError(res, error);
    }
  });

  app.get('/api/sessions/:instituteId/status', async (req, res) => {
    try {
      const data = await rpc.dispatch(req.params.instituteId, 'session-status');
      res.json({ success: true, data });
    } catch (error) {
      sendError(res, error);
    }
  });

  app.post('/api/sessions/:instituteId/reset', async (req, res) => {
    try {
      const data = await rpc.dispatch(req.params.instituteId, 'session-reset', {
        restart: req.body?.restart !== false
      });
      res.json({ success: true, data });
    } catch (error) {
      sendError(res, error);
    }
  });

  app.post('/api/sessions/:instituteId/pairing-code', async (req, res) => {
    try {
      const { phone, showNotification, intervalMs } = req.body || {};
      if (!phone) throw new Error('phone is required');

      const data = await rpc.dispatch(req.params.instituteId, 'session-pairing-code', {
        phone,
        showNotification,
        intervalMs
      });
      res.json({ success: true, data });
    } catch (error) {
      sendError(res, error);
    }
  });

  app.post('/api/sessions/:instituteId/pairing-code/cancel', async (req, res) => {
    try {
      const data = await rpc.dispatch(req.params.instituteId, 'session-pairing-code-cancel');
      res.json({ success: true, data });
    } catch (error) {
      sendError(res, error);
    }
  });

  app.get('/api/sessions/:instituteId/qr', async (req, res) => {
    try {
      const data = await rpc.dispatch(req.params.instituteId, 'session-qr');
      res.json({ success: true, data });
    } catch (error) {
      sendError(res, error);
    }
  });

  app.get('/api/sessions', async (_, res) => {
    try {
      const shards = Array.from({ length: totalShards }, (_, shardId) =>
        rpc.dispatchToShard(shardId, 'session-list')
      );
      const rows = await Promise.all(shards);
      res.json({ success: true, data: rows.flat().sort((a, b) => Number(a.instituteId) - Number(b.instituteId)) });
    } catch (error) {
      sendError(res, error, 500);
    }
  });

  app.post('/api/messages/send', async (req, res) => {
    try {
      const { instituteId, phone, message } = req.body || {};

      if (!instituteId || !phone || !String(message || '').trim()) {
        return res.status(400).json({
          success: false,
          error: 'instituteId, phone and message are required'
        });
      }

      const shardId = getShardIdForInstitute(
          instituteId,
          totalShards
      );

      const whatsappMessageId = await createMessage({
        instituteId,
        phone,
        message,
        shardId
      });

      const job = await rpc.enqueue(
          instituteId,
          'send-text',
          {
            whatsappMessageId,
            phone,
            message
          }
      );

      await setJobId(
          whatsappMessageId,
          job.id
      );

      return res.status(202).json({
        success: true,
        data: {
          status: 'QUEUED',
          messageId: whatsappMessageId,
          jobId: job.id,
          shardId
        }
      });

    } catch (error) {
      sendError(res, error, 500);
    }
  });

  app.get('/api/messages', async (req, res) => {
    try {
      const {
        instituteId,
        status,
        phone,
        messageId,
        dateFrom,
        dateTo,
        limit,
        offset
      } = req.query;

      const result = await searchMessages({
        instituteId,
        status,
        phone,
        messageId,
        dateFrom,
        dateTo,
        limit,
        offset
      });

      return res.json({
        success: true,
        data: {
          records: result.rows,
          pagination: {
            total: result.total,
            limit: result.limit,
            offset: result.offset
          }
        }
      });

    } catch (error) {
      sendError(res, error, 500);
    }
  });

  app.get('/api/messages/:messageId', async (req, res) => {
    try {
      const data = await getMessage(
          req.params.messageId
      );

      if (!data) {
        return res.status(404).json({
          success: false,
          error: 'Message not found'
        });
      }

      return res.json({
        success: true,
        data
      });

    } catch (error) {
      sendError(res, error, 500);
    }
  });

  app.post('/api/messages/queue-test', async (req, res) => {
    try {
      const { instituteId, phone, message } = req.body || {};
      if (!instituteId || !phone || !message) {
        return res.status(400).json({
          success: false,
          error: 'instituteId, phone and message are required'
        });
      }

      const shardId = getShardIdForInstitute(instituteId, totalShards);
      const job = await rpc.enqueueToShard(shardId, 'send-text', {
        instituteId: String(instituteId),
        phone,
        message
      });

      return res.status(202).json({
        success: true,
        data: {
          status: 'QUEUED',
          shardId,
          jobId: job.id
        }
      });
    } catch (error) {
      sendError(res, error, 500);
    }
  });

  app.post('/api/messages/send-file', upload.single('file'), async (req, res) => {
    let uploadedFile = null;
    try {
      uploadedFile = req.file?.path || null;
      if (!uploadedFile) throw new Error('file is required');

      const { instituteId, phone, caption } = req.body || {};
      const data = await rpc.dispatch(instituteId, 'send-file', {
        phone,
        caption,
        filePath: uploadedFile
      });
      res.json({ success: true, data });
    } catch (error) {
      sendError(res, error);
    } finally {
      if (uploadedFile && fs.existsSync(uploadedFile)) fs.unlink(uploadedFile, () => {});
    }
  });

  /*
   * Durable attachment upload.
   *
   * Unlike /api/messages/send-file, the bytes survive the request, so the send
   * itself can be queued. Upload once, then reference the fileId from as many
   * send-stored-file calls as there are parents.
   */
  app.post('/api/files', upload.single('file'), async (req, res) => {
    let tmpPath = req.file?.path || null;
    try {
      if (!tmpPath) throw new Error('file is required');

      const data = await fileStore.store({
        tmpPath,
        originalName: req.file.originalname,
        mimeType: req.file.mimetype
      });

      // The store now owns the bytes; do not let the finally block delete them.
      tmpPath = null;

      res.status(201).json({ success: true, data });
    } catch (error) {
      sendError(res, error);
    } finally {
      if (tmpPath && fs.existsSync(tmpPath)) fs.unlink(tmpPath, () => {});
    }
  });

  app.get('/api/files/:fileId', async (req, res) => {
    try {
      const { path: _absolutePath, ...data } = await fileStore.get(req.params.fileId);
      res.json({ success: true, data });
    } catch (error) {
      sendError(res, error, 404);
    }
  });

  app.delete('/api/files/:fileId', async (req, res) => {
    try {
      const data = await fileStore.remove(req.params.fileId);
      res.json({ success: true, data });
    } catch (error) {
      sendError(res, error);
    }
  });

  /*
   * Queued file send. Same 202-plus-messageId contract as /api/messages/send,
   * and the same retry and whatsapp_message status tracking.
   */
  app.post('/api/messages/send-stored-file', async (req, res) => {
    try {
      const { instituteId, phone, fileId, caption } = req.body || {};

      if (!instituteId || !phone || !fileId) {
        return res.status(400).json({
          success: false,
          error: 'instituteId, phone and fileId are required'
        });
      }

      // Resolve up front so a bad fileId is a 404 on this call rather than a
      // job that fails minutes later behind the pacing queue.
      const stored = await fileStore.get(fileId);

      const shardId = getShardIdForInstitute(
          instituteId,
          totalShards
      );

      const whatsappMessageId = await createMessage({
        instituteId,
        phone,
        message: String(caption || ''),
        messageType: 'file',
        shardId,
        fileId: stored.fileId,
        fileName: stored.filename
      });

      const job = await rpc.enqueue(
          instituteId,
          'send-stored-file',
          {
            whatsappMessageId,
            phone,
            fileId: stored.fileId,
            caption: String(caption || '')
          }
      );

      await setJobId(
          whatsappMessageId,
          job.id
      );

      return res.status(202).json({
        success: true,
        data: {
          status: 'QUEUED',
          messageId: whatsappMessageId,
          jobId: job.id,
          shardId,
          fileId: stored.fileId,
          filename: stored.filename
        }
      });

    } catch (error) {
      sendError(res, error, 500);
    }
  });

  app.post('/api/sessions/:instituteId/disconnect', async (req, res) => {
    try {
      const data = await rpc.dispatch(req.params.instituteId, 'session-disconnect', {
        logout: Boolean(req.body?.logout)
      });
      res.json({ success: true, data });
    } catch (error) {
      sendError(res, error);
    }
  });

  app.use((error, req, res, next) => {
    if (error instanceof multer.MulterError) {
      return res.status(400).json({ success: false, error: error.message });
    }
    console.error(`[api][pid:${process.pid}]`, error);
    res.status(500).json({ success: false, error: 'Internal server error' });
  });

  const server = app.listen(config.port, () => {
    console.log(
      `Pearl WhatsApp API v3.0.0 listening on port ${config.port} (pid ${process.pid})`
    );
    console.log(`[api] uploadPath=${config.uploadPath}`);
    console.log(`[api] fileStorePath=${fileStore.rootPath}`);
    console.log(`[api] totalShards=${totalShards}`);
  });

  async function shutdown(signal) {
    console.log(`[api][pid:${process.pid}] received ${signal}; shutting down`);
    server.close(async () => {
      fileStore.stopPruneTimer();
      await rpc.close();
      process.exit(0);
    });
    setTimeout(async () => {
      await rpc.close().catch(() => {});
      process.exit(0);
    }, 5000).unref();
  }

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('unhandledRejection', (error) => {
    console.error(`[api][pid:${process.pid}] unhandledRejection`, error);
  });
  process.on('uncaughtException', (error) => {
    console.error(`[api][pid:${process.pid}] uncaughtException`, error);
  });
}

module.exports = { startApiServer };
