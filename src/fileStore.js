'use strict';

/*
 * Durable attachment store.
 *
 * POST /api/messages/send-file keeps its temp file only for the life of the
 * request, which is why it has to stay synchronous. Files written here instead
 * outlive the request, so a file send can be queued like a text send and get
 * the same retries and whatsapp_message bookkeeping.
 *
 * Layout, under UPLOAD_PATH:
 *
 *   store/<fileId>/meta.json     written first, so an entry is never nameless
 *   store/<fileId>/<safeName>    the attachment itself
 *
 * The blob keeps its (sanitised) original name because whatsapp-web.js takes
 * the document name WhatsApp shows from the file's basename. A parent should
 * see "term-result-1043.pdf", not "blob".
 *
 * The API process writes; API and shard workers both read. They only agree
 * because every process resolves UPLOAD_PATH the same way — see
 * resolveUploadPath().
 */

const fs = require('fs');
const fsp = require('fs/promises');
const os = require('os');
const path = require('path');
const crypto = require('crypto');

const STORE_DIRNAME = 'store';
const META_FILENAME = 'meta.json';
const DEFAULT_TTL_HOURS = 48;
const PRUNE_INTERVAL_MS = 3600000;
const FILE_ID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function resolvePersistentDataPath() {
  return path.resolve(
    process.env.PERSISTENT_DATA_PATH || path.join(os.homedir(), 'pearlnotify-data')
  );
}

function resolveUploadPath() {
  return path.resolve(
    process.env.UPLOAD_PATH || path.join(resolvePersistentDataPath(), 'uploads')
  );
}

/*
 * `unrecoverable` tells the worker that retrying cannot help: the file is
 * missing, expired, or the id is malformed. Those fail identically on every
 * attempt, so the job should stop rather than burn its backoff schedule.
 */
function storeError(message, statusCode, unrecoverable = true) {
  const error = new Error(message);
  error.statusCode = statusCode;
  error.unrecoverable = unrecoverable;
  return error;
}

function sanitizeFilename(name, fallback = 'attachment') {
  const base = path.basename(String(name || ''));
  const safe = base
    .replace(/[^a-zA-Z0-9._-]/g, '_')
    .replace(/^\.+/, '')
    .slice(0, 120);
  return safe || fallback;
}

class FileStore {
  constructor(options = {}) {
    this.uploadPath = options.uploadPath || resolveUploadPath();
    this.rootPath = path.join(this.uploadPath, STORE_DIRNAME);
    this.ttlMs =
      Math.max(
        1,
        Number(options.ttlHours || process.env.FILE_STORE_TTL_HOURS || DEFAULT_TTL_HOURS)
      ) * 3600000;
    this.pruneTimer = null;

    fs.mkdirSync(this.rootPath, { recursive: true });
  }

  /*
   * The only place a path is built from caller-supplied input. The id must be a
   * UUID, and the result must sit directly under the store root — a malformed
   * id can never escape into another directory.
   */
  entryPath(fileId) {
    const value = String(fileId || '').toLowerCase();
    if (!FILE_ID_PATTERN.test(value)) throw storeError('Invalid fileId', 400);

    const dir = path.join(this.rootPath, value);
    if (path.dirname(dir) !== this.rootPath) throw storeError('Invalid fileId', 400);
    return dir;
  }

  async store({ tmpPath, originalName, mimeType }) {
    const fileId = crypto.randomUUID();
    const dir = this.entryPath(fileId);
    const filename = sanitizeFilename(originalName);
    const createdAt = new Date();

    const meta = {
      fileId,
      filename,
      mimeType: mimeType || 'application/octet-stream',
      sizeBytes: 0,
      createdAt: createdAt.toISOString(),
      expiresAt: new Date(createdAt.getTime() + this.ttlMs).toISOString()
    };

    await fsp.mkdir(dir, { recursive: true });
    await fsp.writeFile(path.join(dir, META_FILENAME), JSON.stringify(meta, null, 2), 'utf8');

    const target = path.join(dir, filename);
    try {
      await fsp.rename(tmpPath, target);
    } catch (error) {
      // multer's temp dir is the parent of the store, so this is normally a
      // same-device rename; fall back only if the deployment splits them.
      if (error?.code !== 'EXDEV') throw error;
      await fsp.copyFile(tmpPath, target);
      await fsp.unlink(tmpPath).catch(() => {});
    }

    const stats = await fsp.stat(target);
    meta.sizeBytes = stats.size;
    await fsp.writeFile(path.join(dir, META_FILENAME), JSON.stringify(meta, null, 2), 'utf8');

    return meta;
  }

  async get(fileId) {
    const dir = this.entryPath(fileId);

    let meta;
    try {
      meta = JSON.parse(await fsp.readFile(path.join(dir, META_FILENAME), 'utf8'));
    } catch (_) {
      throw storeError('Stored file not found', 404);
    }

    // Re-sanitise on read: the name is trusted because we wrote it, but the
    // store root is on disk and this costs nothing.
    const filename = sanitizeFilename(meta.filename);
    const filePath = path.join(dir, filename);
    if (path.dirname(filePath) !== dir) throw storeError('Stored file not found', 404);

    let stats;
    try {
      stats = await fsp.stat(filePath);
    } catch (_) {
      throw storeError('Stored file not found', 404);
    }
    if (!stats.isFile()) throw storeError('Stored file not found', 404);

    if (meta.expiresAt && Date.parse(meta.expiresAt) <= Date.now()) {
      throw storeError('Stored file has expired', 410);
    }

    return {
      fileId: meta.fileId,
      filename,
      mimeType: meta.mimeType || 'application/octet-stream',
      sizeBytes: stats.size,
      createdAt: meta.createdAt,
      expiresAt: meta.expiresAt,
      path: filePath
    };
  }

  async remove(fileId) {
    const dir = this.entryPath(fileId);
    await fsp.rm(dir, { recursive: true, force: true });
    return { fileId: String(fileId).toLowerCase(), deleted: true };
  }

  /*
   * A file may be sent to many parents, so nothing deletes on send. TTL is the
   * only thing that reclaims space.
   */
  async prune() {
    let entries;
    try {
      entries = await fsp.readdir(this.rootPath, { withFileTypes: true });
    } catch (_) {
      return { scanned: 0, removed: 0 };
    }

    let removed = 0;
    let scanned = 0;

    for (const entry of entries) {
      if (!entry.isDirectory() || !FILE_ID_PATTERN.test(entry.name)) continue;
      scanned += 1;

      const dir = path.join(this.rootPath, entry.name);
      let expired = false;

      try {
        const meta = JSON.parse(await fsp.readFile(path.join(dir, META_FILENAME), 'utf8'));
        expired = !meta.expiresAt || Date.parse(meta.expiresAt) <= Date.now();
      } catch (_) {
        // No readable metadata: fall back to directory age so a half-written
        // entry from a crashed upload still gets collected.
        try {
          const stats = await fsp.stat(dir);
          expired = Date.now() - stats.mtimeMs > this.ttlMs;
        } catch (_) {
          expired = false;
        }
      }

      if (expired) {
        await fsp.rm(dir, { recursive: true, force: true }).catch(() => {});
        removed += 1;
      }
    }

    return { scanned, removed };
  }

  startPruneTimer(intervalMs = PRUNE_INTERVAL_MS) {
    if (this.pruneTimer) return this.pruneTimer;

    const run = () => {
      this.prune()
        .then(({ scanned, removed }) => {
          if (removed > 0) {
            console.log(`[filestore] pruned ${removed}/${scanned} expired attachment(s)`);
          }
        })
        .catch((error) => {
          console.error(`[filestore] prune failed: ${error.message}`);
        });
    };

    run();
    this.pruneTimer = setInterval(run, intervalMs);
    this.pruneTimer.unref();
    return this.pruneTimer;
  }

  stopPruneTimer() {
    if (this.pruneTimer) {
      clearInterval(this.pruneTimer);
      this.pruneTimer = null;
    }
  }
}

module.exports = {
  DEFAULT_TTL_HOURS,
  FILE_ID_PATTERN,
  FileStore,
  resolvePersistentDataPath,
  resolveUploadPath,
  sanitizeFilename
};
