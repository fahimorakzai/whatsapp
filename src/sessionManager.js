const os = require('os');
const path = require('path');

const _pearlPersistentDataPath = path.resolve(
    process.env.PERSISTENT_DATA_PATH || path.join(os.homedir(), 'pearlnotify-data')
);

// Hostinger may inject/normalize PUPPETEER_CACHE_DIR unexpectedly.
// Always force Puppeteer to use the same persistent base directory as the WhatsApp service.
process.env.PUPPETEER_CACHE_DIR = path.join(_pearlPersistentDataPath, 'puppeteer');

const fs = require('fs');
const fsp = fs.promises;
const QRCode = require('qrcode');
const { Client, LocalAuth, MessageMedia } = require('whatsapp-web.js');

/*
 * A session in one of these states is waiting for a human -- to scan a QR or
 * type a pairing code. Retrying cannot move it forward, so a send must fail
 * immediately rather than tear down and cold-start Chrome for nothing.
 *
 * This is the single most important guard in this file. Without it one unlinked
 * institute relaunches a full browser on every queued message, forever: the
 * 2026-09 incident was 2,138 queued jobs each destroying and rebooting Chrome,
 * timing out after 45s, and retrying three times.
 */
const UNLINKED_STATUSES = new Set([
  'QR_REQUIRED',
  'PAIRING_CODE_REQUIRED',
  'AUTH_FAILED',
  'UNPAIRED'
]);

/*
 * Chrome cold start is the most expensive thing this service does, so a failing
 * institute gets a cooldown between browser restarts and a circuit breaker once
 * restarts keep failing.
 */
const RECREATE_COOLDOWN_MS = Number(process.env.WHATSAPP_RECREATE_COOLDOWN_MS || 300000);
const RECREATE_FAILURE_LIMIT = Number(process.env.WHATSAPP_RECREATE_FAILURE_LIMIT || 3);
const CIRCUIT_OPEN_MS = Number(process.env.WHATSAPP_CIRCUIT_OPEN_MS || 900000);

/*
 * restoreAll() boots sessions one at a time. client.initialize() is deliberately
 * not awaited by start() (the HTTP start endpoint must return immediately), so
 * restore has to await the promise itself or every session launches at once --
 * which is exactly what put 32 browsers, 17.4 GB of demand, onto an 8 GB box.
 */
const RESTORE_STAGGER_MS = Number(process.env.WHATSAPP_RESTORE_STAGGER_MS || 15000);
const RESTORE_INIT_TIMEOUT_MS = Number(process.env.WHATSAPP_RESTORE_INIT_TIMEOUT_MS || 120000);

/*
 * `unrecoverable` is the flag shardWorkerProcess turns into BullMQ's
 * UnrecoverableError, so the job is marked Failed once instead of burning its
 * whole retry schedule on something that cannot succeed.
 */
function unsendableError(message) {
  const error = new Error(message);
  error.unrecoverable = true;
  error.statusCode = 409;
  return error;
}

class SessionManager {
  constructor(options = {}) {
    this.sessionPath = path.resolve(options.sessionPath);
    this.statePath = path.resolve(options.statePath);
    this.stateFile = path.join(this.statePath, 'sessions.json');
    this.clients = new Map();
    this.registry = new Map();
    // Per-institute browser-restart guards, and a dedupe map so a wedged page
    // does not write the same getState error to disk twice a second.
    this.recreateGuards = new Map();
    this.lastLogged = new Map();

    fs.mkdirSync(this.sessionPath, { recursive: true });
    fs.mkdirSync(this.statePath, { recursive: true });
    this.loadRegistry();
  }

  log(instituteId, message, extra = '') {
    const suffix = extra ? ` ${extra}` : '';
    console.log(`[wa][pid:${process.pid}][institute:${instituteId}] ${message}${suffix}`);
  }

  /*
   * Log only when the message changes. waitUntilConnected polls twice a second
   * for 45s, and a wedged page fails identically every time -- that is how one
   * shard produced an 11 MB log file in a few hours.
   */
  logOnce(instituteId, key, message) {
    const mapKey = `${instituteId}:${key}`;
    if (this.lastLogged.get(mapKey) === message) return;
    this.lastLogged.set(mapKey, message);
    this.log(instituteId, key, message);
  }

  recreateGuard(instituteId) {
    let guard = this.recreateGuards.get(instituteId);
    if (!guard) {
      guard = { lastAttemptAt: 0, failures: 0, openUntil: 0 };
      this.recreateGuards.set(instituteId, guard);
    }
    return guard;
  }

  /*
   * Fail fast when a send cannot possibly succeed: the institute is switched
   * off, or its session is sitting on a QR/pairing screen waiting for a person.
   * Called before anything touches Chrome.
   */
  assertSendable(instituteId) {
    const registry = this.registry.get(instituteId);

    /*
     * A send must never bring a session into existence. Without this an
     * institute PearlIMS has queued work for -- but which nobody ever linked --
     * falls through to recreateClient(), cold-starts a browser, sits on a QR
     * screen, and start() then records it as enabled: true. Creating sessions
     * is the job of the explicit start / pairing-code endpoints only.
     */
    if (!registry) {
      throw unsendableError(
          `No WhatsApp session registered for institute ${instituteId} -- ` +
          'start and link it before sending'
      );
    }

    if (registry.enabled === false) {
      throw unsendableError(`WhatsApp session for institute ${instituteId} is disabled`);
    }

    const status = this.clients.get(instituteId)?.status || registry?.lastStatus;
    if (status && UNLINKED_STATUSES.has(status)) {
      throw unsendableError(
          `WhatsApp session for institute ${instituteId} is not linked (${status}) -- ` +
          'scan the QR or request a pairing code'
      );
    }
  }

  assertRecreateAllowed(instituteId) {
    const guard = this.recreateGuard(instituteId);
    const now = Date.now();

    if (guard.openUntil > now) {
      throw unsendableError(
          `WhatsApp session for institute ${instituteId} is in cooldown after repeated ` +
          `browser restart failures (${Math.ceil((guard.openUntil - now) / 1000)}s remaining)`
      );
    }

    if (guard.lastAttemptAt && now - guard.lastAttemptAt < RECREATE_COOLDOWN_MS) {
      throw unsendableError(
          `WhatsApp browser for institute ${instituteId} was restarted ` +
          `${Math.round((now - guard.lastAttemptAt) / 1000)}s ago; not restarting again yet`
      );
    }

    guard.lastAttemptAt = now;
  }

  noteRecreateOutcome(instituteId, succeeded) {
    const guard = this.recreateGuard(instituteId);

    if (succeeded) {
      guard.failures = 0;
      guard.openUntil = 0;
      return;
    }

    guard.failures += 1;
    if (guard.failures >= RECREATE_FAILURE_LIMIT) {
      guard.openUntil = Date.now() + CIRCUIT_OPEN_MS;
      guard.failures = 0;
      this.log(
          instituteId,
          'CIRCUIT_OPEN',
          `no further browser restarts for ${Math.round(CIRCUIT_OPEN_MS / 1000)}s`
      );
    }
  }

  normalizeInstituteId(instituteId) {
    const value = String(instituteId || '').trim();
    if (!/^\d+$/.test(value)) throw new Error('Invalid instituteId');
    return value;
  }

  clientId(instituteId) {
    return `pearl-institute-${this.normalizeInstituteId(instituteId)}`;
  }

  loadRegistry() {
    try {
      if (!fs.existsSync(this.stateFile)) return;
      const parsed = JSON.parse(fs.readFileSync(this.stateFile, 'utf8'));
      for (const row of Array.isArray(parsed.sessions) ? parsed.sessions : []) {
        if (row && /^\d+$/.test(String(row.instituteId || ''))) {
          this.registry.set(String(row.instituteId), {
            instituteId: String(row.instituteId),
            enabled: row.enabled !== false,
            phoneNumber: row.phoneNumber || null,
            lastStatus: row.lastStatus || 'NOT_STARTED',
            lastError: row.lastError || null,
            updatedAt: row.updatedAt || null
          });
        }
      }
    } catch (error) {
      console.error(`[wa][pid:${process.pid}] Could not load session registry: ${error.message}`);
    }
  }

  saveRegistry() {
    const payload = {
      version: 1,
      updatedAt: new Date().toISOString(),
      sessions: Array.from(this.registry.values())
    };
    const temp = `${this.stateFile}.${process.pid}.tmp`;
    fs.writeFileSync(temp, JSON.stringify(payload, null, 2));
    fs.renameSync(temp, this.stateFile);
  }

  updateRegistry(instituteId, patch = {}) {
    instituteId = this.normalizeInstituteId(instituteId);
    const current = this.registry.get(instituteId) || {
      instituteId,
      enabled: true,
      phoneNumber: null,
      lastStatus: 'NOT_STARTED',
      lastError: null,
      updatedAt: null
    };
    const next = { ...current, ...patch, instituteId, updatedAt: new Date().toISOString() };
    this.registry.set(instituteId, next);
    this.saveRegistry();
    return next;
  }

  discoverSessionsFromDisk(options = {}) {
    const belongsToShard = typeof options.belongsToShard === 'function'
      ? options.belongsToShard
      : () => true;
    try {
      const prefix = 'session-pearl-institute-';
      for (const entry of fs.readdirSync(this.sessionPath, { withFileTypes: true })) {
        if (!entry.isDirectory() || !entry.name.startsWith(prefix)) continue;
        const instituteId = entry.name.substring(prefix.length);
        if (/^\d+$/.test(instituteId) && belongsToShard(instituteId) && !this.registry.has(instituteId)) {
          this.registry.set(instituteId, {
            instituteId,
            enabled: true,
            phoneNumber: null,
            lastStatus: 'DISCOVERED',
            lastError: null,
            updatedAt: new Date().toISOString()
          });
        }
      }
      this.saveRegistry();
    } catch (error) {
      console.error(`[wa][pid:${process.pid}] Session discovery failed: ${error.message}`);
    }
  }

  baseState(instituteId) {
    const registry = this.registry.get(instituteId);
    return {
      instituteId,
      status: registry?.enabled ? (registry.lastStatus || 'NOT_STARTED') : 'STOPPED',
      phoneNumber: registry?.phoneNumber || null,
      qr: null,
      qrDataUrl: null,
      pairingCode: null,
      lastError: registry?.lastError || null
    };
  }

  getState(instituteId) {
    instituteId = this.normalizeInstituteId(instituteId);
    const session = this.clients.get(instituteId);
    if (!session) return this.baseState(instituteId);

    return {
      instituteId,
      status: session.status,
      phoneNumber: session.phoneNumber || null,
      qr: session.qr || null,
      qrDataUrl: session.qrDataUrl || null,
      pairingCode: session.pairingCode || null,
      lastError: session.lastError || null
    };
  }

  async getStateOrRestore(instituteId) {
    return this.getStateVerified(instituteId);
  }

  async start(instituteId, options = {}) {
    instituteId = this.normalizeInstituteId(instituteId);
    if (this.clients.has(instituteId)) return this.getState(instituteId);

    this.updateRegistry(instituteId, {
      enabled: true,
      lastStatus: options.restoring ? 'RESTORING' : 'STARTING',
      lastError: null
    });

    const session = {
      client: null,
      status: options.restoring ? 'RESTORING' : 'STARTING',
      phoneNumber: this.registry.get(instituteId)?.phoneNumber || null,
      qr: null,
      qrDataUrl: null,
      pairingCode: null,
      lastError: null
    };

    const client = new Client({
      authStrategy: new LocalAuth({
        clientId: this.clientId(instituteId),
        dataPath: this.sessionPath
      }),
      puppeteer: {
        headless: true,
        args: [
          '--no-sandbox',
          '--disable-setuid-sandbox',
          '--disable-dev-shm-usage',
          '--disable-gpu'
        ]
      }
    });

    session.client = client;
    this.clients.set(instituteId, session);
    this.log(instituteId, options.restoring ? 'restoring session' : 'starting session');

    const setStatus = (status, error = null) => {
      session.status = status;
      session.lastError = error;
      this.updateRegistry(instituteId, {
        lastStatus: status,
        lastError: error,
        phoneNumber: session.phoneNumber || null
      });
    };

    client.on('qr', async (qr) => {
      session.qr = qr;
      session.pairingCode = null;
      try {
        session.qrDataUrl = await QRCode.toDataURL(qr);
        setStatus('QR_REQUIRED');
        this.log(instituteId, 'QR generated');
      } catch (error) {
        setStatus('QR_REQUIRED', `QR generation failed: ${error.message}`);
      }
    });

    client.on('code', (code) => {
      session.pairingCode = String(code || '');
      session.qr = null;
      session.qrDataUrl = null;
      session.status = 'PAIRING_CODE_REQUIRED';
      this.updateRegistry(instituteId, {
        lastStatus: 'PAIRING_CODE_REQUIRED',
        lastError: null,
        phoneNumber: session.phoneNumber || null
      });
      this.log(instituteId, 'pairing code generated');
    });

    client.on('authenticated', () => {
      session.qr = null;
      session.qrDataUrl = null;
      session.pairingCode = null;
      setStatus('AUTHENTICATED');
      this.log(instituteId, 'authenticated');
    });

    client.on('ready', () => {
      session.qr = null;
      session.qrDataUrl = null;
      session.pairingCode = null;
      session.phoneNumber = client.info?.wid?.user || null;
      setStatus('CONNECTED');
      this.log(instituteId, 'connected', session.phoneNumber ? `phone=${session.phoneNumber}` : '');
    });

    client.on('auth_failure', (message) => {
      const error = String(message || 'Authentication failed');
      setStatus('AUTH_FAILED', error);
      this.log(instituteId, 'authentication failed', error);
    });

    client.on('disconnected', (reason) => {
      const error = reason ? String(reason) : null;
      setStatus('DISCONNECTED', error);
      this.log(instituteId, 'disconnected', error || '');
    });

    /*
     * Deliberately not awaited -- POST /api/sessions/:id/start must return
     * immediately. The promise is kept on the session so restoreAll() can await
     * it and boot sessions one at a time instead of all at once.
     */
    session.initPromise = client.initialize().then(
        () => true,
        (error) => {
          setStatus('ERROR', error.message);
          this.log(instituteId, 'initialization error', error.message);
          try { client.destroy(); } catch (_) {}
          this.clients.delete(instituteId);
          return false;
        }
    );

    return this.getState(instituteId);
  }

  async restoreAll(options = {}) {
    const belongsToShard = typeof options.belongsToShard === 'function'
      ? options.belongsToShard
      : () => true;
    this.discoverSessionsFromDisk({ belongsToShard });
    const ids = Array.from(this.registry.values())
        .filter((row) => row.enabled)
        .filter((row) => belongsToShard(row.instituteId))
        .map((row) => row.instituteId);

    console.log(`[wa][pid:${process.pid}] restoring ${ids.length} registered session(s)`);
    for (const [index, instituteId] of ids.entries()) {
      try {
        await this.start(instituteId, { restoring: true });

        /*
         * Wait for this browser to finish booting before starting the next one.
         * start() returns as soon as the client object exists, so without this
         * the loop launches every session simultaneously -- 11 Chrome processes
         * and ~556 MB each, which is what exhausted the box.
         */
        const session = this.clients.get(instituteId);
        if (session?.initPromise) {
          await Promise.race([session.initPromise, this.sleep(RESTORE_INIT_TIMEOUT_MS)]);
        }

        if (RESTORE_STAGGER_MS > 0 && index < ids.length - 1) {
          await this.sleep(RESTORE_STAGGER_MS);
        }
      } catch (error) {
        this.updateRegistry(instituteId, { lastStatus: 'ERROR', lastError: error.message });
        console.error(`[wa][pid:${process.pid}][institute:${instituteId}] restore failed: ${error.message}`);
      }
    }
    return ids.length;
  }

  async sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  async getClientStateSafe(client) {
    try {
      if (typeof client.getState === 'function') {
        return await client.getState();
      }
    } catch (error) {
      // Ignore while WhatsApp Web is still booting.
    }
    return null;
  }

  async waitForPairingReady(instituteId, timeoutMs = 45000) {
    instituteId = this.normalizeInstituteId(instituteId);
    const startedAt = Date.now();

    while (Date.now() - startedAt < timeoutMs) {
      const session = this.clients.get(instituteId);

      if (!session?.client) {
        await this.start(instituteId);
        await this.sleep(500);
        continue;
      }

      if (session.status === 'CONNECTED') {
        throw new Error('WhatsApp session is already connected');
      }

      const client = session.client;
      const hasPage = Boolean(client.pupPage);

      if (hasPage) {
        const state = await this.getClientStateSafe(client);

        // A generated QR is the strongest signal that WhatsApp Web has loaded
        // into its unauthenticated linking screen.
        if (session.status === 'QR_REQUIRED' || session.qr) {
          return session;
        }

        // Some builds may expose an unpaired page before emitting the first QR.
        // Give WhatsApp Web a little more time after the page exists.
        if (!state && Date.now() - startedAt > 5000) {
          return session;
        }
      }

      if (session.status === 'ERROR' || session.status === 'AUTH_FAILED') {
        throw new Error(session.lastError || `WhatsApp session entered ${session.status}`);
      }

      await this.sleep(500);
    }

    throw new Error('WhatsApp Web did not become ready for phone-number pairing within 45 seconds');
  }

  formatPairingError(error) {
    const details = {
      message: error?.message || String(error),
      name: error?.name || null,
      stack: error?.stack || null
    };

    if (error?.response) {
      details.responseStatus = error.response.status || null;
      details.responseData = error.response.data || null;
    }

    if (error?.status) details.status = error.status;
    if (error?.statusCode) details.statusCode = error.statusCode;
    if (error?.code) details.code = error.code;

    return details;
  }

  async waitForClientPage(instituteId, timeoutMs = 20000) {
    instituteId = this.normalizeInstituteId(instituteId);
    const startedAt = Date.now();

    while (Date.now() - startedAt < timeoutMs) {
      const session = this.clients.get(instituteId);
      if (session?.client?.pupPage) return session;

      if (!session) {
        await this.start(instituteId);
      }

      await new Promise((resolve) => setTimeout(resolve, 250));
    }

    throw new Error('WhatsApp client is still initializing. Please try again in a few seconds.');
  }

  async requestPairingCode(instituteId, phone, options = {}) {
    instituteId = this.normalizeInstituteId(instituteId);
    const normalizedPhone = this.normalizePhone(phone);

    let session = this.clients.get(instituteId);
    if (!session) {
      await this.start(instituteId);
    }

    session = await this.waitForPairingReady(instituteId);

    if (session.status === 'CONNECTED') {
      throw new Error('WhatsApp session is already connected');
    }

    const showNotification = options.showNotification !== false;
    let intervalMs = Number(options.intervalMs || 180000);
    if (!Number.isFinite(intervalMs) || intervalMs < 60000) intervalMs = 180000;

    this.log(
        instituteId,
        'requesting pairing code',
        `phone=${normalizedPhone}`,
        `status=${session.status}`,
        `hasPage=${Boolean(session.client?.pupPage)}`
    );

    try {
      const code = await session.client.requestPairingCode(
          normalizedPhone,
          showNotification,
          intervalMs
      );

      session.pairingCode = String(code || '');
      session.qr = null;
      session.qrDataUrl = null;
      session.status = 'PAIRING_CODE_REQUIRED';
      session.lastError = null;

      this.updateRegistry(instituteId, {
        enabled: true,
        phoneNumber: normalizedPhone,
        lastStatus: 'PAIRING_CODE_REQUIRED',
        lastError: null
      });

      this.log(instituteId, 'pairing code requested successfully', `phone=${normalizedPhone}`);

      return {
        instituteId,
        status: session.status,
        phoneNumber: normalizedPhone,
        pairingCode: session.pairingCode,
        intervalMs
      };
    } catch (error) {
      const details = this.formatPairingError(error);
      session.lastError = details.message;

      this.updateRegistry(instituteId, {
        enabled: true,
        phoneNumber: normalizedPhone,
        lastStatus: session.status || 'PAIRING_ERROR',
        lastError: details.message
      });

      this.log(
          instituteId,
          'pairing code error',
          JSON.stringify({
            phone: normalizedPhone,
            ...details
          })
      );

      const wrapped = new Error(details.message);
      wrapped.details = details;
      throw wrapped;
    }
  }

  async cancelPairingCode(instituteId) {
    instituteId = this.normalizeInstituteId(instituteId);
    const session = this.clients.get(instituteId);

    if (!session?.client) {
      throw new Error('WhatsApp session is not started');
    }

    if (typeof session.client.cancelPairingCode !== 'function') {
      throw new Error('Pairing-code cancellation is not supported by this whatsapp-web.js version');
    }

    await session.client.cancelPairingCode();
    session.pairingCode = null;
    session.status = 'QR_REQUIRED';

    this.updateRegistry(instituteId, {
      lastStatus: 'QR_REQUIRED',
      lastError: null
    });

    this.log(instituteId, 'pairing code cancelled; returned to QR mode');

    return {
      instituteId,
      status: session.status
    };
  }

  getSessionDirectory(instituteId) {
    instituteId = this.normalizeInstituteId(instituteId);

    // LocalAuth prefixes folders as session-<clientId>.
    // Our clientId is pearl-institute-<id>.
    return path.join(
        this.sessionPath,
        `session-pearl-institute-${instituteId}`
    );
  }

  removeRegistry(instituteId) {
    instituteId = this.normalizeInstituteId(instituteId);
    this.registry.delete(instituteId);
    this.saveRegistry();
  }

  async resetSession(instituteId, options = {}) {
    instituteId = this.normalizeInstituteId(instituteId);

    const restart = options.restart !== false;
    const existing = this.clients.get(instituteId);

    this.log(instituteId, 'resetting session');

    if (existing?.client) {
      try {
        await existing.client.destroy();
      } catch (error) {
        this.log(instituteId, 'client destroy during reset failed', error?.message || String(error));
      }
    }

    this.clients.delete(instituteId);

    const sessionDir = this.getSessionDirectory(instituteId);

    try {
      if (fs.existsSync(sessionDir)) {
        fs.rmSync(sessionDir, { recursive: true, force: true });
        this.log(instituteId, 'deleted LocalAuth directory', sessionDir);
      }
    } catch (error) {
      this.log(instituteId, 'failed deleting LocalAuth directory', error?.message || String(error));
      throw new Error(`Failed to delete WhatsApp auth directory: ${error.message}`);
    }

    this.removeRegistry(instituteId);

    if (!restart) {
      return {
        instituteId,
        status: 'NOT_STARTED',
        reset: true,
        restarted: false
      };
    }

    const data = await this.start(instituteId);

    return {
      ...data,
      reset: true,
      restarted: true
    };
  }

  isRecoverableBrowserError(error) {
    const message = String(error?.message || error || '').toLowerCase();

    return (
        message.includes('detached frame') ||
        message.includes('target closed') ||
        message.includes('execution context was destroyed') ||
        message.includes('most likely because of a navigation') ||
        message.includes('session closed') ||
        message.includes('protocol error') ||
        message.includes('cannot find context with specified id')
    );
  }

  async getLiveClientState(instituteId) {
    instituteId = this.normalizeInstituteId(instituteId);
    const session = this.clients.get(instituteId);

    if (!session?.client) {
      return { state: null, pageAlive: false, connected: false };
    }

    let pageAlive = false;
    try {
      pageAlive = Boolean(
          session.client.pupPage &&
          typeof session.client.pupPage.isClosed === 'function' &&
          !session.client.pupPage.isClosed()
      );
    } catch (_) {
      pageAlive = false;
    }

    let state = null;
    try {
      if (typeof session.client.getState === 'function') {
        state = await session.client.getState();
      }
    } catch (error) {
      if (!this.isRecoverableBrowserError(error)) {
        this.logOnce(instituteId, 'getState error', error?.message || String(error));
      }
    }

    return {
      state,
      pageAlive,
      connected: pageAlive && state === 'CONNECTED'
    };
  }

  async waitUntilConnected(instituteId, timeoutMs = 45000) {
    instituteId = this.normalizeInstituteId(instituteId);
    const startedAt = Date.now();

    while (Date.now() - startedAt < timeoutMs) {
      const live = await this.getLiveClientState(instituteId);

      if (live.connected) {
        const session = this.clients.get(instituteId);
        if (session) {
          session.status = 'CONNECTED';
          session.lastError = null;
        }

        this.updateRegistry(instituteId, {
          enabled: true,
          lastStatus: 'CONNECTED',
          lastError: null
        });

        return this.clients.get(instituteId);
      }

      const session = this.clients.get(instituteId);
      if (session?.status === 'ERROR' || session?.status === 'AUTH_FAILED') {
        throw new Error(session.lastError || `WhatsApp session entered ${session.status}`);
      }

      await this.sleep(500);
    }

    throw new Error('WhatsApp session did not become CONNECTED within 45 seconds');
  }

  async recreateClient(instituteId, reason = 'stale client') {
    instituteId = this.normalizeInstituteId(instituteId);

    // Throws (unrecoverable) if this institute restarted recently or its
    // circuit is open. Must run before anything touches the browser.
    this.assertRecreateAllowed(instituteId);

    const existing = this.clients.get(instituteId);

    this.log(instituteId, 'STALE_CLIENT_RECOVERY start', reason);

    if (existing?.client) {
      try {
        await existing.client.destroy();
      } catch (error) {
        this.log(instituteId, 'destroy during stale recovery warning', error?.message || String(error));
      }
    }

    this.clients.delete(instituteId);

    /*
     * Note: no `enabled: true` here. Recovery must never re-enable an institute
     * an operator switched off -- that silently resurrected a disabled session
     * on the first queued job during the 2026-09 incident.
     */
    this.updateRegistry(instituteId, {
      lastStatus: 'RESTORING',
      lastError: null
    });

    try {
      await this.start(instituteId, { restoring: true });
      const session = await this.waitUntilConnected(instituteId, 45000);
      this.noteRecreateOutcome(instituteId, true);
      this.log(instituteId, 'STALE_CLIENT_RECOVERY complete');
      return session;
    } catch (error) {
      this.noteRecreateOutcome(instituteId, false);
      throw error;
    }
  }

  async getStateVerified(instituteId) {
    instituteId = this.normalizeInstituteId(instituteId);

    if (!this.clients.has(instituteId)) {
      const registry = this.registry.get(instituteId);
      if (registry?.enabled) {
        await this.start(instituteId, { restoring: true });
      }
    }

    const session = this.clients.get(instituteId);
    if (!session) return this.baseState(instituteId);

    const live = await this.getLiveClientState(instituteId);

    if (live.connected) {
      session.status = 'CONNECTED';
      session.lastError = null;
      this.updateRegistry(instituteId, {
        enabled: true,
        lastStatus: 'CONNECTED',
        phoneNumber: session.phoneNumber || null,
        lastError: null
      });
    } else if (session.status === 'CONNECTED') {
      session.status = live.pageAlive ? 'RESTORING' : 'STOPPED';
      this.updateRegistry(instituteId, {
        lastStatus: session.status
      });
    }

    return this.getState(instituteId);
  }

  requireConnected(instituteId) {
    instituteId = this.normalizeInstituteId(instituteId);
    const session = this.clients.get(instituteId);
    if (!session || session.status !== 'CONNECTED' || !session.client) {
      throw new Error(`WhatsApp session is not connected (status: ${this.getState(instituteId).status})`);
    }
    return session;
  }

  normalizePhone(phone) {
    let number = String(phone || '').replace(/\D/g, '');
    if (!number) throw new Error('Phone number is required');
    if (number.startsWith('0') && number.length === 11) number = `92${number.substring(1)}`;
    return number;
  }

  async resolveChatId(client, phone) {
    const normalized = this.normalizePhone(phone);
    const directId = `${normalized}@c.us`;
    try {
      const registered = await client.getNumberId(normalized);
      return registered?._serialized || directId;
    } catch (_) {
      return directId;
    }
  }

  async sendText(instituteId, phone, message) {
    instituteId = this.normalizeInstituteId(instituteId);
    if (!String(message || '').trim()) throw new Error('Message is required');

    this.assertSendable(instituteId);

    /*
     * No separate cold-start branch. An absent client reports connected=false,
     * so recreateClient() covers both "never started" and "went stale" through
     * one guarded path -- a second start() here bypassed the cooldown entirely
     * and relaunched Chrome on every queued job.
     */
    let session = this.clients.get(instituteId);
    let live = await this.getLiveClientState(instituteId);
    if (!live.connected) {
      session = await this.recreateClient(
          instituteId,
          `pre-send state=${live.state || 'unknown'} pageAlive=${live.pageAlive}`
      );
    }

    let chatId = await this.resolveChatId(session.client, phone);

    try {
      const sent = await session.client.sendMessage(chatId, String(message));
      return {
        messageId: sent?.id?._serialized || null,
        to: chatId,
        timestamp: sent?.timestamp || null,
        retried: false
      };
    } catch (error) {
      if (!this.isRecoverableBrowserError(error)) throw error;

      this.log(instituteId, 'STALE_CLIENT_RECOVERY sendText', error?.message || String(error));

      session = await this.recreateClient(
          instituteId,
          error?.message || 'recoverable text send failure'
      );

      chatId = await this.resolveChatId(session.client, phone);
      const sent = await session.client.sendMessage(chatId, String(message));

      return {
        messageId: sent?.id?._serialized || null,
        to: chatId,
        timestamp: sent?.timestamp || null,
        retried: true
      };
    }
  }

  async sendFile(instituteId, phone, filePath, caption = '') {
    instituteId = this.normalizeInstituteId(instituteId);
    const resolvedPath = path.resolve(filePath);
    if (!fs.existsSync(resolvedPath)) throw new Error('Attachment file not found');

    this.assertSendable(instituteId);

    /*
     * No separate cold-start branch. An absent client reports connected=false,
     * so recreateClient() covers both "never started" and "went stale" through
     * one guarded path -- a second start() here bypassed the cooldown entirely
     * and relaunched Chrome on every queued job.
     */
    let session = this.clients.get(instituteId);
    let live = await this.getLiveClientState(instituteId);
    if (!live.connected) {
      session = await this.recreateClient(
          instituteId,
          `pre-file-send state=${live.state || 'unknown'} pageAlive=${live.pageAlive}`
      );
    }

    const media = MessageMedia.fromFilePath(resolvedPath);
    let chatId = await this.resolveChatId(session.client, phone);

    try {
      const sent = await session.client.sendMessage(chatId, media, {
        caption: String(caption || ''),
        // Always a document. Without this WhatsApp is free to treat the
        // attachment as previewable media and re-encode it, which is how a
        // valid PDF arrives openable-but-blank.
        sendMediaAsDocument: true
      });
      return {
        messageId: sent?.id?._serialized || null,
        to: chatId,
        timestamp: sent?.timestamp || null,
        retried: false
      };
    } catch (error) {
      if (!this.isRecoverableBrowserError(error)) throw error;

      this.log(instituteId, 'STALE_CLIENT_RECOVERY sendFile', error?.message || String(error));

      session = await this.recreateClient(
          instituteId,
          error?.message || 'recoverable file send failure'
      );

      chatId = await this.resolveChatId(session.client, phone);
      const retryMedia = MessageMedia.fromFilePath(resolvedPath);
      const sent = await session.client.sendMessage(chatId, retryMedia, {
        caption: String(caption || ''),
        sendMediaAsDocument: true
      });

      return {
        messageId: sent?.id?._serialized || null,
        to: chatId,
        timestamp: sent?.timestamp || null,
        retried: true
      };
    }
  }

  async disconnect(instituteId, logout = false) {
    instituteId = this.normalizeInstituteId(instituteId);
    const session = this.clients.get(instituteId);

    if (session?.client) {
      try {
        if (logout) await session.client.logout();
        else await session.client.destroy();
      } catch (error) {
        this.log(instituteId, 'disconnect warning', error.message);
      }
    }
    this.clients.delete(instituteId);

    if (logout) {
      this.updateRegistry(instituteId, {
        enabled: false,
        lastStatus: 'LOGGED_OUT',
        phoneNumber: null,
        lastError: null
      });
      await this.removeLocalAuth(instituteId);
    } else {
      this.updateRegistry(instituteId, {
        enabled: false,
        lastStatus: 'STOPPED',
        lastError: null
      });
    }

    return { instituteId, status: logout ? 'LOGGED_OUT' : 'STOPPED' };
  }

  async removeLocalAuth(instituteId) {
    const dir = path.join(this.sessionPath, `session-${this.clientId(instituteId)}`);
    if (fs.existsSync(dir)) await fsp.rm(dir, { recursive: true, force: true });
  }

  listStates() {
    const ids = new Set([...this.registry.keys(), ...this.clients.keys()]);
    return Array.from(ids).sort((a, b) => Number(a) - Number(b)).map((id) => this.getState(id));
  }
}

module.exports = SessionManager;
