/*
 * Guard regression check -- `npm run check`.
 *
 * These are the protections added after the 2026-09 incident, where one
 * unlinked institute (157) relaunched Chrome on every queued job: 2,138 jobs
 * each destroying and cold-starting a browser, timing out after 45s, retried
 * three times. The VPS sat at 98% CPU until Hostinger throttled it to ~12%,
 * which made the 45s timeout unreachable and the loop self-sustaining.
 *
 * No browser or network is touched; this exercises the guards directly.
 */

const os = require('os');
const path = require('path');
const fs = require('fs');

const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pearlnotify-guardcheck-'));
process.env.PERSISTENT_DATA_PATH = dataDir;
process.env.WHATSAPP_RECREATE_COOLDOWN_MS = '60000';
process.env.WHATSAPP_RECREATE_FAILURE_LIMIT = '3';
process.env.WHATSAPP_CIRCUIT_OPEN_MS = '900000';

const SessionManager = require('../src/sessionManager.js');

const sessions = new SessionManager({
  sessionPath: path.join(dataDir, 'sessions'),
  statePath: path.join(dataDir, 'state')
});

let pass = 0;
let fail = 0;

function check(name, fn) {
  try {
    fn();
    console.log(`  ok   ${name}`);
    pass += 1;
  } catch (error) {
    console.log(`  FAIL ${name} -> ${error.message}`);
    fail += 1;
  }
}

// Every guard must throw an error carrying `unrecoverable`, or BullMQ retries it.
function throwsUnrecoverable(fn) {
  try {
    fn();
  } catch (error) {
    if (!error.unrecoverable) throw new Error(`not marked unrecoverable: ${error.message}`);
    return;
  }
  throw new Error('expected a throw, got none');
}

function register(id, patch) {
  sessions.registry.set(String(id), { instituteId: String(id), enabled: true, ...patch });
}

console.log('assertSendable -- a send that cannot succeed must not reach Chrome');
// Regression: an institute nobody ever linked used to fall through to
// recreateClient(), cold-start a browser and register itself enabled: true.
check('unregistered institute fails fast', () => throwsUnrecoverable(() => sessions.assertSendable('999')));
register(10, { lastStatus: 'QR_REQUIRED' });
check('QR_REQUIRED fails fast', () => throwsUnrecoverable(() => sessions.assertSendable('10')));
register(11, { enabled: false, lastStatus: 'CONNECTED' });
check('disabled institute fails fast', () => throwsUnrecoverable(() => sessions.assertSendable('11')));
register(12, { lastStatus: 'AUTH_FAILED' });
check('AUTH_FAILED fails fast', () => throwsUnrecoverable(() => sessions.assertSendable('12')));
register(13, { lastStatus: 'CONNECTED' });
check('CONNECTED passes through', () => sessions.assertSendable('13'));
register(14, { lastStatus: 'DISCONNECTED' });
check('DISCONNECTED still reaches recovery', () => sessions.assertSendable('14'));

console.log('assertRecreateAllowed -- browser restarts are rate limited');
check('first restart allowed', () => sessions.assertRecreateAllowed('20'));
check('restart inside cooldown blocked', () => throwsUnrecoverable(() => sessions.assertRecreateAllowed('20')));
check('other institutes unaffected', () => sessions.assertRecreateAllowed('21'));

console.log('circuit breaker -- repeated restart failures stop trying');
sessions.noteRecreateOutcome('30', false);
sessions.noteRecreateOutcome('30', false);
check('closed after 2 failures', () => {
  if (sessions.recreateGuard('30').openUntil > Date.now()) throw new Error('opened too early');
});
sessions.noteRecreateOutcome('30', false);
check('opens on the 3rd failure', () => {
  if (sessions.recreateGuard('30').openUntil <= Date.now()) throw new Error('did not open');
});
check('open circuit blocks restarts', () => throwsUnrecoverable(() => sessions.assertRecreateAllowed('30')));
check('a success resets it', () => {
  sessions.noteRecreateOutcome('30', true);
  if (sessions.recreateGuard('30').openUntil !== 0) throw new Error('not reset');
});

console.log('recovery must never re-enable a disabled institute');
check('recreateClient does not set enabled: true', () => {
  const src = fs.readFileSync(path.join(__dirname, '..', 'src', 'sessionManager.js'), 'utf8');
  const body = src
      .slice(src.indexOf('async recreateClient'), src.indexOf('async getStateVerified'))
      .replace(/\/\*[\s\S]*?\*\//g, '')
      .replace(/\/\/.*$/gm, '');
  if (/enabled:\s*true/.test(body)) throw new Error('recreateClient still sets enabled: true');
});

console.log('log dedupe -- a wedged page must not fill the disk');
const captured = [];
const realLog = console.log;
console.log = (...args) => captured.push(args.join(' '));
sessions.logOnce('40', 'getState error', 'boom');
sessions.logOnce('40', 'getState error', 'boom');
sessions.logOnce('40', 'getState error', 'boom');
sessions.logOnce('40', 'getState error', 'different');
console.log = realLog;
check('identical errors logged once', () => {
  if (captured.length !== 2) throw new Error(`expected 2 lines, got ${captured.length}`);
});

/*
 * On-demand session pool. A browser is 556 MB, so the pool cap is what lets 40+
 * institutes share a box that can only hold ~10 resident browsers.
 */
console.log('session pool -- cap, eviction, and idle reclaim');

const pool = new SessionManager({
  sessionPath: path.join(dataDir, 'sessions'),
  statePath: path.join(dataDir, 'state-pool'),
  maxLiveSessions: 3,
  sessionIdleMs: 50,
  minIdleBeforeEvictMs: 40
});

// Fake live sessions: the pool only looks at lastActivityAt and client.destroy.
const fake = (id, idleMs) => {
  pool.clients.set(String(id), {
    client: { destroy: async () => {} },
    lastActivityAt: Date.now() - idleMs,
    status: 'CONNECTED'
  });
  pool.registry.set(String(id), { instituteId: String(id), enabled: true, lastStatus: 'CONNECTED' });
};

(async () => {
  fake(1, 0); fake(2, 0); fake(3, 0);
  let busyErr = null;
  try { await pool.makeRoomFor('9'); } catch (e) { busyErr = e; }
  check('full pool of busy sessions rejects', () => { if (!busyErr) throw new Error('no throw'); });
  check('rejection is capacity, not failure', () => {
    if (!busyErr.capacity) throw new Error('missing capacity flag');
    if (busyErr.unrecoverable) throw new Error('must not be unrecoverable -- would drop a good job');
  });
  check('rejection carries a retry hint', () => {
    if (!(busyErr.retryAfterMs > 0)) throw new Error('no retryAfterMs');
  });

  // Institute 2 goes quiet past the eviction threshold; it should be the victim.
  pool.clients.get('2').lastActivityAt = Date.now() - 5000;
  await pool.makeRoomFor('9');
  check('evicts the idle session', () => { if (pool.clients.has('2')) throw new Error('2 still live'); });
  check('leaves busy sessions alone', () => {
    if (!pool.clients.has('1') || !pool.clients.has('3')) throw new Error('evicted a busy session');
  });
  check('evicted session stays linked and enabled', () => {
    const row = pool.registry.get('2');
    if (row.enabled === false) throw new Error('disabled it');
    if (row.lastStatus !== 'IDLE') throw new Error('status is ' + row.lastStatus);
  });

  // A session on a QR screen must not be relabelled IDLE -- the admin screen
  // uses that difference to decide whether a human needs to re-link.
  pool.registry.set('7', { instituteId: '7', enabled: true, lastStatus: 'QR_REQUIRED' });
  pool.clients.set('7', { client: { destroy: async () => {} }, lastActivityAt: 0, status: 'QR_REQUIRED' });
  await pool.closeSession('7', 'test');
  check('QR_REQUIRED survives a close', () => {
    if (pool.registry.get('7').lastStatus !== 'QR_REQUIRED') throw new Error('overwrote it with IDLE');
  });

  check('already-live institute needs no room', async () => {});
  await pool.ensureCapacityFor('1');
  check('touching a live session refreshes it', () => {
    if (Date.now() - pool.clients.get('1').lastActivityAt > 100) throw new Error('not touched');
  });

  // Idle sweep reclaims anything quiet longer than sessionIdleMs (50ms here).
  for (const s of pool.clients.values()) s.lastActivityAt = Date.now() - 5000;
  const swept = await pool.sweepIdleSessions();
  check('idle sweep closes stale sessions', () => { if (swept < 1) throw new Error('swept ' + swept); });
  check('pool is empty after sweep', () => { if (pool.clients.size !== 0) throw new Error(pool.clients.size + ' left'); });

  fs.rmSync(dataDir, { recursive: true, force: true });

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})();


