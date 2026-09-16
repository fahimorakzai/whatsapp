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

fs.rmSync(dataDir, { recursive: true, force: true });

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
