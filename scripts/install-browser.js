// Load .env before resolving PERSISTENT_DATA_PATH. Without this the installer
// silently falls back to the Hostinger default while sessionManager.js -- which
// does get dotenv, via the service entrypoint -- looks somewhere else, and the
// worker then fails at runtime with "Could not find Chrome".
try {
  require('dotenv').config();
} catch (_) {
  // dotenv is a declared dependency; this only trips in an odd install order.
}

const { spawnSync } = require('child_process');
const os = require('os');
const path = require('path');
const fs = require('fs');

const domainHome = path.resolve(
  process.env.PERSISTENT_DATA_PATH ||
  path.join(os.homedir(), 'domains', 'pearlnotify.com', 'pearlnotify-data')
);

const cacheDir = path.join(domainHome, 'puppeteer');

fs.mkdirSync(cacheDir, { recursive: true });

console.log(`[install-browser] Puppeteer cache: ${cacheDir}`);

const env = {
  ...process.env,
  PUPPETEER_CACHE_DIR: cacheDir
};

const result = spawnSync(
  process.platform === 'win32' ? 'npx.cmd' : 'npx',
  ['puppeteer', 'browsers', 'install', 'chrome'],
  {
    stdio: 'inherit',
    env,
    shell: false
  }
);

if (result.error) {
  console.error('[install-browser] Failed to run Puppeteer browser installer:', result.error);
  process.exit(1);
}

process.exit(result.status ?? 1);
