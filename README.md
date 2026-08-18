# Pearl WhatsApp Service v3

Sharded WhatsApp service for Pearl IMS:

```text
Pearl IMS -> pearlnotify API -> Redis -> whatsapp-shard-0..15 -> SessionManager -> Institute WhatsApp
```

The API process no longer owns WhatsApp sessions directly. It routes every institute command to exactly one shard worker through Redis. Session ownership is deterministic:

```text
shardId = instituteId % WHATSAPP_TOTAL_SHARDS
```

Default shard count is `16`.

## Process roles

API:

```bash
npm run start:api
```

Worker:

```bash
WHATSAPP_SHARD_ID=0 npm run start:worker
WHATSAPP_SHARD_ID=1 npm run start:worker
...
WHATSAPP_SHARD_ID=15 npm run start:worker
```

PM2:

```bash
pm2 start ecosystem.config.js
```

This starts:

- `pearlnotify-api`
- `pearlnotify-shard-0`
- `pearlnotify-shard-1`
- ...
- `pearlnotify-shard-15`

Every shard worker runs with a unique `WHATSAPP_SHARD_ID`, and all of them share:

```env
WHATSAPP_TOTAL_SHARDS=16
```

Do not run two independent workers with the same `WHATSAPP_SHARD_ID` while institute pacing is still in memory.

You may also use the shared bootstrap entrypoint:

```bash
node src/server.js api
node src/server.js worker
```

Required worker environment:

```env
WHATSAPP_TOTAL_SHARDS=16
WHATSAPP_SHARD_ID=0
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
```

The API only needs `WHATSAPP_TOTAL_SHARDS` plus the same Redis connection settings.

Recommended initial KVM 2 baseline:

```env
WHATSAPP_TOTAL_SHARDS=16
WHATSAPP_WORKER_CONCURRENCY=10
WHATSAPP_PER_INSTITUTE_MAX_PER_MINUTE=5
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
```

With `WHATSAPP_WORKER_CONCURRENCY=1`, a sleeping paced job can stall the shard. Start with `10` on KVM 2 so one institute waiting on its pacing window does not block unrelated institutes on the same shard.

## Message flow

Control-plane endpoints such as session status, QR, pairing code, reset, and disconnect still use request/reply over Redis so the API can return the worker result directly.

Message sending is now asynchronous:

```text
POST /api/messages/send
  -> determine shard
  -> queue send-text job
  -> return HTTP 202 with jobId immediately
```

Example response:

```json
{
  "success": true,
  "data": {
    "status": "QUEUED",
    "jobId": "123",
    "shardId": 3
  }
}
```

Queued text sends default to `3` attempts with exponential backoff starting at `5` seconds.

## Throttling

Send pacing is institute-scoped, not shard-scoped.

Use:

```env
WHATSAPP_PER_INSTITUTE_MAX_PER_MINUTE=5
```

This means each institute gets its own pacing lane on its assigned shard, instead of all institutes on the shard sharing one limiter.

The current pacing state is in memory inside the shard worker. That is correct for one worker process per shard. If you later run the same shard on multiple machines or multiple processes, move institute pacing state into Redis first.

This scheduler still sleeps inside active BullMQ jobs. For the current single-worker-per-shard deployment, raising `WHATSAPP_WORKER_CONCURRENCY` reduces cross-institute blocking. For the next scaling step beyond one worker per shard, move institute scheduling into Redis rather than relying on sleeping jobs in process memory.

## PM2 rollout

For a clean production restart:

```bash
pm2 delete all
pm2 start ecosystem.config.js
pm2 save
pm2 status
```

You should see `17` processes total: `pearlnotify-api` plus `pearlnotify-shard-0` through `pearlnotify-shard-15`.

## File send caveat

`POST /api/messages/send-file` intentionally remains synchronous.

It still uses request/reply because uploaded files are currently stored as temporary local files and deleted after the request finishes. Do not switch file sends to queued background processing until attachments are persisted to durable shared storage first.

Restart-safe Node.js/Express service for Pearl IMS linked-device WhatsApp sessions.

## What changed in v2

- Persistent WhatsApp authentication defaults to `$HOME/pearlnotify-data/sessions` instead of the versioned app build directory.
- Session registry is stored in `$HOME/pearlnotify-data/state/sessions.json`.
- Registered sessions are automatically restored when Node restarts.
- `/status` and `/qr` can restore a registered session if the current Node process lost its in-memory client.
- Better per-process and per-institute runtime logging.
- Health endpoint exposes version, PID and uptime to make Hostinger restarts easy to identify.
- Multer upgraded to 2.x.

## Hostinger environment variables

Set at minimum:

```env
API_KEY=YOUR_LONG_RANDOM_SECRET
PERSISTENT_DATA_PATH=/home/YOUR_HOSTINGER_USER/pearlnotify-data
MAX_UPLOAD_MB=15
```

You may omit `PERSISTENT_DATA_PATH`; the application will automatically use `$HOME/pearlnotify-data`. Explicit configuration is easier to diagnose.

Do not set `SESSION_PATH=./sessions` on Hostinger because `./sessions` would be inside the versioned deployment.

## API

- `GET /health`
- `POST /api/sessions/:instituteId/start`
- `GET /api/sessions/:instituteId/status`
- `GET /api/sessions/:instituteId/qr`
- `GET /api/sessions`
- `POST /api/messages/send`
- `POST /api/messages/send-file`
- `POST /api/sessions/:instituteId/disconnect`

Protected routes require:

```text
x-api-key: YOUR_API_KEY
```

### Disconnect behavior

Body `{ "logout": false }` stops the session and disables automatic restore while preserving LocalAuth files.

Body `{ "logout": true }` logs out, disables automatic restore, and deletes that institute's LocalAuth directory.

## First Hostinger test

After deployment:

```bash
curl https://pearlnotify.com/health
```

Start a test session:

```bash
curl -X POST https://pearlnotify.com/api/sessions/696/start \
  -H "x-api-key: YOUR_API_KEY"
```

Check status:

```bash
curl https://pearlnotify.com/api/sessions/696/status \
  -H "x-api-key: YOUR_API_KEY"
```

Fetch QR:

```bash
curl https://pearlnotify.com/api/sessions/696/qr \
  -H "x-api-key: YOUR_API_KEY"
```

## Verify persistent storage over SSH

```bash
ls -la ~/pearlnotify-data
ls -la ~/pearlnotify-data/sessions
cat ~/pearlnotify-data/state/sessions.json
```

After a Hostinger restart, `/health` may show a new PID, but registered institute sessions should be restored from this persistent directory.

## Important operational note

`whatsapp-web.js` is an unofficial WhatsApp Web automation library. It can be less stable than Meta's official WhatsApp Business Platform and may carry account/terms risk. Use rate limits and keep an official API path available for customers that require supported production messaging.


## Hostinger / Puppeteer deployment note (v2.1)

This version installs the Chrome browser required by Puppeteer during deployment:

```bash
puppeteer browsers install chrome
```

On Hostinger, set:

```env
PUPPETEER_CACHE_DIR=/home/u127257491/domains/pearlnotify.com/pearlnotify-data/puppeteer
```

Keep `PERSISTENT_DATA_PATH` configured for persistent WhatsApp authentication data.

## v2.2 Hostinger browser-cache fix

v2.2 forces Puppeteer's browser cache to live under the persistent Pearl Notify data directory in code.

Default:
`~/domains/pearlnotify.com/pearlnotify-data/puppeteer`

The deployment `postinstall` script also installs Chrome into the same cache path.

You may override the base directory with `PERSISTENT_DATA_PATH`.

## v2.3.0 - Phone-number pairing

Generate a WhatsApp pairing code:

POST `/api/sessions/:instituteId/pairing-code`

JSON body:
```json
{
  "phone": "923001234567"
}
```

Optional:
- `showNotification`: boolean (default true)
- `intervalMs`: pairing-code refresh interval, minimum 60000; default 180000

Cancel pairing-code mode and return to QR:
POST `/api/sessions/:instituteId/pairing-code/cancel`

Phone numbers must be in international digits-only format. Pakistani local numbers such as 03001234567 are also normalized to 923001234567 by this service.

## v2.3.1 - Pairing readiness + diagnostics

Changes:
- waits for WhatsApp Web to reach an unauthenticated/QR-ready state before requesting a phone-number pairing code;
- uses a longer 45-second readiness window;
- logs pairing request state and the normalized phone number;
- returns structured error details from `/pairing-code` when WhatsApp rejects the request.

Example:
POST `/api/sessions/3/pairing-code`
```json
{
  "phone": "923001234567"
}
```

## v2.4.0 - Clean session reset

Use this after a failed QR or pairing-code attempt to remove the institute's LocalAuth state and start clean.

POST `/api/sessions/:instituteId/reset`

Default behavior:
```json
{}
```
Deletes the institute's saved WhatsApp auth directory, removes its registry entry, and immediately starts a fresh session.

To reset without immediately restarting:
```json
{
  "restart": false
}
```

Example:
```bash
curl -X POST \
  https://pearlnotify.com/api/sessions/3/reset \
  -H "Content-Type: application/json" \
  -H "x-api-key: YOUR_API_KEY" \
  -d '{}'
```

Then poll:
- `GET /api/sessions/3/status`
- `GET /api/sessions/3/qr`

until the status becomes `QR_REQUIRED`, and scan the freshest QR immediately.

## v2.4.1

Fixes the `/reset` endpoint error:

`this.removeRegistry is not a function`

The registry-removal helper is now defined as a proper `SessionManager` class method.

## v2.4.2

Fixes reset registry removal by using the SessionManager's actual persistence model:

```js
this.registry.delete(instituteId);
this.saveRegistry();
```

This replaces the incorrect `readRegistry()` / `writeRegistry()` calls.

## v2.5.0 - stale browser recovery

- status now verifies the live WhatsApp/Puppeteer client;
- text and file sends verify client state before sending;
- detached frame / target closed / execution-context errors trigger automatic client recreation from LocalAuth;
- one automatic retry is attempted after a recoverable browser error;
- recovery is logged as `STALE_CLIENT_RECOVERY`.
