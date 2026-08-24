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

## Running locally

Redis and MySQL come from `docker-compose.dev.yml`; the Node processes run on the host so Puppeteer can use your own Chrome download.

```bash
docker compose -f docker-compose.dev.yml up -d   # redis :6380, mysql :3308
cp .env.dev.example .env                         # already points at those ports
npm install                                      # postinstall downloads Chrome (~1 min)

npm run dev            # API on :3100
npm run dev:worker     # shard 0, in a second terminal
```

`.env.dev.example` sets `WHATSAPP_TOTAL_SHARDS=1`, so every institute maps to shard 0 and one worker covers them all. Raise it only when you are specifically testing sharding — with the production value of `16` you must run the worker whose id equals `instituteId % 16`, or jobs queue and nothing ever picks them up.

The MySQL container applies `sql/` on first boot. To reset the database:

```bash
docker compose -f docker-compose.dev.yml down -v
docker compose -f docker-compose.dev.yml up -d
```

Smoke test without touching WhatsApp:

```bash
curl localhost:3100/health

curl -X POST localhost:3100/api/files \
  -H "x-api-key: local-dev-key" -F "file=@some.pdf"

curl -X POST localhost:3100/api/messages/send-stored-file \
  -H "x-api-key: local-dev-key" -H "Content-Type: application/json" \
  -d '{"instituteId":696,"phone":"03001234567","fileId":"<id>","caption":"test"}'

curl "localhost:3100/api/messages?instituteId=696" -H "x-api-key: local-dev-key"
```

That exercises upload, queueing, the worker's job dispatch and the `whatsapp_message` status transitions. With no linked device the job fails on `WhatsApp session did not become CONNECTED within 45 seconds` after 3 attempts, which is the correct outcome and still proves the whole path.

To send for real you must link a device, and that means a real WhatsApp account — use a spare SIM, not a school's number:

```bash
curl -X POST localhost:3100/api/sessions/696/start -H "x-api-key: local-dev-key"
curl localhost:3100/api/sessions/696/qr -H "x-api-key: local-dev-key"   # qrDataUrl -> paste in a browser
```

Linked-device credentials land in `PERSISTENT_DATA_PATH/sessions/`, which `.env.dev.example` points at `/tmp/pearlnotify-data` so a local experiment never mixes with production data.

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

## Sending files

There are two file paths. Prefer the second.

### `POST /api/messages/send-file` (legacy, synchronous)

Multipart upload plus send in one request. It intentionally remains synchronous: the uploaded file is a temp file that the API deletes in a `finally` block once the RPC returns, so the send has to finish before the bytes disappear. No retries, no `whatsapp_message` row.

### `POST /api/files` + `POST /api/messages/send-stored-file` (queued)

Upload once to durable storage, then queue as many sends as there are recipients. This is what the "results to parents" flow uses.

Step 1 — upload the attachment:

```bash
curl -X POST http://localhost:3100/api/files \
  -H "x-api-key: $API_KEY" \
  -F "file=@term-result-1043.pdf"
```

```json
{
  "success": true,
  "data": {
    "fileId": "dcfd74f6-e48c-4780-97a4-018b3b562a67",
    "filename": "term-result-1043.pdf",
    "mimeType": "application/pdf",
    "sizeBytes": 51221,
    "createdAt": "2026-08-23T08:52:56.394Z",
    "expiresAt": "2026-08-25T08:52:56.394Z"
  }
}
```

Step 2 — queue the send:

```bash
curl -X POST http://localhost:3100/api/messages/send-stored-file \
  -H "x-api-key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
        "instituteId": 696,
        "phone": "03001234567",
        "fileId": "dcfd74f6-e48c-4780-97a4-018b3b562a67",
        "caption": "Term result for Ali Khan"
      }'
```

Returns `202` with the same shape as `/api/messages/send`, plus the file:

```json
{
  "success": true,
  "data": {
    "status": "QUEUED",
    "messageId": 1,
    "jobId": "1",
    "shardId": 0,
    "fileId": "dcfd74f6-e48c-4780-97a4-018b3b562a67",
    "filename": "term-result-1043.pdf"
  }
}
```

Track it exactly like a text send: `GET /api/messages/:messageId`, or `GET /api/messages?instituteId=696&status=Failed`.

Supporting routes:

- `GET /api/files/:fileId` — metadata; `404` once gone, `410` once expired.
- `DELETE /api/files/:fileId` — remove immediately rather than waiting for TTL.

### Storage and lifetime

Attachments live under `UPLOAD_PATH/store/<fileId>/`, which is below `PERSISTENT_DATA_PATH` and therefore survives redeploys. The API and every shard worker read the same directory, which is why they must run on one host — a worker on a second machine cannot see the file.

Nothing deletes on send, because one file may go to many parents. `FILE_STORE_TTL_HOURS` (default `48`) governs expiry; the API sweeps hourly. Workers never prune, so sixteen sweepers cannot race each other.

Uploads are capped by `MAX_UPLOAD_MB` (default `15`). That is a deliberate ceiling, not WhatsApp's: `MessageMedia.fromFilePath` base64s the whole file inside a process that is already running Chrome.

### Failure behaviour

Queued file sends get `send-text`'s treatment — 3 attempts, exponential backoff from 5s, `whatsapp_message` status transitions.

The exception is a missing, expired or malformed `fileId`. Those fail identically on every attempt, so they raise BullMQ's `UnrecoverableError`: the row goes straight to `Failed` on attempt 1 rather than sitting on `Retrying` for a job that will never run again.

A bad `fileId` known at request time is rejected as `404`/`410` by `send-stored-file` itself, before anything is queued.

### Sending one result per parent

Each parent gets a different PDF, so the loop is: generate PDF → `POST /api/files` → `POST /api/messages/send-stored-file`. Upload-once-send-many only helps for a shared document such as a notice.

Remember the pacing ceiling. `WHATSAPP_PER_INSTITUTE_MAX_PER_MINUTE=5` means 300/hour for one institute, so 400 result cards take roughly 80 minutes. That dial is ban-risk management, not throughput — raise it knowingly. Pacing is per-institute, so unrelated schools run in parallel.

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
- `POST /api/files`
- `GET /api/files/:fileId`
- `DELETE /api/files/:fileId`
- `POST /api/messages/send-stored-file`
- `GET /api/messages`
- `GET /api/messages/:messageId`
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
