# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`pearl-whatsapp-service` v3: a two-role Node.js service that runs unofficial WhatsApp linked-device
sessions (`whatsapp-web.js` + Puppeteer) on behalf of many institutes, for Pearl IMS.

```
Pearl IMS -> API process -> Redis/BullMQ -> shard worker 0..15 -> SessionManager -> WhatsApp Web
                  |                                                       |
                MySQL (whatsapp_message audit trail)          LocalAuth session dirs on disk
```

There is no test suite, linter, or build step. Verification is done by running the processes and
hitting the HTTP API.

## Commands

```bash
npm install                 # postinstall also runs scripts/install-browser.js (puppeteer chrome)
npm run dev                 # nodemon, API role
npm run start:api           # node src/server.js api
WHATSAPP_SHARD_ID=0 npm run start:worker   # one process per shard; ID is mandatory and unique

pm2 start ecosystem.config.js   # production: 1 API + 16 shard workers = 17 processes
pm2 delete all && pm2 start ecosystem.config.js && pm2 save
```

Requires a running Redis and MySQL. For local work:

```bash
docker compose -f docker-compose.dev.yml up -d   # redis :6380, mysql :3308, sql/ applied on first boot
cp .env.dev.example .env
npm run dev / npm run dev:worker
```

`.env.dev.example` sets `WHATSAPP_TOTAL_SHARDS=1` so a single worker owns every institute. With the
production value of `16` you must run the worker whose id equals `instituteId % 16` or jobs sit in
the queue unclaimed — the commonest local dead end.

`sql/` holds the `whatsapp_message` DDL, reconstructed from `messageRepository.js` because the table
was only ever created by hand on production. Treat it as good enough for local, not authoritative
for production.

Smoke test a change:

```bash
curl localhost:3100/health
curl -X POST localhost:3100/api/sessions/696/start -H "x-api-key: $API_KEY"
curl localhost:3100/api/sessions/696/qr -H "x-api-key: $API_KEY"
```

## Architecture

**One entrypoint, two roles.** `src/server.js` reads `process.argv[2]` (or `SERVICE_ROLE`) and
delegates to `apiServer.startApiServer()` or `shardWorkerProcess.startShardWorker()`. The API
process never touches WhatsApp or Puppeteer; only workers do.

**Deterministic sharding is the core invariant.** `src/shardConfig.js` owns it:
`shardId = instituteId % WHATSAPP_TOTAL_SHARDS`. Every institute's WhatsApp session lives in exactly
one worker process, so `sendMessage` for institute N always reaches the process holding N's browser.
Anything that changes `WHATSAPP_TOTAL_SHARDS` re-maps institutes to different workers and orphans
their live sessions — treat it as a migration, not a config tweak.

**Two transport paths over the same BullMQ queue** (`whatsapp-shard-<id>`), both in `src/shardRpc.js`:

- `dispatch()` / `dispatchToShard()` — request/reply. Enqueues then `job.waitUntilFinished()`, so
  the API returns the worker's actual result. Used for all session control-plane commands and for
  `send-file`. Bounded by `SHARD_RPC_TIMEOUT_MS` (default 60s).
- `enqueue()` — fire-and-forget. Used by `send-text` and `send-stored-file`, both of which return
  HTTP 202 with a jobId. Those two get 3 attempts with exponential backoff from 5s (the `isMessage`
  branch); everything else gets 1 attempt.

`send-file` (multipart) is deliberately **not** queued: `apiServer.js` deletes the multer temp file
in a `finally` block once the RPC returns, so the send must outlive nothing. It is legacy — it gets
no retries and writes no `whatsapp_message` row.

`send-stored-file` is the queued replacement, and the durable shared storage that caveat was waiting
for is `src/fileStore.js`. `POST /api/files` moves the upload to
`UPLOAD_PATH/store/<fileId>/<safeName>` and returns a UUID; `POST /api/messages/send-stored-file`
then queues by `fileId` and gets `send-text`'s retries and status tracking. The blob keeps its
sanitised original name because whatsapp-web.js takes the document name WhatsApp displays from the
file's basename — a parent should see `term-result-1043.pdf`, not `blob`.

`fileStore.entryPath()` is the only place a path is built from caller input: the id must match a
UUID pattern and must resolve directly under the store root. Keep both checks.

The store is a **single-host** design. The API writes and all sixteen workers read the same
directory, so moving a worker to a second machine breaks file sends silently — the job resolves to a
path that does not exist there. That is the point at which S3 (or any shared object store) becomes
necessary; until then it would only add an AWS dependency and a bucket allowlist to maintain.

Nothing deletes on send — one file may go to many parents — so `FILE_STORE_TTL_HOURS` (default 48)
is what reclaims space. Only the API prunes; workers never do, so sixteen sweepers cannot race.

**Worker command dispatch** is a single `switch (job.name)` in `createCommandProcessor`
(`shardWorkerProcess.js`). Adding an endpoint means adding a case there *and* a route in
`apiServer.js` — the job name is the wire protocol between the two. A job that should be retried
also needs its name added to `isMessage` in `shardRpc.js`, or it silently gets one attempt.

`processTrackedSend()` holds the pacing, `whatsapp_message` transitions and retry accounting for
every tracked send; `send-text` and `send-stored-file` differ only in the `performSend` callback
they pass. Resolve attachments *inside* that callback so the base64 read happens after the pacing
wait — a job sleeping out its rate-limit window should not also be holding the file in memory.

An error carrying `unrecoverable: true` (everything `fileStore` throws) is marked `Failed` on the
spot and rethrown as BullMQ's `UnrecoverableError`. Without that, the retry branch would leave the
row on `Retrying` forever for a job BullMQ will never run again.

**Per-institute pacing.** `InstituteSendScheduler` gives every institute its own promise chain plus a
`nextAllowedAt` timestamp, derived from `WHATSAPP_PER_INSTITUTE_MAX_PER_MINUTE`. It sleeps *inside*
the active BullMQ job, so a waiting institute occupies a concurrency slot — hence
`WHATSAPP_WORKER_CONCURRENCY=10` rather than 1. This state is in process memory, which is only
correct while exactly one process owns each shard. Running two processes with the same
`WHATSAPP_SHARD_ID` breaks both pacing and session ownership; move pacing into Redis before scaling
past one worker per shard.

**Session state lives in three places** (`src/sessionManager.js`):

1. `this.clients` — in-memory live Puppeteer/whatsapp-web.js clients, lost on restart.
2. `state/shard-<id>/sessions.json` — the registry (enabled, lastStatus, phoneNumber), written
   atomically via tmp-file + rename on every status change. Drives `restoreAll()` at worker boot.
3. `sessions/session-pearl-institute-<instituteId>/` — LocalAuth credentials, the only thing that
   actually survives a wipe of the other two. `discoverSessionsFromDisk()` reverse-engineers the
   registry from these directory names.

That `session-pearl-institute-<id>` path is constructed in three separate places (`clientId()`,
`getSessionDirectory()`, `removeLocalAuth()`, plus the prefix literal in
`discoverSessionsFromDisk()`). Change it in one place and reset/logout silently stop deleting the
right directory.

**Status verification, not status caching.** `getStateVerified()` (behind `session-status` and
`session-qr`) actively probes the live client — `pupPage.isClosed()` plus `client.getState()` — and
demotes a stale `CONNECTED` rather than trusting the cached field. Sends do the same check first,
and `isRecoverableBrowserError()` (detached frame, target closed, destroyed execution context…)
triggers `recreateClient()` + one retry, logged as `STALE_CLIENT_RECOVERY`.

**MySQL is the durable message record**, separate from BullMQ's transient job state.
`src/messageRepository.js` writes to `whatsapp_message` keyed by `whatsapp_message_id`, with status
`Queued -> Processing -> Sent | Retrying -> Failed`. The API inserts the row *before* enqueuing, then
patches `job_id`. The worker distinguishes `Retrying` from `Failed` using
`attemptsMade + 1 < job.opts.attempts` (BullMQ's `attemptsMade` is 0 on the first pass), and must
re-throw after recording the failure or BullMQ marks the job complete and never retries.
`whatsapp_message` gained nullable `file_id` / `file_name` columns for queued file sends
(`sql/002_add_file_columns.sql`). `createMessage()` names them on **every** insert, text sends
included, so that migration has to be applied before this code is deployed.

**Puppeteer cache path is set as a require-time side effect** at the top of `sessionManager.js`,
before `whatsapp-web.js` is required, forcing `PUPPETEER_CACHE_DIR` under `PERSISTENT_DATA_PATH`
because Hostinger overrides it. Do not reorder those requires or move the assignment into a function.

## Deployment constraints

Everything stateful must live under `PERSISTENT_DATA_PATH` (default `$HOME/pearlnotify-data`),
outside the versioned deploy directory — sessions, state, uploads, and the Puppeteer browser cache.
Pointing `SESSION_PATH` at `./sessions` orphans every linked device on the next redeploy.

Auth is a single shared `x-api-key` compared with `crypto.timingSafeEqual`; the API refuses to serve
`/api/*` at all if `API_KEY` is unset.
