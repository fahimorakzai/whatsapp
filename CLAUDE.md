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

Changing `WHATSAPP_TOTAL_SHARDS` re-maps institutes to different workers, but it does **not** require
re-pairing: LocalAuth credentials live in the *shared* `sessions/` directory, and
`discoverSessionsFromDisk()` rebuilds each shard's registry from there filtered by `belongsToShard`.
Only the in-memory clients are lost, and those die on any restart anyway. Stop pm2, change the
number, start pm2. Size it to the institute count — each shard is a ~90 MB Node process whether it
owns a session or not, and sixteen of them for a handful of institutes was 1.5 GB doing nothing.

**Browser lifecycle guards** (`src/sessionManager.js`, added after the 2026-09 runaway-CPU incident).
Chrome cold start is the most expensive operation in this service, so three things stop a broken
institute from triggering one on every job:

- `assertSendable()` runs before anything touches Chrome. A session in `QR_REQUIRED`,
  `PAIRING_CODE_REQUIRED`, `AUTH_FAILED` or `UNPAIRED` is waiting for a human and can never send, so
  the job fails immediately with `unrecoverable: true` instead of relaunching a browser and timing
  out 45s later, three times.
- `assertRecreateAllowed()` / `noteRecreateOutcome()` give each institute a restart cooldown
  (`WHATSAPP_RECREATE_COOLDOWN_MS`) and a circuit breaker that opens after
  `WHATSAPP_RECREATE_FAILURE_LIMIT` consecutive failed restarts.
- `recreateClient()` must **never** set `enabled: true`. It used to, which silently resurrected
  institutes an operator had switched off.

There is no cold-start branch in `sendText`/`sendFile` any more: an absent client reports
`connected: false`, so `recreateClient()` handles "never started" and "went stale" through the one
guarded path. Adding a second `start()` call there re-opens the hole.

`restoreAll()` boots sessions **serially**, awaiting each `session.initPromise` and sleeping
`WHATSAPP_RESTORE_STAGGER_MS` between them. `start()` does not await `client.initialize()` (the HTTP
start endpoint must return immediately), so without this the loop launches every session at once —
which is how 32 browsers, ~17.4 GB of demand, landed on an 8 GB box simultaneously.

`npm run check` exercises all of the above without a browser or network. Run it after touching
`sessionManager.js`.

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

**Sessions are on-demand, not resident.** A browser is ~556 MB and 11 processes (measured), so
holding one open per linked institute capped the fleet at about ten on an 8 GB box — while a school
actually sends in a burst once a month. Instead:

- `restoreAll()` **registers and starts nothing.** The first message for an institute creates its
  browser. Re-adding a `start()` there re-creates the boot storm.
- `makeRoomFor()` enforces `WHATSAPP_MAX_LIVE_SESSIONS` (6). It is called from `start()` — the only
  place a browser is created — and from the worker preflight via `ensureCapacityFor()`.
- The eviction victim is the least-recently-used session idle longer than
  `WHATSAPP_MIN_IDLE_BEFORE_EVICT_MS`. A session mid-send is never evicted.
- `sweepIdleSessions()` closes anything untouched for `WHATSAPP_SESSION_IDLE_MS` (30 min) — long
  enough that one school's paced run (80 min for 400 students at 5/min) stays a single session.
- `closeSession()` keeps the registry row **enabled** and the LocalAuth directory intact. Only the
  browser goes.

**`IDLE` and `QR_REQUIRED` mean opposite things and the difference is load-bearing.** `IDLE` is a
healthy session asleep — nobody needs to do anything. `QR_REQUIRED` means a human must re-link. The
PearlIMS admin screen renders that distinction in plain language, and there is no keepalive (a
deliberate choice), so it is the only signal an admin gets that a dormant institute has lapsed.
`closeSession()` and `restoreAll()` both refuse to overwrite an `UNLINKED_STATUSES` value with
`IDLE` for exactly this reason.

**A full pool is not a failure.** When every live session is busy, `makeRoomFor()` throws
`capacityError` (`error.capacity`, never `unrecoverable`), and `processTrackedSend`'s preflight
turns it into `job.moveToDelayed()` + `DelayedError` — re-queued **without consuming a retry
attempt**, with the row still `Queued` because `markProcessing` runs inside the pacing callback.
Marking these `Failed` would drop legitimate sends whenever the service was merely working.

**`getStateVerified()` must never start a session.** The admin screen polls it; opening a settings
page must not cold-start Chrome. It reports from the registry.

**Prefer one shard.** Institutes shard by `id % WHATSAPP_TOTAL_SHARDS`, so splitting a pool across
two workers lets the parity of the ids decide the balance — six even-numbered schools would queue on
one worker while the other sat idle. CPU is never the constraint (six live sessions are 11% of one
core), so `WHATSAPP_TOTAL_SHARDS=1` with `WHATSAPP_MAX_LIVE_SESSIONS=6` is the right shape until
something else forces a change.

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
