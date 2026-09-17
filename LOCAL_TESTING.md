# Testing pearlnotify locally, end to end

Runs the WhatsApp service on your machine and points a local PearlIMS at it, so
the **Settings → WhatsApp Connection** screen works exactly as it does in
production — Start Session, QR, pairing code, status, test message.

You will link a real phone to a real WhatsApp account. Nothing else about this
is production.

> **This database has real guardian phone numbers.** Only ever send to a number
> you own. `scripts/throughput-test.js` takes its recipient on the command line
> and never reads one from the database; keep it that way.

---

## 1. The service

```bash
cd whatsapp
cp .env.dev.example .env          # redis :6380, mysql :3308, API_KEY=local-dev-key
docker compose -f docker-compose.dev.yml up -d
```

`sql/` is applied on first boot, so `whatsapp_message` arrives with `file_id`
and `file_name` already on it.

**Chrome.** `sessionManager.js` forces Puppeteer's cache under
`PERSISTENT_DATA_PATH`, so a Chrome you already have elsewhere is invisible to
it. Either let it download:

```bash
npm install                        # postinstall fetches Chrome into the right place
```

or, if `~/.cache/puppeteer` already has one, skip the download:

```bash
mkdir -p /tmp/pearlnotify-data
ln -sfn ~/.cache/puppeteer /tmp/pearlnotify-data/puppeteer
```

Start both roles:

```bash
node src/server.js api                      > /tmp/pearlnotify-data/api.log 2>&1 &
WHATSAPP_SHARD_ID=0 node src/server.js worker > /tmp/pearlnotify-data/worker.log 2>&1 &
curl -s localhost:3100/health
```

`.env.dev.example` sets `WHATSAPP_TOTAL_SHARDS=1`, so one worker owns every
institute. With the production value you must run the worker whose id equals
`instituteId % TOTAL_SHARDS` or jobs sit unclaimed — the commonest local dead
end.

The worker should say:

```
registered 0 session(s); browsers start on demand (pool max 6)
```

---

## 2. Point PearlIMS at it

PearlIMS runs in Docker; pearlnotify runs on your host. **`localhost` from
inside the PHP container is the container, not your machine.** Use the docker
gateway:

```bash
docker inspect pearlims-php-1 \
  --format '{{range .NetworkSettings.Networks}}{{.Gateway}}{{end}}'
```

Usually `172.17.0.1` or `172.19.0.1`. Check the container can reach the service:

```bash
docker exec pearlims-php-1 php -r 'echo file_get_contents("http://<GATEWAY>:3100/health");'
```

Then set the institute's config — the same two fields the Settings screen writes:

```sql
UPDATE setting
   SET whatsapp_api_host  = 'http://<GATEWAY>:3100',
       whatsapp_api_token = 'local-dev-key'
 WHERE institute_id = <ID>;
```

`whatsapp_api_token` must equal `API_KEY` in `whatsapp/.env`; it is sent as the
`x-api-key` header.

---

## 3. Use the screen

Log into PearlIMS as a user of that institute, then
**Settings → WhatsApp Connection → Start Session**.

Expected sequence, with the plain-English badges:

| Badge | Meaning |
|---|---|
| Connecting… | Chrome is booting |
| Waiting to be linked | QR is ready — scan it |
| Connected | linked, sending |
| Linked and ready | idle; the browser closed to save memory, and reopens on the next message |

That last one is the on-demand pool working. **It is not an error and must not
be "fixed" with Reset Connection**, which deletes the link.

Watch it happen:

```bash
tail -f /tmp/pearlnotify-data/worker.log
```

To see a close-and-reopen without waiting 30 minutes, restart the worker with
`WHATSAPP_SESSION_IDLE_MS=120000 WHATSAPP_IDLE_SWEEP_INTERVAL_MS=15000`.

---

## 4. Send something

Use **Send Test Message** on the screen (you type the recipient), or measure
throughput properly:

```bash
node scripts/throughput-test.js <instituteId> <your-number> 20
```

Expect `5.00 msg/min` with 12,000 ms gaps — that is
`WHATSAPP_PER_INSTITUTE_MAX_PER_MINUTE`, not a limit of the browser.

---

## Hitting the API without a browser session

`UserInfo` accepts a `PI-AuthToken` JWT by header, so you can curl the PearlIMS
endpoints directly:

```bash
TOKEN=$(docker exec pearlims-php-1 php -r '
  require "/srv/pearlims/vendor/autoload.php";
  echo Firebase\JWT\JWT::encode([
    "id"=>1,"userName"=>"dev","name"=>"Dev","userType"=>2,
    "instituteId"=>4,"sessionId"=>31,"teacherId"=>0,
    "loginTime"=>time(),"departments"=>"[]"
  ], getenv("JWT_KEY"), "HS512");')

curl -s -X POST localhost/activitybank/whatsapp/statussession -H "PI-AuthToken: $TOKEN"
```

Change `instituteId` to test another institute.

---

## When something returns HTTP 500 with an empty body

php-fpm discards worker output unless told not to, so PHP fatals vanish and you
get a bare 500 with nothing in the log. Add to `pearlims/docker/php/www.conf`:

```ini
catch_workers_output = yes
decorate_workers_output = no
```

then `docker restart pearlims-php-1`. This is worth doing before you start
debugging anything, not after.

If the error is a service-container failure, ask the container directly rather
than guessing:

```php
$app = Laminas\Mvc\Application::init(require "config/application.config.php");
try { $app->getServiceManager()->get("ControllerManager")
        ->get(Activitybank\Controller\WhatsappController::class); }
catch (\Throwable $e) { echo get_class($e) . ": " . $e->getMessage(); }
```

That is how "every action returns 500, including the one that only reads the
database" turned out to be `WhatsAppModel` failing to construct.

---

## Tearing down

```bash
pkill -f "node src/server.js"
docker compose -f docker-compose.dev.yml down
```

Session credentials live under `PERSISTENT_DATA_PATH` (default
`/tmp/pearlnotify-data`), outside the repo. Delete that directory to start from
an unlinked state; keep it to stay linked across restarts.
