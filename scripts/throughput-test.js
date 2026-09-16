/*
 * Throughput probe:  node scripts/throughput-test.js <instituteId> <phone> <count>
 *
 * Queues <count> text messages to ONE number and reports what the service
 * actually achieved end to end -- enqueue rate, time to first send, time to
 * last send, achieved messages/minute, and the gaps between consecutive sends.
 *
 * The gaps are the interesting part: they should sit at
 * 60000 / WHATSAPP_PER_INSTITUTE_MAX_PER_MINUTE (12s at the default 5/min),
 * which shows the binding constraint is the pacing dial rather than Chrome.
 * Anything much larger means time is going somewhere else.
 *
 * Sends to exactly one number, supplied on the command line -- it never reads a
 * recipient from the database.
 */

require('dotenv').config({ path: require('path').join(__dirname, '..', '.env') });

const instituteId = process.argv[2];
const phone = process.argv[3];
const count = Number(process.argv[4] || 20);

if (!instituteId || !phone || !Number.isInteger(count) || count < 1) {
  console.error('usage: node scripts/throughput-test.js <instituteId> <phone> <count>');
  process.exit(1);
}

const base = `http://localhost:${process.env.PORT || 3100}`;
const headers = { 'Content-Type': 'application/json', 'x-api-key': process.env.API_KEY };
const tag = `tput-${Date.now()}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function main() {
  console.log(`institute=${instituteId} phone=${phone} count=${count} tag=${tag}\n`);

  const ids = [];
  const enqueueStart = Date.now();

  for (let i = 1; i <= count; i += 1) {
    const res = await fetch(`${base}/api/messages/send`, {
      method: 'POST',
      headers,
      body: JSON.stringify({ instituteId, phone, message: `${tag} ${i}/${count}` })
    });
    const body = await res.json();
    if (!body.success) {
      console.error(`enqueue ${i} failed: ${body.error}`);
      process.exit(1);
    }
    ids.push(body.data.messageId);
  }

  const enqueueMs = Date.now() - enqueueStart;
  console.log(`queued ${count} in ${enqueueMs}ms (${(count / (enqueueMs / 1000)).toFixed(1)}/s)\n`);

  const idSet = new Set(ids);
  let last = null;

  for (;;) {
    const res = await fetch(`${base}/api/messages?instituteId=${instituteId}&limit=500`, { headers });
    const body = await res.json();
    const rows = (body?.data?.records || []).filter((r) => idSet.has(r.whatsapp_message_id));

    const done = rows.filter((r) => r.status === 'Sent' || r.status === 'Failed');
    const sent = rows.filter((r) => r.status === 'Sent');
    const failed = rows.filter((r) => r.status === 'Failed');

    const line = `sent=${sent.length} failed=${failed.length} pending=${count - done.length}`;
    if (line !== last) { console.log(line); last = line; }

    if (done.length >= count) {
      report(sent, failed, enqueueStart);
      return;
    }
    await sleep(5000);
  }
}

function report(sent, failed, enqueueStart) {
  console.log('\n--- result ---');
  console.log(`sent    : ${sent.length}`);
  console.log(`failed  : ${failed.length}`);

  if (failed.length) {
    const reasons = [...new Set(failed.map((r) => r.last_error))];
    reasons.slice(0, 3).forEach((r) => console.log(`  reason: ${r}`));
  }

  if (!sent.length) return;

  const times = sent.map((r) => new Date(r.sent_at).getTime()).sort((a, b) => a - b);
  const firstSent = times[0];
  const lastSent = times[times.length - 1];
  const spanMs = lastSent - firstSent;

  const gaps = [];
  for (let i = 1; i < times.length; i += 1) gaps.push(times[i] - times[i - 1]);
  gaps.sort((a, b) => a - b);

  console.log(`time to first send : ${((firstSent - enqueueStart) / 1000).toFixed(1)}s`);
  console.log(`first -> last send : ${(spanMs / 1000).toFixed(1)}s`);
  console.log(`total wall clock   : ${((lastSent - enqueueStart) / 1000).toFixed(1)}s`);

  if (spanMs > 0) {
    console.log(`achieved rate      : ${((sent.length - 1) / (spanMs / 60000)).toFixed(2)} msg/min`);
  }

  if (gaps.length) {
    const median = gaps[Math.floor(gaps.length / 2)];
    const mean = gaps.reduce((a, b) => a + b, 0) / gaps.length;
    console.log(`gap between sends  : min ${gaps[0]}ms  median ${median}ms  mean ${Math.round(mean)}ms  max ${gaps[gaps.length - 1]}ms`);
    console.log(`configured pace    : ${Math.ceil(60000 / Number(process.env.WHATSAPP_PER_INSTITUTE_MAX_PER_MINUTE || 5))}ms`);
  }
}

main().catch((error) => { console.error(error); process.exit(1); });
