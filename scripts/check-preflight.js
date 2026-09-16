/*
 * processTrackedSend preflight path -- part of `npm run check`.
 *
 * The normal failure bookkeeping (markFailed + UnrecoverableError) lives inside
 * the pacing callback. The preflight runs BEFORE that callback, so it has to do
 * its own bookkeeping; forgetting that strands the whatsapp_message row on
 * 'Queued' and lets BullMQ retry a job that can never succeed.
 *
 * Why the preflight exists at all: InstituteSendScheduler sleeps the
 * per-institute interval inside the active job, so a doomed send would hold a
 * worker concurrency slot for 12s doing nothing. An institute PearlIMS keeps
 * queueing for but nobody linked then starves the worker -- that is what made
 * session-pairing-code time out during the 2026-09 bring-up.
 *
 * messageRepository is stubbed so this needs no database.
 */

const Module = require('module');

const calls = { markProcessing: 0, markSent: 0, markRetrying: 0, markFailed: [] };

const originalLoad = Module._load;
Module._load = function (request, ...rest) {
  if (request === './messageRepository') {
    return {
      markProcessing: async () => { calls.markProcessing += 1; },
      markSent: async () => { calls.markSent += 1; },
      markRetrying: async () => { calls.markRetrying += 1; },
      markFailed: async (id, attempt, message) => { calls.markFailed.push({ id, attempt, message }); }
    };
  }
  return originalLoad.call(this, request, ...rest);
};

const { processTrackedSend, InstituteSendScheduler } = require('../src/shardWorkerProcess.js');
const { UnrecoverableError } = require('bullmq');

(async () => {
  const scheduler = new InstituteSendScheduler({ maxPerMinute: 5 }); // 12s interval
  // Prime nextAllowedAt so a scheduled task would genuinely have to wait.
  await scheduler.schedule('4', async () => 'warm');

  let performSendCalled = false;
  const job = {
    id: 'j1',
    name: 'send-text',
    attemptsMade: 0,
    opts: { attempts: 3 },
    data: { instituteId: '4', whatsappMessageId: 555, phone: 'x', message: 'y' }
  };

  const startedAt = Date.now();
  let caught = null;

  try {
    await processTrackedSend(
        job,
        scheduler,
        async () => { performSendCalled = true; return {}; },
        () => {
          const error = new Error('not linked');
          error.unrecoverable = true;
          throw error;
        }
    );
  } catch (error) {
    caught = error;
  }

  const elapsed = Date.now() - startedAt;

  const results = [
    ['rejected', Boolean(caught)],
    ['as BullMQ UnrecoverableError', caught instanceof UnrecoverableError],
    ['performSend never ran', performSendCalled === false],
    ['skipped the pacing wait', elapsed < 1000],
    ['row marked Failed exactly once', calls.markFailed.length === 1],
    ['with the right message id', calls.markFailed[0]?.id === 555],
    ['never marked Processing', calls.markProcessing === 0],
    ['never marked Retrying', calls.markRetrying === 0]
  ];

  let failed = 0;
  for (const [name, ok] of results) {
    console.log(`  ${ok ? 'ok  ' : 'FAIL'} ${name}`);
    if (!ok) failed += 1;
  }

  console.log(`\n${results.length - failed} passed, ${failed} failed  (preflight took ${elapsed}ms)`);
  process.exit(failed ? 1 : 0);
})();
