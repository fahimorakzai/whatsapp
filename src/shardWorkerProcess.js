require('dotenv').config();

const path = require('path');
const os = require('os');
const fs = require('fs');
const { UnrecoverableError, Worker } = require('bullmq');

const {
  markProcessing,
  markRetrying,
  markSent,
  markFailed
} = require('./messageRepository');

const SessionManager = require('./sessionManager');
const { FileStore } = require('./fileStore');
const connection = require('./queueConnection');

const {
  belongsToShard,
  getCurrentShardId,
  getShardQueueName,
  getTotalShards
} = require('./shardConfig');


function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}


class InstituteSendScheduler {

  constructor(options = {}) {

    this.maxPerMinute = Number(
        options.maxPerMinute ||
        process.env.WHATSAPP_PER_INSTITUTE_MAX_PER_MINUTE ||
        5
    );

    this.intervalMs = Math.max(
        1,
        Math.ceil(
            60000 / Math.max(1, this.maxPerMinute)
        )
    );

    this.chains = new Map();
    this.nextAllowedAt = new Map();
  }


  async schedule(instituteId, task) {

    const key = String(instituteId);

    const previous =
        this.chains.get(key) ||
        Promise.resolve();


    const current = previous
        .catch(() => {})
        .then(async () => {

          const now = Date.now();

          const nextAt =
              this.nextAllowedAt.get(key) ||
              now;

          const waitMs =
              Math.max(0, nextAt - now);


          if (waitMs > 0) {

            console.log(
                `[rate][institute:${key}] waiting ${waitMs}ms`
            );

            await sleep(waitMs);
          }


          const startedAt = Date.now();

          this.nextAllowedAt.set(
              key,
              startedAt + this.intervalMs
          );


          return task();
        });


    this.chains.set(
        key,
        current
    );


    try {

      return await current;

    } finally {

      if (
          this.chains.get(key) === current
      ) {

        this.chains.delete(key);
      }
    }
  }
}


/*
 * Shared bookkeeping for every tracked send.
 *
 * Text and file sends differ only in how the bytes reach WhatsApp, so the
 * pacing, whatsapp_message status transitions and retry accounting live here
 * once. `performSend` runs inside the institute's pacing window and returns
 * whatever SessionManager returns.
 */
async function processTrackedSend(
    job,
    sendScheduler,
    performSend,
    preflight
) {

  const instituteId =
      job.data?.instituteId;

  const whatsappMessageId =
      job.data?.whatsappMessageId;


  /*
   * BullMQ attemptsMade starts at 0
   * during the first processing attempt.
   */
  const currentAttempt =
      Number(job.attemptsMade || 0) + 1;

  const maxAttempts =
      Number(job.opts?.attempts || 1);


  console.log(
      `[message]` +
      `[id:${whatsappMessageId || 'none'}]` +
      `[job:${job.id}]` +
      `[institute:${instituteId}]` +
      `[${job.name}]` +
      ` attempt=${currentAttempt}/${maxAttempts}`
  );


  /*
   * Refuse impossible sends BEFORE entering the pacing queue.
   *
   * The scheduler serialises one institute and sleeps the per-institute
   * interval inside the active job, so a job that can never succeed would
   * otherwise hold a concurrency slot for 12s doing nothing. With an institute
   * PearlIMS keeps queueing for but nobody has linked, that starves the workers
   * -- it is what made session-pairing-code time out during the 2026-09 bring-up.
   */
  if (typeof preflight === 'function') {

    try {

      preflight();

    } catch (error) {

      /*
       * The normal failure handling lives inside the scheduler callback below,
       * so a preflight rejection has to record its own outcome -- otherwise the
       * whatsapp_message row is stranded on 'Queued' and BullMQ retries a job
       * that can never succeed. preflight throws unsendableError, which is
       * always unrecoverable; the flag is still honoured rather than assumed.
       */
      const errorMessage =
          error?.message ||
          String(error);

      if (whatsappMessageId) {

        await markFailed(
            whatsappMessageId,
            currentAttempt,
            errorMessage
        );
      }

      console.error(
          `[message]` +
          `[id:${whatsappMessageId || 'none'}]` +
          `[job:${job.id}]` +
          `[institute:${instituteId}]` +
          ` rejected before pacing: ${errorMessage}`
      );

      if (error?.unrecoverable) {
        throw new UnrecoverableError(errorMessage);
      }

      throw error;
    }
  }


  /*
   * Keep each institute independently paced.
   */
  return sendScheduler.schedule(
      instituteId,
      async () => {

        /*
         * Update permanent database record before
         * attempting the WhatsApp send.
         */
        if (whatsappMessageId) {

          await markProcessing(
              whatsappMessageId,
              currentAttempt
          );
        }


        try {

          const result =
              await performSend();


          /*
           * Successful WhatsApp send.
           */
          if (whatsappMessageId) {

            await markSent(
                whatsappMessageId,
                currentAttempt
            );
          }


          console.log(
              `[message]` +
              `[id:${whatsappMessageId || 'none'}]` +
              `[job:${job.id}]` +
              `[institute:${instituteId}] sent`
          );


          return result;

        } catch (error) {

          const errorMessage =
              error?.message ||
              String(error);


          /*
           * Some failures fail identically on every
           * attempt - a missing or expired attachment,
           * a malformed fileId. Retrying those only
           * burns the backoff schedule, and leaving the
           * row on 'Retrying' would strand it there
           * because BullMQ never runs the job again.
           */
          const isUnrecoverable =
              Boolean(error?.unrecoverable);


          /*
           * BullMQ will retry if there are
           * attempts remaining.
           */
          const hasMoreAttempts =
              !isUnrecoverable &&
              currentAttempt < maxAttempts;


          if (whatsappMessageId) {

            if (hasMoreAttempts) {

              await markRetrying(
                  whatsappMessageId,
                  currentAttempt,
                  errorMessage
              );

            } else {

              await markFailed(
                  whatsappMessageId,
                  currentAttempt,
                  errorMessage
              );
            }
          }


          if (hasMoreAttempts) {

            console.error(
                `[message]` +
                `[id:${whatsappMessageId || 'none'}]` +
                `[job:${job.id}]` +
                `[institute:${instituteId}]` +
                ` attempt=${currentAttempt} failed; retrying: ${errorMessage}`
            );

          } else {

            console.error(
                `[message]` +
                `[id:${whatsappMessageId || 'none'}]` +
                `[job:${job.id}]` +
                `[institute:${instituteId}]` +
                ` permanently failed: ${errorMessage}`
            );
          }


          /*
           * CRITICAL:
           *
           * Throw the error again so BullMQ knows
           * that this attempt failed.
           *
           * Without this, BullMQ would mark the
           * job as completed and would not retry it.
           */
          if (isUnrecoverable) {

            throw new UnrecoverableError(
                errorMessage
            );
          }

          throw error;
        }
      }
  );
}


function createCommandProcessor(
    sessions,
    sendScheduler,
    fileStore
) {

  return async (job) => {

    const instituteId =
        job.data?.instituteId;


    switch (job.name) {

        /*
         * SESSION MANAGEMENT
         */

      case 'session-start':

        return sessions.start(
            instituteId
        );


      case 'session-status':

        return sessions.getStateOrRestore(
            instituteId
        );


      case 'session-reset':

        return sessions.resetSession(
            instituteId,
            {
              restart:
                  job.data?.restart !== false
            }
        );


      case 'session-pairing-code':

        return sessions.requestPairingCode(
            instituteId,
            job.data?.phone,
            {
              showNotification:
              job.data?.showNotification,

              intervalMs:
              job.data?.intervalMs
            }
        );


      case 'session-pairing-code-cancel':

        return sessions.cancelPairingCode(
            instituteId
        );


      case 'session-qr': {

        const data =
            await sessions.getStateOrRestore(
                instituteId
            );


        return {
          instituteId:
          data.instituteId,

          status:
          data.status,

          qr:
          data.qr,

          qrDataUrl:
          data.qrDataUrl,

          lastError:
          data.lastError
        };
      }


      case 'session-list':

        return sessions.listStates();


      case 'session-disconnect':

        return sessions.disconnect(
            instituteId,
            Boolean(job.data?.logout)
        );


        /*
         * TEXT MESSAGE
         *
         * This uses:
         *
         * - BullMQ
         * - per-institute pacing
         * - MySQL message status
         * - retries
         */
      case 'send-text':

        return processTrackedSend(
            job,
            sendScheduler,
            () =>
                sessions.sendText(
                    instituteId,
                    job.data?.phone,
                    job.data?.message
                ),
            () => sessions.assertSendable(instituteId)
        );


        /*
         * STORED FILE MESSAGE
         *
         * The attachment already lives in the durable
         * file store, so this gets the same retries and
         * whatsapp_message tracking as send-text.
         *
         * Resolving inside the callback keeps the
         * base64 read after the pacing wait, so a job
         * sleeping out its rate-limit window is not
         * also holding the whole file in memory.
         */
      case 'send-stored-file':

        return processTrackedSend(
            job,
            sendScheduler,
            async () => {

              const stored =
                  await fileStore.get(
                      job.data?.fileId
                  );

              return sessions.sendFile(
                  instituteId,
                  job.data?.phone,
                  stored.path,
                  job.data?.caption
              );
            },
            () => sessions.assertSendable(instituteId)
        );


        /*
         * FILE MESSAGE (legacy, multipart)
         *
         * Stays synchronous because apiServer deletes
         * the temporary file after RPC completes.
         * Prefer send-stored-file.
         */
      case 'send-file':

        return sendScheduler.schedule(
            instituteId,
            () =>
                sessions.sendFile(
                    instituteId,
                    job.data?.phone,
                    job.data?.filePath,
                    job.data?.caption
                )
        );


      default:

        throw new Error(
            `Unsupported command: ${job.name}`
        );
    }
  };
}


function startShardWorker() {

  const totalShards =
      getTotalShards();

  const shardId =
      getCurrentShardId(
          totalShards
      );


  const persistentDataPath =
      path.resolve(
          process.env.PERSISTENT_DATA_PATH ||
          path.join(
              os.homedir(),
              'pearlnotify-data'
          )
      );


  const sessionPath =
      path.resolve(
          process.env.SESSION_PATH ||
          path.join(
              persistentDataPath,
              'sessions'
          )
      );


  const baseStatePath =
      path.resolve(
          process.env.STATE_PATH ||
          path.join(
              persistentDataPath,
              'state'
          )
      );


  /*
   * Each shard gets its own state directory.
   */
  const shardStatePath =
      path.join(
          baseStatePath,
          `shard-${shardId}`
      );


  fs.mkdirSync(
      sessionPath,
      {
        recursive: true
      }
  );


  fs.mkdirSync(
      shardStatePath,
      {
        recursive: true
      }
  );


  const sessions =
      new SessionManager({
        sessionPath,
        statePath:
        shardStatePath
      });


  const sendScheduler =
      new InstituteSendScheduler();


  /*
   * Same UPLOAD_PATH the API writes to. Workers never prune - the API owns
   * expiry so a single sweeper cannot race sixteen others.
   */
  const fileStore =
      new FileStore();


  const queueName =
      getShardQueueName(
          shardId
      );


  const concurrency =
      Number(
          process.env.WHATSAPP_WORKER_CONCURRENCY ||
          10
      );


  const worker =
      new Worker(
          queueName,

          createCommandProcessor(
              sessions,
              sendScheduler,
              fileStore
          ),

          {
            connection,
            concurrency
          }
      );


  /*
   * JOB EVENTS
   */

  worker.on(
      'completed',
      (job) => {

        console.log(
            `[shard:${shardId}]` +
            `[job:${job.id}]` +
            ` completed ${job.name}`
        );
      }
  );


  worker.on(
      'failed',
      (job, error) => {

        console.error(
            `[shard:${shardId}]` +
            `[job:${job?.id}]` +
            ` failed ${job?.name}: ` +
            `${error.message}`
        );
      }
  );


  worker.on(
      'error',
      (error) => {

        console.error(
            `[shard:${shardId}]` +
            ` worker error: ` +
            `${error.message}`
        );
      }
  );


  /*
   * WAIT FOR REDIS/BULLMQ WORKER
   */

  worker
      .waitUntilReady()
      .then(async () => {

        console.log(
            `Pearl WhatsApp shard worker v3.0.0 ready ` +
            `shard=${shardId}/${totalShards} ` +
            `pid=${process.pid}`
        );


        console.log(
            `[shard:${shardId}] ` +
            `queue=${queueName}`
        );


        console.log(
            `[shard:${shardId}] ` +
            `sessionPath=${sessionPath}`
        );


        console.log(
            `[shard:${shardId}] ` +
            `statePath=${shardStatePath}`
        );


        console.log(
            `[shard:${shardId}] ` +
            `fileStorePath=${fileStore.rootPath}`
        );


        console.log(
            `[shard:${shardId}] ` +
            `concurrency=${concurrency}`
        );


        console.log(
            `[shard:${shardId}] ` +
            `perInstituteMaxPerMinute=` +
            `${sendScheduler.maxPerMinute}`
        );


        /*
         * Restore only the WhatsApp sessions that
         * belong to this shard.
         */
        try {

          await sessions.restoreAll({

            belongsToShard:
                (instituteId) =>
                    belongsToShard(
                        instituteId,
                        shardId,
                        totalShards
                    )
          });

        } catch (error) {

          console.error(
              `[shard:${shardId}] ` +
              `startup restore failed: ` +
              `${error.message}`
          );
        }

      })
      .catch((error) => {

        console.error(
            `[shard:${shardId}] ` +
            `readiness failed: ` +
            `${error.message}`
        );

        process.exit(1);
      });


  /*
   * GRACEFUL SHUTDOWN
   */

  async function shutdown(signal) {

    console.log(
        `[shard:${shardId}] ` +
        `received ${signal}; shutting down`
    );


    try {

      await worker.close();

      console.log(
          `[shard:${shardId}] ` +
          `worker closed`
      );

    } catch (error) {

      console.error(
          `[shard:${shardId}] ` +
          `shutdown error: ` +
          `${error.message}`
      );
    }


    process.exit(0);
  }


  process.on(
      'SIGTERM',
      () =>
          shutdown(
              'SIGTERM'
          )
  );


  process.on(
      'SIGINT',
      () =>
          shutdown(
              'SIGINT'
          )
  );


  process.on(
      'unhandledRejection',
      (error) => {

        console.error(
            `[shard:${shardId}] ` +
            `unhandledRejection`,
            error
        );
      }
  );


  process.on(
      'uncaughtException',
      (error) => {

        console.error(
            `[shard:${shardId}] ` +
            `uncaughtException`,
            error
        );
      }
  );
}


module.exports = {
  startShardWorker,
  // Exported for scripts/check-guards.js -- the preflight rejection path has
  // its own failure bookkeeping and is easy to break silently.
  processTrackedSend,
  InstituteSendScheduler
};