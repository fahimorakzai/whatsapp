const { Queue, QueueEvents } = require('bullmq');
const connection = require('./queueConnection');
const { getShardIdForInstitute, getShardQueueName, getTotalShards } = require('./shardConfig');

class ShardRpcClient {
  constructor(options = {}) {
    this.connection = options.connection || connection;
    this.totalShards = options.totalShards || getTotalShards();
    this.timeoutMs = Number(options.timeoutMs || process.env.SHARD_RPC_TIMEOUT_MS || 60000);
    this.queues = new Map();
    this.queueEvents = new Map();
  }

  getQueue(shardId) {
    if (!this.queues.has(shardId)) {
      this.queues.set(
        shardId,
        new Queue(getShardQueueName(shardId), { connection: this.connection })
      );
    }
    return this.queues.get(shardId);
  }

  getQueueEvents(shardId) {
    if (!this.queueEvents.has(shardId)) {
      const events = new QueueEvents(getShardQueueName(shardId), { connection: this.connection });
      this.queueEvents.set(shardId, events);
    }
    return this.queueEvents.get(shardId);
  }

  async dispatch(instituteId, command, payload = {}, options = {}) {
    const shardId = getShardIdForInstitute(instituteId, this.totalShards);
    return this.dispatchToShard(shardId, command, {
      ...payload,
      instituteId: String(instituteId)
    }, options);
  }

  async enqueue(instituteId, command, payload = {}, options = {}) {
    const shardId = getShardIdForInstitute(instituteId, this.totalShards);
    return this.enqueueToShard(shardId, command, {
      ...payload,
      instituteId: String(instituteId)
    }, options);
  }

  async dispatchToShard(shardId, command, payload = {}, options = {}) {
    const job = await this.enqueueToShard(shardId, command, payload, options);
    const events = this.getQueueEvents(shardId);
    await events.waitUntilReady();
    return job.waitUntilFinished(events, options.timeoutMs || this.timeoutMs);
  }

  async enqueueToShard(shardId, command, payload = {}, options = {}) {
    const queue = this.getQueue(shardId);
    const isMessage = command === 'send-text';
    return queue.add(command, payload, {
      removeOnComplete: { age: 3600, count: isMessage ? 5000 : 1000 },
      removeOnFail: { age: 86400, count: isMessage ? 10000 : 5000 },
      attempts: options.attempts ?? (isMessage ? 3 : 1),
      backoff: isMessage
        ? {
            type: 'exponential',
            delay: 5000
          }
        : undefined
    });
  }

  async close() {
    await Promise.all(
      Array.from(this.queues.values()).map((queue) => queue.close())
    );
    await Promise.all(
      Array.from(this.queueEvents.values()).map((events) => events.close())
    );
  }
}

module.exports = ShardRpcClient;
