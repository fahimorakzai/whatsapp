const DEFAULT_TOTAL_SHARDS = 16;

function normalizeInstituteId(instituteId) {
  const value = String(instituteId || '').trim();
  if (!/^\d+$/.test(value)) throw new Error('Invalid instituteId');
  return value;
}

function getTotalShards() {
  const configured = Number(process.env.WHATSAPP_TOTAL_SHARDS || DEFAULT_TOTAL_SHARDS);
  if (!Number.isInteger(configured) || configured <= 0) {
    throw new Error('WHATSAPP_TOTAL_SHARDS must be a positive integer');
  }
  return configured;
}

function getShardIdForInstitute(instituteId, totalShards = getTotalShards()) {
  const normalized = normalizeInstituteId(instituteId);
  return Number(BigInt(normalized) % BigInt(totalShards));
}

function getCurrentShardId(totalShards = getTotalShards()) {
  const configured = Number(process.env.WHATSAPP_SHARD_ID || 0);
  if (!Number.isInteger(configured) || configured < 0 || configured >= totalShards) {
    throw new Error(`WHATSAPP_SHARD_ID must be an integer from 0 to ${totalShards - 1}`);
  }
  return configured;
}

function getShardQueueName(shardId) {
  return `whatsapp-shard-${shardId}`;
}

function belongsToShard(instituteId, shardId, totalShards = getTotalShards()) {
  return getShardIdForInstitute(instituteId, totalShards) === shardId;
}

module.exports = {
  DEFAULT_TOTAL_SHARDS,
  belongsToShard,
  getCurrentShardId,
  getShardIdForInstitute,
  getShardQueueName,
  getTotalShards,
  normalizeInstituteId
};
