const roleArg = process.argv[2];
const configuredRole = String(roleArg || process.env.SERVICE_ROLE || 'api').trim().toLowerCase();

if (configuredRole === 'worker' || configuredRole === 'shard') {
  require('./shardWorkerProcess').startShardWorker();
} else if (configuredRole === 'api') {
  require('./apiServer').startApiServer();
} else {
  console.error(
    `[bootstrap] Unknown service role "${configuredRole}". Use "api" or "worker".`
  );
  process.exit(1);
}
