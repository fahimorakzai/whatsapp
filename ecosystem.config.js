/*
 * One pm2 process per shard, so this is also the idle cost of the fleet: each
 * worker is ~90 MB of Node whether it owns a session or not. Sixteen of them
 * for a handful of institutes was 1.5 GB doing nothing.
 *
 * Size it to the number of institutes, not to some future maximum. Raising it
 * later is safe: LocalAuth credentials live in the SHARED sessions/ directory,
 * and discoverSessionsFromDisk() rebuilds each shard's registry from there, so
 * re-sharding moves an institute to a different worker without re-pairing it.
 * Stop pm2, change the number, start pm2.
 */
const TOTAL_SHARDS = Number(process.env.WHATSAPP_TOTAL_SHARDS || 2);

const sharedEnv = {
  NODE_ENV: 'production',
  WHATSAPP_TOTAL_SHARDS: String(TOTAL_SHARDS),
  // Concurrency is per shard. Ten slots each made a single wedged institute
  // occupy the whole worker; the pacing chain serialises one institute anyway.
  WHATSAPP_WORKER_CONCURRENCY: '4',
  WHATSAPP_PER_INSTITUTE_MAX_PER_MINUTE: '5'
};

const shardWorkers = Array.from({ length: TOTAL_SHARDS }, (_, shardId) => ({
  name: `pearlnotify-shard-${shardId}`,
  script: 'src/server.js',
  args: 'worker',
  cwd: __dirname,
  instances: 1,
  exec_mode: 'fork',
  autorestart: true,
  max_restarts: 10,
  env: {
    ...sharedEnv,
    WHATSAPP_SHARD_ID: String(shardId)
  }
}));

module.exports = {
  apps: [
    {
      name: 'pearlnotify-api',
      script: 'src/server.js',
      args: 'api',
      cwd: __dirname,
      instances: 1,
      exec_mode: 'fork',
      autorestart: true,
      max_restarts: 10,
      env: sharedEnv
    },
    ...shardWorkers
  ]
};
