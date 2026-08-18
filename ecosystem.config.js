const sharedEnv = {
  NODE_ENV: 'production',
  WHATSAPP_TOTAL_SHARDS: '16',
  WHATSAPP_WORKER_CONCURRENCY: '10',
  WHATSAPP_PER_INSTITUTE_MAX_PER_MINUTE: '5'
};

const shardWorkers = Array.from({ length: 16 }, (_, shardId) => ({
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
