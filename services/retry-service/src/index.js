require('dotenv').config();
const { connectProducer, disconnectProducer } = require('./kafka-producer');
const { connectConsumer, disconnectConsumer } = require('./kafka-consumer');

const MAX_RETRIES = process.env.MAX_RETRIES || '3';

const run = async () => {
  console.log('====================================================');
  console.log('[Retry Service] Starting...');
  console.log(`[Retry Service] MAX_RETRIES = ${MAX_RETRIES}`);
  console.log('[Retry Service] Consume: send_failed');
  console.log('[Retry Service] Publish: send_retry | dead_letter');
  console.log('====================================================');

  await connectProducer();
  await connectConsumer();

  console.log('[Retry Service] Started successfully!');
};


const errorTypes = ['unhandledRejection', 'uncaughtException'];
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];

errorTypes.forEach((type) => {
  process.on(type, async (e) => {
    try {
      console.log(`\n[Retry Service] process.on ${type}`);
      console.error(e);
      await disconnectConsumer();
      await disconnectProducer();
      process.exit(0);
    } catch (_) {
      process.exit(1);
    }
  });
});

signalTraps.forEach((type) => {
  process.once(type, async () => {
    try {
      await disconnectConsumer();
      await disconnectProducer();
    } finally {
      process.kill(process.pid, type);
    }
  });
});

run().catch(console.error);
