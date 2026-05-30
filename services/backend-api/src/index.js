require('dotenv').config();
const { connectProducer, disconnectProducer } = require('./kafka-producer');
const { connectConsumer, disconnectConsumer } = require('./kafka-consumer');

const run = async () => {
  console.log('====================================================');
  console.log('[Backend API] Khởi động...');
  console.log(`[Backend API] FAKE_MODE = ${process.env.FAKE_MODE === 'true' ? 'BẬT (giả lập FB API)' : 'TẮT (gọi FB API thật)'}`);
  console.log('====================================================');

  await connectProducer();

  await connectConsumer();

  console.log('[Backend API] Đã khởi động thành công!');
  console.log('[Backend API] Consume: reply_commands, send_retry');
  console.log('[Backend API] Publish: send_failed (khi lỗi)');
};

const errorTypes = ['unhandledRejection', 'uncaughtException'];
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];

errorTypes.forEach((type) => {
  process.on(type, async (e) => {
    try {
      console.log(`\n[Backend API] process.on ${type}`);
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
