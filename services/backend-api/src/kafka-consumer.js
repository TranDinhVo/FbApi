const { Kafka } = require('kafkajs');
const { handleCommand } = require('./handlers/command-handler');

const kafka = new Kafka({
  clientId: 'backend-api-consumer',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
});

const consumer = kafka.consumer({
  groupId: process.env.KAFKA_GROUP_ID || 'backend-api-group',
});

const connectConsumer = async () => {
  await consumer.connect();

  await consumer.subscribe({ topics: ['reply_commands', 'send_retry'], fromBeginning: false });

  console.log('[Backend API Consumer] Đang lắng nghe topics: "reply_commands", "send_retry"');

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const command = JSON.parse(message.value.toString());
        console.log(`\n[Backend API Consumer] Nhận từ topic [${topic}]`);
        await handleCommand(command);
      } catch (error) {
        console.error('[Backend API Consumer] Lỗi parse hoặc xử lý message:', error.message);
      }
    },
  });
};

const disconnectConsumer = async () => {
  await consumer.disconnect();
};

module.exports = { connectConsumer, disconnectConsumer };
