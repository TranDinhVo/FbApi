const { Kafka } = require('kafkajs');
const { handleRetry } = require('./retry-handler');

const kafka = new Kafka({
  clientId: 'retry-service-consumer',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
});

const consumer = kafka.consumer({
  groupId: process.env.KAFKA_GROUP_ID || 'retry-service-group',
});

const connectConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'send_failed', fromBeginning: false });

  console.log('[Retry Service Consumer] Listening on topic: "send_failed"');

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const failedMessage = JSON.parse(message.value.toString());
        console.log(`\n[Retry Service Consumer] Received from topic [${topic}]`);
        await handleRetry(failedMessage);
      } catch (error) {
        console.error('[Retry Service Consumer] Error processing message:', error.message);
      }
    },
  });
};

const disconnectConsumer = async () => {
  await consumer.disconnect();
};

module.exports = { connectConsumer, disconnectConsumer };
